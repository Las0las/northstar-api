"""
NorthStar — Ontology Integration (Live Schema)

Full classification pipeline:
  candidate → role match → ontology-aware scoring → routing → enriched result

Uses live scoring profile from ontology_scoring_profiles:
  weights: {technical_match: 0.3, stability: 0.2, communication: 0.15, domain: 0.15, initiative: 0.1, extras: 0.1}
  thresholds: {auto: 90, assisted: 75, manual: 0}
"""

import hashlib
import json
import logging
import re
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional

from .role_matcher import RoleMatchResult, get_matcher

log = logging.getLogger("ontology.integration")


# ═══════════════════════════════════════════════════════
# TYPES
# ═══════════════════════════════════════════════════════

@dataclass
class OntologyClassification:
    role_match: dict
    scores: dict
    routing: dict
    ingestion_hash: str
    classified_at: str

    def to_dict(self) -> dict:
        return {
            "role_match": self.role_match,
            "scores": self.scores,
            "routing": self.routing,
            "ingestion_hash": self.ingestion_hash,
            "classified_at": self.classified_at,
        }


# ═══════════════════════════════════════════════════════
# CANONICAL HASHING
# ═══════════════════════════════════════════════════════

def canonicalize(obj) -> str:
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return json.dumps(obj, ensure_ascii=True)
    if isinstance(obj, list):
        return "[" + ",".join(canonicalize(item) for item in obj) + "]"
    if isinstance(obj, dict):
        return "{" + ",".join(
            f"{json.dumps(k)}:{canonicalize(obj[k])}" for k in sorted(obj.keys())
        ) + "}"
    return json.dumps(str(obj))


def hash_candidate(candidate_data: dict) -> str:
    return hashlib.sha256(canonicalize(candidate_data).encode("utf-8")).hexdigest()


# ═══════════════════════════════════════════════════════
# SCORING — uses live profile weights
# ═══════════════════════════════════════════════════════

# Fallback weights if DB profile unavailable
DEFAULT_WEIGHTS = {
    "technical_match": 0.30,
    "stability": 0.20,
    "communication": 0.15,
    "domain": 0.15,
    "initiative": 0.10,
    "extras": 0.10,
}

DEFAULT_THRESHOLDS = {
    "auto": 90,
    "assisted": 75,
    "manual": 0,
}


def compute_ontology_scores(
    candidate: dict,
    role_match: RoleMatchResult,
    weights: Optional[dict] = None,
    thresholds: Optional[dict] = None,
) -> dict:
    """
    6-dimension ontology-aware scoring.
    Uses the live scoring profile weights from ontology_scoring_profiles.
    """
    w = weights or DEFAULT_WEIGHTS
    t = thresholds or DEFAULT_THRESHOLDS

    # Technical match: skill overlap + role confidence
    technical = (role_match.skill_overlap_score * 0.6) + (role_match.confidence * 0.4)

    # Stability: years of experience + role count
    experience = candidate.get("experience", [])
    years = _estimate_years(experience)
    role_count = len(experience)
    stability = min(years / 15, 1.0) * 0.7 + min(role_count / 5, 1.0) * 0.3

    # Communication: bullet quality proxy
    all_bullets = [b for exp in experience for b in exp.get("bullets", [])]
    if all_bullets:
        avg_len = sum(len(b) for b in all_bullets) / len(all_bullets)
        has_metrics = sum(1 for b in all_bullets if any(c.isdigit() for c in b)) / len(all_bullets)
        communication = min(avg_len / 150, 1.0) * 0.5 + has_metrics * 0.5
    else:
        communication = 0.2

    # Domain experience
    domain = min(role_match.confidence, 1.0) if role_match.role_id else 0.0

    # Initiative/leadership from seniority
    seniority_scores = {
        "associate": 0.2, "consultant": 0.4, "senior": 0.6,
        "lead": 0.75, "principal": 0.9, "architect": 1.0,
    }
    leadership = seniority_scores.get(role_match.seniority, 0.3)

    # Credentials
    certs = candidate.get("certifications", [])
    credentials = min(len(certs) / 4, 1.0)

    # Weighted composite
    composite = (
        w.get("technical_match", 0.3) * technical +
        w.get("stability", 0.2) * stability +
        w.get("communication", 0.15) * communication +
        w.get("domain", 0.15) * domain +
        w.get("initiative", 0.1) * leadership +
        w.get("extras", 0.1) * credentials
    )
    composite_pct = round(composite * 100, 1)

    # Threshold classification using live thresholds
    auto_threshold = t.get("auto", 90)
    assisted_threshold = t.get("assisted", 75)
    if composite_pct >= auto_threshold:
        disposition = "auto_shortlist"
    elif composite_pct >= assisted_threshold:
        disposition = "recruiter_review"
    else:
        disposition = "recycle_or_nurture"

    return {
        "composite_score": composite_pct,
        "disposition": disposition,
        "dimensions": {
            "technical_match": round(technical * 100, 1),
            "stability": round(stability * 100, 1),
            "communication": round(communication * 100, 1),
            "domain": round(domain * 100, 1),
            "initiative": round(leadership * 100, 1),
            "credentials": round(credentials * 100, 1),
        },
        "thresholds": t,
        "years_experience": years,
        "role_count": role_count,
        "cert_count": len(certs),
    }


# ═══════════════════════════════════════════════════════
# ROUTING
# ═══════════════════════════════════════════════════════

def compute_routing(role_match: RoleMatchResult, scores: dict) -> dict:
    """Determine routing queue from role match and scores."""
    queue = role_match.routing_profile_id or "general_review"

    blocked = False
    block_reason = None

    if not role_match.role_id:
        blocked = True
        block_reason = "No ontology role match (fail-closed)"
    elif scores["disposition"] == "recycle_or_nurture" and scores["composite_score"] < 30:
        blocked = True
        block_reason = f"Composite {scores['composite_score']}% below minimum threshold"

    return {
        "queue": queue,
        "routing_profile_id": role_match.routing_profile_id,
        "blocked": blocked,
        "block_reason": block_reason,
        "disposition": scores["disposition"],
        "human_control": "recruiter_required",
    }


# ═══════════════════════════════════════════════════════
# MAIN PIPELINE
# ═══════════════════════════════════════════════════════

def classify_candidate(
    candidate: dict,
    db=None,
    tenant_id: str = None,
    scoring_weights: Optional[dict] = None,
    scoring_thresholds: Optional[dict] = None,
) -> OntologyClassification:
    """
    Full ontology classification pipeline:
      1. Hash candidate for audit
      2. Match to canonical role (817 roles, 1548 aliases, 2470 graph edges)
      3. Compute 6-dimension scores
      4. Compute routing decision
    """
    ingestion_hash = hash_candidate(candidate)

    matcher = get_matcher(db=db, tenant_id=tenant_id)

    title = candidate.get("title", "")
    competencies = candidate.get("competencies", [])
    experience = candidate.get("experience", [])
    years = _estimate_years(experience)

    # Normalize competencies to skill_id format
    candidate_skills = [
        c.lower().strip().replace(" ", "_").replace("-", "_").replace("/", "_")
        for c in competencies
    ]

    role_match = matcher.match(
        candidate_title=title,
        candidate_skills=candidate_skills,
        candidate_years=years,
    )

    scores = compute_ontology_scores(
        candidate, role_match,
        weights=scoring_weights,
        thresholds=scoring_thresholds,
    )

    routing = compute_routing(role_match, scores)

    classified_at = datetime.now(timezone.utc).isoformat()

    log.info(
        f"Classified '{candidate.get('name', '?')}': "
        f"role={role_match.role_id or 'NONE'}, "
        f"confidence={role_match.confidence:.0%}, "
        f"composite={scores['composite_score']}%, "
        f"queue={routing['queue']}, blocked={routing['blocked']}"
    )

    return OntologyClassification(
        role_match=asdict(role_match),
        scores=scores,
        routing=routing,
        ingestion_hash=ingestion_hash,
        classified_at=classified_at,
    )


# ═══════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════

def _estimate_years(experience: list) -> int:
    now = datetime.now().year
    earliest = now
    for exp in experience:
        dates = exp.get("dates", "")
        years = re.findall(r"((?:19|20)\d{2})", dates)
        for y in years:
            earliest = min(earliest, int(y))
    return max(0, now - earliest)


def load_scoring_profile(db, tenant_id: str) -> tuple:
    """Load weights and thresholds from live ontology_scoring_profiles."""
    try:
        res = db.table("ontology_scoring_profiles").select(
            "weights, thresholds"
        ).eq("tenant_id", tenant_id).eq("is_active", True).maybe_single().execute()
        if res.data:
            return res.data["weights"], res.data["thresholds"]
    except Exception as e:
        log.warning(f"Failed to load scoring profile: {e}")
    return DEFAULT_WEIGHTS, DEFAULT_THRESHOLDS
