"""
NorthStar — Ontology Role Matcher (Live Schema)

Matches parsed candidate data to a canonical ontology role_id.
Targets the LIVE Supabase schema:
  - ontology_roles: role_id, canonical_title, domain_id, family_id, seniority_levels (jsonb[])
  - ontology_role_aliases: role_id, alias_text, normalized_alias
  - ontology_role_skill_requirements: role_id, skill_id, requirement_type ('core'|'optional'), weight
  - ontology_skill_edges: from_skill_id, to_skill_id, weight, confidence
  - ontology_scoring_profiles: weights (jsonb), thresholds (jsonb)
  - ontology_routing_profiles: profile_id, rules (jsonb)

Three-stage matching:
  1. Exact alias match (normalized_alias lookup)
  2. Fuzzy alias match (SequenceMatcher >= 0.70)
  3. Weighted skill overlap fallback (core=1.0, optional=0.5, graph expansion)
"""

import hashlib
import json
import logging
import threading
import time
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Optional

log = logging.getLogger("ontology.role_matcher")


# ═══════════════════════════════════════════════════════
# DATA TYPES
# ═══════════════════════════════════════════════════════

@dataclass
class RoleMatchResult:
    role_id: Optional[str] = None
    canonical_title: str = ""
    domain_id: str = ""
    family_id: str = ""
    seniority: str = ""
    confidence: float = 0.0
    match_method: str = ""  # 'alias_exact', 'alias_fuzzy', 'skill_overlap', 'none'
    skill_overlap_score: float = 0.0
    matched_skills: list = field(default_factory=list)
    missing_core_skills: list = field(default_factory=list)
    routing_profile_id: str = ""
    scoring_profile_id: str = ""


@dataclass
class OntologyRole:
    role_id: str
    canonical_title: str
    domain_id: str
    family_id: str
    seniority_levels: list  # from jsonb array
    primary_seniority: str  # first element or inferred
    aliases: list           # from ontology_role_aliases
    core_skills: list       # requirement_type = 'core'
    optional_skills: list   # requirement_type = 'optional'


# ═══════════════════════════════════════════════════════
# SENIORITY
# ═══════════════════════════════════════════════════════

SENIORITY_ORDER = {
    "associate": 0, "consultant": 1, "senior": 2,
    "lead": 3, "principal": 4, "architect": 5,
}

SENIORITY_KEYWORDS = {
    "associate": ["associate", "junior", "jr", "entry"],
    "consultant": ["consultant", "specialist", "analyst", "developer", "engineer"],
    "senior": ["senior", "sr", "sr."],
    "lead": ["lead", "team lead", "manager"],
    "principal": ["principal", "director", "head"],
    "architect": ["architect", "chief", "fellow", "distinguished", "vp"],
}


# ═══════════════════════════════════════════════════════
# OVERLAP RESULT
# ═══════════════════════════════════════════════════════

@dataclass
class _OverlapResult:
    score: float = 0.0
    matched: list = field(default_factory=list)
    missing_core: list = field(default_factory=list)


# ═══════════════════════════════════════════════════════
# CORE MATCHER
# ═══════════════════════════════════════════════════════

class RoleMatcher:
    """Match candidates to canonical ontology roles. Thread-safe."""

    GRAPH_EXPANSION_THRESHOLD = 0.6

    def __init__(
        self,
        roles: list,
        skill_graph: Optional[dict] = None,
        routing_map: Optional[dict] = None,
    ):
        self.roles = roles
        self._alias_index: dict = {}
        self._graph: dict = skill_graph or {}
        self._routing_map: dict = routing_map or {}
        self._all_ontology_skills: set = set()
        self._role_index: dict = {}
        self._build_indexes()

    def _build_indexes(self):
        for role in self.roles:
            self._role_index[role.role_id] = role
            for alias in role.aliases:
                self._alias_index[alias.lower().strip()] = role.role_id
            self._alias_index[role.canonical_title.lower().strip()] = role.role_id
            self._all_ontology_skills.update(role.core_skills)
            self._all_ontology_skills.update(role.optional_skills)
        self._all_ontology_skills.update(self._graph.keys())

    def match(
        self,
        candidate_title: str,
        candidate_skills: list,
        candidate_years: int = 0,
    ) -> RoleMatchResult:
        title_lower = candidate_title.lower().strip()
        raw_skills = set(
            s.lower().strip().replace(" ", "_").replace("-", "_").replace("/", "_")
            for s in candidate_skills
        )
        inferred_seniority = self._infer_seniority(candidate_title, candidate_years)

        # Filter to ontology-known skills only
        direct_skills = raw_skills & self._all_ontology_skills

        # Graph expansion
        expanded_skills = set()
        if self._graph:
            expandable = direct_skills & set(self._graph.keys())
            for skill in expandable:
                for edge in self._graph.get(skill, []):
                    if edge.get("weight", 0) >= self.GRAPH_EXPANSION_THRESHOLD:
                        expanded_skills.add(edge["to_skill_id"])
        skills_set = direct_skills | expanded_skills

        # ─── Stage 1: Exact alias ───
        if title_lower in self._alias_index:
            role = self._role_index.get(self._alias_index[title_lower])
            if role:
                overlap = self._skill_overlap(skills_set, role)
                return self._build_result(
                    role, "alias_exact",
                    min(0.95 + overlap.score * 0.05, 1.0),
                    overlap, inferred_seniority,
                )

        # ─── Stage 2: Fuzzy alias ───
        best_fuzzy_role_id = None
        best_fuzzy_ratio = 0.0
        for alias_lower, role_id in self._alias_index.items():
            ratio = SequenceMatcher(None, title_lower, alias_lower).ratio()
            if ratio > best_fuzzy_ratio and ratio >= 0.70:
                best_fuzzy_ratio = ratio
                best_fuzzy_role_id = role_id

        if best_fuzzy_role_id and best_fuzzy_ratio >= 0.70:
            role = self._role_index.get(best_fuzzy_role_id)
            if role:
                overlap = self._skill_overlap(skills_set, role)
                seniority_bonus = self._seniority_bonus(inferred_seniority, role.primary_seniority)
                confidence = (best_fuzzy_ratio * 0.5) + (overlap.score * 0.35) + (seniority_bonus * 0.15)
                return self._build_result(
                    role, "alias_fuzzy",
                    round(min(confidence, 1.0), 3),
                    overlap, inferred_seniority,
                )

        # ─── Stage 3: Skill overlap fallback ───
        best_role = None
        best_score = 0.0
        for role in self.roles:
            overlap = self._skill_overlap(skills_set, role)
            seniority_bonus = self._seniority_bonus(inferred_seniority, role.primary_seniority)
            combined = (overlap.score * 0.80) + (seniority_bonus * 0.20)
            if combined > best_score:
                best_score = combined
                best_role = (role, overlap)

        if best_role and best_score >= 0.30 and best_role[1].score >= 0.25:
            role, overlap = best_role
            # Require >= 2 DIRECT core skill hits (graph-expanded don't count)
            core_hits = sum(
                1 for s in overlap.matched
                if s in set(role.core_skills) and s in direct_skills
            )
            if core_hits < 2:
                return RoleMatchResult(confidence=0.0, match_method="none")
            return self._build_result(
                role, "skill_overlap",
                round(min(best_score, 1.0), 3),
                overlap, inferred_seniority,
            )

        # ─── Fail-closed ───
        return RoleMatchResult(confidence=0.0, match_method="none")

    # ─── Internal helpers ───

    def _build_result(self, role, method, confidence, overlap, inferred_seniority):
        # Determine routing profile from domain
        routing_profile = self._routing_map.get(role.domain_id, "")
        return RoleMatchResult(
            role_id=role.role_id,
            canonical_title=role.canonical_title,
            domain_id=role.domain_id,
            family_id=role.family_id,
            seniority=role.primary_seniority,
            confidence=confidence,
            match_method=method,
            skill_overlap_score=overlap.score,
            matched_skills=overlap.matched,
            missing_core_skills=overlap.missing_core,
            routing_profile_id=routing_profile,
            scoring_profile_id="default_consulting_v1",
        )

    def _skill_overlap(self, candidate_skills: set, role) -> _OverlapResult:
        core = set(role.core_skills)
        optional = set(role.optional_skills)
        if not (core or optional):
            return _OverlapResult()

        matched = []
        total_weight = 0.0
        earned_weight = 0.0
        for skill in core:
            total_weight += 1.0
            if skill in candidate_skills:
                earned_weight += 1.0
                matched.append(skill)
        for skill in optional:
            total_weight += 0.5
            if skill in candidate_skills:
                earned_weight += 0.5
                matched.append(skill)

        missing_core = [s for s in core if s not in candidate_skills]
        score = earned_weight / total_weight if total_weight > 0 else 0.0
        return _OverlapResult(score=round(score, 3), matched=matched, missing_core=missing_core)

    def _infer_seniority(self, title: str, years: int) -> str:
        title_lower = title.lower()
        best_level = "consultant"
        best_order = SENIORITY_ORDER["consultant"]
        for level, keywords in SENIORITY_KEYWORDS.items():
            for kw in keywords:
                if kw in title_lower:
                    order = SENIORITY_ORDER[level]
                    if order > best_order:
                        best_order = order
                        best_level = level
        if years >= 15 and best_order < SENIORITY_ORDER["principal"]:
            best_level = "principal"
        elif years >= 10 and best_order < SENIORITY_ORDER["lead"]:
            best_level = "lead"
        elif years >= 7 and best_order < SENIORITY_ORDER["senior"]:
            best_level = "senior"
        return best_level

    def _seniority_bonus(self, inferred: str, role_seniority: str) -> float:
        diff = abs(
            SENIORITY_ORDER.get(inferred, 1) - SENIORITY_ORDER.get(role_seniority, 1)
        )
        return {0: 1.0, 1: 0.7, 2: 0.3}.get(diff, 0.0)


# ═══════════════════════════════════════════════════════
# LOADERS — from live Supabase
# ═══════════════════════════════════════════════════════

def load_matcher_from_supabase(db, tenant_id: str) -> RoleMatcher:
    """Load all ontology data from live Supabase and build a RoleMatcher."""

    # 1. Roles
    roles_raw = db.table("ontology_roles").select(
        "role_id, canonical_title, domain_id, family_id, seniority_levels"
    ).eq("tenant_id", tenant_id).eq("is_active", True).execute().data or []

    # 2. Aliases
    aliases_raw = db.table("ontology_role_aliases").select(
        "role_id, normalized_alias"
    ).eq("tenant_id", tenant_id).eq("is_active", True).execute().data or []
    alias_map: dict = {}
    for a in aliases_raw:
        alias_map.setdefault(a["role_id"], []).append(a["normalized_alias"])

    # 3. Skill requirements
    reqs_raw = db.table("ontology_role_skill_requirements").select(
        "role_id, skill_id, requirement_type, weight"
    ).eq("tenant_id", tenant_id).execute().data or []
    core_map: dict = {}
    optional_map: dict = {}
    for r in reqs_raw:
        if r["requirement_type"] == "core":
            core_map.setdefault(r["role_id"], []).append(r["skill_id"])
        else:
            optional_map.setdefault(r["role_id"], []).append(r["skill_id"])

    # 4. Build OntologyRole objects
    roles = []
    for rd in roles_raw:
        rid = rd["role_id"]
        seniority_levels = rd.get("seniority_levels") or ["consultant"]
        # Infer primary seniority from role_id prefix or first level
        primary = _infer_primary_seniority(rid, seniority_levels)
        roles.append(OntologyRole(
            role_id=rid,
            canonical_title=rd["canonical_title"],
            domain_id=rd["domain_id"],
            family_id=rd["family_id"],
            seniority_levels=seniority_levels,
            primary_seniority=primary,
            aliases=alias_map.get(rid, []),
            core_skills=core_map.get(rid, []),
            optional_skills=optional_map.get(rid, []),
        ))
    log.info(f"Loaded {len(roles)} roles, {sum(len(v) for v in alias_map.values())} aliases, {len(reqs_raw)} skill reqs")

    # 5. Skill graph edges
    edges_raw = db.table("ontology_skill_edges").select(
        "from_skill_id, to_skill_id, weight, confidence"
    ).eq("tenant_id", tenant_id).eq("is_active", True).execute().data or []
    skill_graph: dict = {}
    for e in edges_raw:
        skill_graph.setdefault(e["from_skill_id"], []).append({
            "to_skill_id": e["to_skill_id"],
            "weight": float(e["weight"]),
            "confidence": float(e.get("confidence", 0)),
        })
    log.info(f"Loaded {len(edges_raw)} graph edges for {len(skill_graph)} skill nodes")

    # 6. Routing profile map (domain_id → routing_profile_id)
    routing_raw = db.table("ontology_routing_profiles").select(
        "profile_id, rules"
    ).eq("tenant_id", tenant_id).eq("is_active", True).execute().data or []
    # Build domain → profile_id mapping from the seed data structure
    # The routing profiles map domains to queues
    routing_map = _build_routing_map(roles, routing_raw)

    return RoleMatcher(roles, skill_graph=skill_graph, routing_map=routing_map)


def _infer_primary_seniority(role_id: str, seniority_levels: list) -> str:
    """Extract seniority from role_id prefix (e.g., 'senior_react_developer' → 'senior')."""
    for level in ["architect", "principal", "lead", "senior", "associate"]:
        if role_id.startswith(level + "_"):
            return level
    if seniority_levels:
        return seniority_levels[0]
    return "consultant"


def _build_routing_map(roles: list, routing_profiles: list) -> dict:
    """Map domain_id → routing_profile_id using naming convention."""
    profile_ids = {p["profile_id"] for p in routing_profiles}
    # Convention: domain maps to a routing queue
    domain_to_profile = {
        "digital_strategy_transformation": "route_advisory_queue",
        "microsoft_power_platform": "route_delivery_queue",
        "ehr_epic_health_it": "route_healthcare_queue",
        "software_engineering_devops": "route_engineering_queue",
        "erp_sap_oracle": "route_erp_queue",
    }
    # Only include mappings where the profile actually exists
    return {d: p for d, p in domain_to_profile.items() if p in profile_ids}


# ═══════════════════════════════════════════════════════
# CACHED SINGLETON (thread-safe, 5-min TTL)
# ═══════════════════════════════════════════════════════

_cached_matcher: Optional[RoleMatcher] = None
_cache_loaded_at: float = 0.0
_cache_lock = threading.Lock()
_CACHE_TTL = 300  # 5 minutes


def get_matcher(db=None, tenant_id: str = None, force_refresh: bool = False) -> RoleMatcher:
    """Get or refresh the cached RoleMatcher singleton. Thread-safe."""
    global _cached_matcher, _cache_loaded_at

    now = time.time()
    if _cached_matcher and not force_refresh and (now - _cache_loaded_at) < _CACHE_TTL:
        return _cached_matcher

    with _cache_lock:
        now = time.time()
        if _cached_matcher and not force_refresh and (now - _cache_loaded_at) < _CACHE_TTL:
            return _cached_matcher

        if not db or not tenant_id:
            raise ValueError("Supabase client and tenant_id required to initialize matcher")

        _cached_matcher = load_matcher_from_supabase(db, tenant_id)
        _cache_loaded_at = now
        log.info(f"Matcher cache refreshed ({len(_cached_matcher.roles)} roles)")
        return _cached_matcher
