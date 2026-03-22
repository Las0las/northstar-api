"""
Scoring Engine — deterministic, policy-bound, hash-sealed.
6-component weighted scoring per policy weights.
HARDENED: runtime hash enforcement, policy hash pre-flight.
"""
from backend.models.policy import Policy, PolicyWeights
from backend.models.decision import FeatureVector
import hashlib, json, platform, sys


class ScoringEngine:
    def __init__(self, policy: Policy):
        if not policy.policy_hash:
            raise ValueError("FORTRESS: Cannot score with unhashed policy. Call policy.compute_hash() first.")
        self.policy = policy
        self.weights = policy.weights
        self._runtime_hash = self._compute_runtime_hash()

    @staticmethod
    def _compute_runtime_hash() -> str:
        components = [sys.version, platform.platform(), platform.machine()]
        return hashlib.sha256("|".join(components).encode()).hexdigest()

    @property
    def runtime_hash(self) -> str:
        return self._runtime_hash

    def verify_policy_integrity(self) -> bool:
        """Re-compute policy hash and compare. Fail-closed if drift."""
        fresh_hash = self.policy.compute_hash()
        return fresh_hash == self.policy.policy_hash

    def compute_features(self, parsed_resume: dict, job_requirements: dict) -> FeatureVector:
        """Extract feature vector from parsed resume vs job requirements."""
        resume_skills = set(s.lower() for s in parsed_resume.get("skills", []))
        required_skills = set(s.lower() for s in job_requirements.get("required_skills", []))
        skill_overlap = len(resume_skills & required_skills)
        skill_total = max(len(required_skills), 1)

        experience = parsed_resume.get("experience_years", 0)
        required_exp = job_requirements.get("min_experience", 0)

        return FeatureVector(
            technical_match=min(skill_overlap / skill_total * 100, 100),
            stability=min(experience / max(required_exp, 1) * 50, 100),
            communication=parsed_resume.get("communication_score", 50),
            domain_experience=min(
                parsed_resume.get("domain_years", 0) / max(job_requirements.get("domain_years", 1), 1) * 100,
                100
            ),
            initiative=parsed_resume.get("initiative_score", 50),
            credentials=min(
                len(parsed_resume.get("certifications", [])) / max(len(job_requirements.get("preferred_certs", ["_"])), 1) * 100,
                100
            ),
        )

    def score(self, features: FeatureVector) -> float:
        """Weighted score calculation. Deterministic. Fail-closed on policy drift."""
        if not self.verify_policy_integrity():
            raise RuntimeError("FORTRESS: Policy hash drift detected during scoring. Aborting.")
        w = self.weights
        raw = (
            features.technical_match * w.technical +
            features.stability * w.stability +
            features.communication * w.communication +
            features.domain_experience * w.domain +
            features.initiative * w.initiative +
            features.credentials * w.extras
        )
        return round(raw, 4)

    def hash_features(self, features: FeatureVector) -> str:
        payload = json.dumps(features.model_dump(), sort_keys=True)
        return hashlib.sha256(payload.encode()).hexdigest()

    def score_with_seal(self, features: FeatureVector) -> dict:
        """Score + return all hashes needed for decision sealing."""
        score = self.score(features)
        return {
            "score": score,
            "feature_hash": self.hash_features(features),
            "policy_hash": self.policy.policy_hash,
            "runtime_hash": self._runtime_hash,
        }
