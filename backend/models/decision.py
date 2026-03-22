"""
Decision domain models — sealed, append-only.
"""
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from uuid import UUID, uuid4
import hashlib, json


class FeatureVector(BaseModel):
    technical_match: float = 0.0
    stability: float = 0.0
    communication: float = 0.0
    domain_experience: float = 0.0
    initiative: float = 0.0
    credentials: float = 0.0


class DecisionInput(BaseModel):
    job_id: str
    candidate_id: str
    resume_text: Optional[str] = None
    parsed_resume: Optional[Dict[str, Any]] = None
    skills: list[str] = []
    experience_years: float = 0
    certifications: list[str] = []


class Decision(BaseModel):
    decision_id: UUID = Field(default_factory=uuid4)
    job_id: str
    candidate_id: str
    input_snapshot: Dict[str, Any]
    feature_snapshot: Dict[str, Any]
    policy_version: str
    policy_hash: str
    runtime_hash: str
    economic_snapshot: Dict[str, Any]
    score: float
    expected_profit: float
    selected: bool = False
    rank: int = 0
    decision_hash: Optional[str] = None

    def seal(self) -> str:
        """Compute decision_hash from all fields."""
        payload = json.dumps({
            "decision_id": str(self.decision_id),
            "job_id": self.job_id,
            "candidate_id": self.candidate_id,
            "policy_version": self.policy_version,
            "policy_hash": self.policy_hash,
            "runtime_hash": self.runtime_hash,
            "score": self.score,
            "expected_profit": self.expected_profit,
            "rank": self.rank,
        }, sort_keys=True)
        self.decision_hash = hashlib.sha256(payload.encode()).hexdigest()
        return self.decision_hash
