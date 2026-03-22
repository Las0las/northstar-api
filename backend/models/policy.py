"""
Policy domain models — versioned, hash-sealed, single-active.
"""
from pydantic import BaseModel, Field
from typing import Optional
import hashlib, json


class PolicyWeights(BaseModel):
    technical: float = 0.30
    stability: float = 0.20
    communication: float = 0.15
    domain: float = 0.15
    initiative: float = 0.10
    extras: float = 0.10

    def validate_sum(self) -> bool:
        return abs(sum(self.model_dump().values()) - 1.0) < 0.001


class EconomicPolicy(BaseModel):
    min_score_threshold: int = 75
    max_slate_size: int = 3


class AutonomyPolicy(BaseModel):
    auto_apply_threshold: int = 90
    assist_threshold: int = 75
    sla_hours: int = 4
    override_penalty: int = 5


class Policy(BaseModel):
    version: str
    weights: PolicyWeights = PolicyWeights()
    economic: EconomicPolicy = EconomicPolicy()
    autonomy: AutonomyPolicy = AutonomyPolicy()
    policy_hash: Optional[str] = None

    def compute_hash(self) -> str:
        payload = json.dumps({
            "version": self.version,
            "weights": self.weights.model_dump(),
            "economic": self.economic.model_dump(),
            "autonomy": self.autonomy.model_dump(),
        }, sort_keys=True)
        self.policy_hash = hashlib.sha256(payload.encode()).hexdigest()
        return self.policy_hash

    def max_weight_change_valid(self, other: "Policy") -> bool:
        """Enforce max 10% weight change rule."""
        for field in self.weights.model_fields:
            old = getattr(self.weights, field)
            new = getattr(other.weights, field)
            if abs(new - old) > 0.10:
                return False
        return True
