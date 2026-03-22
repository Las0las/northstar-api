"""
Execution classification models — autonomy bands.
"""
from pydantic import BaseModel
from enum import Enum


class AutonomyBand(str, Enum):
    AUTO = "auto"
    ASSISTED = "assisted"
    MANUAL = "manual"


class ExecutionClassification(BaseModel):
    decision_id: str
    score: float
    band: AutonomyBand
    requires_approval: bool
    auto_submit: bool
    sla_hours: int = 4

    @classmethod
    def classify(cls, decision_id: str, score: float,
                 auto_threshold: int = 90, assist_threshold: int = 75,
                 sla_hours: int = 4) -> "ExecutionClassification":
        if score >= auto_threshold:
            band = AutonomyBand.AUTO
        elif score >= assist_threshold:
            band = AutonomyBand.ASSISTED
        else:
            band = AutonomyBand.MANUAL
        return cls(
            decision_id=decision_id,
            score=score,
            band=band,
            requires_approval=band == AutonomyBand.ASSISTED,
            auto_submit=band == AutonomyBand.AUTO,
            sla_hours=sla_hours,
        )
