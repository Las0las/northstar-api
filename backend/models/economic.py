"""
Economic domain models — revenue optimization.
"""
from pydantic import BaseModel
from typing import Optional


class JobEconomics(BaseModel):
    job_id: str
    bill_rate: float
    pay_rate: float
    priority_weight: float = 1.0
    estimated_duration_weeks: int = 12

    @property
    def margin(self) -> float:
        return self.bill_rate - self.pay_rate

    @property
    def margin_pct(self) -> float:
        return (self.margin / self.bill_rate * 100) if self.bill_rate > 0 else 0


class EconomicSnapshot(BaseModel):
    job_id: str
    bill_rate: float
    pay_rate: float
    margin: float
    margin_pct: float
    priority_weight: float
    estimated_duration_weeks: int
    placement_probability: float
    expected_profit: float
