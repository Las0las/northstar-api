"""
Economic Engine — expected profit maximization.
Formula: P(placement) * margin * availability * priority_weight
"""
from supabase import Client
from backend.models.economic import JobEconomics, EconomicSnapshot


class EconomicEngine:
    def __init__(self, db: Client, tenant_id: str):
        self.db = db
        self.tenant_id = tenant_id

    async def get_job_economics(self, job_id: str) -> JobEconomics:
        res = self.db.table("job_economics").select("*").eq(
            "tenant_id", self.tenant_id
        ).eq("job_id", job_id).single().execute()
        row = res.data
        return JobEconomics(
            job_id=row["job_id"],
            bill_rate=float(row["bill_rate"]),
            pay_rate=float(row["pay_rate"]),
            priority_weight=float(row["priority_weight"]),
            estimated_duration_weeks=row["estimated_duration_weeks"],
        )

    def compute_expected_profit(self, score: float,
                                 econ: JobEconomics) -> EconomicSnapshot:
        """
        P(placement) derived from score:
        - score >= 90 → 0.85
        - score >= 75 → 0.55
        - score >= 50 → 0.25
        - else → 0.10
        """
        if score >= 90:
            p_placement = 0.85
        elif score >= 75:
            p_placement = 0.55
        elif score >= 50:
            p_placement = 0.25
        else:
            p_placement = 0.10

        weekly_margin = econ.margin
        total_margin = weekly_margin * econ.estimated_duration_weeks
        expected_profit = round(
            p_placement * total_margin * econ.priority_weight, 2
        )

        return EconomicSnapshot(
            job_id=econ.job_id,
            bill_rate=econ.bill_rate,
            pay_rate=econ.pay_rate,
            margin=econ.margin,
            margin_pct=econ.margin_pct,
            priority_weight=econ.priority_weight,
            estimated_duration_weeks=econ.estimated_duration_weeks,
            placement_probability=p_placement,
            expected_profit=expected_profit,
        )
