"""
Counterfactual Engine — "what if" analysis on decisions.
Re-runs a decision under a different policy to show impact.
"""
from supabase import Client
from backend.models.policy import Policy
from backend.models.decision import FeatureVector
from backend.services.scoring import ScoringEngine
from backend.services.economic import EconomicEngine


class CounterfactualEngine:
    def __init__(self, db: Client, tenant_id: str):
        self.db = db
        self.tenant_id = tenant_id

    async def run(self, decision_id: str,
                  alt_policy: Policy) -> dict:
        """Re-score under alternative policy, compare outcomes."""
        dec = self.db.table("v12_decision_ledger").select("*").eq(
            "decision_id", decision_id
        ).single().execute().data

        features = FeatureVector(**dec["feature_snapshot"])

        # Score under alternative policy
        alt_scorer = ScoringEngine(alt_policy)
        alt_score = alt_scorer.score(features)

        # Economic under alternative
        econ_engine = EconomicEngine(self.db, self.tenant_id)
        try:
            job_econ = await econ_engine.get_job_economics(dec["job_id"])
            alt_econ = econ_engine.compute_expected_profit(alt_score, job_econ)
            alt_profit = alt_econ.expected_profit
        except Exception:
            alt_profit = 0

        original_score = float(dec["score"])
        original_profit = float(dec["expected_profit"])

        return {
            "decision_id": decision_id,
            "original": {
                "policy_version": dec["policy_version"],
                "score": original_score,
                "expected_profit": original_profit,
                "selected": dec["selected"],
                "rank": dec["rank"],
            },
            "counterfactual": {
                "policy_version": alt_policy.version,
                "score": alt_score,
                "expected_profit": alt_profit,
                "would_be_selected": alt_score >= alt_policy.economic.min_score_threshold,
            },
            "delta": {
                "score": round(alt_score - original_score, 4),
                "expected_profit": round(alt_profit - original_profit, 2),
            },
        }
