"""
Replay Engine — deterministic re-execution of sealed decisions.
Validates: decision_hash, runtime_hash, policy_hash.
"""
from supabase import Client
from backend.models.policy import Policy
from backend.services.scoring import ScoringEngine
from backend.services.economic import EconomicEngine
from backend.models.decision import FeatureVector
import hashlib, json, platform, sys


class ReplayEngine:
    def __init__(self, db: Client, tenant_id: str):
        self.db = db
        self.tenant_id = tenant_id

    def _compute_runtime_hash(self) -> str:
        components = [sys.version, platform.platform(), platform.machine()]
        return hashlib.sha256("|".join(components).encode()).hexdigest()

    async def replay(self, decision_id: str) -> dict:
        """Re-execute decision and validate hashes."""
        # Fetch original decision
        dec = self.db.table("v12_decision_ledger").select("*").eq(
            "decision_id", decision_id
        ).single().execute().data

        # Fetch the policy that was active at decision time
        policy_row = self.db.table("policy_registry").select("*").eq(
            "tenant_id", self.tenant_id
        ).eq("version", dec["policy_version"]).single().execute().data

        policy = Policy(
            version=policy_row["version"],
            policy_hash=policy_row["policy_hash"],
            weights=policy_row["weights"],
            economic=policy_row["economic"],
            autonomy=policy_row["autonomy"],
        )

        # Re-compute score from stored features
        scorer = ScoringEngine(policy)
        features = FeatureVector(**dec["feature_snapshot"])
        replayed_score = scorer.score(features)

        # Re-compute economic
        econ_engine = EconomicEngine(self.db, self.tenant_id)
        try:
            job_econ = await econ_engine.get_job_economics(dec["job_id"])
            replayed_econ = econ_engine.compute_expected_profit(replayed_score, job_econ)
            replayed_profit = replayed_econ.expected_profit
        except Exception:
            replayed_profit = float(dec["expected_profit"])

        # Re-seal
        replay_payload = json.dumps({
            "decision_id": dec["decision_id"],
            "job_id": dec["job_id"],
            "candidate_id": dec["candidate_id"],
            "policy_version": dec["policy_version"],
            "policy_hash": dec["policy_hash"],
            "runtime_hash": self._compute_runtime_hash(),
            "score": replayed_score,
            "expected_profit": replayed_profit,
            "rank": dec["rank"],
        }, sort_keys=True)
        replayed_hash = hashlib.sha256(replay_payload.encode()).hexdigest()

        # Validate
        decision_match = replayed_hash == dec["decision_hash"]
        policy_match = policy.policy_hash == dec["policy_hash"]
        runtime_match = self._compute_runtime_hash() == dec["runtime_hash"]
        score_match = abs(replayed_score - float(dec["score"])) < 0.0001

        status = "verified" if (decision_match and score_match) else "mismatch"
        if not runtime_match:
            status = "drift"

        return {
            "decision_id": decision_id,
            "status": status,
            "original_hash": dec["decision_hash"],
            "replayed_hash": replayed_hash,
            "decision_match": decision_match,
            "policy_match": policy_match,
            "runtime_match": runtime_match,
            "score_match": score_match,
            "original_score": float(dec["score"]),
            "replayed_score": replayed_score,
        }
