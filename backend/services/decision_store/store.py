"""
Decision Store — tamper-detecting read layer.
HARDENED: Every read re-computes decision_hash and compares to stored.
Fail-closed on mismatch.
"""
from supabase import Client
import hashlib, json
from typing import Optional, List


class TamperDetectedError(Exception):
    """Raised when a stored decision's hash doesn't match recomputation."""
    pass


class DecisionStore:
    def __init__(self, db: Client, tenant_id: str):
        self.db = db
        self.tenant_id = tenant_id

    def _recompute_hash(self, row: dict) -> str:
        """Canonical hash — must match Decision.seal() exactly."""
        payload = json.dumps({
            "decision_id": str(row["decision_id"]),
            "job_id": row["job_id"],
            "candidate_id": row["candidate_id"],
            "policy_version": row["policy_version"],
            "policy_hash": row["policy_hash"],
            "runtime_hash": row["runtime_hash"],
            "score": float(row["score"]),
            "expected_profit": float(row["expected_profit"]),
            "rank": int(row["rank"]),
        }, sort_keys=True)
        return hashlib.sha256(payload.encode()).hexdigest()

    def _verify_and_return(self, row: dict) -> dict:
        """Verify hash integrity before returning. Fail-closed."""
        expected = self._recompute_hash(row)
        if expected != row["decision_hash"]:
            # Log tamper event
            try:
                self.db.table("v12_audit_events").insert({
                    "tenant_id": self.tenant_id,
                    "event_type": "tamper_detected",
                    "entity_type": "decision",
                    "entity_id": str(row["decision_id"]),
                    "actor": "system",
                    "payload": {
                        "stored_hash": row["decision_hash"],
                        "recomputed_hash": expected,
                        "score": float(row["score"]),
                        "rank": int(row["rank"]),
                    },
                }).execute()
            except Exception:
                pass  # Never let audit failure block detection
            raise TamperDetectedError(
                f"FORTRESS: Decision {row['decision_id']} hash mismatch. "
                f"Stored: {row['decision_hash'][:16]}..., "
                f"Recomputed: {expected[:16]}..."
            )
        return row

    async def get(self, decision_id: str) -> dict:
        """Fetch and verify a single decision."""
        res = self.db.table("v12_decision_ledger").select("*").eq(
            "tenant_id", self.tenant_id
        ).eq("decision_id", decision_id).single().execute()
        return self._verify_and_return(res.data)

    async def get_by_job(self, job_id: str) -> List[dict]:
        """Fetch all decisions for a job, verifying each."""
        res = self.db.table("v12_decision_ledger").select("*").eq(
            "tenant_id", self.tenant_id
        ).eq("job_id", job_id).order("rank").execute()
        return [self._verify_and_return(row) for row in res.data]

    async def get_slate(self, job_id: str) -> List[dict]:
        """Fetch selected decisions (the slate), verifying each."""
        res = self.db.table("v12_decision_ledger").select("*").eq(
            "tenant_id", self.tenant_id
        ).eq("job_id", job_id).eq(
            "selected", True
        ).order("rank").execute()
        verified = [self._verify_and_return(row) for row in res.data]

        # FORTRESS: Validate rank ordering is contiguous 1..N
        ranks = [r["rank"] for r in verified]
        expected_ranks = list(range(1, len(ranks) + 1))
        if ranks != expected_ranks:
            raise ValueError(
                f"FORTRESS: Rank violation in slate for job {job_id}. "
                f"Expected {expected_ranks}, got {ranks}"
            )
        return verified

    async def verify_all(self, job_id: str) -> dict:
        """Bulk integrity check for all decisions on a job."""
        res = self.db.table("v12_decision_ledger").select("*").eq(
            "tenant_id", self.tenant_id
        ).eq("job_id", job_id).execute()

        results = {"total": len(res.data), "valid": 0, "tampered": 0, "errors": []}
        for row in res.data:
            try:
                self._verify_and_return(row)
                results["valid"] += 1
            except TamperDetectedError as e:
                results["tampered"] += 1
                results["errors"].append(str(e))
        return results
