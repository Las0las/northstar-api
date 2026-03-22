"""
Spine Validator — structural integrity self-check.
HARDENED: Validates policy uniqueness, decision hash integrity,
          lease exclusivity, idempotency consistency, audit completeness.
Run on startup and periodically via /spine/check endpoint.
"""
from supabase import Client
from typing import List
import hashlib, json


class SpineViolation:
    def __init__(self, check: str, severity: str, detail: str):
        self.check = check
        self.severity = severity  # "critical", "warning"
        self.detail = detail

    def to_dict(self):
        return {"check": self.check, "severity": self.severity, "detail": self.detail}


class SpineValidator:
    def __init__(self, db: Client, tenant_id: str):
        self.db = db
        self.tenant_id = tenant_id

    async def run_all(self) -> dict:
        """Run all spine integrity checks. Returns report."""
        violations: List[SpineViolation] = []

        violations.extend(await self._check_single_active_policy())
        violations.extend(await self._check_decision_hash_integrity())
        violations.extend(await self._check_lease_exclusivity())
        violations.extend(await self._check_idempotency_consistency())
        violations.extend(await self._check_rank_contiguity())
        violations.extend(await self._check_audit_coverage())

        critical = [v for v in violations if v.severity == "critical"]
        warnings = [v for v in violations if v.severity == "warning"]

        return {
            "status": "FAIL" if critical else ("WARN" if warnings else "PASS"),
            "total_checks": 6,
            "violations": len(violations),
            "critical": len(critical),
            "warnings": len(warnings),
            "details": [v.to_dict() for v in violations],
        }

    async def _check_single_active_policy(self) -> List[SpineViolation]:
        """Only one policy may be active per tenant."""
        res = self.db.table("policy_registry").select("id, version").eq(
            "tenant_id", self.tenant_id
        ).eq("is_active", True).execute()
        count = len(res.data or [])
        if count == 0:
            return [SpineViolation("single_active_policy", "critical",
                                   "No active policy found")]
        if count > 1:
            versions = [r["version"] for r in res.data]
            return [SpineViolation("single_active_policy", "critical",
                                   f"Multiple active policies: {versions}")]
        return []

    async def _check_decision_hash_integrity(self) -> List[SpineViolation]:
        """Sample decisions and verify hashes match."""
        res = self.db.table("v12_decision_ledger").select("*").eq(
            "tenant_id", self.tenant_id
        ).order("created_at", desc=True).limit(50).execute()

        violations = []
        for row in (res.data or []):
            recomputed = self._hash_decision(row)
            if recomputed != row["decision_hash"]:
                violations.append(SpineViolation(
                    "decision_hash_integrity", "critical",
                    f"Decision {row['decision_id']}: stored={row['decision_hash'][:16]}... "
                    f"recomputed={recomputed[:16]}..."
                ))
        return violations

    async def _check_lease_exclusivity(self) -> List[SpineViolation]:
        """No decision should have >1 active lease."""
        res = self.db.rpc("check_lease_exclusivity", {
            "p_tenant_id": self.tenant_id,
        }).execute()
        # Fallback: manual check
        if not res.data:
            leases = self.db.table("execution_leases").select(
                "decision_id"
            ).eq("tenant_id", self.tenant_id).eq("status", "active").execute()
            seen = {}
            violations = []
            for r in (leases.data or []):
                did = r["decision_id"]
                if did in seen:
                    violations.append(SpineViolation(
                        "lease_exclusivity", "critical",
                        f"Decision {did} has multiple active leases"
                    ))
                seen[did] = True
            return violations
        return []

    async def _check_idempotency_consistency(self) -> List[SpineViolation]:
        """No key should be stuck in 'pending' for >5 minutes."""
        from datetime import datetime, timezone, timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        res = self.db.table("v12_idempotency_keys").select(
            "idempotency_key, endpoint, created_at"
        ).eq("tenant_id", self.tenant_id).eq(
            "status", "pending"
        ).lt("created_at", cutoff).execute()

        violations = []
        for r in (res.data or []):
            violations.append(SpineViolation(
                "idempotency_stale", "warning",
                f"Key {r['idempotency_key'][:8]}... on {r['endpoint']} "
                f"stuck pending since {r['created_at']}"
            ))
        return violations

    async def _check_rank_contiguity(self) -> List[SpineViolation]:
        """Selected decisions for each job must have contiguous ranks 1..N."""
        res = self.db.table("v12_decision_ledger").select(
            "job_id, rank"
        ).eq("tenant_id", self.tenant_id).eq("selected", True).order("job_id").order("rank").execute()

        violations = []
        by_job = {}
        for r in (res.data or []):
            by_job.setdefault(r["job_id"], []).append(r["rank"])
        for job_id, ranks in by_job.items():
            expected = list(range(1, len(ranks) + 1))
            if ranks != expected:
                violations.append(SpineViolation(
                    "rank_contiguity", "critical",
                    f"Job {job_id}: expected ranks {expected}, got {ranks}"
                ))
        return violations

    async def _check_audit_coverage(self) -> List[SpineViolation]:
        """Every decision should have at least one audit event."""
        decisions = self.db.table("v12_decision_ledger").select(
            "decision_id"
        ).eq("tenant_id", self.tenant_id).limit(100).execute()

        violations = []
        for d in (decisions.data or []):
            did = str(d["decision_id"])
            events = self.db.table("v12_audit_events").select("id").eq(
                "tenant_id", self.tenant_id
            ).eq("entity_id", did).limit(1).execute()
            if not events.data:
                violations.append(SpineViolation(
                    "audit_coverage", "warning",
                    f"Decision {did[:8]}... has no audit events"
                ))
        return violations

    @staticmethod
    def _hash_decision(row: dict) -> str:
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
