"""
Lease Manager — exclusive execution lock per decision.
HARDENED: audit logging on conflict, double-check after acquire,
          holder identity validation.
"""
from supabase import Client
from datetime import datetime, timedelta, timezone
from typing import Optional


class LeaseManager:
    def __init__(self, db: Client, tenant_id: str):
        self.db = db
        self.tenant_id = tenant_id

    async def acquire(self, decision_id: str, holder: str,
                      ttl_seconds: int = 300) -> dict:
        """Acquire exclusive lease. Raises on conflict. Logs to audit."""
        if not holder or len(holder) < 1:
            raise ValueError("FORTRESS: Lease holder identity required")

        # Expire stale leases first
        self.db.table("execution_leases").update({
            "status": "expired"
        }).eq("tenant_id", self.tenant_id).eq(
            "decision_id", decision_id
        ).eq("status", "active").lt(
            "expires_at", datetime.now(timezone.utc).isoformat()
        ).execute()

        # Check for active lease
        existing = self.db.table("execution_leases").select("*").eq(
            "tenant_id", self.tenant_id
        ).eq("decision_id", decision_id).eq(
            "status", "active"
        ).maybe_single().execute()

        if existing.data:
            # HARDENED: Log conflict to audit
            try:
                self.db.table("v12_audit_events").insert({
                    "tenant_id": self.tenant_id,
                    "event_type": "lease_conflict",
                    "entity_type": "execution_lease",
                    "entity_id": decision_id,
                    "actor": holder,
                    "payload": {
                        "held_by": existing.data["holder"],
                        "acquired_at": existing.data["acquired_at"],
                        "expires_at": existing.data["expires_at"],
                        "contested_by": holder,
                    },
                }).execute()
            except Exception:
                pass
            raise ValueError(
                f"FORTRESS: Lease conflict on decision {decision_id}, "
                f"held by {existing.data['holder']}"
            )

        expires = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
        res = self.db.table("execution_leases").insert({
            "tenant_id": self.tenant_id,
            "decision_id": decision_id,
            "holder": holder,
            "expires_at": expires.isoformat(),
            "status": "active",
        }).execute()

        # HARDENED: Double-check — re-read to confirm we actually hold it
        verify = self.db.table("execution_leases").select("holder").eq(
            "tenant_id", self.tenant_id
        ).eq("decision_id", decision_id).eq(
            "status", "active"
        ).execute()
        active_holders = [r["holder"] for r in (verify.data or [])]
        if len(active_holders) > 1:
            # Race condition — release ours and fail
            self.db.table("execution_leases").update({
                "status": "released",
                "released_at": datetime.now(timezone.utc).isoformat(),
            }).eq("id", res.data[0]["id"]).execute()
            raise ValueError(
                f"FORTRESS: Lease race detected on {decision_id}. "
                f"Multiple holders: {active_holders}"
            )

        return res.data[0]

    async def release(self, decision_id: str, holder: str) -> None:
        self.db.table("execution_leases").update({
            "status": "released",
            "released_at": datetime.now(timezone.utc).isoformat(),
        }).eq("tenant_id", self.tenant_id).eq(
            "decision_id", decision_id
        ).eq("holder", holder).eq("status", "active").execute()

    async def verify(self, decision_id: str, holder: str) -> bool:
        """Verify lease is still active and not expired."""
        res = self.db.table("execution_leases").select("*").eq(
            "tenant_id", self.tenant_id
        ).eq("decision_id", decision_id).eq(
            "holder", holder
        ).eq("status", "active").maybe_single().execute()
        if not res.data:
            return False
        expires = datetime.fromisoformat(res.data["expires_at"].replace("Z", "+00:00"))
        return expires > datetime.now(timezone.utc)

    async def force_expire_all(self, decision_id: str, reason: str) -> int:
        """Emergency: expire all leases on a decision. Audit-logged."""
        res = self.db.table("execution_leases").select("id, holder").eq(
            "tenant_id", self.tenant_id
        ).eq("decision_id", decision_id).eq("status", "active").execute()

        count = len(res.data or [])
        if count > 0:
            self.db.table("execution_leases").update({
                "status": "expired",
            }).eq("tenant_id", self.tenant_id).eq(
                "decision_id", decision_id
            ).eq("status", "active").execute()

            self.db.table("v12_audit_events").insert({
                "tenant_id": self.tenant_id,
                "event_type": "lease_force_expired",
                "entity_type": "execution_lease",
                "entity_id": decision_id,
                "actor": "system",
                "payload": {"reason": reason, "expired_count": count},
            }).execute()
        return count
