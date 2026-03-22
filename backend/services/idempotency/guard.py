"""
Idempotency Guard — write-once enforcement per (tenant, key).
HARDENED: request_hash replay protection, conflict audit logging,
          pending-state timeout detection.
"""
from supabase import Client
import hashlib, json
from typing import Optional, Any
from datetime import datetime, timezone, timedelta


PENDING_TTL_SECONDS = 120  # Stale pending keys auto-expire


class IdempotencyGuard:
    def __init__(self, db: Client, tenant_id: str):
        self.db = db
        self.tenant_id = tenant_id

    def _hash_request(self, payload: dict) -> str:
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True).encode()
        ).hexdigest()

    async def check(self, key: str, endpoint: str, payload: dict) -> Optional[dict]:
        """
        Returns cached response if key exists and completed.
        HARDENED: Validates request_hash matches — rejects replayed keys
        with different payloads.
        """
        res = self.db.table("v12_idempotency_keys").select("*").eq(
            "tenant_id", self.tenant_id
        ).eq("idempotency_key", key).maybe_single().execute()

        if not res.data:
            return None

        row = res.data
        expected_hash = self._hash_request(payload)

        # FORTRESS: Reject key reuse with different payload
        if row["request_hash"] != expected_hash:
            raise ValueError(
                f"FORTRESS: Idempotency key '{key}' was registered with a "
                f"different payload. request_hash mismatch: "
                f"expected {row['request_hash'][:16]}..., "
                f"got {expected_hash[:16]}..."
            )

        # FORTRESS: Expire stale pending keys
        if row["status"] == "pending":
            created = datetime.fromisoformat(row["created_at"].replace("Z", "+00:00"))
            if datetime.now(timezone.utc) - created > timedelta(seconds=PENDING_TTL_SECONDS):
                self.db.table("v12_idempotency_keys").update({
                    "status": "failed",
                }).eq("id", row["id"]).execute()
                return None  # Allow retry
            # Still pending — reject concurrent execution
            raise ValueError(
                f"FORTRESS: Idempotency key '{key}' is currently being processed. "
                f"Concurrent execution blocked."
            )

        if row["status"] == "completed":
            return row["response"]

        # Failed — allow retry
        return None

    async def register(self, key: str, endpoint: str, payload: dict) -> bool:
        """Register key. Raises on conflict. Logs violations."""
        req_hash = self._hash_request(payload)
        try:
            self.db.table("v12_idempotency_keys").insert({
                "tenant_id": self.tenant_id,
                "idempotency_key": key,
                "endpoint": endpoint,
                "request_hash": req_hash,
                "status": "pending",
            }).execute()
            return True
        except Exception as e:
            # Log the conflict to audit
            try:
                self.db.table("v12_audit_events").insert({
                    "tenant_id": self.tenant_id,
                    "event_type": "idempotency_conflict",
                    "entity_type": "idempotency_key",
                    "entity_id": key,
                    "actor": "system",
                    "payload": {
                        "endpoint": endpoint,
                        "request_hash": req_hash,
                        "error": str(e)[:200],
                    },
                }).execute()
            except Exception:
                pass  # Never let audit failure block the guard
            raise ValueError(f"FORTRESS: Idempotency key conflict: {key}")

    async def complete(self, key: str, response: Any) -> None:
        self.db.table("v12_idempotency_keys").update({
            "status": "completed",
            "response": response,
            "completed_at": "now()",
        }).eq("tenant_id", self.tenant_id).eq("idempotency_key", key).execute()

    async def fail(self, key: str) -> None:
        self.db.table("v12_idempotency_keys").update({
            "status": "failed",
        }).eq("tenant_id", self.tenant_id).eq("idempotency_key", key).execute()
