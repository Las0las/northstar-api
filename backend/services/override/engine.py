"""
Override Engine — tracked manual rank overrides with penalty.
"""
from supabase import Client
from backend.services.audit import AuditLogger


class OverrideEngine:
    def __init__(self, db: Client, tenant_id: str, penalty: int = 5):
        self.db = db
        self.tenant_id = tenant_id
        self.penalty = penalty
        self.audit = AuditLogger(db, tenant_id)

    async def override_rank(self, decision_id: str, new_rank: int,
                            override_by: str, reason: str) -> dict:
        dec = self.db.table("v12_decision_ledger").select("rank, score").eq(
            "decision_id", decision_id
        ).single().execute().data

        original_rank = dec["rank"]
        if original_rank == new_rank:
            raise ValueError("FORTRESS: New rank equals current rank")

        self.db.table("override_events").insert({
            "tenant_id": self.tenant_id,
            "decision_id": decision_id,
            "override_by": override_by,
            "reason": reason,
            "original_rank": original_rank,
            "new_rank": new_rank,
            "penalty_applied": self.penalty,
        }).execute()

        await self.audit.log(
            event_type="rank_override",
            entity_type="decision",
            entity_id=decision_id,
            actor=override_by,
            payload={
                "original_rank": original_rank,
                "new_rank": new_rank,
                "penalty": self.penalty,
                "reason": reason,
            },
        )

        return {
            "decision_id": decision_id,
            "original_rank": original_rank,
            "new_rank": new_rank,
            "penalty_applied": self.penalty,
        }
