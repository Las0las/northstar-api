"""
Audit Logger — append-only event recording.
"""
from supabase import Client
from typing import Any


class AuditLogger:
    def __init__(self, db: Client, tenant_id: str):
        self.db = db
        self.tenant_id = tenant_id

    async def log(self, event_type: str, entity_type: str,
                  entity_id: str, payload: dict = None,
                  actor: str = "system", metadata: dict = None) -> dict:
        res = self.db.table("v12_audit_events").insert({
            "tenant_id": self.tenant_id,
            "event_type": event_type,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "actor": actor,
            "payload": payload or {},
            "metadata": metadata or {},
        }).execute()
        return res.data[0]

    async def get_events(self, entity_type: str = None,
                         entity_id: str = None,
                         limit: int = 100) -> list:
        q = self.db.table("v12_audit_events").select("*").eq(
            "tenant_id", self.tenant_id
        )
        if entity_type:
            q = q.eq("entity_type", entity_type)
        if entity_id:
            q = q.eq("entity_id", entity_id)
        return q.order("created_at", desc=True).limit(limit).execute().data
