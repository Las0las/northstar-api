"""
Policy Registry — versioned, single-active, hash-sealed.
"""
from supabase import Client
from backend.models.policy import Policy, PolicyWeights, EconomicPolicy, AutonomyPolicy
import hashlib, json


class PolicyRegistry:
    def __init__(self, db: Client, tenant_id: str):
        self.db = db
        self.tenant_id = tenant_id

    async def get_active(self) -> Policy:
        res = self.db.table("policy_registry").select("*").eq(
            "tenant_id", self.tenant_id
        ).eq("is_active", True).single().execute()
        row = res.data
        return Policy(
            version=row["version"],
            weights=PolicyWeights(**row["weights"]),
            economic=EconomicPolicy(**row["economic"]),
            autonomy=AutonomyPolicy(**row["autonomy"]),
            policy_hash=row["policy_hash"],
        )

    async def propose_migration(self, new_policy: Policy, requested_by: str) -> dict:
        current = await self.get_active()
        if not current.max_weight_change_valid(new_policy):
            raise ValueError("FORTRESS: Weight change exceeds 10% limit")
        new_policy.compute_hash()
        diff = {
            "weights": {
                k: {"old": getattr(current.weights, k), "new": getattr(new_policy.weights, k)}
                for k in current.weights.model_fields
                if getattr(current.weights, k) != getattr(new_policy.weights, k)
            },
            "economic": {
                k: {"old": getattr(current.economic, k), "new": getattr(new_policy.economic, k)}
                for k in current.economic.model_fields
                if getattr(current.economic, k) != getattr(new_policy.economic, k)
            },
        }
        res = self.db.table("policy_migrations").insert({
            "tenant_id": self.tenant_id,
            "from_version": current.version,
            "to_version": new_policy.version,
            "diff": diff,
            "requested_by": requested_by,
        }).execute()
        return res.data[0]

    async def apply_migration(self, migration_id: str, approved_by: str) -> Policy:
        mig = self.db.table("policy_migrations").select("*").eq(
            "id", migration_id
        ).single().execute().data
        if mig["status"] != "pending":
            raise ValueError(f"FORTRESS: Migration {migration_id} is {mig['status']}")

        new_policy = Policy(
            version=mig["to_version"],
            weights=PolicyWeights(**mig["diff"].get("weights_full", {})) if "weights_full" in mig["diff"] else (await self.get_active()).weights,
            economic=EconomicPolicy(**mig["diff"].get("economic_full", {})) if "economic_full" in mig["diff"] else (await self.get_active()).economic,
            autonomy=(await self.get_active()).autonomy,
        )
        new_policy.compute_hash()
        # Deactivate current
        self.db.table("policy_registry").update({"is_active": False}).eq(
            "tenant_id", self.tenant_id
        ).eq("is_active", True).execute()
        # Insert new active
        self.db.table("policy_registry").insert({
            "tenant_id": self.tenant_id,
            "version": new_policy.version,
            "policy_hash": new_policy.policy_hash,
            "weights": new_policy.weights.model_dump(),
            "economic": new_policy.economic.model_dump(),
            "autonomy": new_policy.autonomy.model_dump(),
            "is_active": True,
            "created_by": "migration",
            "approved_by": approved_by,
            "approved_at": "now()",
        }).execute()
        # Mark migration applied
        self.db.table("policy_migrations").update({
            "status": "applied",
            "approved_by": approved_by,
            "applied_at": "now()",
        }).eq("id", migration_id).execute()
        return new_policy
