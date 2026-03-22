"""
Execution Engine — classifies decisions into autonomy bands,
manages submit gate, SLA enforcement.
"""
from supabase import Client
from backend.models.execution import ExecutionClassification, AutonomyBand
from backend.models.policy import Policy
from backend.services.lease import LeaseManager
from backend.services.audit import AuditLogger
from datetime import datetime, timedelta, timezone


class ExecutionEngine:
    def __init__(self, db: Client, tenant_id: str, policy: Policy):
        self.db = db
        self.tenant_id = tenant_id
        self.policy = policy
        self.lease_mgr = LeaseManager(db, tenant_id)
        self.audit = AuditLogger(db, tenant_id)

    async def classify(self, decision_id: str, score: float) -> ExecutionClassification:
        return ExecutionClassification.classify(
            decision_id=decision_id,
            score=score,
            auto_threshold=self.policy.autonomy.auto_apply_threshold,
            assist_threshold=self.policy.autonomy.assist_threshold,
            sla_hours=self.policy.autonomy.sla_hours,
        )

    async def execute(self, decision_id: str, score: float,
                      holder: str = "system") -> dict:
        """Full execution flow: classify → lease → act → audit."""
        classification = await self.classify(decision_id, score)

        # Acquire exclusive lease
        lease = await self.lease_mgr.acquire(decision_id, holder)

        try:
            if classification.band == AutonomyBand.AUTO:
                result = await self._auto_submit(decision_id)
            elif classification.band == AutonomyBand.ASSISTED:
                result = await self._request_approval(decision_id, classification)
            else:
                result = {"action": "manual_review", "decision_id": decision_id}

            await self.audit.log(
                event_type="execution_classified",
                entity_type="decision",
                entity_id=decision_id,
                payload={
                    "band": classification.band.value,
                    "score": score,
                    "action": result.get("action"),
                },
            )
            return {**result, "classification": classification.model_dump()}
        finally:
            await self.lease_mgr.release(decision_id, holder)

    async def _auto_submit(self, decision_id: str) -> dict:
        self.db.table("autonomous_actions").insert({
            "tenant_id": self.tenant_id,
            "decision_id": decision_id,
            "action_type": "auto_submit",
            "metadata": {},
        }).execute()
        return {"action": "auto_submitted", "decision_id": decision_id}

    async def _request_approval(self, decision_id: str,
                                 classification: ExecutionClassification) -> dict:
        deadline = datetime.now(timezone.utc) + timedelta(
            hours=classification.sla_hours
        )
        self.db.table("approval_requests").insert({
            "tenant_id": self.tenant_id,
            "decision_id": decision_id,
            "sla_deadline": deadline.isoformat(),
        }).execute()
        return {
            "action": "approval_requested",
            "decision_id": decision_id,
            "sla_deadline": deadline.isoformat(),
        }

    async def submit(self, decision_id: str, approved_by: str) -> dict:
        """Server-side submit gate — validates rank + approval."""
        # Check decision exists
        dec = self.db.table("v12_decision_ledger").select("*").eq(
            "decision_id", decision_id
        ).single().execute().data

        if not dec["selected"]:
            raise ValueError(
                f"FORTRESS: Cannot submit non-selected decision {decision_id}"
            )

        # Check approval if assisted
        if dec["score"] < self.policy.autonomy.auto_apply_threshold:
            approval = self.db.table("approval_requests").select("*").eq(
                "decision_id", decision_id
            ).eq("status", "approved").maybe_single().execute()
            if not approval.data:
                raise ValueError(
                    f"FORTRESS: Decision {decision_id} requires approval"
                )

        await self.audit.log(
            event_type="decision_submitted",
            entity_type="decision",
            entity_id=decision_id,
            actor=approved_by,
            payload={"score": float(dec["score"]), "rank": dec["rank"]},
        )
        return {"action": "submitted", "decision_id": decision_id}
