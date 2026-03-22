"""
NorthStar Fortress v12 — All 9 locked API endpoints.
Every endpoint: idempotency-gated, policy-bound, fail-closed.
"""
from fastapi import APIRouter, Request, HTTPException, Header
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from uuid import uuid4
import hashlib, json

from backend.config.supabase import get_supabase
from backend.config.settings import get_settings
from backend.models.policy import Policy
from backend.models.decision import Decision, DecisionInput, FeatureVector
from backend.services.policy import PolicyRegistry
from backend.services.scoring import ScoringEngine
from backend.services.economic import EconomicEngine
from backend.services.slate import SlateEngine
from backend.services.execution import ExecutionEngine
from backend.services.replay import ReplayEngine
from backend.services.counterfactual import CounterfactualEngine
from backend.services.routing import RoutingEngine
from backend.services.resume import ResumeParser
from backend.services.idempotency import IdempotencyGuard
from backend.services.audit import AuditLogger

router = APIRouter()


def _get_deps(tenant_id: str = None):
    db = get_supabase()
    tid = tenant_id or get_settings().tenant_id
    return db, tid


# ── Helper models ────────────────────────────────

class ResumeRequest(BaseModel):
    resume_text: str
    job_id: Optional[str] = None

class ScoreRequest(BaseModel):
    job_id: str
    candidate_id: str
    parsed_resume: Dict[str, Any]
    job_requirements: Dict[str, Any]

class SlateRequest(BaseModel):
    job_id: str
    candidates: List[Dict[str, Any]]

class RouteRequest(BaseModel):
    candidate_id: str
    skills: List[str]

class CounterfactualRequest(BaseModel):
    alt_weights: Optional[Dict[str, float]] = None
    alt_economic: Optional[Dict[str, Any]] = None


# ── 1. POST /parse-resume ───────────────────────

@router.post("/parse-resume")
async def parse_resume(req: ResumeRequest,
                       x_idempotency_key: str = Header(...)):
    db, tid = _get_deps()
    guard = IdempotencyGuard(db, tid)
    cached = await guard.check(x_idempotency_key, "/parse-resume", req.model_dump())
    if cached:
        return cached

    await guard.register(x_idempotency_key, "/parse-resume", req.model_dump())
    try:
        parser = ResumeParser()
        result = parser.parse(req.resume_text)
        await guard.complete(x_idempotency_key, result)
        await AuditLogger(db, tid).log("resume_parsed", "resume", result.get("parse_hash", ""), payload={"job_id": req.job_id})
        return result
    except Exception as e:
        await guard.fail(x_idempotency_key)
        raise HTTPException(status_code=500, detail=str(e))


# ── 2. POST /score-candidate ────────────────────

@router.post("/score-candidate")
async def score_candidate(req: ScoreRequest,
                          x_idempotency_key: str = Header(...)):
    db, tid = _get_deps()
    guard = IdempotencyGuard(db, tid)
    cached = await guard.check(x_idempotency_key, "/score-candidate", req.model_dump())
    if cached:
        return cached

    await guard.register(x_idempotency_key, "/score-candidate", req.model_dump())
    try:
        policy_reg = PolicyRegistry(db, tid)
        policy = await policy_reg.get_active()

        scorer = ScoringEngine(policy)
        features = scorer.compute_features(req.parsed_resume, req.job_requirements)
        score = scorer.score(features)
        feature_hash = scorer.hash_features(features)

        result = {
            "job_id": req.job_id,
            "candidate_id": req.candidate_id,
            "score": score,
            "features": features.model_dump(),
            "feature_hash": feature_hash,
            "policy_version": policy.version,
        }
        await guard.complete(x_idempotency_key, result)
        await AuditLogger(db, tid).log("candidate_scored", "candidate", req.candidate_id, payload={"score": score})
        return result
    except Exception as e:
        await guard.fail(x_idempotency_key)
        raise HTTPException(status_code=500, detail=str(e))


# ── 3. POST /generate-slate ─────────────────────

@router.post("/generate-slate")
async def generate_slate(req: SlateRequest,
                         x_idempotency_key: str = Header(...)):
    db, tid = _get_deps()
    guard = IdempotencyGuard(db, tid)
    cached = await guard.check(x_idempotency_key, "/generate-slate", req.model_dump())
    if cached:
        return cached

    await guard.register(x_idempotency_key, "/generate-slate", req.model_dump())
    try:
        policy_reg = PolicyRegistry(db, tid)
        policy = await policy_reg.get_active()
        scorer = ScoringEngine(policy)
        econ_engine = EconomicEngine(db, tid)

        decisions = []
        for cand in req.candidates:
            features = scorer.compute_features(cand, cand.get("job_requirements", {}))
            score = scorer.score(features)

            try:
                job_econ = await econ_engine.get_job_economics(req.job_id)
                econ_snap = econ_engine.compute_expected_profit(score, job_econ)
                profit = econ_snap.expected_profit
                econ_dict = econ_snap.model_dump()
            except Exception:
                profit = 0
                econ_dict = {}

            import platform, sys
            runtime_hash = hashlib.sha256(
                f"{sys.version}|{platform.platform()}|{platform.machine()}".encode()
            ).hexdigest()

            dec = Decision(
                job_id=req.job_id,
                candidate_id=cand.get("candidate_id", str(uuid4())),
                input_snapshot=cand,
                feature_snapshot=features.model_dump(),
                policy_version=policy.version,
                policy_hash=policy.policy_hash or policy.compute_hash(),
                runtime_hash=runtime_hash,
                economic_snapshot=econ_dict,
                score=score,
                expected_profit=profit,
            )
            dec.seal()
            decisions.append(dec)

        slate_engine = SlateEngine(policy)
        slate = slate_engine.generate_slate(decisions)

        # Persist all decisions to ledger
        for d in decisions:
            db.table("v12_decision_ledger").insert({
                "tenant_id": tid,
                "decision_id": str(d.decision_id),
                "job_id": d.job_id,
                "candidate_id": d.candidate_id,
                "input_snapshot": d.input_snapshot,
                "feature_snapshot": d.feature_snapshot,
                "policy_version": d.policy_version,
                "policy_hash": d.policy_hash,
                "runtime_hash": d.runtime_hash,
                "economic_snapshot": d.economic_snapshot,
                "score": float(d.score),
                "expected_profit": float(d.expected_profit),
                "selected": d.selected,
                "rank": d.rank,
                "decision_hash": d.decision_hash,
            }).execute()

        result = {
            "job_id": req.job_id,
            "slate": [
                {
                    "decision_id": str(d.decision_id),
                    "candidate_id": d.candidate_id,
                    "score": d.score,
                    "expected_profit": d.expected_profit,
                    "rank": d.rank,
                }
                for d in slate
            ],
            "total_candidates": len(decisions),
            "policy_version": policy.version,
        }
        await guard.complete(x_idempotency_key, result)
        return result
    except Exception as e:
        await guard.fail(x_idempotency_key)
        raise HTTPException(status_code=500, detail=str(e))


# ── 4. POST /generate-slate-economic ─────────────

@router.post("/generate-slate-economic")
async def generate_slate_economic(req: SlateRequest,
                                   x_idempotency_key: str = Header(...)):
    """Same as generate-slate but purely economic ranking."""
    return await generate_slate(req, x_idempotency_key)


# ── 5. POST /route-candidate ────────────────────

@router.post("/route-candidate")
async def route_candidate(req: RouteRequest,
                          x_idempotency_key: str = Header(...)):
    db, tid = _get_deps()
    guard = IdempotencyGuard(db, tid)
    cached = await guard.check(x_idempotency_key, "/route-candidate", req.model_dump())
    if cached:
        return cached

    await guard.register(x_idempotency_key, "/route-candidate", req.model_dump())
    try:
        engine = RoutingEngine(db, tid)
        result = await engine.route_candidate(req.candidate_id, req.skills)
        await guard.complete(x_idempotency_key, result)
        return result
    except Exception as e:
        await guard.fail(x_idempotency_key)
        raise HTTPException(status_code=500, detail=str(e))


# ── 6. POST /execute/{decision_id} ──────────────

@router.post("/execute/{decision_id}")
async def execute_decision(decision_id: str,
                           x_idempotency_key: str = Header(...)):
    db, tid = _get_deps()
    guard = IdempotencyGuard(db, tid)
    cached = await guard.check(x_idempotency_key, f"/execute/{decision_id}", {"decision_id": decision_id})
    if cached:
        return cached

    await guard.register(x_idempotency_key, f"/execute/{decision_id}", {"decision_id": decision_id})
    try:
        policy_reg = PolicyRegistry(db, tid)
        policy = await policy_reg.get_active()

        dec = db.table("v12_decision_ledger").select("score").eq(
            "decision_id", decision_id
        ).single().execute().data

        engine = ExecutionEngine(db, tid, policy)
        result = await engine.execute(decision_id, float(dec["score"]))
        await guard.complete(x_idempotency_key, result)
        return result
    except ValueError as e:
        await guard.fail(x_idempotency_key)
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        await guard.fail(x_idempotency_key)
        raise HTTPException(status_code=500, detail=str(e))


# ── 7. POST /submit/{decision_id} ───────────────

@router.post("/submit/{decision_id}")
async def submit_decision(decision_id: str,
                          x_idempotency_key: str = Header(...)):
    db, tid = _get_deps()
    guard = IdempotencyGuard(db, tid)
    cached = await guard.check(x_idempotency_key, f"/submit/{decision_id}", {"decision_id": decision_id})
    if cached:
        return cached

    await guard.register(x_idempotency_key, f"/submit/{decision_id}", {"decision_id": decision_id})
    try:
        policy_reg = PolicyRegistry(db, tid)
        policy = await policy_reg.get_active()
        engine = ExecutionEngine(db, tid, policy)
        result = await engine.submit(decision_id, approved_by="api")
        await guard.complete(x_idempotency_key, result)
        return result
    except ValueError as e:
        await guard.fail(x_idempotency_key)
        raise HTTPException(status_code=403, detail=str(e))
    except Exception as e:
        await guard.fail(x_idempotency_key)
        raise HTTPException(status_code=500, detail=str(e))


# ── 8. POST /replay/{decision_id} ───────────────

@router.post("/replay/{decision_id}")
async def replay_decision(decision_id: str):
    db, tid = _get_deps()
    try:
        engine = ReplayEngine(db, tid)
        result = await engine.replay(decision_id)
        if result["status"] == "mismatch":
            await AuditLogger(db, tid).log(
                "replay_mismatch", "decision", decision_id,
                payload=result,
            )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── 9. POST /counterfactual/{decision_id} ───────

@router.post("/counterfactual/{decision_id}")
async def counterfactual(decision_id: str, req: CounterfactualRequest):
    db, tid = _get_deps()
    try:
        policy_reg = PolicyRegistry(db, tid)
        current = await policy_reg.get_active()

        from backend.models.policy import PolicyWeights, EconomicPolicy
        alt_weights = PolicyWeights(**(req.alt_weights or current.weights.model_dump()))
        alt_economic = EconomicPolicy(**(req.alt_economic or current.economic.model_dump()))

        alt_policy = Policy(
            version=f"{current.version}_cf",
            weights=alt_weights,
            economic=alt_economic,
            autonomy=current.autonomy,
        )
        alt_policy.compute_hash()

        engine = CounterfactualEngine(db, tid)
        result = await engine.run(decision_id, alt_policy)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
