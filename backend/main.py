"""
NorthStar Fortress v12 — FastAPI Application
HARDENED: spine check endpoint, startup validation.
"""
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from backend.middleware.fortress import FortressMiddleware
from backend.routes.api import router
from backend.routes.ontology import ontology_router
from backend.config.supabase import get_supabase
from backend.config.settings import get_settings

app = FastAPI(
    title="NorthStar RecruitmentOS",
    version="v12.0.0",
    description="Fortress-grade governed decision system",
)

# CORS for React cockpit
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Fortress fail-closed middleware
app.add_middleware(FortressMiddleware)

# Mount all routes
app.include_router(router)
app.include_router(ontology_router)


@app.get("/health")
async def health():
    return {"status": "ok", "version": "v12.0.0", "mode": "fortress"}


@app.get("/spine/check")
async def spine_check():
    """Run full spine integrity validation. Fail-closed."""
    from backend.services.spine import SpineValidator
    db = get_supabase()
    tid = get_settings().tenant_id
    validator = SpineValidator(db, tid)
    report = await validator.run_all()
    if report["status"] == "FAIL":
        raise HTTPException(status_code=503, detail=report)
    return report


@app.on_event("startup")
async def startup_validation():
    """Validate system integrity on boot."""
    try:
        db = get_supabase()
        tid = get_settings().tenant_id
        # Verify active policy exists
        res = db.table("policy_registry").select("version, policy_hash").eq(
            "tenant_id", tid
        ).eq("is_active", True).maybe_single().execute()
        if not res.data:
            print("FORTRESS WARNING: No active policy found on startup")
        else:
            print(f"FORTRESS: Active policy {res.data['version']} verified")
    except Exception as e:
        print(f"FORTRESS: Startup validation skipped — {e}")
