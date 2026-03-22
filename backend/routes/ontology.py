"""
NorthStar — Ontology Classification Routes

Additive routes — does NOT touch existing fortress endpoints.
  POST /ontology/classify          — classify a structured candidate
  POST /ontology/parse-and-classify — upload file → extract → classify (full pipeline)
  GET  /ontology/health            — matcher status
  GET  /ontology/queue-assignments  — read-only queue triage surface
"""
from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import hashlib
import logging

from backend.config.supabase import get_supabase
from backend.config.settings import get_settings
from backend.services.ontology.role_matcher import get_matcher
from backend.services.ontology.integration import (
    classify_candidate, load_scoring_profile, OntologyClassification,
)

log = logging.getLogger("routes.ontology")
ontology_router = APIRouter(prefix="/ontology", tags=["ontology"])


# ── Models ───────────────────────────────────────

class CandidateInput(BaseModel):
    name: str = ""
    title: str = ""
    location: str = ""
    vertical: str = ""
    summary: str = ""
    competencies: List[str] = []
    experience: List[Dict[str, Any]] = []
    certifications: List[str] = []
    education: List[Dict[str, Any]] = []


class ClassifyResponse(BaseModel):
    role_match: Dict[str, Any]
    scores: Dict[str, Any]
    routing: Dict[str, Any]
    ingestion_hash: str
    classified_at: str


# ── POST /ontology/classify ──────────────────────

@ontology_router.post("/classify", response_model=ClassifyResponse)
async def classify(candidate: CandidateInput):
    """Classify a structured candidate against the ontology (817 roles, 2470 graph edges)."""
    db = get_supabase()
    tid = get_settings().tenant_id

    try:
        # Load live scoring profile
        weights, thresholds = load_scoring_profile(db, tid)

        result = classify_candidate(
            candidate=candidate.model_dump(),
            db=db,
            tenant_id=tid,
            scoring_weights=weights,
            scoring_thresholds=thresholds,
        )
        return ClassifyResponse(**result.to_dict())

    except ValueError as e:
        raise HTTPException(status_code=503, detail=f"Matcher not ready: {e}")
    except Exception as e:
        log.error(f"Classification failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Classification failed")


# ── POST /ontology/parse-and-classify ────────────

@ontology_router.post("/parse-and-classify")
async def parse_and_classify(
    file: UploadFile = File(...),
):
    """
    Full pipeline: upload resume file → extract text → LLM parse → classify.
    Returns both parsed resume and ontology classification.
    """
    db = get_supabase()
    tid = get_settings().tenant_id

    # File validation
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file provided")
    content = await file.read()
    if len(content) == 0:
        raise HTTPException(status_code=400, detail="Empty file")
    if len(content) > 10 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="File exceeds 10MB limit")

    # Extract text based on file type
    text = ""
    fname = file.filename.lower()
    try:
        if fname.endswith(".pdf"):
            text = _extract_pdf(content)
        elif fname.endswith(".docx"):
            text = _extract_docx(content)
        elif fname.endswith(".txt"):
            text = content.decode("utf-8", errors="replace")
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported file type: {fname}")
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Text extraction failed: {e}")
        raise HTTPException(status_code=422, detail="Failed to extract text from file")

    if len(text.strip()) < 50:
        raise HTTPException(status_code=422, detail="Extracted text too short — likely not a resume")

    # For now, use the existing stub parser to structure the text
    # In production, this calls Claude API for structured extraction
    from backend.services.resume.parser import ResumeParser
    parser = ResumeParser()
    parsed = parser.parse(text)

    # Build a candidate dict from parsed output
    candidate_data = {
        "name": "",  # parser stub doesn't extract names
        "title": "",
        "competencies": parsed.get("skills", []),
        "experience": [{"dates": f"{int(parsed.get('experience_years', 0))} years", "bullets": []}],
        "certifications": parsed.get("certifications", []),
    }

    # Classify
    try:
        weights, thresholds = load_scoring_profile(db, tid)
        classification = classify_candidate(
            candidate=candidate_data,
            db=db,
            tenant_id=tid,
            scoring_weights=weights,
            scoring_thresholds=thresholds,
        )
        return {
            "parsed": parsed,
            "classification": classification.to_dict(),
        }
    except Exception as e:
        log.error(f"Classification failed after parse: {e}")
        return {
            "parsed": parsed,
            "classification": None,
            "classification_error": str(e),
        }


# ── GET /ontology/health ─────────────────────────

@ontology_router.get("/health")
async def ontology_health():
    """Check matcher status — roles loaded, aliases indexed, graph edges."""
    db = get_supabase()
    tid = get_settings().tenant_id
    try:
        matcher = get_matcher(db=db, tenant_id=tid)
        return {
            "status": "ok",
            "roles_loaded": len(matcher.roles),
            "aliases_indexed": len(matcher._alias_index),
            "graph_nodes": len(matcher._graph),
            "ontology_skills": len(matcher._all_ontology_skills),
        }
    except Exception as e:
        return {"status": "error", "detail": str(e)}


# ── GET /ontology/queue-assignments ──────────────

@ontology_router.get("/queue-assignments")
async def queue_assignments(
    routing_queue: Optional[str] = None,
    status: Optional[str] = None,
    candidate_id: Optional[str] = None,
    limit: int = 50,
):
    """Read-only queue assignment surface for recruiter cockpit."""
    db = get_supabase()
    tid = get_settings().tenant_id

    query = db.table("candidate_queue_assignments").select("*").eq("tenant_id", tid)

    if routing_queue:
        query = query.eq("routing_queue", routing_queue)
    if status:
        query = query.eq("assignment_status", status)
    if candidate_id:
        query = query.eq("candidate_id", candidate_id)

    query = query.order("match_score", desc=True).limit(limit)

    try:
        result = query.execute()
        return {"assignments": result.data or [], "count": len(result.data or [])}
    except Exception as e:
        log.error(f"Queue assignment query failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to load queue assignments")


# ── Text extraction helpers ──────────────────────

def _extract_pdf(content: bytes) -> str:
    """Extract text from PDF bytes. Max 50 pages."""
    import io
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            pages = pdf.pages[:50]
            return "\n".join(page.extract_text() or "" for page in pages)
    except ImportError:
        # Fallback: try PyPDF2
        from PyPDF2 import PdfReader
        reader = PdfReader(io.BytesIO(content))
        return "\n".join(
            reader.pages[i].extract_text() or ""
            for i in range(min(len(reader.pages), 50))
        )


def _extract_docx(content: bytes) -> str:
    """Extract text from DOCX bytes. Max 2000 paragraphs."""
    import io
    from docx import Document
    doc = Document(io.BytesIO(content))
    return "\n".join(p.text for p in doc.paragraphs[:2000])
