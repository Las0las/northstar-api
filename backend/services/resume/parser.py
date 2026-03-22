"""
Resume Parser — LLM-structured parse with deterministic fallback.
"""
from typing import Dict, Any
import hashlib, json


class ResumeParser:
    """
    Parses resume text into structured format.
    In production, delegates to LLM. Stub provides deterministic extraction.
    """

    def parse(self, resume_text: str) -> Dict[str, Any]:
        """Parse resume into structured fields."""
        text_lower = resume_text.lower()
        skills = self._extract_skills(text_lower)
        experience = self._estimate_experience(text_lower)
        certs = self._extract_certifications(text_lower)

        parsed = {
            "skills": skills,
            "experience_years": experience,
            "certifications": certs,
            "communication_score": self._score_communication(resume_text),
            "initiative_score": self._score_initiative(text_lower),
            "domain_years": max(experience - 2, 0),
            "raw_length": len(resume_text),
        }
        parsed["parse_hash"] = hashlib.sha256(
            json.dumps(parsed, sort_keys=True).encode()
        ).hexdigest()
        return parsed

    def _extract_skills(self, text: str) -> list:
        known_skills = [
            "python", "javascript", "typescript", "react", "node",
            "sql", "aws", "docker", "kubernetes", "java", "go",
            "rust", "c++", "terraform", "graphql", "redis",
            "postgresql", "mongodb", "fastapi", "django", "flask",
        ]
        return [s for s in known_skills if s in text]

    def _estimate_experience(self, text: str) -> float:
        import re
        matches = re.findall(r"(\d+)\+?\s*years?", text)
        return max((float(m) for m in matches), default=0)

    def _extract_certifications(self, text: str) -> list:
        known_certs = ["aws certified", "pmp", "cka", "ckad", "cissp", "ceh"]
        return [c for c in known_certs if c in text]

    def _score_communication(self, text: str) -> float:
        word_count = len(text.split())
        if word_count > 500:
            return 75
        elif word_count > 200:
            return 60
        return 40

    def _score_initiative(self, text: str) -> float:
        signals = ["led", "founded", "built", "created", "launched",
                   "open source", "contributed", "mentored"]
        hits = sum(1 for s in signals if s in text)
        return min(40 + hits * 10, 100)
