"""
Routing Engine — candidate-to-job matching via skill graph.
Leverages existing ontology tables for graph expansion.
"""
from supabase import Client
from typing import List


class RoutingEngine:
    def __init__(self, db: Client, tenant_id: str):
        self.db = db
        self.tenant_id = tenant_id

    async def find_matching_jobs(self, candidate_skills: List[str],
                                  limit: int = 10) -> list:
        """Find jobs whose required skills overlap with candidate."""
        if not candidate_skills:
            return []
        skills_lower = [s.lower() for s in candidate_skills]
        # Query jobs that require any of these skills
        res = self.db.rpc("match_jobs_by_skills", {
            "p_tenant_id": self.tenant_id,
            "p_skills": skills_lower,
            "p_limit": limit,
        }).execute()
        return res.data or []

    async def route_candidate(self, candidate_id: str,
                               candidate_skills: List[str]) -> dict:
        """Route a candidate to best-fit jobs."""
        matches = await self.find_matching_jobs(candidate_skills)
        return {
            "candidate_id": candidate_id,
            "matched_jobs": matches,
            "match_count": len(matches),
        }
