"""Ontology services — role matching, scoring, routing against live Supabase tables."""
from .role_matcher import RoleMatcher, RoleMatchResult, get_matcher
from .integration import classify_candidate, OntologyClassification
