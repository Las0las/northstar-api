"""
Supabase client factory — service-role for backend operations.
"""
from supabase import create_client, Client
from functools import lru_cache
from .settings import get_settings


@lru_cache()
def get_supabase() -> Client:
    s = get_settings()
    return create_client(s.supabase_url, s.supabase_service_key)
