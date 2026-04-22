import os
from typing import Optional, Any

from supabase import create_client

_client: Optional[Any] = None


def get_supabase_client():
    global _client
    if _client is None:
        url = os.getenv("SUPABASE_URL")
        key = (
            os.getenv("SUPABASE_SECRET_KEY")
            or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
            or os.getenv("SUPABASE_PUBLISHABLE_KEY")
            or os.getenv("SUPABASE_ANON_KEY")
        )
        if not url or not key:
            raise RuntimeError(
                "Missing SUPABASE_URL and SUPABASE_SECRET_KEY/SUPABASE_SERVICE_ROLE_KEY or SUPABASE_PUBLISHABLE_KEY/SUPABASE_ANON_KEY"
            )
        _client = create_client(url, key)
    return _client
