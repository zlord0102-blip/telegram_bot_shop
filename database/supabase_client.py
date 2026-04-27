import os
from typing import Optional, Any

from supabase import create_client

_client: Optional[Any] = None


def get_supabase_client():
    global _client
    if _client is None:
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_SECRET_KEY")
        if not url or not key:
            raise RuntimeError(
                "Missing SUPABASE_URL and SUPABASE_SECRET_KEY for Telegram Bot runtime"
            )
        _client = create_client(url, key)
    return _client
