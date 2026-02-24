"""Admin router for managing API keys.

This basic implementation allows creation of new API keys (raw value returned)
and listing of existing hashed entries. In production this would be locked down
behind authentication and audit logging.
"""
from fastapi import APIRouter, HTTPException
from typing import Optional, List
import uuid
import hmac
import hashlib

import redis.asyncio as aioredis

from config.settings import get_settings

router = APIRouter()

settings = get_settings()


@router.post("/v2/admin/keys")
async def create_api_key(tier: str = "research", org_id: Optional[str] = None):
    """Generate a new API key and store its hash in Redis."""
    raw = uuid.uuid4().hex
    key_hash = hmac.new(
        settings.api_key_salt.encode(), raw.encode(), hashlib.sha256
    ).hexdigest()

    r = aioredis.from_url(settings.redis_url)
    await r.set(f"apikey:{key_hash}", f"{tier}:{org_id or ''}")

    return {"api_key": raw, "tier": tier, "org_id": org_id}


@router.get("/v2/admin/keys")
async def list_api_keys():
    """Return all stored API key hashes (no raw values)."""
    r = aioredis.from_url(settings.redis_url)
    keys = await r.keys("apikey:*")
    return {"stored_keys": keys}
