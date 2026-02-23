"""
api/dependencies.py
FastAPI dependency injection functions.
Routers use these via Depends() to access shared resources cleanly.
"""
from __future__ import annotations

import hashlib
import hmac
import time
from typing import Annotated, Optional

import redis.asyncio as aioredis
import structlog
from dask.distributed import Client as DaskClient
from fastapi import Depends, Header, HTTPException, Request, status

from config.settings import get_settings

settings = get_settings()
log = structlog.get_logger()


# ─── SHARED RESOURCE ACCESSORS ────────────────────────────────────────────────

async def get_dask(request: Request) -> DaskClient:
    """Return the Dask client from application state."""
    client = request.app.state
    # Access via the module-level app_state object set during lifespan
    from api.main import app_state
    if app_state.dask_client is None:
        raise HTTPException(status_code=503, detail="Dask cluster unavailable")
    return app_state.dask_client


async def get_redis(request: Request) -> aioredis.Redis:
    """Return the Redis client from application state."""
    from api.main import app_state
    if app_state.redis is None:
        raise HTTPException(status_code=503, detail="Redis unavailable")
    return app_state.redis


# ─── API KEY TIERS ────────────────────────────────────────────────────────────

class APIKeyTier:
    RESEARCH = "research"
    INSTITUTIONAL = "institutional"
    SOVEREIGN = "sovereign"

    RATE_LIMITS = {
        RESEARCH: settings.rate_limit_research,
        INSTITUTIONAL: settings.rate_limit_institutional,
        SOVEREIGN: settings.rate_limit_sovereign,  # 0 = unlimited
    }


def _hash_api_key(raw_key: str) -> str:
    """Derive the stored hash from a raw API key. We store hashes, never raw keys."""
    return hmac.new(
        settings.api_key_salt.encode(),
        raw_key.encode(),
        hashlib.sha256,
    ).hexdigest()


async def get_api_key_tier(
    x_api_key: Annotated[Optional[str], Header()] = None,
    redis: aioredis.Redis = Depends(get_redis),
) -> APIKeyTier:
    """
    Validate API key and return the tier.
    Key metadata is stored in Redis as: apikey:{hash} → {tier}:{org_id}
    Rate limiting uses a sliding window counter: ratelimit:{hash}:{hour_bucket}
    """
    if x_api_key is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key required. Include X-API-Key header.",
            headers={"WWW-Authenticate": "APIKey"},
        )

    key_hash = _hash_api_key(x_api_key)
    meta = await redis.get(f"apikey:{key_hash}")

    if meta is None:
        log.warning("invalid_api_key_attempt", key_prefix=x_api_key[:8] + "...")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key.",
        )

    tier, org_id = meta.split(":", 1)
    log.bind(api_tier=tier, org_id=org_id)

    # Rate limiting (skip for sovereign tier)
    if tier != APIKeyTier.SOVEREIGN:
        limit = APIKeyTier.RATE_LIMITS[tier]
        hour_bucket = int(time.time() // 3600)
        rate_key = f"ratelimit:{key_hash}:{hour_bucket}"

        current = await redis.incr(rate_key)
        if current == 1:
            await redis.expire(rate_key, 3600)

        if current > limit:
            log.warning("rate_limit_exceeded", tier=tier, org=org_id, count=current)
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"Rate limit exceeded: {limit} requests/hour for {tier} tier.",
                headers={"Retry-After": str(3600 - (int(time.time()) % 3600))},
            )

    return tier


# ─── COMMON DEPENDENCIES ──────────────────────────────────────────────────────
DaskDep = Annotated[DaskClient, Depends(get_dask)]
RedisDep = Annotated[aioredis.Redis, Depends(get_redis)]
TierDep = Annotated[str, Depends(get_api_key_tier)]
