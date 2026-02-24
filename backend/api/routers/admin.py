"""Administrative endpoints for local development.

- Create/list API keys (existing in `keys.py` but left here for grouping).
- File upload to MinIO for adding real Zarr stores.
"""
from fastapi import APIRouter, UploadFile, Form, HTTPException
import hmac, hashlib, uuid
from typing import Optional
import aiofiles

import redis.asyncio as aioredis
import s3fs
from config.settings import get_settings

router = APIRouter()
settings = get_settings()

# reuse some of keys logic here to avoid circular import

def compute_hash(raw: str) -> str:
    return hmac.new(
        settings.api_key_salt.encode(), raw.encode(), hashlib.sha256
    ).hexdigest()

@router.post("/v2/admin/keys")
async def create_api_key(tier: str = "research", org_id: Optional[str] = None):
    raw = uuid.uuid4().hex
    key_hash = compute_hash(raw)
    r = aioredis.from_url(settings.redis_url)
    await r.set(f"apikey:{key_hash}", f"{tier}:{org_id or ''}")
    return {"api_key": raw, "tier": tier, "org_id": org_id}

@router.get("/v2/admin/keys")
async def list_api_keys():
    r = aioredis.from_url(settings.redis_url)
    keys = await r.keys("apikey:*")
    return {"stored_keys": keys}

@router.post("/v2/admin/upload")
async def upload_zarr(
    file: UploadFile,
    dest_path: str = Form(..., description="S3 key under pcmip-archive, e.g. zarr/obs/era5/sample"),
):
    """Receive a zipped Zarr archive and write it to MinIO at the given prefix."""
    fs = s3fs.S3FileSystem(**settings.s3_storage_options)
    full_key = f"pcmip-archive/{dest_path}"
    # write directly from upload stream to S3
    try:
        with fs.open(full_key, "wb") as f:
            content = await file.read()
            f.write(content)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"status": "uploaded", "path": full_key}
