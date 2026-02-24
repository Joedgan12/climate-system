"""Placeholder models router returning fixed list or message."""
from fastapi import APIRouter

router = APIRouter()

@router.get("/models/list")
async def list_models():
    return {"models": []}
