"""Placeholder ensemble router.
Returns simple message until real logic is added.
"""
from fastapi import APIRouter

router = APIRouter()

@router.get("/ensemble/stats")
async def ensemble_stats():
    return {"message": "Ensemble stats endpoint not yet implemented."}
