"""Simple lineage router placeholder."""
from fastapi import APIRouter

router = APIRouter()

@router.get("/lineage/events")
async def lineage_events():
    return {"events": []}
