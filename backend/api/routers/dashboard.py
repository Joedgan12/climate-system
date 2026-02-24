"""Legacy dashboard stub (kept so imports succeed)."""
from fastapi import APIRouter

router = APIRouter()

@router.get("/dashboard/info")
async def dashboard_info():
    return {"message": "Dashboard endpoints will be reimplemented later."}
