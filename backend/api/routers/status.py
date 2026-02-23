"""Status endpoints that replace the old dashboard demo data.
These are the new permanent locations for ingestion/compute/validation
status information. Frontend pages should hit /v2/status/* rather than the
legacy /api/* routes.

Eventually these will proxy or return real telemetry from the respective
services. For now they simply mirror the mock payloads previously defined
in dashboard.py so the UI continues to work unchanged.
"""
from fastapi import APIRouter
from typing import List, Dict
import time, math, random

router = APIRouter()

# same mock arrays as before
ticker_items = [
    "🟢 ERA5 ingestion nominal · 2.2 TB/h",
    "🟡 MODIS-Terra health degraded · 61%",
    "🟢 CESM2 JOB-04821 · 68% complete",
    "🔵 GraphCast validation cleared · SHORT-MEDIUM range approved",
    "🟢 CMIP7 compliance · 100% on new records",
    "🔴 Dead-letter queue: 12 physics constraint violations · under review",
    "🟢 API p99 latency · 340ms",
    "🟢 ARGO Float array · 4,000 active floats ingesting",
    "🟢 STAC catalog · 847,291 datasets indexed",
    "🟡 Pangu-Weather validation · drift check running",
]

ingestion_sources = [
    {"name": "ERA5 Reanalysis", "org": "ECMWF", "volume": "2.2 TB/h", "status": "online", "rating": "compatible", "latency": "4 min"},
    {"name": "GOES-16/17/18", "org": "NOAA", "volume": "840 GB/h", "status": "online", "rating": "compatible", "latency": "6 min"},
    {"name": "SENTINEL-6 MF", "org": "Copernicus", "volume": "120 GB/h", "status": "online", "rating": "compatible", "latency": "12 min"},
    {"name": "ARGO Float Array", "org": "Argo International", "volume": "8 GB/h", "status": "online", "rating": "almost", "latency": "22 min"},
    {"name": "NEXRAD Radar", "org": "NOAA NWS", "volume": "480 GB/h", "status": "online", "rating": "compatible", "latency": "5 min"},
    {"name": "MODIS Terra/Aqua", "org": "NASA", "volume": "360 GB/h", "status": "warning", "rating": "insufficient", "latency": "48 min"},
]

compute_jobs = [
    {"id": "JOB-04821", "model": "CESM2.1", "type": "Physics", "progress": 68, "status": "running", "cores": "16,384", "eta": "23h 14m", "rating": "compatible"},
    {"id": "JOB-04822", "model": "IFS CY48r1", "type": "Physics", "progress": 31, "status": "running", "cores": "8,192", "eta": "33h 2m", "rating": "compatible"},
    {"id": "JOB-04823", "model": "GraphCast v2", "type": "AI", "progress": 84, "status": "running", "cores": "512 GPU", "eta": "38m", "rating": "almost"},
    {"id": "JOB-04824", "model": "CESM2 VAL", "type": "PostProc", "progress": 0, "status": "queued", "cores": "256", "eta": "—", "rating": "insufficient"},
]

validation_models = [
    {"name": "IFS CY48r1", "org": "ECMWF", "type": "Physics", "rmse": 119.4, "consistency": 99.8, "safeRange": "All ranges", "rating": "compatible", "warnings": 0},
    {"name": "AIFS v1.4", "org": "ECMWF", "type": "AI-Hybrid", "rmse": 128.1, "consistency": 97.1, "safeRange": "Medium (120h)", "rating": "almost", "warnings": 0},
    {"name": "GraphCast v2", "org": "Google DeepMind", "type": "AI", "rmse": 142.3, "consistency": 94.2, "safeRange": "Short-Medium (72h)", "rating": "almost", "warnings": 2},
    {"name": "Pangu-Weather", "org": "Huawei", "type": "AI", "rmse": 156.8, "consistency": 91.7, "safeRange": "Short (48h)", "rating": "insufficient", "warnings": 3},
    {"name": "Fuxi v1.0", "org": "Fudan Univ.", "type": "AI", "rmse": "—", "consistency": "—", "safeRange": "Suspended", "rating": "critical", "warnings": "—"},
]

@router.get("/ingestion/sources")
async def get_ingestion_sources() -> Dict[str, List[dict]]:
    return {"sources": ingestion_sources}

@router.get("/compute/jobs")
async def get_compute_jobs() -> Dict[str, List[dict]]:
    return {"jobs": compute_jobs}

@router.get("/validation/models")
async def get_validation_models() -> Dict[str, List[dict]]:
    return {"models": validation_models}

@router.get("/thermometer")
async def get_thermometer() -> Dict[str, float]:
    baseTemp = 1.42
    t = time.time() / 2.0
    fluctuation = (math.sin(t) * 0.03) + (math.sin(t * 0.4) * 0.02)
    return {"current": round(baseTemp + fluctuation, 3)}
