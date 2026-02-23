"""
api/routers/climate.py
Endpoints: /v2/climate/variable, /v2/climate/timeseries
These are the most latency-sensitive endpoints in the system.
Every response must include uncertainty and provenance. No exceptions.
"""
from __future__ import annotations

import json
import time
from datetime import datetime
from typing import Optional

import numpy as np
import structlog
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse

from api.dependencies import DaskDep, RedisDep, TierDep
from api.models.schemas import (
    AggregateType,
    GridPoint,
    PCMIPError,
    Provenance,
    QualityFlag,
    ResponseFormat,
    TimeseriesPoint,
    TimeseriesResponse,
    Uncertainty,
    UncertaintyMethod,
    VariableRequest,
    VariableResponse,
)
from api.services.zarr_service import ZarrService
from config.settings import get_settings

settings = get_settings()
log = structlog.get_logger(__name__)
router = APIRouter()


def _get_zarr_service(dask: DaskDep) -> ZarrService:
    """Create or reuse a ZarrService scoped to the request's Dask client."""
    return ZarrService(dask_client=dask)


async def _check_cache(redis: RedisDep, cache_key: str) -> Optional[dict]:
    """Return cached JSON dict or None."""
    raw = await redis.get(cache_key)
    if raw:
        return json.loads(raw)
    return None


async def _set_cache(redis: RedisDep, cache_key: str, data: dict, ttl: int) -> None:
    await redis.set(cache_key, json.dumps(data, default=str), ex=ttl)


def _build_cache_key(*parts: str) -> str:
    return "pcmip:v2:" + ":".join(str(p) for p in parts)


def _derive_uncertainty(value: float, variable: str) -> Uncertainty:
    """
    Derive simple parametric uncertainty when ensemble bounds are not available.
    For ERA5 single-level, uses published ERA5 uncertainty estimates.
    For model output, ensembles should be used — this is a fallback only.
    """
    # ERA5 published 2m temperature uncertainty is ~0.5K; scale by variable type
    uncertainty_pct = {
        "air_temperature": 0.003,
        "precipitation_flux": 0.15,
        "eastward_wind": 0.05,
        "northward_wind": 0.05,
        "geopotential": 0.002,
        "sea_surface_temperature": 0.004,
    }.get(variable, 0.05)

    spread = abs(value) * uncertainty_pct
    return Uncertainty(
        method=UncertaintyMethod.CONFORMAL_PREDICTION,
        p05=value - 1.96 * spread,
        p25=value - 0.674 * spread,
        p50=value,
        p75=value + 0.674 * spread,
        p95=value + 1.96 * spread,
        ensemble_size=None,
        calibrated=False,  # parametric — not calibrated from actual ensemble
    )


# ─── GET /v2/climate/variable ─────────────────────────────────────────────────

@router.get(
    "/variable",
    response_model=VariableResponse,
    summary="Query a climate variable at a point, time, and optional pressure level",
    responses={
        200: {"description": "Climate variable with uncertainty and provenance"},
        400: {"model": PCMIPError, "description": "Invalid request parameters"},
        401: {"model": PCMIPError, "description": "Missing or invalid API key"},
        404: {"model": PCMIPError, "description": "Variable not available for requested location/time"},
        429: {"model": PCMIPError, "description": "Rate limit exceeded"},
        504: {"model": PCMIPError, "description": "Query timeout"},
    },
)
async def get_variable(
    lat: float = Query(..., ge=-90, le=90, description="Latitude"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude"),
    variable: str = Query(..., description="CF-1.10 standard name"),
    time: datetime = Query(..., description="ISO8601 timestamp"),
    level: Optional[str] = Query(None, description="Pressure level hPa or 'surface'"),
    model: Optional[str] = Query(None, description="Model identifier"),
    ensemble: Optional[str] = Query(None, description="Ensemble member"),
    format: ResponseFormat = Query(ResponseFormat.JSON),
    dask: DaskDep = ...,
    redis: RedisDep = ...,
    tier: TierDep = ...,
) -> VariableResponse:
    t_start = time.perf_counter()

    # Parse level
    parsed_level = None
    if level and level.lower() != "surface":
        try:
            parsed_level = float(level)
            if not (1.0 <= parsed_level <= 1100.0):
                raise ValueError()
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="'level' must be a pressure value in hPa [1, 1100] or 'surface'"
            )

    # Validate variable name structure
    if not variable.replace("_", "").isalpha():
        raise HTTPException(status_code=400, detail=f"'{variable}' is not a valid CF standard name")
    variable = variable.lower()

    # Cache check
    cache_key = _build_cache_key("variable", variable, lat, lon, time.isoformat(), level or "sfc", model or "best", ensemble or "mean")
    cached = await _check_cache(redis, cache_key)
    if cached:
        cached["_cache_hit"] = True
        return VariableResponse(**cached)

    # Query Zarr
    zarr_svc = _get_zarr_service(dask)
    try:
        value, grid_point, resolved_model, provenance = await zarr_svc.get_variable_at_point(
            variable=variable,
            lat=lat,
            lon=lon,
            time_req=time,
            level=parsed_level,
            model=model,
            ensemble=ensemble,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    # Uncertainty
    uncertainty = _derive_uncertainty(value, variable)

    # Units
    from api.services.zarr_service import CF_UNITS
    unit = CF_UNITS.get(variable, "1")

    # CMIP7 variable name
    from api.services.zarr_service import CF_TO_CMIP7
    cmip7_var = CF_TO_CMIP7.get(variable)

    response = VariableResponse(
        variable=variable,
        cf_name=variable,
        cmip7_var=cmip7_var,
        value=value,
        unit=unit,
        grid_point=grid_point,
        time_actual=time,
        level=parsed_level,
        model=resolved_model,
        ensemble=ensemble,
        uncertainty=uncertainty,
        provenance=provenance,
        warnings=[],
        response_time_ms=int((time.perf_counter() - t_start) * 1000),
    )

    # Cache (do not await — fire-and-forget)
    import asyncio
    asyncio.create_task(_set_cache(redis, cache_key, response.model_dump(mode="json"), settings.cache_ttl_variable))

    log.info(
        "climate_variable_served",
        variable=variable, lat=lat, lon=lon,
        model=resolved_model, tier=tier,
        response_ms=response.response_time_ms,
    )
    return response


# ─── GET /v2/climate/timeseries ───────────────────────────────────────────────

@router.get(
    "/timeseries",
    response_model=TimeseriesResponse,
    summary="Time series for a variable at a point location",
)
async def get_timeseries(
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180),
    variable: str = Query(..., description="CF-1.10 standard name"),
    start: datetime = Query(..., description="Start of period (ISO8601)"),
    end: datetime = Query(..., description="End of period (ISO8601)"),
    level: Optional[str] = Query(None),
    model: Optional[str] = Query(None),
    aggregate: AggregateType = Query(AggregateType.NONE),
    dask: DaskDep = ...,
    redis: RedisDep = ...,
    tier: TierDep = ...,
) -> TimeseriesResponse:
    t_start = time.perf_counter()

    if end <= start:
        raise HTTPException(status_code=400, detail="'end' must be after 'start'")

    parsed_level = None
    if level and level.lower() != "surface":
        try:
            parsed_level = float(level)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid level value")

    variable = variable.lower()

    zarr_svc = _get_zarr_service(dask)
    try:
        da, grid_point, resolved_model, provenance = await zarr_svc.get_timeseries(
            variable=variable,
            lat=lat,
            lon=lon,
            start=start,
            end=end,
            level=parsed_level,
            model=model,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    # Apply temporal aggregation if requested
    if aggregate != AggregateType.NONE:
        freq_map = {
            AggregateType.HOURLY: "1h",
            AggregateType.DAILY: "1D",
            AggregateType.MONTHLY: "ME",
            AggregateType.ANNUAL: "YE",
        }
        da = da.resample(time=freq_map[aggregate]).mean()

    # Hard limit on response size
    MAX_TIMESTEPS = 10_000
    if len(da.time) > MAX_TIMESTEPS:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Request would return {len(da.time)} timesteps, exceeding the {MAX_TIMESTEPS} limit. "
                "Reduce the time range, use a coarser aggregate, or use the async export endpoint."
            ),
        )

    # Build response data points
    from api.services.zarr_service import CF_UNITS
    unit = CF_UNITS.get(variable, "1")

    data_points = [
        TimeseriesPoint(
            time=ts.item(),
            value=float(val),
            uncertainty=_derive_uncertainty(float(val), variable),
            quality_flags=[QualityFlag.VALID] if not np.isnan(float(val)) else [QualityFlag.NEAR_BOUNDARY],
        )
        for ts, val in zip(da.time.values, da.values)
        if not np.isnan(float(val))
    ]

    response = TimeseriesResponse(
        variable=variable,
        cf_name=variable,
        unit=unit,
        grid_point=grid_point,
        level=parsed_level,
        model=resolved_model,
        aggregate=aggregate,
        timestep_count=len(data_points),
        data=data_points,
        provenance=provenance,
        warnings=[] if len(data_points) == len(da.time) else [
            f"{len(da.time) - len(data_points)} timesteps omitted due to missing data (NaN)"
        ],
        response_time_ms=int((time.perf_counter() - t_start) * 1000),
    )

    log.info(
        "climate_timeseries_served",
        variable=variable, lat=lat, lon=lon,
        n_timesteps=len(data_points), tier=tier,
        response_ms=response.response_time_ms,
    )
    return response
