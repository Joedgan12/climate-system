"""
api/services/zarr_service.py
Zarr dataset access layer. All climate data reads go through here.
This service is responsible for:
  - Locating the correct Zarr store for a given variable + source
  - Selecting data at the requested coordinates and time
  - Returning xarray DataArrays to the router for post-processing
  - Never returning raw data without provenance metadata attached

Performance critical: every method here hits S3. Chunk reads are the bottleneck.
The STAC catalog is queried first to find the optimal dataset for each request.
Zarr consolidate_metadata must be enabled on all production stores.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import time
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

import numpy as np
import s3fs
import structlog
import xarray as xr
import zarr
from dask.distributed import Client as DaskClient

from config.settings import get_settings
from api.models.schemas import GridPoint, Provenance, QualityFlag, UncertaintyMethod

settings = get_settings()
log = structlog.get_logger(__name__)

# CF standard name → CMIP7 variable name mapping (partial; full table in cf_table.json)
CF_TO_CMIP7: Dict[str, str] = {
    "air_temperature": "tas",
    "air_temperature_at_2m": "tas",
    "precipitation_flux": "pr",
    "eastward_wind": "ua",
    "northward_wind": "va",
    "geopotential": "zg",
    "specific_humidity": "hus",
    "sea_surface_temperature": "tos",
    "surface_downwelling_shortwave_flux_in_air": "rsds",
    "toa_outgoing_longwave_flux": "rlut",
}

# Default variable units by CF standard name
CF_UNITS: Dict[str, str] = {
    "air_temperature": "K",
    "precipitation_flux": "kg m-2 s-1",
    "eastward_wind": "m s-1",
    "northward_wind": "m s-1",
    "geopotential": "m2 s-2",
    "specific_humidity": "kg kg-1",
    "sea_surface_temperature": "K",
    "toa_outgoing_longwave_flux": "W m-2",
}


class ZarrService:
    """
    Thread-safe Zarr dataset accessor. Instantiated once per API worker.
    Caches open Zarr stores in memory — reopening an S3-backed Zarr store
    per request is expensive (~200ms cold open). Cache is bounded by
    MAX_OPEN_STORES to prevent memory growth.
    """

    MAX_OPEN_STORES = 32

    def __init__(self, dask_client: DaskClient) -> None:
        self.dask = dask_client
        self._store_cache: Dict[str, xr.Dataset] = {}
        self._fs = s3fs.S3FileSystem(**settings.s3_storage_options)

    def _store_key(self, store_url: str) -> str:
        return hashlib.md5(store_url.encode()).hexdigest()

    async def _open_store(self, store_url: str) -> xr.Dataset:
        """
        Open a Zarr store, with in-process caching.
        consolidated=True requires that the Zarr store has been consolidated
        with zarr.consolidate_metadata() — this must be enforced at write time.
        """
        cache_key = self._store_key(store_url)
        if cache_key not in self._store_cache:
            if len(self._store_cache) >= self.MAX_OPEN_STORES:
                # Evict oldest (FIFO)
                oldest = next(iter(self._store_cache))
                ds = self._store_cache.pop(oldest)
                ds.close()
                log.info("zarr_store_evicted", evicted=oldest)

            log.info("zarr_store_opening", url=store_url)
            t0 = time.perf_counter()

            # Run blocking I/O in thread pool to avoid blocking the event loop
            ds = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: xr.open_zarr(
                    store_url,
                    consolidated=True,
                    storage_options=settings.s3_storage_options,
                    chunks="auto",  # Dask-backed lazy arrays
                ),
            )
            elapsed = time.perf_counter() - t0
            log.info("zarr_store_opened", url=store_url, elapsed_ms=int(elapsed * 1000))
            self._store_cache[cache_key] = ds

        return self._store_cache[cache_key]

    def _resolve_store_url(self, variable: str, model: Optional[str], level: Optional[float]) -> str:
        """
        Determine the best Zarr store URL for a given variable + model combination.
        In production this would query the STAC catalog. Here we use deterministic
        path resolution based on the PCMIP store layout convention.
        """
        base = settings.zarr_store_url

        if model is None or model.upper() in ("ERA5", "ERA5-LAND", "BEST_OBS"):
            # Default to ERA5 reanalysis for surface/pressure level variables
            if level is None or str(level) == "surface":
                return f"{base}/{settings.zarr_obs_prefix}/era5/single-levels"
            else:
                return f"{base}/{settings.zarr_obs_prefix}/era5/pressure-levels"

        # Model output path follows CMIP7 DRS
        return f"{base}/{settings.zarr_models_prefix}/{model.lower()}"

    async def get_variable_at_point(
        self,
        variable: str,
        lat: float,
        lon: float,
        time_req: datetime,
        level: Optional[float] = None,
        model: Optional[str] = None,
        ensemble: Optional[str] = None,
    ) -> Tuple[float, GridPoint, str, Provenance]:
        """
        Extract a single scalar value from the Zarr store.
        Returns: (value, resolved_grid_point, resolved_model_name, provenance)

        Raises ValueError if the variable does not exist in the store.
        Raises TimeoutError if the Dask query exceeds the configured timeout.
        """
        store_url = self._resolve_store_url(variable, model, level)
        ds = await self._open_store(store_url)

        if variable not in ds.data_vars and variable not in ds.coords:
            # Try CMIP7 name mapping
            cmip7_var = CF_TO_CMIP7.get(variable)
            if cmip7_var and cmip7_var in ds.data_vars:
                zarr_var = cmip7_var
            else:
                available = list(ds.data_vars)
                raise ValueError(
                    f"Variable '{variable}' not found in store. "
                    f"Available: {available[:10]}"
                )
        else:
            zarr_var = variable

        # Build selection kwargs
        sel_kwargs: Dict[str, Any] = {
            "latitude": lat,
            "longitude": lon,
            "time": time_req,
        }
        method_kwargs: Dict[str, str] = {
            "latitude": "nearest",
            "longitude": "nearest",
            "time": "nearest",
        }

        if level is not None and "level" in ds.coords:
            sel_kwargs["level"] = level
            method_kwargs["level"] = "nearest"

        if ensemble is not None and "member_id" in ds.coords:
            sel_kwargs["member_id"] = ensemble

        # Execute lazy selection (no data moved yet)
        da = ds[zarr_var].sel(**sel_kwargs, method=method_kwargs if len(method_kwargs) > 1 else None)

        # Compute with Dask timeout
        t0 = time.perf_counter()
        try:
            value_arr = await asyncio.wait_for(
                asyncio.get_event_loop().run_in_executor(None, da.compute),
                timeout=settings.dask_query_timeout,
            )
        except asyncio.TimeoutError:
            raise TimeoutError(
                f"Query timed out after {settings.dask_query_timeout}s. "
                "Try a coarser spatial resolution or shorter time range."
            )

        elapsed_ms = int((time.perf_counter() - t0) * 1000)
        log.info("zarr_point_query", var=variable, lat=lat, lon=lon, elapsed_ms=elapsed_ms)

        value = float(value_arr.values)
        if np.isnan(value):
            raise ValueError(f"No data at ({lat}, {lon}) for '{variable}' at requested time — NaN returned by store.")

        # Resolve actual grid coordinates
        actual_lat = float(da.coords["latitude"])
        actual_lon = float(da.coords["longitude"])
        grid_point = GridPoint(
            lat=actual_lat,
            lon=actual_lon,
            grid_spacing_deg=self._estimate_grid_spacing(ds),
            source_grid=ds.attrs.get("grid_label", "unknown"),
        )

        resolved_model = model or ds.attrs.get("source_id", "ERA5")

        # Build provenance from dataset attributes
        provenance = self._build_provenance(ds, variable)

        return value, grid_point, resolved_model, provenance

    async def get_timeseries(
        self,
        variable: str,
        lat: float,
        lon: float,
        start: datetime,
        end: datetime,
        level: Optional[float] = None,
        model: Optional[str] = None,
    ) -> Tuple[xr.DataArray, GridPoint, str, Provenance]:
        """
        Extract a time series. Returns the raw DataArray for the router to
        post-process (aggregation, uncertainty attachment) before serialisation.
        Maximum: 10,000 timesteps enforced in the router.
        """
        store_url = self._resolve_store_url(variable, model, level)
        ds = await self._open_store(store_url)

        zarr_var = variable if variable in ds.data_vars else CF_TO_CMIP7.get(variable, variable)
        if zarr_var not in ds.data_vars:
            raise ValueError(f"Variable '{variable}' not available in {store_url}")

        da = ds[zarr_var].sel(
            latitude=lat, longitude=lon, method="nearest"
        ).sel(time=slice(start, end))

        if level is not None and "level" in ds.coords:
            da = da.sel(level=level, method="nearest")

        # Compute — this pulls multiple chunks
        t0 = time.perf_counter()
        computed = await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(None, da.compute),
            timeout=settings.dask_query_timeout,
        )
        elapsed_ms = int((time.perf_counter() - t0) * 1000)
        log.info("zarr_timeseries_query", var=variable, n_timesteps=len(computed.time), elapsed_ms=elapsed_ms)

        grid_point = GridPoint(
            lat=float(da.coords["latitude"]),
            lon=float(da.coords["longitude"]),
            grid_spacing_deg=self._estimate_grid_spacing(ds),
            source_grid=ds.attrs.get("grid_label", "unknown"),
        )
        resolved_model = model or ds.attrs.get("source_id", "ERA5")
        provenance = self._build_provenance(ds, variable)

        return computed, grid_point, resolved_model, provenance

    def _estimate_grid_spacing(self, ds: xr.Dataset) -> float:
        """Estimate grid spacing from coordinate arrays."""
        try:
            lats = ds.coords["latitude"].values
            return abs(float(lats[1]) - float(lats[0]))
        except (KeyError, IndexError):
            return 0.0

    def _build_provenance(self, ds: xr.Dataset, variable: str) -> Provenance:
        """Build a Provenance object from Zarr dataset global attributes."""
        attrs = ds.attrs
        return Provenance(
            dataset_id=attrs.get("tracking_id", attrs.get("dataset_id", "unknown")),
            source=attrs.get("source_id", attrs.get("institution_id", "unknown")),
            raw_hash=attrs.get("pcmip_raw_hash", "not-available"),
            ingest_timestamp=datetime.fromisoformat(
                attrs.get("pcmip_ingest_ts", "2000-01-01T00:00:00")
            ),
            schema_version=attrs.get("pcmip_schema_version", "unknown"),
            cmip_standard=attrs.get("mip_era", "CMIP7"),
            cf_version=attrs.get("Conventions", "CF-1.10"),
            bias_corrected=attrs.get("pcmip_bias_corrected", False),
            bias_correction_version=attrs.get("pcmip_bias_correction_version"),
            quality_flags=[QualityFlag.VALID],
            lineage_url=(
                f"{settings.marquez_url}/api/v1/namespaces/{settings.lineage_namespace}"
                f"/datasets/{attrs.get('tracking_id', '')}"
            ),
        )

    def get_unit(self, variable: str, ds: Optional[xr.Dataset] = None) -> str:
        """Return unit string for a variable."""
        if ds is not None:
            zarr_var = CF_TO_CMIP7.get(variable, variable)
            if zarr_var in (ds.data_vars or {}):
                unit = ds[zarr_var].attrs.get("units", "")
                if unit:
                    return unit
        return CF_UNITS.get(variable, "1")  # "1" = dimensionless per CF convention
