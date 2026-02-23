"""
api/main.py
FastAPI application factory. Middleware, startup, shutdown, exception handlers.
Do not put business logic here — this file wires the application together.
"""
from __future__ import annotations

import logging
import time
import uuid
from contextlib import asynccontextmanager
from typing import AsyncGenerator

# optional dependencies stubbed for lightweight demo
try:
    import redis.asyncio as aioredis
except ImportError:
    aioredis = None
import structlog
try:
    from dask.distributed import Client as DaskClient
except ImportError:
    DaskClient = None
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
# OpenLineage client used for provenance; optional for demo
try:
    from openlineage.client import OpenLineageClient
except ImportError:
    OpenLineageClient = None
# opentelemetry imports removed for lightweight demo
trace = None
OTLPSpanExporter = None
FastAPIInstrumentor = None
TracerProvider = None
BatchSpanProcessor = None

from config.settings import get_settings
from api.models.schemas import PCMIPError
from api.routers import climate, ensemble, models, lineage, status

settings = get_settings()

# ─── STRUCTURED LOGGING ───────────────────────────────────────────────────────
structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    logger_factory=structlog.PrintLoggerFactory(),
)
log = structlog.get_logger()


# ─── OPENTELEMETRY SETUP ──────────────────────────────────────────────────────
def setup_tracing() -> None:
    # no-op tracing for demo
    pass


# ─── APPLICATION STATE (shared across requests via app.state) ─────────────────
class AppState:
    # if dependencies are missing, these remain None
    dask_client: DaskClient | None = None
    redis: aioredis.Redis | None = None
    lineage_client: OpenLineageClient | None = None


app_state = AppState()


# ─── LIFESPAN ─────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Startup: establish connections to Dask, Redis, OpenLineage.
    Shutdown: gracefully close all connections.
    Connections are held on app.state so routers can access via dependency injection.
    """
    log.info("pcmip_api.startup", version=settings.app_version)

    # Tracing (disabled in demo)
    # setup_tracing()

    # Dask & Redis connections are optional for demo; skip if packages absent
    if DaskClient:
        log.info("connecting_to_dask", scheduler=settings.dask_scheduler)
        app_state.dask_client = await DaskClient(
            settings.dask_scheduler,
            asynchronous=True,
            name="pcmip-api",
            timeout=30,
        )
        log.info("dask_connected", workers=len(app_state.dask_client.scheduler_info()["workers"]))
    else:
        log.info("dask_not_available")

    if aioredis:
        log.info("connecting_to_redis", url=settings.redis_url)
        app_state.redis = await aioredis.from_url(
            settings.redis_url,
            encoding="utf-8",
            decode_responses=True,
            max_connections=50,
        )
        await app_state.redis.ping()
        log.info("redis_connected")
    else:
        log.info("redis_not_available")

    # OpenLineage (optional)
    if settings.lineage_enabled and OpenLineageClient:
        app_state.lineage_client = OpenLineageClient(url=settings.marquez_url)
        log.info("lineage_client_connected", marquez=settings.marquez_url)
    elif settings.lineage_enabled:
        log.info("openlineage_not_installed")

    log.info("pcmip_api.ready")
    yield  # Application runs here

    # Shutdown
    log.info("pcmip_api.shutdown")
    if app_state.dask_client:
        await app_state.dask_client.close()
    if app_state.redis:
        await app_state.redis.aclose()
    log.info("pcmip_api.shutdown_complete")


# ─── APPLICATION FACTORY ──────────────────────────────────────────────────────
def create_app() -> FastAPI:
    app = FastAPI(
        title="PCMIP API",
        version=settings.app_version,
        description=(
            "Planetary Climate Modeling Infrastructure Platform — "
            "versioned, uncertainty-aware, provenance-tagged climate data API."
        ),
        openapi_url="/openapi.json",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan,
    )

    # ── CORS ──────────────────────────────────────────────────────────────────
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origin_list,
        allow_credentials=True,
        allow_methods=["GET", "POST"],
        allow_headers=["*"],
    )

    # ── REQUEST ID MIDDLEWARE ─────────────────────────────────────────────────
    @app.middleware("http")
    async def request_id_middleware(request: Request, call_next):
        request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
        structlog.contextvars.bind_contextvars(request_id=request_id)
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        return response

    # ── REQUEST TIMING MIDDLEWARE ─────────────────────────────────────────────
    @app.middleware("http")
    async def timing_middleware(request: Request, call_next):
        start = time.perf_counter()
        response = await call_next(request)
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        response.headers["X-Response-Time-Ms"] = str(elapsed_ms)
        log.info(
            "http_request",
            method=request.method,
            path=request.url.path,
            status=response.status_code,
            duration_ms=elapsed_ms,
        )
        return response

    # ── EXCEPTION HANDLERS ────────────────────────────────────────────────────
    @app.exception_handler(ValueError)
    async def value_error_handler(request: Request, exc: ValueError) -> JSONResponse:
        return JSONResponse(
            status_code=400,
            content=PCMIPError(
                error_code="INVALID_REQUEST",
                message=str(exc),
                docs_url="https://docs.pcmip.earth/api/errors",
            ).model_dump(mode="json"),
        )

    @app.exception_handler(TimeoutError)
    async def timeout_handler(request: Request, exc: TimeoutError) -> JSONResponse:
        return JSONResponse(
            status_code=504,
            content=PCMIPError(
                error_code="QUERY_TIMEOUT",
                message="The query exceeded the maximum allowed execution time.",
                detail="Try a smaller spatial region, shorter time range, or use the async export endpoint.",
                docs_url="https://docs.pcmip.earth/api/async-export",
            ).model_dump(mode="json"),
        )

    @app.exception_handler(Exception)
    async def generic_handler(request: Request, exc: Exception) -> JSONResponse:
        log.error("unhandled_exception", exc_type=type(exc).__name__, exc_msg=str(exc))
        return JSONResponse(
            status_code=500,
            content=PCMIPError(
                error_code="INTERNAL_ERROR",
                message="An unexpected error occurred.",
                detail="This has been logged with the request ID for investigation.",
            ).model_dump(mode="json"),
        )

    # ── HEALTH ENDPOINTS ──────────────────────────────────────────────────────
    @app.get("/health", tags=["Infrastructure"])
    async def health() -> dict:
        """Shallow health check — used by load balancer."""
        return {"status": "ok", "version": settings.app_version}

    @app.get("/health/deep", tags=["Infrastructure"])
    async def deep_health() -> dict:
        """Deep health check — verifies all upstream dependencies."""
        checks: dict = {}

        # Dask
        try:
            info = app_state.dask_client.scheduler_info()
            checks["dask"] = {
                "status": "ok",
                "workers": len(info["workers"]),
            }
        except Exception as e:
            checks["dask"] = {"status": "degraded", "error": str(e)}

        # Redis
        try:
            await app_state.redis.ping()
            checks["redis"] = {"status": "ok"}
        except Exception as e:
            checks["redis"] = {"status": "degraded", "error": str(e)}

        overall = "ok" if all(c["status"] == "ok" for c in checks.values()) else "degraded"
        return {"status": overall, "checks": checks, "version": settings.app_version}

    # ── ROUTERS ───────────────────────────────────────────────────────────────
    app.include_router(climate.router, prefix="/v2/climate", tags=["Climate Data"])
    app.include_router(ensemble.router, prefix="/v2/ensemble", tags=["Ensemble"])
    app.include_router(models.router, prefix="/v2/models", tags=["Model Validation"])
    app.include_router(lineage.router, prefix="/v2/lineage", tags=["Lineage"])
    # temporary dashboard paths used by front-end
    app.include_router(dashboard.router, prefix="/api", tags=["Dashboard"])

    # ── OTEL INSTRUMENTATION ─────────────────────────────────────────────────
    # FastAPIInstrumentor.instrument_app(app)  # disabled

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=8000,
        workers=settings.workers,
        log_config=None,  # use structlog
        access_log=False,
    )
