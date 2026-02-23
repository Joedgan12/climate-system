"""
config/settings.py
Centralised configuration using pydantic-settings.
All values come from environment variables; this file documents every required setting.
"""
from functools import lru_cache
from typing import List
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # ── Application ───────────────────────────────────────────────────────────
    app_name: str = "PCMIP API"
    app_version: str = "2.0.0"
    debug: bool = False
    log_level: str = "INFO"
    workers: int = 4

    # ── Kafka ─────────────────────────────────────────────────────────────────
    kafka_brokers: str = Field(..., description="Comma-separated broker list")
    kafka_raw_topic: str = "raw.ingest"
    kafka_validated_topic: str = "validated.records"
    kafka_dead_letter_topic: str = "dead.letter"
    kafka_consumer_group: str = "pcmip-api"
    kafka_schema_registry_url: str = "http://schema-registry:8081"

    @property
    def kafka_broker_list(self) -> List[str]:
        return [b.strip() for b in self.kafka_brokers.split(",")]

    # ── Storage ───────────────────────────────────────────────────────────────
    zarr_store_url: str = Field(..., description="s3://bucket or gs://bucket")
    zarr_obs_prefix: str = "zarr/obs"
    zarr_models_prefix: str = "zarr/models"
    zarr_derived_prefix: str = "zarr/derived"
    zarr_raw_prefix: str = "raw"

    aws_access_key_id: str = ""
    aws_secret_access_key: str = ""
    aws_region: str = "eu-west-1"
    aws_endpoint_url: str = ""  # for MinIO / local testing

    # ── Redis ─────────────────────────────────────────────────────────────────
    redis_url: str = "redis://localhost:6379/0"
    cache_ttl_variable: int = 300          # seconds, single variable query
    cache_ttl_timeseries: int = 1800       # seconds, timeseries
    cache_ttl_ensemble: int = 3600         # seconds, ensemble stats
    rate_limit_research: int = 100         # requests/hour
    rate_limit_institutional: int = 10000  # requests/hour
    rate_limit_sovereign: int = 0          # unlimited

    # ── Dask ──────────────────────────────────────────────────────────────────
    dask_scheduler: str = "tcp://dask-scheduler:8786"
    dask_max_workers: int = 4096
    dask_min_workers: int = 64
    dask_query_timeout: int = 300          # seconds

    # ── Authentication ────────────────────────────────────────────────────────
    api_key_salt: str = Field(..., description="HMAC salt for API key hashing")
    api_key_header: str = "X-API-Key"
    jwt_secret: str = Field(default="", description="For sovereign mTLS sessions")

    # ── OpenLineage / Marquez ─────────────────────────────────────────────────
    marquez_url: str = "http://marquez:5000"
    lineage_enabled: bool = True
    lineage_namespace: str = "pcmip-production"

    # ── SLURM ─────────────────────────────────────────────────────────────────
    slurm_host: str = "hpc-login.pcmip.internal"
    slurm_user: str = "pcmip-scheduler"
    slurm_key_path: str = "/secrets/slurm_ed25519"
    lustre_root: str = "/lustre/pcmip"

    # ── Validation service ────────────────────────────────────────────────────
    validation_era5_ref_store: str = "s3://pcmip-archive/zarr/obs/era5/pressure-levels"
    validation_max_conservation_error: float = 0.5   # W/m2
    validation_drift_threshold_pct: float = 5.0      # % RMSE degradation/week
    validation_min_physical_consistency: float = 90.0 # %

    # ── Physical plausibility bounds (REJECT thresholds) ─────────────────────
    physics_t_min: float = 150.0    # Kelvin
    physics_t_max: float = 340.0    # Kelvin
    physics_sst_min: float = 271.0  # Kelvin (ice point)
    physics_q_min: float = 0.0      # kg/kg
    physics_q_max: float = 0.04     # kg/kg
    physics_precip_min: float = 0.0 # mm/h
    physics_precip_max: float = 500.0
    physics_wind_warn: float = 80.0  # m/s — FLAG
    physics_wind_reject: float = 120.0  # m/s — REJECT

    # ── CORS ──────────────────────────────────────────────────────────────────
    cors_origins: str = "https://pcmip.earth,https://dashboard.pcmip.earth"

    @property
    def cors_origin_list(self) -> List[str]:
        return [o.strip() for o in self.cors_origins.split(",")]

    @property
    def s3_storage_options(self) -> dict:
        opts: dict = {"key": self.aws_access_key_id, "secret": self.aws_secret_access_key}
        if self.aws_endpoint_url:
            opts["client_kwargs"] = {"endpoint_url": self.aws_endpoint_url}
        return opts


@lru_cache()
def get_settings() -> Settings:
    """Return cached settings singleton. Cache invalidated between test runs."""
    return Settings()
