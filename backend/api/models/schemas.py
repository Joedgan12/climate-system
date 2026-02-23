"""
api/models/schemas.py
Pydantic v2 schemas for all request parameters and response bodies.
Every numeric response carries uncertainty. Every response carries provenance.
These are the API contracts — changing them requires a major version bump.
"""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Annotated, Any, Dict, List, Optional, Union
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator, model_validator


# ─── ENUMS ────────────────────────────────────────────────────────────────────

class QualityFlag(str, Enum):
    VALID = "QF_VALID"
    SCHEMA_WARN = "QF_SCHEMA_WARN"
    PHYSICS_WARN = "QF_PHYSICS_WARN"
    BIAS_CORRECTED = "QF_BIAS_CORRECTED"
    INTERPOLATED = "QF_INTERPOLATED"
    NEAR_BOUNDARY = "QF_NEAR_BOUNDARY"

class ValidationStatus(str, Enum):
    CERTIFIED = "CERTIFIED"
    CONDITIONAL = "CONDITIONAL"
    UNDER_REVIEW = "UNDER_REVIEW"
    SUSPENDED = "SUSPENDED"
    FAILED = "FAILED"

class TierType(str, Enum):
    HOT = "HOT"
    WARM = "WARM"
    COLD = "COLD"

class UncertaintyMethod(str, Enum):
    ENSEMBLE_PERCENTILE = "ensemble-percentile"
    MONTE_CARLO_DROPOUT = "monte-carlo-dropout"
    CONFORMAL_PREDICTION = "conformal-prediction"
    DEEP_ENSEMBLE = "deep-ensemble"
    NOT_AVAILABLE = "not-available"

class AggregateType(str, Enum):
    NONE = "none"
    HOURLY = "hourly"
    DAILY = "daily"
    MONTHLY = "monthly"
    ANNUAL = "annual"

class ResponseFormat(str, Enum):
    JSON = "json"
    ZARR = "zarr"  # returns presigned URL to zarr store


# ─── SHARED MODELS ────────────────────────────────────────────────────────────

class Uncertainty(BaseModel):
    """Uncertainty quantification. Mandatory on all numeric responses."""
    method: UncertaintyMethod
    p05: Optional[float] = Field(None, description="5th percentile")
    p25: Optional[float] = Field(None, description="25th percentile")
    p50: Optional[float] = Field(None, description="Median")
    p75: Optional[float] = Field(None, description="75th percentile")
    p95: Optional[float] = Field(None, description="95th percentile")
    ensemble_size: Optional[int] = Field(None, ge=1)
    calibrated: Optional[bool] = Field(None, description="Whether bounds are statistically calibrated")

    @model_validator(mode="after")
    def check_percentile_order(self) -> "Uncertainty":
        vals = [v for v in [self.p05, self.p25, self.p50, self.p75, self.p95] if v is not None]
        if vals != sorted(vals):
            raise ValueError("Percentiles must be monotonically increasing")
        return self


class Provenance(BaseModel):
    """Lineage metadata. Every API response must include this."""
    dataset_id: str = Field(..., description="UUID of source dataset in STAC catalog")
    source: str = Field(..., description="Human-readable source identifier")
    raw_hash: str = Field(..., description="SHA-256 of the original raw record")
    ingest_timestamp: datetime
    schema_version: str
    cmip_standard: str = "CMIP7"
    cf_version: str = "CF-1.10"
    bias_corrected: bool = False
    bias_correction_version: Optional[str] = None
    quality_flags: List[QualityFlag] = Field(default_factory=list)
    lineage_url: Optional[str] = Field(None, description="URL to full OpenLineage graph")


class GridPoint(BaseModel):
    """Resolved grid point (may differ from requested lat/lon due to nearest-cell selection)."""
    lat: float = Field(..., ge=-90, le=90)
    lon: float = Field(..., ge=-180, le=180)
    grid_spacing_deg: float
    source_grid: str  # e.g. "N320 reduced Gaussian"


# ─── REQUEST SCHEMAS ──────────────────────────────────────────────────────────

class VariableRequest(BaseModel):
    lat: float = Field(..., ge=-90, le=90, description="Latitude of query point")
    lon: float = Field(..., ge=-180, le=180, description="Longitude of query point")
    variable: str = Field(..., description="CF-1.10 standard name, e.g. air_temperature")
    time: datetime = Field(..., description="ISO8601 timestamp; nearest timestep selected")
    level: Optional[Union[float, str]] = Field(None, description="Pressure level hPa, or 'surface'")
    model: Optional[str] = Field(None, description="Model identifier; default: best-available obs product")
    ensemble: Optional[str] = Field(None, description="Ensemble member, e.g. r1i1p1f1; default: mean")
    format: ResponseFormat = ResponseFormat.JSON

    @field_validator("variable")
    @classmethod
    def variable_must_be_cf_name(cls, v: str) -> str:
        # Basic structural check — full validation against CF table happens in the service
        if not v.replace("_", "").isalpha():
            raise ValueError(f"variable '{v}' does not look like a CF standard name")
        return v.lower()

    @field_validator("level")
    @classmethod
    def validate_level(cls, v: Any) -> Any:
        if v is None or v == "surface":
            return v
        if isinstance(v, (int, float)) and 1.0 <= float(v) <= 1100.0:
            return float(v)
        raise ValueError("level must be 'surface' or a pressure value in hPa [1, 1100]")


class TimeseriesRequest(VariableRequest):
    start: datetime
    end: datetime
    aggregate: AggregateType = AggregateType.NONE
    max_timesteps: int = Field(10_000, ge=1, le=10_000)

    @model_validator(mode="after")
    def check_time_range(self) -> "TimeseriesRequest":
        if self.end <= self.start:
            raise ValueError("end must be after start")
        return self


class EnsembleStatsRequest(BaseModel):
    dataset: str = Field(..., description="CMIP7 MIP table, e.g. CMIP7-ScenarioMIP")
    scenario: str = Field(..., description="SSP scenario, e.g. ssp245")
    variable: str = Field(..., description="CMIP7 variable name, e.g. tasmax")
    region: str = Field(..., description="ISO 3166-1 alpha-3 or bounding box 'lat1,lon1,lat2,lon2'")
    horizon: str = Field(..., description="Year range, e.g. '2050-2100'")
    baseline: str = Field("1981-2010", description="Baseline period for anomaly calculation")

    @field_validator("horizon", "baseline")
    @classmethod
    def validate_year_range(cls, v: str) -> str:
        parts = v.split("-")
        if len(parts) != 2 or not all(p.isdigit() and 1850 <= int(p) <= 2300 for p in parts):
            raise ValueError(f"'{v}' must be YYYY-YYYY with years in [1850, 2300]")
        if int(parts[0]) >= int(parts[1]):
            raise ValueError("Start year must be before end year")
        return v


class ModelValidationRequest(BaseModel):
    ai_model: str = Field(..., description="Model identifier in PCMIP registry")
    physics_baseline: str = Field(..., description="Physics model to compare against")
    variable: str = Field(..., description="Variable to validate, e.g. z500")
    region: str = Field(default="GLOBAL")
    lead_times: List[int] = Field(
        default=[24, 72, 120, 240],
        description="Forecast lead times in hours to evaluate"
    )
    period: str = Field("2020-2024", description="Evaluation period")

    @field_validator("lead_times")
    @classmethod
    def validate_lead_times(cls, v: List[int]) -> List[int]:
        if not all(1 <= lt <= 720 for lt in v):
            raise ValueError("Lead times must be between 1 and 720 hours")
        return sorted(v)


# ─── RESPONSE SCHEMAS ─────────────────────────────────────────────────────────

class VariableResponse(BaseModel):
    """Response from /v2/climate/variable. Must include uncertainty and provenance."""
    variable: str
    cf_name: str
    cmip7_var: Optional[str] = None
    value: float
    unit: str
    grid_point: GridPoint
    time_actual: datetime = Field(..., description="Actual timestep returned (nearest to requested)")
    level: Optional[float] = None
    model: str
    ensemble: Optional[str] = None
    uncertainty: Uncertainty
    provenance: Provenance
    warnings: List[str] = Field(default_factory=list)
    request_id: UUID = Field(default_factory=uuid4)
    response_time_ms: int


class TimeseriesPoint(BaseModel):
    time: datetime
    value: float
    uncertainty: Uncertainty
    quality_flags: List[QualityFlag] = Field(default_factory=list)


class TimeseriesResponse(BaseModel):
    variable: str
    cf_name: str
    unit: str
    grid_point: GridPoint
    level: Optional[float] = None
    model: str
    aggregate: AggregateType
    timestep_count: int
    data: List[TimeseriesPoint]
    provenance: Provenance
    warnings: List[str] = Field(default_factory=list)
    request_id: UUID = Field(default_factory=uuid4)
    response_time_ms: int


class EnsembleStatsResponse(BaseModel):
    dataset: str
    scenario: str
    variable: str
    unit: str
    region: str
    horizon: str
    baseline: str
    ensemble_size: int
    mean_change: float
    median_change: float
    p10: float
    p90: float
    models_agreeing_pct: float = Field(..., description="% of models with same sign change")
    physically_consistent_pct: float
    warnings: List[str] = Field(default_factory=list)
    request_id: UUID = Field(default_factory=uuid4)
    response_time_ms: int


class PhysicsCheckResult(BaseModel):
    check_name: str
    passed: bool
    value: Optional[float] = None
    threshold: Optional[float] = None
    message: str


class ValidationJobResponse(BaseModel):
    """Initial response from POST /v2/models/validate — async job."""
    job_id: UUID
    ai_model: str
    physics_baseline: str
    status: str = "QUEUED"
    estimated_completion_minutes: int
    status_url: str


class ValidationResultResponse(BaseModel):
    """Full result from GET /v2/models/validate/{job_id}."""
    job_id: UUID
    ai_model: str
    physics_baseline: str
    status: ValidationStatus
    variable: str
    region: str
    period: str
    # Statistical metrics per lead time
    rmse_by_lead: Dict[str, float]  # {"24h": 119.4, "72h": 142.1}
    bias_by_lead: Dict[str, float]
    acc_by_lead: Dict[str, float]  # Anomaly Correlation Coefficient
    # Physics checks
    physics_checks: List[PhysicsCheckResult]
    physical_consistency_pct: float
    conservation_error_wm2: Optional[float] = None
    # Drift
    drift_detected: bool
    drift_rate_pct_per_week: Optional[float] = None
    # Overall
    safe_range: str
    recommendation: str
    warnings: List[str] = Field(default_factory=list)
    validated_at: datetime
    next_validation_due: datetime


class LineageNode(BaseModel):
    node_id: str
    node_type: str  # "observation", "job_run", "dataset", "api_response"
    label: str
    timestamp: datetime
    source: Optional[str] = None
    hash: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class LineageEdge(BaseModel):
    from_node: str
    to_node: str
    relationship: str  # "derived_from", "validated_against", "produced_by"


class LineageResponse(BaseModel):
    dataset_id: str
    nodes: List[LineageNode]
    edges: List[LineageEdge]
    format: str = "OpenLineage-1.0"
    query_time_ms: int


# ─── ERROR SCHEMAS ────────────────────────────────────────────────────────────

class PCMIPError(BaseModel):
    error_code: str
    message: str
    detail: Optional[str] = None
    request_id: UUID = Field(default_factory=uuid4)
    docs_url: Optional[str] = None


class ValidationError(PCMIPError):
    field: Optional[str] = None
    received_value: Optional[Any] = None
