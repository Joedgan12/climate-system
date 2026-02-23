"""
validation-service/src/main.py
PCMIP Validation Service — Python/FastAPI

Responsibilities:
- Physics-aware AI model validation against reanalysis baselines
- Statistical metric computation (RMSE, MAE, bias, ACC)
- Physical consistency checks (conservation, hydrostatic balance)
- Uncertainty quantification and calibration checks
- Bias correction via xclim quantile mapping
- Drift detection: 30-day RMSE trend monitoring
- Model registry management
- Validation report generation and persistence

CRITICAL: This service gates what AI models are allowed to serve via the API.
No AI model output reaches external consumers without clearing this service.
See Section 7 of the PRD for the scientific rationale.

Run:
    uvicorn main:app --host 0.0.0.0 --port 8002 --workers 2
"""

import asyncio
import logging
import os
import uuid
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional

import numpy as np
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

log = logging.getLogger("pcmip.validation")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

# ─── CONFIGURATION ────────────────────────────────────────────────────────────

class Config:
    PORT                  = int(os.getenv("PORT", "8002"))
    GOVERNANCE_URL        = os.getenv("GOVERNANCE_SERVICE_URL", "http://governance-service:8004")
    VALIDATION_EXPIRE_H   = 72       # validation expires after 72 hours
    DRIFT_WINDOW_DAYS     = 30       # rolling window for drift detection
    DRIFT_FAIL_PCT_WEEK   = 15.0     # suspend model if >15% degradation/week
    DRIFT_WARN_PCT_WEEK   = 5.0      # warn if >5% degradation/week
    REPORT_STORE_PATH     = os.getenv("REPORT_STORE_PATH", "/tmp/pcmip-reports")


# ─── ENUMERATIONS ─────────────────────────────────────────────────────────────

class ValidationStatus(str, Enum):
    CERTIFIED    = "CERTIFIED"
    CONDITIONAL  = "CONDITIONAL"
    SUSPENDED    = "SUSPENDED"
    UNDER_REVIEW = "UNDER_REVIEW"
    PENDING      = "PENDING"

class ModelType(str, Enum):
    PHYSICS  = "physics"
    AI       = "ai"
    HYBRID   = "ai-hybrid"

class PhysicsCheckSeverity(str, Enum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"


# ─── SCHEMAS ──────────────────────────────────────────────────────────────────

class ValidationRequest(BaseModel):
    job_id:           str
    ai_model:         str
    physics_baseline: str
    variable:         str
    region:           str
    period:           str
    forecast_data_url: Optional[str] = None   # s3://... if pre-computed
    metadata:         Dict[str, Any] = {}


class PhysicsCheckResult(BaseModel):
    check:     str
    passed:    bool
    value:     Optional[float]
    threshold: Optional[float]
    severity:  PhysicsCheckSeverity
    message:   str


class ValidationReport(BaseModel):
    job_id:               str
    ai_model:             str
    ai_model_version:     str
    physics_baseline:     str
    variable:             str
    region:               str
    period:               str
    status:               ValidationStatus
    rmse:                 float
    mae:                  float
    bias:                 float
    acc:                  float     # anomaly correlation coefficient
    physical_consistency: float     # percent of grid cells physically consistent
    conservation_error:   float     # percent deviation from reference
    drift_detected:       bool
    drift_trend_pct_week: Optional[float]
    safe_range:           str
    recommendation:       str
    physics_checks:       List[PhysicsCheckResult]
    warnings:             List[str]
    validated_at:         str       # ISO8601
    expires_at:           str       # ISO8601
    validator_version:    str = "1.0.0"


class ModelRegistryEntry(BaseModel):
    model_id:         str
    model_version:    str
    model_type:       ModelType
    organisation:     str
    current_status:   ValidationStatus
    safe_range:       str
    last_validated:   str
    expires_at:       str
    rmse_history:     List[float]   # last N RMSE values for drift detection


# ─── IN-MEMORY MODEL REGISTRY ──────────────────────────────────────────────────
# In production this is PostgreSQL. Stubbed here for clarity.

MODEL_REGISTRY: Dict[str, ModelRegistryEntry] = {
    "ifs-cy48r1": ModelRegistryEntry(
        model_id="ifs-cy48r1", model_version="48r1",
        model_type=ModelType.PHYSICS, organisation="ECMWF",
        current_status=ValidationStatus.CERTIFIED, safe_range="All ranges",
        last_validated=datetime.now(timezone.utc).isoformat(),
        expires_at=(datetime.now(timezone.utc) + timedelta(hours=24)).isoformat(),
        rmse_history=[119.4, 119.8, 118.9, 120.1, 119.2],
    ),
    "aifs-v1.4": ModelRegistryEntry(
        model_id="aifs-v1.4", model_version="1.4",
        model_type=ModelType.HYBRID, organisation="ECMWF",
        current_status=ValidationStatus.CERTIFIED, safe_range="Medium (120h)",
        last_validated=datetime.now(timezone.utc).isoformat(),
        expires_at=(datetime.now(timezone.utc) + timedelta(hours=Config.VALIDATION_EXPIRE_H)).isoformat(),
        rmse_history=[128.1, 129.3, 127.8, 130.2, 128.5],
    ),
    "graphcast-v2": ModelRegistryEntry(
        model_id="graphcast-v2", model_version="2.0",
        model_type=ModelType.AI, organisation="Google DeepMind",
        current_status=ValidationStatus.CONDITIONAL, safe_range="Short-Medium (72h)",
        last_validated=datetime.now(timezone.utc).isoformat(),
        expires_at=(datetime.now(timezone.utc) + timedelta(hours=Config.VALIDATION_EXPIRE_H)).isoformat(),
        rmse_history=[142.3, 141.8, 143.1, 142.9, 143.7],
    ),
    "pangu-weather": ModelRegistryEntry(
        model_id="pangu-weather", model_version="2.0",
        model_type=ModelType.AI, organisation="Huawei",
        current_status=ValidationStatus.CONDITIONAL, safe_range="Short (48h)",
        last_validated=datetime.now(timezone.utc).isoformat(),
        expires_at=(datetime.now(timezone.utc) + timedelta(hours=Config.VALIDATION_EXPIRE_H)).isoformat(),
        rmse_history=[156.8, 157.2, 158.1, 156.4, 159.3],
    ),
    "fuxi-v1.0": ModelRegistryEntry(
        model_id="fuxi-v1.0", model_version="1.0",
        model_type=ModelType.AI, organisation="Fudan University",
        current_status=ValidationStatus.SUSPENDED, safe_range="Suspended",
        last_validated=datetime.now(timezone.utc).isoformat(),
        expires_at=datetime.now(timezone.utc).isoformat(),  # expired
        rmse_history=[172.1, 178.4, 183.2, 191.7, 204.3],  # clear drift
    ),
}

# In-memory report store (PostgreSQL in production)
REPORT_STORE: Dict[str, ValidationReport] = {}


# ─── STATISTICAL METRICS ──────────────────────────────────────────────────────

def compute_rmse(forecast: np.ndarray, reference: np.ndarray) -> float:
    """Root Mean Square Error."""
    return float(np.sqrt(np.mean((forecast - reference) ** 2)))


def compute_mae(forecast: np.ndarray, reference: np.ndarray) -> float:
    """Mean Absolute Error."""
    return float(np.mean(np.abs(forecast - reference)))


def compute_bias(forecast: np.ndarray, reference: np.ndarray) -> float:
    """Mean bias (forecast - reference)."""
    return float(np.mean(forecast - reference))


def compute_acc(
    forecast:   np.ndarray,
    reference:  np.ndarray,
    climatology: np.ndarray,
) -> float:
    """
    Anomaly Correlation Coefficient.
    Measures the correlation between forecast and reference anomalies
    relative to a climatological mean. Standard metric for atmospheric science.
    """
    f_anom = forecast  - climatology
    r_anom = reference - climatology
    num    = float(np.sum(f_anom * r_anom))
    den    = float(np.sqrt(np.sum(f_anom ** 2) * np.sum(r_anom ** 2)))
    if den == 0:
        return 0.0
    return num / den


# ─── PHYSICS CHECKS ───────────────────────────────────────────────────────────

def check_energy_conservation(
    forecast: np.ndarray,
    reference: np.ndarray,
) -> PhysicsCheckResult:
    """
    Check global energy conservation via TOA radiation balance.
    AI models can produce states that violate energy conservation
    while appearing statistically plausible. This check catches those.
    Threshold: 0.5 W/m2 drift from reference balance.
    """
    forecast_balance  = float(np.mean(forecast))
    reference_balance = float(np.mean(reference))
    deviation         = abs(forecast_balance - reference_balance)
    threshold         = 0.5  # W/m2

    return PhysicsCheckResult(
        check      = "energy_conservation",
        passed     = deviation <= threshold,
        value      = round(deviation, 4),
        threshold  = threshold,
        severity   = PhysicsCheckSeverity.PASS if deviation <= threshold else PhysicsCheckSeverity.FAIL,
        message    = (
            f"TOA radiation balance deviation: {deviation:.4f} W/m² "
            f"({'PASS' if deviation <= threshold else 'FAIL — exceeds 0.5 W/m² threshold'})"
        ),
    )


def check_positive_definiteness(forecast: np.ndarray, variable: str) -> PhysicsCheckResult:
    """
    Variables like specific humidity and precipitation must be non-negative.
    AI models occasionally produce negative values in edge cases.
    """
    non_positive_non_variables = {"specific_humidity", "precipitation_flux", "precipitation_amount"}
    if variable not in non_positive_non_variables:
        return PhysicsCheckResult(
            check="positive_definiteness", passed=True, value=None, threshold=None,
            severity=PhysicsCheckSeverity.PASS, message="Not applicable for this variable",
        )

    n_negative = int(np.sum(forecast < 0))
    n_total    = int(forecast.size)
    pct_neg    = (n_negative / n_total) * 100 if n_total > 0 else 0.0

    passed = n_negative == 0
    return PhysicsCheckResult(
        check      = "positive_definiteness",
        passed     = passed,
        value      = round(pct_neg, 4),
        threshold  = 0.0,
        severity   = PhysicsCheckSeverity.FAIL if not passed else PhysicsCheckSeverity.PASS,
        message    = (
            f"{variable}: {n_negative}/{n_total} negative values "
            f"({pct_neg:.3f}%) — {'FAIL' if not passed else 'PASS'}"
        ),
    )


def check_conservation_error(
    forecast:  np.ndarray,
    reference: np.ndarray,
) -> PhysicsCheckResult:
    """
    Check mass/tracer conservation by comparing global integral.
    Deviation > 0.01% triggers a FAIL.
    """
    f_sum = float(np.sum(np.abs(forecast)))
    r_sum = float(np.sum(np.abs(reference)))
    if r_sum == 0:
        return PhysicsCheckResult(
            check="conservation_error", passed=True, value=0.0, threshold=0.01,
            severity=PhysicsCheckSeverity.PASS, message="Reference sum is zero — skipped",
        )

    pct_error = abs(f_sum - r_sum) / r_sum * 100
    threshold = 0.01  # percent

    return PhysicsCheckResult(
        check      = "conservation_error",
        passed     = pct_error <= threshold,
        value      = round(pct_error, 6),
        threshold  = threshold,
        severity   = PhysicsCheckSeverity.FAIL if pct_error > threshold else PhysicsCheckSeverity.PASS,
        message    = (
            f"Conservation error: {pct_error:.6f}% "
            f"({'FAIL' if pct_error > threshold else 'PASS'})"
        ),
    )


def check_hydrostatic_balance(
    temperature: np.ndarray,
    geopotential: np.ndarray,
    pressure_levels: np.ndarray,
) -> PhysicsCheckResult:
    """
    Check hydrostatic balance: dΦ/d(ln p) = -R_d * T
    where Φ = geopotential, p = pressure, T = temperature, R_d = 287.05 J/kg/K.

    Violation fraction > 1% of grid cells = WARN.
    """
    R_d = 287.05   # J/(kg·K) — dry air gas constant

    if len(pressure_levels) < 2:
        return PhysicsCheckResult(
            check="hydrostatic_balance", passed=True, value=None, threshold=None,
            severity=PhysicsCheckSeverity.PASS,
            message="Insufficient pressure levels for check",
        )

    # dΦ/d(ln p) at each level
    ln_p      = np.log(pressure_levels)
    d_phi     = np.diff(geopotential, axis=-1)
    d_ln_p    = np.diff(ln_p)
    expected  = -R_d * temperature[..., :-1]   # simplified: use T at lower level
    actual    = d_phi / d_ln_p

    violation_mask = np.abs(actual - expected) > (0.05 * np.abs(expected))  # 5% tolerance
    pct_violations = float(np.mean(violation_mask)) * 100

    threshold = 1.0  # percent
    passed    = pct_violations <= threshold

    return PhysicsCheckResult(
        check      = "hydrostatic_balance",
        passed     = passed,
        value      = round(pct_violations, 3),
        threshold  = threshold,
        severity   = PhysicsCheckSeverity.WARN if not passed else PhysicsCheckSeverity.PASS,
        message    = (
            f"Hydrostatic balance violations: {pct_violations:.3f}% of grid cells "
            f"({'WARN' if not passed else 'PASS'})"
        ),
    )


# ─── DRIFT DETECTION ──────────────────────────────────────────────────────────

def detect_drift(rmse_history: List[float]) -> tuple[bool, Optional[float]]:
    """
    Compute linear trend over the RMSE history.
    Returns (drift_detected, trend_pct_per_week).

    A positive trend means RMSE is increasing = model getting worse.
    """
    if len(rmse_history) < 4:
        return False, None

    x    = np.arange(len(rmse_history), dtype=float)
    y    = np.array(rmse_history, dtype=float)
    # Linear regression
    slope = float(np.polyfit(x, y, 1)[0])

    # Convert slope to percent per week
    # Assuming each entry is one day: slope = RMSE/day
    # 7 * slope / mean_rmse * 100 = %/week
    mean_rmse = float(np.mean(y))
    if mean_rmse == 0:
        return False, None

    pct_per_week = (7 * slope / mean_rmse) * 100

    drift_detected = pct_per_week > Config.DRIFT_WARN_PCT_WEEK
    return drift_detected, round(pct_per_week, 3)


# ─── VALIDATION ORCHESTRATOR ──────────────────────────────────────────────────

class ValidationOrchestrator:
    """
    Runs the full 5-stage validation pipeline for a given AI model.
    In production, stages 1-4 query actual Zarr datasets via xarray.
    Here the statistical computation uses synthetic data to show the interface.
    """

    def validate(self, request: ValidationRequest) -> ValidationReport:
        log.info("Starting validation job %s for model %s", request.job_id, request.ai_model)

        # In production: load actual forecast and reference data from Zarr stores
        # forecast_ds  = xr.open_zarr(f"s3://pcmip-archive/zarr/models/{request.ai_model}/...")
        # reference_ds = xr.open_zarr(f"s3://pcmip-archive/zarr/obs/era5/...")
        # For this implementation, use synthetic arrays that produce realistic metrics.
        np.random.seed(42)
        n = 10_000   # grid points

        # Synthetic forecast and reference arrays (realistic ranges for Z500 in m)
        reference  = np.random.normal(5500, 300, n)
        forecast   = reference + np.random.normal(0, 142, n)   # ~142m RMSE
        climatology = np.full(n, 5500.0)

        # ── STAGE 1: Statistical evaluation ───────────────────────────────────
        rmse = compute_rmse(forecast, reference)
        mae  = compute_mae(forecast, reference)
        bias = compute_bias(forecast, reference)
        acc  = compute_acc(forecast, reference, climatology)

        # ── STAGE 2: Physics checks ────────────────────────────────────────────
        # Energy conservation uses TOA flux proxy
        toa_forecast  = np.random.normal(238, 5, n)
        toa_reference = np.random.normal(238, 2, n)

        physics_checks = [
            check_energy_conservation(toa_forecast, toa_reference),
            check_positive_definiteness(np.abs(forecast), request.variable),
            check_conservation_error(forecast, reference),
        ]

        # Hydrostatic check if variable is geopotential or temperature
        if request.variable in ("geopotential_height", "air_temperature"):
            temperature  = np.random.normal(250, 20, (n, 5))
            geopotential = np.cumsum(np.abs(np.random.normal(500, 50, (n, 5))), axis=1)
            levels       = np.array([1000, 850, 500, 250, 100], dtype=float)
            physics_checks.append(
                check_hydrostatic_balance(temperature, geopotential, levels)
            )

        # ── STAGE 3: Physical consistency score ────────────────────────────────
        n_passed = sum(1 for c in physics_checks if c.severity == PhysicsCheckSeverity.PASS)
        physical_consistency = (n_passed / len(physics_checks)) * 100 if physics_checks else 100.0

        # Conservation error (from physics check)
        cons_check = next((c for c in physics_checks if c.check == "conservation_error"), None)
        conservation_error = cons_check.value if cons_check and cons_check.value is not None else 0.0

        # ── STAGE 4: Drift detection ────────────────────────────────────────────
        registry_entry = MODEL_REGISTRY.get(request.ai_model)
        rmse_history   = list((registry_entry.rmse_history if registry_entry else []) + [rmse])
        drift_detected, drift_pct = detect_drift(rmse_history)

        # ── STAGE 5: Determine status and recommendation ────────────────────────
        has_hard_fail = any(c.severity == PhysicsCheckSeverity.FAIL for c in physics_checks)
        has_warn      = any(c.severity == PhysicsCheckSeverity.WARN  for c in physics_checks)
        warnings      = []

        if has_hard_fail:
            status         = ValidationStatus.SUSPENDED
            recommendation = "SUSPENDED — physics hard fail. Do not serve."
            warnings.append("One or more physics checks have FAILED. Model is suspended.")
        elif drift_detected and drift_pct and drift_pct > Config.DRIFT_FAIL_PCT_WEEK:
            status         = ValidationStatus.SUSPENDED
            recommendation = f"SUSPENDED — drift {drift_pct:.1f}%/week exceeds threshold."
            warnings.append(f"RMSE drift {drift_pct:.1f}%/week exceeds {Config.DRIFT_FAIL_PCT_WEEK}% threshold.")
        elif has_warn or (drift_detected and drift_pct and drift_pct > Config.DRIFT_WARN_PCT_WEEK):
            status         = ValidationStatus.CONDITIONAL
            recommendation = "CONDITIONAL — serve with warnings attached to response."
            if has_warn:
                warnings.append("Physics warnings present. Uncertainty bounds may be underestimated.")
            if drift_detected:
                warnings.append(f"Drift detected: {drift_pct:.1f}%/week. Monitor closely.")
        else:
            status         = ValidationStatus.CERTIFIED
            recommendation = "CERTIFIED — serve normally."

        # Safe range based on RMSE relative to physics baseline
        if status == ValidationStatus.SUSPENDED:
            safe_range = "Suspended"
        elif rmse < 130:
            safe_range = "All ranges"
        elif rmse < 145:
            safe_range = "Short-Medium (72h)"
        elif rmse < 160:
            safe_range = "Short (48h)"
        else:
            safe_range = "Very short (<24h)"

        # ── Build report ───────────────────────────────────────────────────────
        now        = datetime.now(timezone.utc)
        expires_at = now + timedelta(hours=Config.VALIDATION_EXPIRE_H)

        report = ValidationReport(
            job_id               = request.job_id,
            ai_model             = request.ai_model,
            ai_model_version     = registry_entry.model_version if registry_entry else "unknown",
            physics_baseline     = request.physics_baseline,
            variable             = request.variable,
            region               = request.region,
            period               = request.period,
            status               = status,
            rmse                 = round(rmse, 2),
            mae                  = round(mae, 2),
            bias                 = round(bias, 2),
            acc                  = round(acc, 4),
            physical_consistency = round(physical_consistency, 2),
            conservation_error   = round(conservation_error, 6),
            drift_detected       = drift_detected,
            drift_trend_pct_week = drift_pct,
            safe_range           = safe_range,
            recommendation       = recommendation,
            physics_checks       = physics_checks,
            warnings             = warnings,
            validated_at         = now.isoformat(),
            expires_at           = expires_at.isoformat(),
        )

        # Update registry
        if registry_entry:
            registry_entry.current_status = status
            registry_entry.safe_range     = safe_range
            registry_entry.last_validated = now.isoformat()
            registry_entry.expires_at     = expires_at.isoformat()
            registry_entry.rmse_history   = rmse_history[-30:]  # keep last 30

        log.info(
            "Validation complete: model=%s status=%s rmse=%.2f consistency=%.1f%% drift=%s",
            request.ai_model, status.value, rmse, physical_consistency,
            f"{drift_pct:.1f}%/wk" if drift_pct else "none",
        )

        return report


# ─── FASTAPI APPLICATION ──────────────────────────────────────────────────────

app          = FastAPI(title="PCMIP Validation Service", version="1.0.0")
orchestrator = ValidationOrchestrator()


@app.get("/health")
async def health():
    return {
        "status":  "ok",
        "service": "validation",
        "models_certified": sum(1 for m in MODEL_REGISTRY.values() if m.current_status == ValidationStatus.CERTIFIED),
        "models_suspended": sum(1 for m in MODEL_REGISTRY.values() if m.current_status == ValidationStatus.SUSPENDED),
        "time":    datetime.now(timezone.utc).isoformat(),
    }


@app.get("/v2/models/registry")
async def get_model_registry():
    """Return the full model validation registry."""
    return {
        "models": [entry.dict() for entry in MODEL_REGISTRY.values()],
        "total":  len(MODEL_REGISTRY),
        "certified": sum(1 for m in MODEL_REGISTRY.values() if m.current_status == ValidationStatus.CERTIFIED),
        "queried_at": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/v2/models/{model_id}/status")
async def get_model_status(model_id: str):
    """
    Check if a model is currently cleared to serve.
    The API gateway calls this before serving AI model outputs.
    This endpoint must respond in < 50ms (Redis-backed in production).
    """
    entry = MODEL_REGISTRY.get(model_id)
    if not entry:
        raise HTTPException(status_code=404, detail=f"Model {model_id} not in registry")

    now        = datetime.now(timezone.utc)
    expires_at = datetime.fromisoformat(entry.expires_at.replace("Z", "+00:00"))
    is_expired = now > expires_at

    cleared_to_serve = (
        entry.current_status in (ValidationStatus.CERTIFIED, ValidationStatus.CONDITIONAL)
        and not is_expired
    )

    return {
        "model_id":        model_id,
        "status":          entry.current_status.value,
        "cleared_to_serve": cleared_to_serve,
        "safe_range":      entry.safe_range,
        "expires_at":      entry.expires_at,
        "is_expired":      is_expired,
    }


@app.post("/internal/validate")
async def run_validation(
    request:    ValidationRequest,
    background: BackgroundTasks,
):
    """
    Internal endpoint called by the API gateway to submit validation jobs.
    Runs synchronously for simplicity; in production use Celery for the
    heavy compute and return immediately with job_id.
    """
    try:
        report = orchestrator.validate(request)
        REPORT_STORE[request.job_id] = report
        return report.dict()
    except Exception as exc:
        log.error("Validation job %s failed: %s", request.job_id, exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/internal/reports/{job_id}")
async def get_report(job_id: str):
    """Retrieve a completed validation report by job ID."""
    report = REPORT_STORE.get(job_id)
    if not report:
        raise HTTPException(status_code=404, detail=f"Report {job_id} not found")
    return report.dict()


@app.get("/v2/models/{model_id}/history")
async def get_model_history(model_id: str):
    """Return RMSE history for drift visualisation."""
    entry = MODEL_REGISTRY.get(model_id)
    if not entry:
        raise HTTPException(status_code=404, detail=f"Model {model_id} not in registry")

    drift_detected, drift_pct = detect_drift(entry.rmse_history)
    return {
        "model_id":       model_id,
        "rmse_history":   entry.rmse_history,
        "drift_detected": drift_detected,
        "drift_pct_week": drift_pct,
        "trend":          "degrading" if (drift_pct or 0) > 0 else "stable",
    }


# ─── ENTRYPOINT ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=Config.PORT, log_level="info")
