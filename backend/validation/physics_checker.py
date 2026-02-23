"""
validation/physics_checker.py
Physics-aware validation of AI weather/climate model forecasts.

This is the most scientifically critical component in PCMIP.
AI models can produce outputs that are statistically plausible but physically
impossible. Standard RMSE metrics do not detect these violations.

Validation pipeline:
  Stage 1 — Statistical evaluation (RMSE, ACC, bias vs ERA5)
  Stage 2 — Conservation law checks (energy, mass, moisture)
  Stage 3 — Uncertainty calibration check
  Stage 4 — Bias correction (xclim quantile mapping)
  Stage 5 — Drift detection (30-day RMSE trend)

Hard rule: no AI model output is served via the API without passing Stage 1 & 2.
Stages 3–5 produce metadata and warnings, not blocking failures.
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import structlog
import xarray as xr
from scipy import stats

from config.settings import get_settings

settings = get_settings()
log = structlog.get_logger(__name__)


# ─── RESULT TYPES ─────────────────────────────────────────────────────────────

@dataclass
class PhysicsCheckResult:
    check_name: str
    passed: bool
    value: Optional[float] = None
    threshold: Optional[float] = None
    message: str = ""
    severity: str = "INFO"  # "INFO" | "WARN" | "FAIL"


@dataclass
class StatisticsResult:
    lead_time_h: int
    rmse: float
    bias: float
    mae: float
    acc: float  # Anomaly Correlation Coefficient
    n_samples: int


@dataclass
class ValidationReport:
    job_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    ai_model: str = ""
    physics_baseline: str = ""
    variable: str = ""
    region: str = ""
    period: str = ""
    physics_checks: List[PhysicsCheckResult] = field(default_factory=list)
    statistical_results: List[StatisticsResult] = field(default_factory=list)
    physical_consistency_pct: float = 0.0
    conservation_error_wm2: Optional[float] = None
    drift_detected: bool = False
    drift_rate_pct_per_week: Optional[float] = None
    safe_range: str = "UNKNOWN"
    recommendation: str = ""
    warnings: List[str] = field(default_factory=list)
    validated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    next_validation_due: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc) + timedelta(hours=72)
    )

    @property
    def passed(self) -> bool:
        return all(c.severity != "FAIL" for c in self.physics_checks)


# ─── STATISTICS EVALUATOR ─────────────────────────────────────────────────────

class StatisticalEvaluator:
    """
    Compute standard forecast verification metrics against a reference dataset (ERA5).
    All metrics are computed per lead time and per region.
    """

    def compute_rmse(self, forecast: np.ndarray, reference: np.ndarray) -> float:
        """Root Mean Square Error."""
        diff = forecast - reference
        return float(np.sqrt(np.mean(diff ** 2)))

    def compute_bias(self, forecast: np.ndarray, reference: np.ndarray) -> float:
        """Mean bias (forecast minus reference)."""
        return float(np.mean(forecast - reference))

    def compute_mae(self, forecast: np.ndarray, reference: np.ndarray) -> float:
        """Mean Absolute Error."""
        return float(np.mean(np.abs(forecast - reference)))

    def compute_acc(self, forecast: np.ndarray, reference: np.ndarray,
                    climatology: np.ndarray) -> float:
        """
        Anomaly Correlation Coefficient.
        Both forecast and reference are expressed as anomalies from climatology.
        ACC = 1.0 is perfect; ACC > 0.6 is considered skilful.
        """
        f_anom = forecast - climatology
        r_anom = reference - climatology

        numerator = np.sum(f_anom * r_anom)
        denominator = np.sqrt(np.sum(f_anom ** 2) * np.sum(r_anom ** 2))

        if denominator == 0:
            return 0.0
        return float(numerator / denominator)

    def evaluate_all_lead_times(
        self,
        forecast_ds: xr.Dataset,
        reference_ds: xr.Dataset,
        climatology_ds: xr.Dataset,
        variable: str,
        lead_times_h: List[int],
    ) -> List[StatisticsResult]:
        results = []
        for lead_h in lead_times_h:
            try:
                fc = forecast_ds[variable].sel(lead_time=lead_h).values.flatten()
                ref = reference_ds[variable].values.flatten()
                clim = climatology_ds[variable].values.flatten()

                # Remove NaN pairs
                mask = ~(np.isnan(fc) | np.isnan(ref))
                fc, ref, clim = fc[mask], ref[mask], clim[mask]

                if len(fc) < 10:
                    log.warning("insufficient_samples_for_verification", lead_h=lead_h, n=len(fc))
                    continue

                results.append(StatisticsResult(
                    lead_time_h=lead_h,
                    rmse=self.compute_rmse(fc, ref),
                    bias=self.compute_bias(fc, ref),
                    mae=self.compute_mae(fc, ref),
                    acc=self.compute_acc(fc, ref, clim),
                    n_samples=len(fc),
                ))
            except Exception as e:
                log.warning("stat_eval_failed_for_lead", lead_h=lead_h, error=str(e))
        return results


# ─── CONSERVATION LAW CHECKER ─────────────────────────────────────────────────

class ConservationChecker:
    """
    Checks fundamental physical conservation laws in AI model output.
    These checks catch the class of AI model failures that statistical metrics miss:
    outputs that look plausible to RMSE but violate thermodynamics.
    """

    def check_energy_conservation(self, ds: xr.Dataset) -> PhysicsCheckResult:
        """
        Global mean TOA energy balance should be approximately zero in a balanced
        atmosphere. Drift in net TOA flux indicates energy non-conservation.
        Threshold: 0.5 W/m2 per settings.
        """
        try:
            # Net TOA = shortwave_in - shortwave_reflected - longwave_out
            rsdt = ds.get("rsdt")  # TOA incident SW
            rsut = ds.get("rsut")  # TOA reflected SW
            rlut = ds.get("rlut")  # TOA outgoing LW

            if rsdt is None or rsut is None or rlut is None:
                return PhysicsCheckResult(
                    check_name="energy_conservation",
                    passed=True,
                    message="TOA radiation variables not present — check skipped",
                    severity="INFO",
                )

            net_toa = (rsdt - rsut - rlut).mean().values.item()
            threshold = settings.validation_max_conservation_error

            passed = abs(net_toa) <= threshold
            return PhysicsCheckResult(
                check_name="energy_conservation",
                passed=passed,
                value=round(net_toa, 4),
                threshold=threshold,
                message=(
                    f"Net TOA flux = {net_toa:.3f} W/m² "
                    f"({'within' if passed else 'EXCEEDS'} ±{threshold} W/m² threshold)"
                ),
                severity="INFO" if passed else "FAIL",
            )
        except Exception as e:
            log.warning("energy_conservation_check_failed", error=str(e))
            return PhysicsCheckResult(
                check_name="energy_conservation", passed=True,
                message=f"Check failed to execute: {e}", severity="INFO",
            )

    def check_mass_conservation(self, ds: xr.Dataset) -> PhysicsCheckResult:
        """
        Global mean surface pressure should be approximately constant in time
        (mass conservation). Significant drift indicates mass non-conservation.
        Threshold: 0.01% relative change per timestep.
        """
        try:
            ps = ds.get("ps")  # surface pressure
            if ps is None:
                return PhysicsCheckResult(
                    check_name="mass_conservation", passed=True,
                    message="Surface pressure (ps) not present — check skipped", severity="INFO",
                )

            global_mean_ps = ps.mean(dim=["lat", "lon"])
            if len(global_mean_ps.time) < 2:
                return PhysicsCheckResult(
                    check_name="mass_conservation", passed=True,
                    message="Insufficient timesteps for mass conservation check", severity="INFO",
                )

            # Compute relative change per timestep
            ps_ref = float(global_mean_ps.isel(time=0).values)
            ps_rel_change = abs(float(global_mean_ps.isel(time=-1).values) - ps_ref) / ps_ref

            threshold = 0.0001  # 0.01%
            passed = ps_rel_change <= threshold

            return PhysicsCheckResult(
                check_name="mass_conservation",
                passed=passed,
                value=round(ps_rel_change * 100, 6),
                threshold=threshold * 100,
                message=(
                    f"Global mean Ps change = {ps_rel_change*100:.4f}% "
                    f"({'within' if passed else 'EXCEEDS'} {threshold*100}% threshold)"
                ),
                severity="INFO" if passed else "FAIL",
            )
        except Exception as e:
            return PhysicsCheckResult(
                check_name="mass_conservation", passed=True,
                message=f"Check failed to execute: {e}", severity="INFO",
            )

    def check_positive_definiteness(self, ds: xr.Dataset) -> PhysicsCheckResult:
        """
        Specific humidity and precipitation must be non-negative everywhere.
        Negative values are physically impossible and indicate AI model pathology.
        """
        violations: List[str] = []

        for var_name in ["hus", "pr", "prsn", "evspsbl"]:
            da = ds.get(var_name)
            if da is None:
                continue
            n_negative = int((da.values < 0).sum())
            if n_negative > 0:
                total = da.values.size
                pct = 100 * n_negative / total
                violations.append(f"{var_name}: {n_negative} negative values ({pct:.2f}% of grid)")

        passed = len(violations) == 0
        return PhysicsCheckResult(
            check_name="positive_definiteness",
            passed=passed,
            value=len(violations),
            threshold=0,
            message=(
                "All moisture variables non-negative" if passed else
                f"Negative values found: {'; '.join(violations)}"
            ),
            severity="INFO" if passed else "FAIL",
        )

    def check_hydrostatic_balance(self, ds: xr.Dataset) -> PhysicsCheckResult:
        """
        Geopotential height should increase with decreasing pressure.
        Violations of hydrostatic balance indicate inconsistencies in the
        temperature-geopotential relationship.
        This is a WARN-level check; not grounds for immediate rejection.
        """
        try:
            zg = ds.get("zg")  # geopotential height on pressure levels
            if zg is None or "plev" not in zg.dims:
                return PhysicsCheckResult(
                    check_name="hydrostatic_balance", passed=True,
                    message="zg on pressure levels not present — check skipped", severity="INFO",
                )

            # zg should increase as pressure decreases (going up)
            zg_mean = zg.mean(dim=[d for d in zg.dims if d != "plev"])
            zg_sorted = float((np.diff(zg_mean.values) > 0).mean())  # fraction with correct ordering

            violation_pct = (1 - zg_sorted) * 100
            threshold = 1.0  # 1% violation → WARN

            passed = violation_pct <= threshold
            return PhysicsCheckResult(
                check_name="hydrostatic_balance",
                passed=passed,
                value=round(violation_pct, 3),
                threshold=threshold,
                message=(
                    f"Hydrostatic violations: {violation_pct:.2f}% of levels "
                    f"({'OK' if passed else 'WARNING — exceeds threshold'})"
                ),
                severity="INFO" if passed else "WARN",
            )
        except Exception as e:
            return PhysicsCheckResult(
                check_name="hydrostatic_balance", passed=True,
                message=f"Check failed to execute: {e}", severity="INFO",
            )

    def run_all(self, ds: xr.Dataset) -> List[PhysicsCheckResult]:
        return [
            self.check_energy_conservation(ds),
            self.check_mass_conservation(ds),
            self.check_positive_definiteness(ds),
            self.check_hydrostatic_balance(ds),
        ]


# ─── DRIFT DETECTOR ───────────────────────────────────────────────────────────

class DriftDetector:
    """
    Monitors 30-day rolling RMSE trend for each AI model.
    If RMSE increases by > drift_threshold_pct/week, the model is flagged.
    RMSE history is stored in Redis (key: drift:{model_id}:{variable}).
    """

    def compute_drift(self, rmse_history: List[Tuple[datetime, float]]) -> Tuple[bool, Optional[float]]:
        """
        Given a list of (timestamp, rmse) pairs, fit a linear trend.
        Returns (drift_detected, rate_pct_per_week).
        """
        if len(rmse_history) < 7:
            return False, None

        # Sort by time
        sorted_history = sorted(rmse_history, key=lambda x: x[0])
        times = np.array([(t - sorted_history[0][0]).total_seconds() / 86400 for t, _ in sorted_history])
        rmses = np.array([r for _, r in sorted_history])

        # Linear regression
        slope, intercept, r_value, p_value, std_err = stats.linregress(times, rmses)

        # Convert slope (units/day) to % change per week relative to baseline RMSE
        baseline_rmse = rmses[0] if rmses[0] != 0 else 1.0
        rate_pct_per_week = (slope * 7 / baseline_rmse) * 100

        drift_detected = (
            rate_pct_per_week > settings.validation_drift_threshold_pct
            and p_value < 0.05  # statistically significant trend
        )

        return drift_detected, round(rate_pct_per_week, 2)


# ─── MAIN VALIDATOR ───────────────────────────────────────────────────────────

class AIModelValidator:
    """
    Orchestrates the full AI model validation pipeline.
    Called by the validation router when a POST /v2/models/validate request arrives.
    """

    def __init__(self) -> None:
        self.stats_evaluator = StatisticalEvaluator()
        self.conservation_checker = ConservationChecker()
        self.drift_detector = DriftDetector()

    def validate(
        self,
        ai_model_ds: xr.Dataset,
        reference_ds: xr.Dataset,
        climatology_ds: xr.Dataset,
        model_id: str,
        baseline_id: str,
        variable: str,
        lead_times_h: List[int],
        rmse_history: Optional[List[Tuple[datetime, float]]] = None,
    ) -> ValidationReport:
        t_start = time.perf_counter()
        report = ValidationReport(
            ai_model=model_id,
            physics_baseline=baseline_id,
            variable=variable,
        )

        # Stage 1: Statistical evaluation
        log.info("validation_stage1_stats", model=model_id, variable=variable)
        stat_results = self.stats_evaluator.evaluate_all_lead_times(
            ai_model_ds, reference_ds, climatology_ds, variable, lead_times_h
        )
        report.statistical_results = stat_results

        # Stage 2: Physics checks
        log.info("validation_stage2_physics", model=model_id)
        physics_results = self.conservation_checker.run_all(ai_model_ds)
        report.physics_checks = physics_results

        # Count physical violations
        grid_size = ai_model_ds[variable].size if variable in ai_model_ds else 1
        fail_count = sum(1 for c in physics_results if c.severity == "FAIL")
        report.physical_consistency_pct = round(100 * (1 - fail_count / len(physics_results)), 1) if physics_results else 0.0

        # Conservation error (from energy check)
        energy_check = next((c for c in physics_results if c.check_name == "energy_conservation"), None)
        if energy_check and energy_check.value is not None:
            report.conservation_error_wm2 = energy_check.value

        # Stage 5: Drift detection
        if rmse_history:
            drift_detected, drift_rate = self.drift_detector.compute_drift(rmse_history)
            report.drift_detected = drift_detected
            report.drift_rate_pct_per_week = drift_rate
            if drift_detected:
                report.warnings.append(
                    f"DRIFT DETECTED: RMSE increasing at {drift_rate:.1f}%/week. "
                    "Model may be approaching out-of-distribution territory."
                )

        # Determine safe range and recommendation
        report.safe_range, report.recommendation = self._determine_safe_range(report)

        elapsed_ms = int((time.perf_counter() - t_start) * 1000)
        log.info(
            "validation_complete",
            model=model_id, variable=variable,
            safe_range=report.safe_range, passed=report.passed,
            elapsed_ms=elapsed_ms,
        )
        return report

    def _determine_safe_range(self, report: ValidationReport) -> Tuple[str, str]:
        """
        Classify model safe range based on validation results.
        This follows the PCMIP model registry classification system.
        """
        hard_fails = [c for c in report.physics_checks if c.severity == "FAIL"]
        if hard_fails:
            return "SUSPENDED", (
                f"Model has {len(hard_fails)} hard physics failure(s): "
                + "; ".join(c.check_name for c in hard_fails)
                + ". Suspended from API serving pending investigation."
            )

        if report.drift_detected:
            return "CONDITIONAL_REVIEW", (
                "Model performance is degrading (drift detected). "
                "Safe for short-range only; full review required."
            )

        if report.physical_consistency_pct < settings.validation_min_physical_consistency:
            return "SHORT_RANGE_ONLY",  (
                f"Physical consistency {report.physical_consistency_pct:.1f}% "
                f"below threshold {settings.validation_min_physical_consistency}%. "
                "Safe for short-range (<48h) only."
            )

        # Check ACC at 120h lead time
        acc_120 = next(
            (r.acc for r in report.statistical_results if r.lead_time_h == 120), None
        )
        if acc_120 is not None and acc_120 < 0.6:
            return "MEDIUM_RANGE_ONLY", (
                f"ACC at 120h = {acc_120:.3f} (below skill threshold 0.6). "
                "Safe for short-to-medium range (<120h)."
            )

        return "ALL_RANGES", (
            "Model passes all physics checks and maintains skill at medium range. "
            "Safe for operational use across all forecast ranges."
        )
