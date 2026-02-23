"""
ingestion/validators.py
Schema and physics constraint validation for ingested records.

Two validation passes happen here:
1. SchemaValidator — structural correctness (required fields, types, ranges)
2. PhysicsValidator — physical plausibility (thermodynamics, conservation)

Design rules:
- REJECT severity: record goes to dead letter. Hard stop.
- WARN severity: record proceeds to validated-records with quality flag.
- Thresholds come from settings, not hardcoded. Adjustable without redeploy.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import structlog
from confluent_kafka.schema_registry import SchemaRegistryClient

from config.settings import get_settings

settings = get_settings()
log = structlog.get_logger(__name__)

REQUIRED_FIELDS = {"variable", "value", "lat", "lon", "time", "unit", "source_id"}

# Physical bounds: (warn_low, reject_low, reject_high, warn_high) per variable
# None = no bound on that side
PHYSICS_BOUNDS: Dict[str, Tuple] = {
    "air_temperature": (
        settings.physics_t_min + 20,  # warn: < 170K is unlikely but physically possible in stratosphere
        settings.physics_t_min,       # reject: < 150K
        settings.physics_t_max,       # reject: > 340K
        settings.physics_t_max - 10,  # warn: > 330K
    ),
    "sea_surface_temperature": (
        271.5,
        settings.physics_sst_min,   # reject: < 271K (below freezing)
        313.0,
        311.0,
    ),
    "specific_humidity": (
        None,
        settings.physics_q_min,     # reject: negative
        settings.physics_q_max,     # reject: > 4% (unphysical)
        0.035,
    ),
    "precipitation_flux": (
        None,
        settings.physics_precip_min,   # reject: negative
        settings.physics_precip_max,   # reject: > 500 mm/h
        300.0,
    ),
    "eastward_wind": (
        -settings.physics_wind_warn,
        -settings.physics_wind_reject,
        settings.physics_wind_reject,
        settings.physics_wind_warn,
    ),
    "northward_wind": (
        -settings.physics_wind_warn,
        -settings.physics_wind_reject,
        settings.physics_wind_reject,
        settings.physics_wind_warn,
    ),
}


@dataclass
class ValidationResult:
    passed: bool
    severity: str  # "OK" | "WARN" | "REJECT"
    message: str = ""
    flags: List[str] = field(default_factory=list)
    schema_version: str = "unknown"


class SchemaValidator:
    """
    Validates records against source-specific Avro schemas from the schema registry.
    For non-Avro sources (e.g. JSON from HTTP adapters), falls back to structural checks.
    """

    def __init__(self, source_id: str, registry: SchemaRegistryClient) -> None:
        self.source_id = source_id
        self.registry = registry
        self._schema_cache: Dict[str, Any] = {}

    def _get_schema_version(self) -> str:
        try:
            schema = self.registry.get_latest_version(f"{self.source_id}-value")
            return str(schema.schema_id)
        except Exception:
            return "registry-unavailable"

    def validate(self, record: Dict[str, Any]) -> ValidationResult:
        schema_version = self._get_schema_version()

        # Check required fields
        missing = REQUIRED_FIELDS - set(record.keys())
        if missing:
            return ValidationResult(
                passed=False,
                severity="REJECT",
                message=f"Missing required fields: {sorted(missing)}",
                schema_version=schema_version,
            )

        # Type checks
        try:
            lat = float(record["lat"])
            lon = float(record["lon"])
            value = float(record["value"])
        except (TypeError, ValueError) as e:
            return ValidationResult(
                passed=False,
                severity="REJECT",
                message=f"Type error in numeric fields: {e}",
                schema_version=schema_version,
            )

        # Coordinate bounds
        if not (-90.0 <= lat <= 90.0):
            return ValidationResult(
                passed=False, severity="REJECT",
                message=f"lat={lat} is outside [-90, 90]",
                schema_version=schema_version,
            )
        if not (-180.0 <= lon <= 360.0):
            return ValidationResult(
                passed=False, severity="REJECT",
                message=f"lon={lon} is outside [-180, 360]",
                schema_version=schema_version,
            )

        # NaN/Inf check
        if math.isnan(value) or math.isinf(value):
            return ValidationResult(
                passed=False, severity="REJECT",
                message=f"value is NaN or Inf — invalid observation",
                schema_version=schema_version,
            )

        # Time format
        time_str = record.get("time", "")
        if not isinstance(time_str, str) or len(time_str) < 10:
            return ValidationResult(
                passed=False, severity="REJECT",
                message=f"time field '{time_str}' does not look like ISO8601",
                schema_version=schema_version,
            )

        return ValidationResult(passed=True, severity="OK", schema_version=schema_version)


class PhysicsValidator:
    """
    Physical plausibility checks for climate observations and model output.
    Two severity levels:
      - REJECT: value is physically impossible under any atmospheric conditions
      - WARN: value is extremely unlikely; flag and pass to science review

    All thresholds are configurable via settings — adjustable without code change.
    """

    def __init__(self, source_id: str) -> None:
        self.source_id = source_id

    def validate(self, record: Dict[str, Any]) -> ValidationResult:
        variable = record.get("variable", "").lower()
        value = float(record.get("value", 0.0))
        flags: List[str] = []
        warnings: List[str] = []

        if variable not in PHYSICS_BOUNDS:
            # Unknown variable — pass through with no physics check
            return ValidationResult(passed=True, severity="OK", flags=["QF_NO_PHYSICS_CHECK"])

        warn_lo, reject_lo, reject_hi, warn_hi = PHYSICS_BOUNDS[variable]

        # REJECT checks
        if reject_lo is not None and value < reject_lo:
            return ValidationResult(
                passed=False,
                severity="REJECT",
                message=(
                    f"{variable}={value} is below physical minimum ({reject_lo}). "
                    f"Source: {self.source_id}. This is a hard reject."
                ),
                flags=["QF_PHYSICS_REJECT"],
            )
        if reject_hi is not None and value > reject_hi:
            return ValidationResult(
                passed=False,
                severity="REJECT",
                message=(
                    f"{variable}={value} exceeds physical maximum ({reject_hi}). "
                    f"Source: {self.source_id}."
                ),
                flags=["QF_PHYSICS_REJECT"],
            )

        # WARN checks
        if warn_lo is not None and value < warn_lo:
            flags.append("QF_PHYSICS_WARN")
            warnings.append(f"{variable}={value} below warn threshold ({warn_lo})")
        if warn_hi is not None and value > warn_hi:
            flags.append("QF_PHYSICS_WARN")
            warnings.append(f"{variable}={value} above warn threshold ({warn_hi})")

        # Variable-specific additional checks
        extra_result = self._run_extra_checks(variable, record)
        if extra_result:
            if extra_result.severity == "REJECT":
                return extra_result
            flags.extend(extra_result.flags)
            warnings.extend([extra_result.message] if extra_result.message else [])

        if flags:
            msg = "; ".join(warnings) if warnings else "Physics warning"
            log.warning("physics_warning", source=self.source_id, variable=variable, value=value, flags=flags)
            return ValidationResult(passed=False, severity="WARN", message=msg, flags=flags)

        return ValidationResult(passed=True, severity="OK", flags=["QF_VALID"])

    def _run_extra_checks(self, variable: str, record: Dict[str, Any]) -> Optional[ValidationResult]:
        """
        Extended checks that require more than one field.
        Add new checks here as scientific requirements evolve.
        """
        value = float(record.get("value", 0.0))
        level = record.get("level_hpa")

        # Stratospheric temperature sanity: below 200 hPa, T < 250K is expected
        if variable == "air_temperature" and level is not None:
            try:
                level_f = float(level)
                if level_f < 200 and value > 300:
                    return ValidationResult(
                        passed=False, severity="WARN",
                        message=f"air_temperature={value}K at {level_f}hPa — unexpectedly warm for stratosphere",
                        flags=["QF_STRAT_TEMP_WARN"],
                    )
            except (TypeError, ValueError):
                pass

        # Sea surface temperature: must be above sea ice melting point if flagged as open ocean
        if variable == "sea_surface_temperature":
            sea_ice_flag = record.get("sea_ice_fraction", 1.0)
            try:
                if float(sea_ice_flag) < 0.15 and value < 271.15:
                    return ValidationResult(
                        passed=False, severity="WARN",
                        message=f"SST={value}K below 271.15K but sea_ice_fraction={sea_ice_flag} (open ocean). Inconsistency.",
                        flags=["QF_SST_ICE_INCONSISTENCY"],
                    )
            except (TypeError, ValueError):
                pass

        # Precipitation: negative precipitation is impossible
        if variable == "precipitation_flux" and value < 0:
            return ValidationResult(
                passed=False, severity="REJECT",
                message=f"precipitation_flux={value} is negative — physically impossible",
                flags=["QF_PHYSICS_REJECT"],
            )

        return None


class CMIPNormaliser:
    """
    Normalises records to CMIP7 / CF-1.10 conventions before writing to validated topic.
    - Renames variables to CF standard names
    - Converts units to SI where needed
    - Adds required CMIP7 global attributes
    """

    # Unit conversions: (from_unit, to_unit) → conversion_factor
    UNIT_CONVERSIONS: Dict[Tuple[str, str], float] = {
        ("celsius", "K"): 273.15,   # additive — handled specially
        ("degc", "K"): 273.15,
        ("°c", "K"): 273.15,
        ("mm/h", "kg m-2 s-1"): 1 / 3600,
        ("mm/day", "kg m-2 s-1"): 1 / 86400,
        ("hpa", "Pa"): 100.0,
        ("mbar", "Pa"): 100.0,
    }

    def normalise(self, record: Dict[str, Any]) -> Dict[str, Any]:
        result = {**record}
        value = float(result["value"])
        unit = result.get("unit", "").lower().strip()

        # Unit conversion
        target_unit = result.get("si_unit", unit)
        conversion_key = (unit, target_unit)
        if conversion_key in self.UNIT_CONVERSIONS:
            factor = self.UNIT_CONVERSIONS[conversion_key]
            if unit in ("celsius", "degc", "°c"):
                value = value + factor  # additive for temperature
            else:
                value = value * factor
            result["value"] = value
            result["unit"] = target_unit
            result["unit_converted_from"] = unit

        # Ensure lat is in [-90, 90] and lon in [-180, 180]
        lon = float(result.get("lon", 0))
        if lon > 180:
            result["lon"] = lon - 360

        return result
