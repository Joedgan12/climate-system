// ─────────────────────────────────────────────────────────────────────────────
// shared/src/types.ts
// Canonical type definitions shared across all PCMIP services
// ─────────────────────────────────────────────────────────────────────────────

// ─── ENUMERATIONS ─────────────────────────────────────────────────────────────

export enum DataQualityFlag {
  VALID           = "QF_VALID",
  SCHEMA_WARN     = "QF_SCHEMA_WARN",
  PHYSICS_WARN    = "QF_PHYSICS_WARN",
  PHYSICS_FAIL    = "QF_PHYSICS_FAIL",
  INTERPOLATED    = "QF_INTERPOLATED",
  SUSPECT         = "QF_SUSPECT",
}

export enum StorageTier {
  HOT  = "hot",
  WARM = "warm",
  COLD = "cold",
}

export enum ModelType {
  PHYSICS  = "physics",
  AI       = "ai",
  HYBRID   = "ai-hybrid",
  POSTPROC = "postproc",
}

export enum ValidationStatus {
  CERTIFIED   = "CERTIFIED",
  CONDITIONAL = "CONDITIONAL",
  SUSPENDED   = "SUSPENDED",
  UNDER_REVIEW = "UNDER_REVIEW",
  PENDING     = "PENDING",
}

export enum APITier {
  RESEARCH    = "research",
  INSTITUTIONAL = "institutional",
  SOVEREIGN   = "sovereign",
}

export enum JobStatus {
  PENDING      = "PENDING",
  RUNNING      = "RUNNING",
  CHECKPOINTING = "CHECKPOINTING",
  COMPLETE     = "COMPLETE",
  FAILED       = "FAILED",
  QUEUED       = "QUEUED",
}

export enum CMIPStandard {
  CMIP6 = "CMIP6",
  CMIP7 = "CMIP7",
}

// ─── PROVENANCE ───────────────────────────────────────────────────────────────

export interface ProvenanceEnvelope {
  dataset_id:      string;         // UUID v4
  source_id:       string;         // e.g. "goes16.ABI-L2-CMIPF"
  ingest_ts:       string;         // ISO8601 UTC
  raw_hash:        string;         // sha256:...
  schema_version:  string;         // e.g. "v2.4.1"
  quality_flags:   DataQualityFlag[];
  cf_standard:     string | null;  // CF-1.10 standard name
  cmip7_var:       string | null;  // CMIP7 variable short name
  cmip_standard:   CMIPStandard;
  fair_compliant:  boolean;
  bias_corrected:  boolean;
  lineage_parent:  string | null;  // parent dataset_id
}

// ─── UNCERTAINTY ──────────────────────────────────────────────────────────────

export interface UncertaintyBounds {
  method:        "ensemble-percentile" | "conformal" | "monte-carlo" | "none";
  p05:           number | null;
  p25:           number | null;
  p50:           number | null;
  p75:           number | null;
  p95:           number | null;
  ensemble_size: number | null;
  calibrated:    boolean;
}

// ─── CLIMATE VARIABLE RESPONSE ────────────────────────────────────────────────

export interface ClimateVariableResponse {
  variable:    string;   // CF standard name
  cmip7_var:   string;   // CMIP7 short name
  value:       number;
  unit:        string;
  lat:         number;
  lon:         number;
  time:        string;   // ISO8601
  level:       number | "surface" | null;
  model:       string;
  ensemble:    string | null;
  uncertainty: UncertaintyBounds;
  provenance:  ProvenanceEnvelope;
  warnings:    string[];
  query_ms:    number;   // server-side query time
}

// ─── ENSEMBLE STATS RESPONSE ──────────────────────────────────────────────────

export interface EnsembleStatsResponse {
  dataset:              string;
  scenario:             string;
  variable:             string;
  region:               string;
  horizon:              string;
  ensemble_size:        number;
  mean_warming:         number;
  p10:                  number;
  p25:                  number;
  p50:                  number;
  p75:                  number;
  p90:                  number;
  models_agreeing_pct:  number;
  physically_consistent_pct: number;
  validation_score:     number;
  bias_corrected:       boolean;
  reanalysis_ref:       string;
  provenance:           ProvenanceEnvelope;
  query_ms:             number;
}

// ─── AI VALIDATION REPORT ─────────────────────────────────────────────────────

export interface PhysicsCheckResult {
  check:     string;
  passed:    boolean;
  value:     number | null;
  threshold: number | null;
  severity:  "FAIL" | "WARN" | "PASS";
  message:   string;
}

export interface ValidationReport {
  job_id:               string;
  ai_model:             string;
  ai_model_version:     string;
  physics_baseline:     string;
  variable:             string;
  region:               string;
  period:               string;
  status:               ValidationStatus;
  rmse:                 number;
  mae:                  number;
  bias:                 number;
  acc:                  number;           // anomaly correlation coefficient
  physical_consistency: number;           // percent
  conservation_error:   number;           // percent
  drift_detected:       boolean;
  drift_trend_pct_week: number | null;
  safe_range:           string;
  recommendation:       string;
  physics_checks:       PhysicsCheckResult[];
  warnings:             string[];
  validated_at:         string;           // ISO8601
  expires_at:           string;           // ISO8601 (72h from validated_at)
  validator_version:    string;
}

// ─── LINEAGE ──────────────────────────────────────────────────────────────────

export interface LineageNode {
  node_id:    string;
  node_type:  "observation" | "transformation" | "model_run" | "api_response";
  label:      string;
  dataset_id: string;
  hash:       string;
  created_at: string;
  parents:    string[];   // node_ids
  metadata:   Record<string, unknown>;
}

export interface LineageGraph {
  root_id:    string;
  nodes:      LineageNode[];
  edges:      Array<{ from: string; to: string; transformation: string }>;
  queried_at: string;
}

// ─── STAC DATASET ─────────────────────────────────────────────────────────────

export interface STACDataset {
  dataset_id:     string;
  stac_item_id:   string;
  collection:     string;
  cmip_drs_path:  string;           // CMIP7 Data Reference Syntax path
  zarr_store:     string;           // s3://... path
  temporal_extent: { start: string; end: string };
  spatial_extent:  { bbox: [number, number, number, number] };
  variables:      string[];         // CF standard names
  chunk_shape:    Record<string, number>;
  compression:    string;
  byte_size:      number;
  storage_tier:   StorageTier;
  cmip_standard:  CMIPStandard;
  provenance_id:  string;
  doi:            string | null;
  registered_at:  string;
}

// ─── API KEY / AUTH ───────────────────────────────────────────────────────────

export interface APIKey {
  key_id:       string;
  key_hash:     string;   // bcrypt hash — never store plaintext
  org_id:       string;
  tier:         APITier;
  rate_limit:   { requests_per_hour: number; gb_per_day: number };
  created_at:   string;
  expires_at:   string | null;
  last_used_at: string | null;
  revoked:      boolean;
  scopes:       string[];
}

// ─── INGESTION RECORD ─────────────────────────────────────────────────────────

export interface RawIngestionRecord {
  source_id:      string;
  received_at:    string;
  raw_bytes:      number;
  raw_format:     "grib2" | "netcdf4" | "hdf5" | "level2" | "csv";
  raw_hash:       string;
  kafka_topic:    string;
  kafka_offset:   number;
  kafka_partition: number;
}

export interface ValidatedRecord extends RawIngestionRecord {
  provenance:     ProvenanceEnvelope;
  variables:      string[];
  temporal_range: { start: string; end: string };
  spatial_bbox:   [number, number, number, number];
  zarr_path:      string | null;   // null until written
  validation_ms:  number;
}

// ─── ERROR TYPES ──────────────────────────────────────────────────────────────

export interface PCMIPError {
  code:      string;
  message:   string;
  details:   Record<string, unknown> | null;
  request_id: string;
  timestamp: string;
}

// ─── JOB / ASYNC ──────────────────────────────────────────────────────────────

export interface AsyncJob {
  job_id:      string;
  job_type:    "validation" | "export" | "ensemble_compute";
  status:      JobStatus;
  submitted_at: string;
  started_at:   string | null;
  completed_at: string | null;
  result_url:   string | null;
  error:        string | null;
  progress_pct: number;
  metadata:     Record<string, unknown>;
}

// ─── SYSTEM HEALTH ────────────────────────────────────────────────────────────

export interface SourceHealth {
  source_id:     string;
  name:          string;
  status:        "online" | "degraded" | "offline";
  health_pct:    number;
  bytes_per_hour: number;
  last_record_at: string;
  lag_minutes:   number;
  schema_pass_rate: number;
  physics_pass_rate: number;
}

export interface SystemHealth {
  status:        "nominal" | "degraded" | "critical";
  checked_at:    string;
  api_p99_ms:    number;
  kafka_lag_max: number;
  archive_bytes: number;
  active_jobs:   number;
  sources:       SourceHealth[];
  services:      Array<{ name: string; healthy: boolean; latency_ms: number }>;
}
