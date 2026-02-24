-- scripts/db/init.sql
-- PCMIP PostgreSQL Schema
-- Run once on database initialisation.

-- ─────────────────────────────────────────────────────────────────────────────
-- EXTENSIONS
-- ─────────────────────────────────────────────────────────────────────────────

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ─────────────────────────────────────────────────────────────────────────────
-- ENUMERATIONS
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TYPE api_tier        AS ENUM ('research', 'institutional', 'sovereign');
CREATE TYPE validation_status AS ENUM ('CERTIFIED', 'CONDITIONAL', 'SUSPENDED', 'UNDER_REVIEW', 'PENDING');
CREATE TYPE storage_tier    AS ENUM ('hot', 'warm', 'cold');
CREATE TYPE cmip_standard   AS ENUM ('CMIP6', 'CMIP7');
CREATE TYPE job_status      AS ENUM ('PENDING', 'RUNNING', 'CHECKPOINTING', 'COMPLETE', 'FAILED', 'QUEUED');
CREATE TYPE quality_flag    AS ENUM ('QF_VALID', 'QF_SCHEMA_WARN', 'QF_PHYSICS_WARN', 'QF_PHYSICS_FAIL', 'QF_INTERPOLATED', 'QF_SUSPECT');

-- ─────────────────────────────────────────────────────────────────────────────
-- API KEYS
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE organisations (
  org_id       UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  name         TEXT NOT NULL,
  contact_email TEXT NOT NULL,
  country      TEXT,
  tier         api_tier NOT NULL DEFAULT 'research',
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  active       BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE api_keys (
  key_id       UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  org_id       UUID NOT NULL REFERENCES organisations(org_id),
  key_hash     TEXT NOT NULL UNIQUE,   -- sha256 of the raw key (never store raw)
  tier         api_tier NOT NULL,
  scopes       TEXT[] NOT NULL DEFAULT '{"climate:read","lineage:read"}',
  rate_limit_rph  INTEGER NOT NULL DEFAULT 100,   -- requests per hour
  rate_limit_gb   NUMERIC NOT NULL DEFAULT 10,    -- GB per day
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at   TIMESTAMPTZ,
  last_used_at TIMESTAMPTZ,
  revoked      BOOLEAN NOT NULL DEFAULT FALSE,
  revoked_at   TIMESTAMPTZ,
  revoked_reason TEXT
);

CREATE INDEX idx_api_keys_hash ON api_keys (key_hash) WHERE NOT revoked;

-- example key for local development (raw value "dev-key-0001")
INSERT INTO organisations (name, contact_email, country) VALUES ('Demo Org','demo@example.org','US');
INSERT INTO api_keys (org_id, key_hash, tier) VALUES (
    (SELECT org_id FROM organisations WHERE name='Demo Org'),
    '539e613ccad3c5d55c23d43df0f9751af27ca132b8c961d1e6c8ccd38cd0fd36',
    'research'
);

-- ─────────────────────────────────────────────────────────────────────────────
-- DATA SOURCES
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE data_sources (
  source_id        TEXT PRIMARY KEY,   -- e.g. "goes16.ABI-L2-CMIPF"
  name             TEXT NOT NULL,
  organisation     TEXT NOT NULL,
  description      TEXT,
  data_format      TEXT NOT NULL,      -- "grib2" | "netcdf4" | "hdf5" | etc
  bytes_per_hour   BIGINT,
  cadence_seconds  INTEGER,
  schema_version   TEXT NOT NULL DEFAULT 'v2.4.1',
  active           BOOLEAN NOT NULL DEFAULT TRUE,
  added_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ─────────────────────────────────────────────────────────────────────────────
-- PROVENANCE / INGESTION
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE provenance_records (
  dataset_id      TEXT PRIMARY KEY,            -- ds_<12hex>
  source_id       TEXT NOT NULL REFERENCES data_sources(source_id),
  ingest_ts       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  raw_hash        TEXT NOT NULL,               -- sha256:...
  schema_version  TEXT NOT NULL,
  quality_flags   quality_flag[] NOT NULL,
  cf_standard     TEXT,
  cmip7_var       TEXT,
  cmip_standard   cmip_standard NOT NULL DEFAULT 'CMIP7',
  fair_compliant  BOOLEAN NOT NULL DEFAULT TRUE,
  bias_corrected  BOOLEAN NOT NULL DEFAULT FALSE,
  lineage_parent  TEXT REFERENCES provenance_records(dataset_id)
);

CREATE INDEX idx_prov_source    ON provenance_records (source_id);
CREATE INDEX idx_prov_ingest_ts ON provenance_records (ingest_ts DESC);
CREATE INDEX idx_prov_cf        ON provenance_records (cf_standard);

-- ─────────────────────────────────────────────────────────────────────────────
-- STAC CATALOG
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE stac_datasets (
  dataset_id      TEXT PRIMARY KEY,
  stac_item_id    TEXT NOT NULL UNIQUE,
  collection      TEXT NOT NULL,
  cmip_drs_path   TEXT NOT NULL,
  zarr_store      TEXT NOT NULL,
  temporal_start  TIMESTAMPTZ NOT NULL,
  temporal_end    TIMESTAMPTZ NOT NULL,
  spatial_bbox    NUMERIC[4] NOT NULL,    -- [lon_min, lat_min, lon_max, lat_max]
  variables       TEXT[] NOT NULL,
  chunk_shape     JSONB NOT NULL DEFAULT '{}',
  compression     TEXT NOT NULL DEFAULT 'blosc:lz4',
  byte_size       BIGINT NOT NULL,
  storage_tier    storage_tier NOT NULL DEFAULT 'hot',
  cmip_standard   cmip_standard NOT NULL DEFAULT 'CMIP7',
  provenance_id   TEXT REFERENCES provenance_records(dataset_id),
  doi             TEXT,
  registered_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_accessed_at TIMESTAMPTZ
);

CREATE INDEX idx_stac_collection  ON stac_datasets (collection);
CREATE INDEX idx_stac_temporal    ON stac_datasets (temporal_start, temporal_end);
CREATE INDEX idx_stac_variables   ON stac_datasets USING GIN (variables);
CREATE INDEX idx_stac_tier        ON stac_datasets (storage_tier);

-- ─────────────────────────────────────────────────────────────────────────────
-- MODEL VALIDATION REGISTRY
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE model_registry (
  model_id         TEXT PRIMARY KEY,
  model_version    TEXT NOT NULL,
  model_type       TEXT NOT NULL CHECK (model_type IN ('physics', 'ai', 'ai-hybrid')),
  organisation     TEXT NOT NULL,
  current_status   validation_status NOT NULL DEFAULT 'PENDING',
  safe_range       TEXT NOT NULL,
  last_validated   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at       TIMESTAMPTZ NOT NULL,
  rmse_z500        NUMERIC,
  mae              NUMERIC,
  bias             NUMERIC,
  acc              NUMERIC,
  physical_consistency_pct NUMERIC,
  conservation_error_pct   NUMERIC,
  drift_detected   BOOLEAN NOT NULL DEFAULT FALSE,
  drift_pct_week   NUMERIC,
  added_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE validation_reports (
  report_id        UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  job_id           TEXT NOT NULL UNIQUE,
  model_id         TEXT NOT NULL REFERENCES model_registry(model_id),
  physics_baseline TEXT NOT NULL,
  variable         TEXT NOT NULL,
  region           TEXT NOT NULL,
  period           TEXT NOT NULL,
  status           validation_status NOT NULL,
  rmse             NUMERIC NOT NULL,
  mae              NUMERIC NOT NULL,
  bias             NUMERIC NOT NULL,
  acc              NUMERIC NOT NULL,
  physical_consistency NUMERIC NOT NULL,
  conservation_error   NUMERIC NOT NULL,
  drift_detected   BOOLEAN NOT NULL,
  drift_pct_week   NUMERIC,
  safe_range       TEXT NOT NULL,
  recommendation   TEXT NOT NULL,
  physics_checks   JSONB NOT NULL DEFAULT '[]',
  warnings         TEXT[] NOT NULL DEFAULT '{}',
  validated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at       TIMESTAMPTZ NOT NULL,
  validator_version TEXT NOT NULL DEFAULT '1.0.0'
);

CREATE INDEX idx_val_reports_model    ON validation_reports (model_id, validated_at DESC);
CREATE INDEX idx_val_reports_status   ON validation_reports (status);

-- ─────────────────────────────────────────────────────────────────────────────
-- OPENLINEAGE EVENTS
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE lineage_events (
  event_id      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  run_id        TEXT NOT NULL,
  event_type    TEXT NOT NULL,
  event_time    TIMESTAMPTZ NOT NULL,
  job_namespace TEXT NOT NULL,
  job_name      TEXT NOT NULL,
  inputs        JSONB NOT NULL DEFAULT '[]',
  outputs       JSONB NOT NULL DEFAULT '[]',
  facets        JSONB NOT NULL DEFAULT '{}',
  received_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_lineage_run_id   ON lineage_events (run_id);
CREATE INDEX idx_lineage_job      ON lineage_events (job_name);
CREATE INDEX idx_lineage_time     ON lineage_events (event_time DESC);

-- ─────────────────────────────────────────────────────────────────────────────
-- LINEAGE NODES (materialised from events for fast graph traversal)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE lineage_nodes (
  node_id      TEXT PRIMARY KEY,
  node_type    TEXT NOT NULL,
  label        TEXT NOT NULL,
  dataset_id   TEXT NOT NULL,
  hash         TEXT NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL,
  parents      TEXT[] NOT NULL DEFAULT '{}',
  metadata     JSONB NOT NULL DEFAULT '{}'
);

CREATE INDEX idx_lineage_nodes_dataset ON lineage_nodes (dataset_id);
CREATE INDEX idx_lineage_nodes_parents ON lineage_nodes USING GIN (parents);

-- ─────────────────────────────────────────────────────────────────────────────
-- ASYNC JOBS
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE async_jobs (
  job_id        TEXT PRIMARY KEY,
  job_type      TEXT NOT NULL CHECK (job_type IN ('validation', 'export', 'ensemble_compute')),
  status        job_status NOT NULL DEFAULT 'PENDING',
  submitted_by  UUID REFERENCES api_keys(key_id),
  submitted_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  started_at    TIMESTAMPTZ,
  completed_at  TIMESTAMPTZ,
  result_url    TEXT,
  error         TEXT,
  progress_pct  SMALLINT NOT NULL DEFAULT 0,
  metadata      JSONB NOT NULL DEFAULT '{}'
);

CREATE INDEX idx_jobs_status ON async_jobs (status, submitted_at DESC);
CREATE INDEX idx_jobs_type   ON async_jobs (job_type);

-- ─────────────────────────────────────────────────────────────────────────────
-- AUDIT TRAIL
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE audit_events (
  event_id    UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  org_id      UUID REFERENCES organisations(org_id),
  key_id      UUID REFERENCES api_keys(key_id),
  action      TEXT NOT NULL,
  resource    TEXT NOT NULL,
  status      TEXT NOT NULL DEFAULT 'success',
  ip_address  TEXT,
  user_agent  TEXT,
  request_id  TEXT,
  duration_ms INTEGER,
  timestamp   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_audit_org       ON audit_events (org_id, timestamp DESC);
CREATE INDEX idx_audit_action    ON audit_events (action, timestamp DESC);
CREATE INDEX idx_audit_timestamp ON audit_events (timestamp DESC);

-- ─────────────────────────────────────────────────────────────────────────────
-- TIERING POLICY (drives the tiering daemon)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE tiering_policy (
  policy_id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  collection          TEXT NOT NULL,
  hot_retention_days  INTEGER NOT NULL DEFAULT 90,
  warm_retention_days INTEGER NOT NULL DEFAULT 1095,   -- 3 years
  cold_permanent      BOOLEAN NOT NULL DEFAULT TRUE,
  priority_variables  TEXT[] NOT NULL DEFAULT '{}',
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ─────────────────────────────────────────────────────────────────────────────
-- SEED DATA: Initial data sources
-- ─────────────────────────────────────────────────────────────────────────────

INSERT INTO data_sources (source_id, name, organisation, data_format, bytes_per_hour, cadence_seconds) VALUES
  ('era5.pressure-levels',    'ERA5 Reanalysis',         'ECMWF',             'grib2',   2200000000000, 3600),
  ('goes16.ABI-L2-CMIPF',    'GOES-16',                 'NOAA',              'netcdf4',  840000000000,  600),
  ('sentinel6.altimetry',    'SENTINEL-6 MF',            'Copernicus',        'netcdf4',  120000000000,  0),
  ('argo.core',              'ARGO Float Array',          'Argo International','netcdf4',    8000000000,  0),
  ('nexrad.level2',          'NEXRAD Radar Network',     'NOAA NWS',          'level2',   480000000000,  300),
  ('modis.terra',            'MODIS-Terra',              'NASA',              'hdf5',     360000000000,  0),
  ('cmorph.crt',             'CMORPH-CRT',               'NOAA',              'netcdf4',  180000000000,  1800),
  ('oisst.v21',              'OISST v2.1',               'NOAA',              'netcdf4',   40000000000,  86400),
  ('gfs.gdas',               'GFS/GDAS',                 'NOAA NWS',          'grib2',    600000000000,  21600);

-- Seed model registry
INSERT INTO model_registry (model_id, model_version, model_type, organisation, current_status, safe_range, expires_at, rmse_z500, physical_consistency_pct) VALUES
  ('ifs-cy48r1',    '48r1', 'physics',  'ECMWF',            'CERTIFIED',    'All ranges',          NOW() + INTERVAL '24 hours',  119.4, 99.8),
  ('cesm2.1.3',     '2.1.3','physics',  'NCAR',             'CERTIFIED',    'All ranges',          NOW() + INTERVAL '24 hours',  134.2, 99.6),
  ('aifs-v1.4',     '1.4',  'ai-hybrid','ECMWF',            'CERTIFIED',    'Medium (120h)',        NOW() + INTERVAL '72 hours',  128.1, 97.1),
  ('graphcast-v2',  '2.0',  'ai',       'Google DeepMind',  'CONDITIONAL',  'Short-Medium (72h)',   NOW() + INTERVAL '72 hours',  142.3, 94.2),
  ('pangu-weather', '2.0',  'ai',       'Huawei',           'CONDITIONAL',  'Short (48h)',          NOW() + INTERVAL '72 hours',  156.8, 91.7),
  ('fuxi-v1.0',     '1.0',  'ai',       'Fudan University', 'SUSPENDED',    'Suspended',            NOW(),                         NULL,  NULL);

-- Seed tiering policy
INSERT INTO tiering_policy (collection, hot_retention_days, warm_retention_days, cold_permanent, priority_variables) VALUES
  ('era5-pressure-levels',   90, 1095, TRUE, '{"air_temperature","specific_humidity","geopotential_height"}'),
  ('goes16-abi-l2',          30,  365, TRUE, '{"toa_outgoing_longwave_flux"}'),
  ('model-output',           60,  730, TRUE, '{"air_temperature","precipitation_flux","sea_surface_temperature"}'),
  ('argo-profiles',          90, 1095, TRUE, '{"sea_water_temperature","sea_water_salinity"}');
