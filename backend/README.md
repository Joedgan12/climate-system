# PCMIP Backend — Architecture & Language Decision

## Language Decision: Why NOT Node.js as Primary

Node.js was evaluated and rejected as the primary backend language. Here is the full reasoning:

### The Hard Constraint

The climate science ecosystem is Python-native. The libraries that make PCMIP possible —
`xarray`, `zarr`, `dask.distributed`, `cfgrib`, `pydantic`, `openlineage-python`, `eccodes`,
`xclim`, `cf-xarray`, `intake-esm` — have no Node.js equivalents. Building a planetary
climate data platform without these libraries means reimplementing years of scientific
computing infrastructure from scratch. That is not a tradeoff — it is a mistake.

### Language Map

| Component | Language | Why |
|---|---|---|
| API Gateway | **Python (FastAPI)** | Async, Pydantic response models, native integration with Dask/Zarr |
| Kafka Ingestion Workers | **Python** | Pydantic validation, physics constraint checks, cfgrib parsing |
| Zarr I/O Layer | **Python** | xarray + zarr are Python-native; no equivalent elsewhere |
| Dask Query Engine | **Python** | Dask is Python only |
| AI Model Validation | **Python** | xarray, scipy, xclim for scientific metrics |
| Real-time WebSocket | **Node.js** | This IS Node.js territory: low-overhead event streaming, excellent WS libraries |
| Admin CLI / Tooling | **Go** | Single binaries, excellent concurrency, fast cold start |
| SLURM Orchestration | **Python + Bash** | SLURM APIs have Python bindings; shell for job templates |
| OpenLineage Events | **Python** | openlineage-python is the reference implementation |

### Where Node.js Actually Belongs

Node.js runs the real-time telemetry layer: a WebSocket server that streams live
system events (ingest lag, job status, validation results, API metrics) to the React
dashboard. This is exactly what Node.js excels at — high-concurrency, low-overhead,
event-driven I/O. It does not touch scientific data.

## Project Structure

```
pcmip-backend/
├── api/                          # FastAPI application (Python 3.12)
│   ├── main.py                   # App factory, middleware, startup
│   ├── config.py                 # Settings from environment
│   ├── dependencies.py           # DI: dask client, redis, zarr stores
│   ├── routers/
│   │   ├── climate.py            # /v2/climate/* endpoints
│   │   ├── ensemble.py           # /v2/ensemble/* endpoints
│   │   ├── models.py             # /v2/models/validate endpoints
│   │   └── lineage.py            # /v2/lineage/* endpoints
│   ├── models/
│   │   └── schemas.py            # Pydantic request/response models
│   ├── services/
│   │   ├── zarr_service.py       # Zarr dataset access and query
│   │   ├── dask_service.py       # Dask distributed query execution
│   │   ├── lineage_service.py    # OpenLineage event emission and query
│   │   └── cache_service.py      # Redis caching layer
│   └── middleware/
│       └── auth.py               # API key validation, rate limiting
├── ingestion/                    # Kafka consumers (Python)
│   ├── consumer.py               # Base consumer with offset management
│   ├── validators.py             # Schema + physics constraint validators
│   ├── normalizer.py             # CMIP7 / CF-1.10 normalisation
│   ├── provenance.py             # Provenance envelope generation
│   └── adapters/
│       ├── era5_adapter.py       # ERA5 GRIB2 → normalised record
│       └── goes_adapter.py       # GOES-16 NetCDF → normalised record
├── validation/                   # AI model validation service (Python)
│   ├── physics_checker.py        # Conservation law checks
│   ├── statistical_eval.py       # RMSE, ACC, bias vs ERA5
│   └── drift_detector.py        # 30-day RMSE trend monitoring
├── realtime/                     # WebSocket server (Node.js)
│   ├── package.json
│   ├── server.js                 # WS server + event aggregator
│   └── streams/
│       ├── ingest_stream.js      # Kafka → WS bridge
│       └── metrics_stream.js     # Prometheus → WS bridge
├── infrastructure/
│   ├── slurm_wrapper.py          # SLURM job submission and monitoring
│   └── checkpoint_runner.py     # Checkpoint validation + restart logic
├── config/
│   └── settings.py              # Centralised config
├── docker-compose.yml            # Full local stack
├── Dockerfile.api                # API container
├── Dockerfile.ingestion          # Ingestion worker container
└── requirements/
    ├── api.txt
    ├── ingestion.txt
    └── validation.txt
```

## Quick Start

```bash
# Prerequisites: Docker, Docker Compose
cp .env.example .env          # fill in S3 credentials, Kafka brokers
docker-compose up --build     # starts all services

# API available at: http://localhost:8000
# WS server at:     ws://localhost:8080
# Docs at:          http://localhost:8000/docs
```

## Environment Variables

See `.env.example` for the full list. Critical:

```
KAFKA_BROKERS=kafka-1:9092,kafka-2:9092,kafka-3:9092
ZARR_STORE_URL=s3://pcmip-archive
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
REDIS_URL=redis://redis:6379/0
DASK_SCHEDULER=dask-scheduler:8786
MARQUEZ_URL=http://marquez:5000
API_KEY_SALT=...  # for API key hashing
```
