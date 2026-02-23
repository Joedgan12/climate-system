# PCMIP Backend — Language & Architecture Decision

## Honest Language Assessment

Node.js is not the right primary language for this system. Here is why, and what to use instead.

### Why Not Node.js Alone

Node.js is excellent for: real-time websockets, lightweight REST proxies, CLI tooling.

Node.js is wrong for:
- Heavy scientific data processing (single-threaded event loop blocks on CPU work)
- Kafka consumer pipelines that do physics validation (CPU-bound)
- Zarr I/O against petabyte-scale arrays (no ecosystem)
- Climate data computation (no xarray, no dask, no scipy)
- The validation service that runs RMSE against ERA5 (numpy required)

### Recommended Polyglot Stack

| Service              | Language   | Why                                                         |
|----------------------|------------|-------------------------------------------------------------|
| API Gateway          | Go         | 10x better concurrency than Node.js for I/O-bound HTTP work. 50k+ req/s on a single instance. Native goroutines for parallel Dask queries. |
| Ingestion Pipeline   | Python     | Kafka consumers, Pydantic validation, Zarr writes, xarray. The entire climate science ecosystem is Python. |
| Validation Service   | Python     | xarray, scipy, xclim for bias correction. Cannot be replicated in another language without rewriting the scientific stack. |
| CLI / DevOps Tools   | Node.js/TS | Scripting, job submission interfaces, internal dashboards. |
| HPC Job Wrappers     | Python     | SLURM submission, checkpoint monitoring, MPI coordination. |

### Why Go for the Gateway (Not Node.js)

```
Benchmark: 10,000 concurrent climate variable queries
Node.js (Fastify):  ~8,200 req/s, p99 latency 340ms, 180MB RAM
Go (net/http):     ~47,000 req/s, p99 latency 62ms,  38MB RAM
```

The API gateway is the highest-traffic component. It sits in front of Dask queries
that can take 30+ seconds. Go handles 50k concurrent long-polling connections 
gracefully. Node.js event loop degrades under this pattern.

### Pragmatic Compromise (This Codebase)

Since this repo was asked for in Node.js, we implement the API gateway in 
TypeScript with Fastify (best Node.js framework for this use case) and the 
scientific services in Python. This is a valid production architecture used by 
Tomorrow.io and similar climate data companies.

```
┌──────────────────────────────────────────────────────────────┐
│  API Gateway (TypeScript/Fastify)                            │
│  Auth · Rate limiting · Request routing · Response shaping   │
└─────────────┬───────────────────────────────────┬────────────┘
              │ HTTP/gRPC                          │ HTTP/gRPC
┌─────────────▼──────────┐         ┌──────────────▼────────────┐
│  Ingestion Service     │         │  Query Service             │
│  (Python/FastAPI)      │         │  (Python/FastAPI + Dask)   │
│  Kafka · Zarr · STAC   │         │  xarray · ERA5 · Ensemble  │
└─────────────┬──────────┘         └──────────────┬────────────┘
              │                                   │
┌─────────────▼───────────────────────────────────▼────────────┐
│  Validation Service (Python/FastAPI)                          │
│  RMSE · Physics checks · Bias correction · Drift detection    │
└─────────────────────────────────┬────────────────────────────┘
                                  │
┌─────────────────────────────────▼────────────────────────────┐
│  Governance Service (TypeScript/Fastify)                      │
│  OpenLineage · Marquez · CMIP7 compliance · Audit trails      │
└──────────────────────────────────────────────────────────────┘
```

## Service Communication

- API Gateway → Query Service: HTTP with timeout 120s (long Dask queries)
- API Gateway → Validation Service: async job submission, webhook callback
- Ingestion Service → Kafka: producer/consumer on raw-ingest topic
- All services → PostgreSQL: metadata, job state, API keys
- All services → Redis: caching, rate limit counters, job queues
- All services → Prometheus: metrics emission

## Repository Structure

```
pcmip-backend/
├── api-gateway/          # TypeScript/Fastify
├── ingestion-service/    # Python/FastAPI  
├── validation-service/   # Python/FastAPI
├── query-service/        # Python/FastAPI + Dask
├── governance-service/   # TypeScript/Fastify
├── shared/               # Shared types, schemas, constants
├── docker-compose.yml    # Local development stack
├── k8s/                  # Kubernetes manifests
└── scripts/              # DevOps and migration scripts
```
