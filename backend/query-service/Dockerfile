# query-service/Dockerfile  (same pattern for ingestion and validation)
FROM python:3.12-slim AS base

# System deps for scientific Python stack
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libhdf5-dev libnetcdf-dev curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

FROM base AS deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

FROM deps AS runtime
RUN addgroup --gid 1001 pcmip && adduser --uid 1001 --gid 1001 --no-create-home pcmip
COPY src/ ./src/
USER pcmip
EXPOSE 8001
HEALTHCHECK --interval=15s --timeout=5s --retries=3 \
  CMD curl -f http://localhost:8001/internal/health || exit 1
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8001", "--workers", "4"]
