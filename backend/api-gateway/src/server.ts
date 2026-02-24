// ─────────────────────────────────────────────────────────────────────────────
// api-gateway/src/server.ts
// PCMIP API Gateway — TypeScript/Fastify
//
// Handles: Authentication, rate limiting, request routing, response shaping,
//          async job management, uncertainty metadata injection, lineage queries.
//
// NOTE: This is the TypeScript API gateway. Scientific computation is delegated
//       to the Python query-service and validation-service over HTTP.
//       Do NOT put xarray, Dask, or Zarr I/O in this service.
// ─────────────────────────────────────────────────────────────────────────────

import Fastify, { FastifyInstance, FastifyRequest, FastifyReply } from "fastify";
import { createClient, RedisClientType } from "redis";
import { Pool } from "pg";
import crypto from "crypto";
import { ClimateVariableResponse, EnsembleStatsResponse, ValidationReport,
         LineageGraph, AsyncJob, SystemHealth, APITier, JobStatus } from "../../shared/src/types";

// ─── CONFIGURATION ────────────────────────────────────────────────────────────

const CONFIG = {
  port:             parseInt(process.env.PORT ?? "8080"),
  host:             process.env.HOST ?? "0.0.0.0",
  environment:      process.env.NODE_ENV ?? "development",

  // Downstream service URLs
  queryServiceUrl:      process.env.QUERY_SERVICE_URL      ?? "http://query-service:8001",
  validationServiceUrl: process.env.VALIDATION_SERVICE_URL ?? "http://validation-service:8002",
  ingestionServiceUrl:  process.env.INGESTION_SERVICE_URL  ?? "http://ingestion-service:8003",
  governanceServiceUrl: process.env.GOVERNANCE_SERVICE_URL ?? "http://governance-service:8004",

  // Redis
  redisUrl:         process.env.REDIS_URL ?? "redis://localhost:6379",

  // PostgreSQL
  pgConnString:     process.env.DATABASE_URL ?? "postgresql://pcmip:pcmip@localhost:5432/pcmip",

  // Rate limits per tier (requests per hour)
  rateLimits: {
    [APITier.RESEARCH]:      100,
    [APITier.INSTITUTIONAL]: 10_000,
    [APITier.SOVEREIGN]:     Infinity,
  } as Record<APITier, number>,

  // Query timeouts
  timeouts: {
    variable:  15_000,   // 15s
    timeseries: 60_000,  // 60s
    ensemble:  120_000,  // 120s
    validation: 600_000, // 10 min (async, but initial response)
  },
} as const;

// ─── REDIS / PG CLIENTS ───────────────────────────────────────────────────────

let redis: RedisClientType;
let db: Pool;

async function initClients(): Promise<void> {
  redis = createClient({ url: CONFIG.redisUrl }) as RedisClientType;
  redis.on("error", (err) => console.error("[redis] error:", err));
  await redis.connect();

  db = new Pool({ connectionString: CONFIG.pgConnString, max: 20 });
  await db.query("SELECT 1"); // health check
  console.log("[db] PostgreSQL connected");
}

// ─── REQUEST ID MIDDLEWARE ─────────────────────────────────────────────────────

function generateRequestId(): string {
  return `req_${crypto.randomBytes(8).toString("hex")}`;
}

// ─── API KEY RESOLUTION ───────────────────────────────────────────────────────

interface ResolvedKey {
  keyId:    string;
  orgId:    string;
  tier:     APITier;
  scopes:   string[];
}

async function resolveAPIKey(rawKey: string): Promise<ResolvedKey | null> {
  // Hash the incoming key and look up in cache first
  const keyHash = crypto.createHash("sha256").update(rawKey).digest("hex");
  const cacheKey = `apikey:${keyHash}`;

  const cached = await redis.get(cacheKey);
  if (cached) {
    return JSON.parse(cached) as ResolvedKey;
  }

  // Fall back to database
  const result = await db.query<{
    key_id: string; org_id: string; tier: APITier; scopes: string[]; revoked: boolean;
  }>(
    `SELECT key_id, org_id, tier, scopes, revoked
     FROM api_keys
     WHERE key_hash = $1 AND (expires_at IS NULL OR expires_at > NOW())`,
    [keyHash]
  );

  if (result.rows.length === 0 || result.rows[0].revoked) return null;

  const row = result.rows[0];
  const resolved: ResolvedKey = {
    keyId:  row.key_id,
    orgId:  row.org_id,
    tier:   row.tier,
    scopes: row.scopes,
  };

  // Cache for 5 minutes
  await redis.setEx(cacheKey, 300, JSON.stringify(resolved));

  // Update last_used_at asynchronously — don't block the request
  db.query("UPDATE api_keys SET last_used_at = NOW() WHERE key_id = $1", [row.key_id])
    .catch((err) => console.error("[db] last_used_at update failed:", err));

  return resolved;
}

// ─── RATE LIMITER ──────────────────────────────────────────────────────────────

async function checkRateLimit(keyId: string, tier: APITier): Promise<{
  allowed:    boolean;
  remaining:  number;
  resetAt:    number;
}> {
  const limit = CONFIG.rateLimits[tier];
  if (limit === Infinity) return { allowed: true, remaining: Infinity, resetAt: 0 };

  const window = "hour";
  const windowKey = `ratelimit:${keyId}:${window}:${Math.floor(Date.now() / 3_600_000)}`;

  const count = await redis.incr(windowKey);
  if (count === 1) {
    // First request in this window — set expiry
    await redis.expire(windowKey, 3600);
  }

  const remaining = Math.max(0, limit - count);
  const resetAt   = (Math.floor(Date.now() / 3_600_000) + 1) * 3_600_000;

  return { allowed: count <= limit, remaining, resetAt };
}

// ─── DOWNSTREAM HTTP CLIENT ───────────────────────────────────────────────────

async function callDownstream<T>(
  url:     string,
  method:  "GET" | "POST",
  body?:   unknown,
  timeout: number = 30_000
): Promise<T> {
  const controller = new AbortController();
  const timer      = setTimeout(() => controller.abort(), timeout);

  try {
    const response = await fetch(url, {
      method,
      headers: { "Content-Type": "application/json" },
      body:    body ? JSON.stringify(body) : undefined,
      signal:  controller.signal,
    });

    const text = await response.text();

    if (!response.ok) {
      // include whatever the downstream returned (could be plain text or JSON)
      throw new Error(`Downstream ${url} responded ${response.status}: ${text}`);
    }

    try {
      return JSON.parse(text) as T;
    } catch (parseErr) {
      throw new Error(
        `Downstream ${url} returned invalid JSON: ${(parseErr as Error).message}. Raw response: ${text}`
      );
    }
  } catch (err) {
    if ((err as Error).name === "AbortError") {
      throw new Error(`Downstream ${url} timed out after ${timeout}ms`);
    }
    throw err;
  } finally {
    clearTimeout(timer);
  }
}

// ─── AUTH PLUGIN (Fastify preHandler) ─────────────────────────────────────────

async function authPlugin(
  request: FastifyRequest & { apiKey?: ResolvedKey },
  reply:   FastifyReply
): Promise<void> {
  const authHeader = request.headers.authorization;
  if (!authHeader?.startsWith("Bearer ")) {
    reply.status(401).send({
      code:       "MISSING_API_KEY",
      message:    "Authorization: Bearer <api_key> header is required",
      request_id: (request as any).requestId,
      timestamp:  new Date().toISOString(),
    });
    return;
  }

  const rawKey = authHeader.slice(7);
  const resolved = await resolveAPIKey(rawKey);

  if (!resolved) {
    reply.status(401).send({
      code:       "INVALID_API_KEY",
      message:    "API key not found, expired, or revoked",
      request_id: (request as any).requestId,
      timestamp:  new Date().toISOString(),
    });
    return;
  }

  // Rate limit check
  const rateResult = await checkRateLimit(resolved.keyId, resolved.tier);
  reply.header("X-RateLimit-Limit",     CONFIG.rateLimits[resolved.tier].toString());
  reply.header("X-RateLimit-Remaining", rateResult.remaining.toString());
  reply.header("X-RateLimit-Reset",     rateResult.resetAt.toString());

  if (!rateResult.allowed) {
    reply.status(429).send({
      code:       "RATE_LIMIT_EXCEEDED",
      message:    `Rate limit exceeded for tier ${resolved.tier}. Resets at ${new Date(rateResult.resetAt).toISOString()}`,
      request_id: (request as any).requestId,
      timestamp:  new Date().toISOString(),
    });
    return;
  }

  (request as any).apiKey = resolved;
}

// ─── REQUEST LOGGING MIDDLEWARE ───────────────────────────────────────────────

async function requestLogger(
  request: FastifyRequest,
  reply:   FastifyReply
): Promise<void> {
  const requestId = generateRequestId();
  (request as any).requestId = requestId;
  (request as any).startTime = Date.now();
  reply.header("X-Request-Id", requestId);
}

// ─── PCMIP STANDARD HEADERS ───────────────────────────────────────────────────

function setPCMIPHeaders(reply: FastifyReply, extras: Record<string, string> = {}): void {
  reply.header("X-PCMIP-Version",     "2.0.0");
  reply.header("X-CMIP-Standard",     "CMIP7");
  reply.header("X-FAIR-Compliant",    "true");
  reply.header("Cache-Control",       "no-store");   // Climate data must not be cached stale
  Object.entries(extras).forEach(([k, v]) => reply.header(k, v));
}

// ─── BUILD SERVER ─────────────────────────────────────────────────────────────

async function buildServer(): Promise<FastifyInstance> {
  const app = Fastify({
    logger:    CONFIG.environment === "development",
    trustProxy: true,
    genReqId:  () => generateRequestId(),
  });

  // Global pre-handler: request ID + timing
  app.addHook("onRequest", requestLogger);

  // Global response: add timing header
  app.addHook("onSend", async (request, reply) => {
    const elapsed = Date.now() - ((request as any).startTime ?? Date.now());
    reply.header("X-Response-Time-Ms", elapsed.toString());
  });

  // ── HEALTH ENDPOINT (no auth) ──────────────────────────────────────────────
  app.get("/health", async (req, reply) => {
    const health = await callDownstream<SystemHealth>(
      `${CONFIG.queryServiceUrl}/internal/health`, "GET", undefined, 5000
    ).catch(() => null);

    reply.status(health ? 200 : 503).send({
      status:     health?.status ?? "unknown",
      gateway:    "ok",
      services:   health?.services ?? [],
      checked_at: new Date().toISOString(),
    });
  });

  // ── V2 ROUTES — all require auth ───────────────────────────────────────────
  app.register(async (v2) => {

    v2.addHook("preHandler", authPlugin);

    // ── GET /v2/climate/variable ─────────────────────────────────────────────
    v2.get<{
      Querystring: {
        lat: string; lon: string; variable: string; time: string;
        level?: string; model?: string; ensemble?: string; format?: "json" | "zarr";
      };
    }>("/climate/variable", {
      schema: {
        querystring: {
          type: "object",
          required: ["lat", "lon", "variable", "time"],
          properties: {
            lat:      { type: "string" },
            lon:      { type: "string" },
            variable: { type: "string" },
            time:     { type: "string" },
            level:    { type: "string" },
            model:    { type: "string" },
            ensemble: { type: "string" },
            format:   { type: "string", enum: ["json", "zarr"] },
          },
        },
      },
    }, async (request, reply) => {
      const { lat, lon, variable, time, level, model, ensemble, format } = request.query;
      const requestId = (request as any).requestId as string;

      // Input validation
      const latNum = parseFloat(lat);
      const lonNum = parseFloat(lon);
      if (isNaN(latNum) || latNum < -90 || latNum > 90) {
        return reply.status(400).send({ code: "INVALID_LAT", message: "lat must be -90 to 90", request_id: requestId, timestamp: new Date().toISOString() });
      }
      if (isNaN(lonNum) || lonNum < -180 || lonNum > 180) {
        return reply.status(400).send({ code: "INVALID_LON", message: "lon must be -180 to 180", request_id: requestId, timestamp: new Date().toISOString() });
      }
      if (!isValidISO8601(time)) {
        return reply.status(400).send({ code: "INVALID_TIME", message: "time must be ISO8601 format", request_id: requestId, timestamp: new Date().toISOString() });
      }

      // Forward to Python query service
      const queryUrl = buildQueryUrl(`${CONFIG.queryServiceUrl}/v2/climate/variable`, {
        lat: latNum.toString(), lon: lonNum.toString(), variable, time,
        ...(level    && { level }),
        ...(model    && { model }),
        ...(ensemble && { ensemble }),
        ...(format   && { format }),
      });

      const result = await callDownstream<ClimateVariableResponse>(
        queryUrl, "GET", undefined, CONFIG.timeouts.variable
      );

      setPCMIPHeaders(reply, {
        "X-Provenance-Id":       result.provenance.dataset_id,
        "X-Uncertainty-Method":  result.uncertainty.method,
        "X-Bias-Corrected":      result.provenance.bias_corrected.toString(),
        "X-Query-Ms":            result.query_ms.toString(),
      });

      return reply.send(result);
    });

    // ── GET /v2/climate/timeseries ───────────────────────────────────────────
    v2.get<{
      Querystring: {
        lat: string; lon: string; variable: string;
        start: string; end: string;
        aggregate?: "none" | "hourly" | "daily" | "monthly" | "annual";
        model?: string; ensemble?: string;
      };
    }>("/climate/timeseries", async (request, reply) => {
      const { lat, lon, variable, start, end, aggregate, model, ensemble } = request.query;
      const requestId = (request as any).requestId as string;

      if (!isValidISO8601(start) || !isValidISO8601(end)) {
        return reply.status(400).send({ code: "INVALID_DATE_RANGE", message: "start and end must be ISO8601 format", request_id: requestId, timestamp: new Date().toISOString() });
      }
      if (new Date(end) <= new Date(start)) {
        return reply.status(400).send({ code: "INVALID_DATE_RANGE", message: "end must be after start", request_id: requestId, timestamp: new Date().toISOString() });
      }

      const queryUrl = buildQueryUrl(`${CONFIG.queryServiceUrl}/v2/climate/timeseries`, {
        lat, lon, variable, start, end,
        ...(aggregate && { aggregate }),
        ...(model     && { model }),
        ...(ensemble  && { ensemble }),
      });

      let result: unknown;
      try {
        result = await callDownstream<unknown>(queryUrl, "GET", undefined, CONFIG.timeouts.timeseries);
      } catch (err) {
        console.error("[gateway] timeseries downstream error:", err);
        // propagate a JSON-friendly error to the client so the frontend can
        // show the underlying message instead of a blank 500 body
        return reply.status(502).send({
          error: "downstream_failure",
          message: (err as Error).message,
          request_id: requestId,
          timestamp: new Date().toISOString(),
        });
      }
      setPCMIPHeaders(reply);
      return reply.send(result);
    });

    // ── GET /v2/ensemble/stats ───────────────────────────────────────────────
    v2.get<{
      Querystring: {
        dataset: string; scenario: string; variable: string;
        region: string; horizon: string;
      };
    }>("/ensemble/stats", async (request, reply) => {
      const queryUrl = buildQueryUrl(`${CONFIG.queryServiceUrl}/v2/ensemble/stats`, request.query as Record<string, string>);
      const result   = await callDownstream<EnsembleStatsResponse>(queryUrl, "GET", undefined, CONFIG.timeouts.ensemble);
      setPCMIPHeaders(reply, { "X-Ensemble-Size": result.ensemble_size.toString() });
      return reply.send(result);
    });

    // ── POST /v2/models/validate ─────────────────────────────────────────────
    v2.post<{
      Body: {
        ai_model: string; physics_baseline: string;
        variable: string; region: string; period: string;
      };
    }>("/models/validate", {
      config: { rateLimit: { max: 10, timeWindow: "1 hour" } } as any,
    }, async (request, reply) => {
      const apiKey    = (request as any).apiKey as { keyId: string; tier: APITier };
      const requestId = (request as any).requestId as string;

      // Create async job
      const jobId = `val_${crypto.randomBytes(8).toString("hex")}`;
      const job: AsyncJob = {
        job_id:       jobId,
        job_type:     "validation",
        status:       JobStatus.PENDING,
        submitted_at: new Date().toISOString(),
        started_at:   null,
        completed_at: null,
        result_url:   `/v2/models/validate/${jobId}`,
        error:        null,
        progress_pct: 0,
        metadata:     { ...request.body, submitted_by: apiKey.keyId },
      };

      // Persist job state in Redis (TTL 24h)
      await redis.setEx(`job:${jobId}`, 86_400, JSON.stringify(job));

      // Fire-and-forget to validation service
      submitValidationJob(jobId, request.body, job).catch((err) =>
        console.error(`[validation] job ${jobId} submission failed:`, err)
      );

      reply.status(202);
      setPCMIPHeaders(reply, { "Location": `/v2/models/validate/${jobId}` });
      return reply.send(job);
    });

    // ── GET /v2/models/validate/:jobId ──────────────────────────────────────
    v2.get<{ Params: { jobId: string } }>("/models/validate/:jobId", async (request, reply) => {
      const { jobId } = request.params;
      const raw       = await redis.get(`job:${jobId}`);

      if (!raw) {
        return reply.status(404).send({
          code: "JOB_NOT_FOUND", message: `Job ${jobId} not found or expired`,
          request_id: (request as any).requestId, timestamp: new Date().toISOString(),
        });
      }

      const job = JSON.parse(raw) as AsyncJob;
      setPCMIPHeaders(reply);

      if (job.status === JobStatus.COMPLETE && job.result_url) {
        // Fetch the actual report from validation service
        const report = await callDownstream<ValidationReport>(
          `${CONFIG.validationServiceUrl}/internal/reports/${jobId}`, "GET"
        ).catch(() => null);

        if (report) {
          return reply.send({ job, report });
        }
      }

      return reply.send({ job });
    });

    // ── GET /v2/lineage/:datasetId ───────────────────────────────────────────
    v2.get<{ Params: { datasetId: string } }>("/lineage/:datasetId", async (request, reply) => {
      const { datasetId } = request.params;
      const result = await callDownstream<LineageGraph>(
        `${CONFIG.governanceServiceUrl}/v2/lineage/${datasetId}`, "GET"
      );
      setPCMIPHeaders(reply);
      return reply.send(result);
    });

    // ── GET /v2/datasets ─────────────────────────────────────────────────────
    v2.get<{
      Querystring: {
        variable?: string; source?: string; start?: string; end?: string;
        bbox?: string; cmip?: string; limit?: string; offset?: string;
      };
    }>("/datasets", async (request, reply) => {
      const queryUrl = buildQueryUrl(`${CONFIG.queryServiceUrl}/v2/datasets`, request.query as Record<string, string>);
      const result   = await callDownstream<unknown>(queryUrl, "GET", undefined, 15_000);
      setPCMIPHeaders(reply);
      return reply.send(result);
    });

    // ── GET /v2/system/health ────────────────────────────────────────────────
    v2.get("/system/health", async (_, reply) => {
      const health = await callDownstream<SystemHealth>(
        `${CONFIG.queryServiceUrl}/internal/health`, "GET", undefined, 5000
      );
      setPCMIPHeaders(reply);
      return reply.send(health);
    });

    // ── GET /v2/sources ──────────────────────────────────────────────────────
    v2.get("/sources", async (_, reply) => {
      const result = await callDownstream<unknown>(
        `${CONFIG.ingestionServiceUrl}/v2/sources`, "GET", undefined, 5000
      );
      setPCMIPHeaders(reply);
      return reply.send(result);
    });

  }, { prefix: "/v2" });

  return app;
}

// ─── VALIDATION JOB FIRE-AND-FORGET ──────────────────────────────────────────

async function submitValidationJob(
  jobId:   string,
  body:    unknown,
  job:     AsyncJob
): Promise<void> {
  // Update job status to RUNNING
  job.status     = JobStatus.RUNNING;
  job.started_at = new Date().toISOString();
  await redis.setEx(`job:${jobId}`, 86_400, JSON.stringify(job));

  try {
    await callDownstream(
      `${CONFIG.validationServiceUrl}/internal/validate`,
      "POST",
      { job_id: jobId, ...body },
      CONFIG.timeouts.validation
    );

    // On success, validation service writes report internally.
    // We just mark complete.
    job.status       = JobStatus.COMPLETE;
    job.completed_at = new Date().toISOString();
    job.progress_pct = 100;
  } catch (err) {
    job.status       = JobStatus.FAILED;
    job.error        = (err as Error).message;
    job.completed_at = new Date().toISOString();
  }

  await redis.setEx(`job:${jobId}`, 86_400, JSON.stringify(job));
}

// ─── UTILITIES ────────────────────────────────────────────────────────────────

function isValidISO8601(s: string): boolean {
  return !isNaN(Date.parse(s));
}

function buildQueryUrl(base: string, params: Record<string, string | undefined>): string {
  const url = new URL(base);
  Object.entries(params).forEach(([k, v]) => {
    if (v !== undefined) url.searchParams.set(k, v);
  });
  return url.toString();
}

// ─── ENTRYPOINT ───────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  try {
    await initClients();
    const app = await buildServer();
    await app.listen({ port: CONFIG.port, host: CONFIG.host });
    console.log(`[gateway] PCMIP API Gateway running on ${CONFIG.host}:${CONFIG.port}`);
    console.log(`[gateway] Environment: ${CONFIG.environment}`);
  } catch (err) {
    console.error("[gateway] Fatal startup error:", err);
    process.exit(1);
  }
}

// ─── GRACEFUL SHUTDOWN ────────────────────────────────────────────────────────

process.on("SIGTERM", async () => {
  console.log("[gateway] SIGTERM received — draining connections");
  await redis?.disconnect();
  await db?.end();
  process.exit(0);
});

process.on("SIGINT", async () => {
  console.log("[gateway] SIGINT received");
  await redis?.disconnect();
  await db?.end();
  process.exit(0);
});

main();
