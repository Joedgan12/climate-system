/**
 * realtime/server.js
 * Node.js WebSocket server for real-time PCMIP telemetry streaming.
 *
 * This IS the right place for Node.js in PCMIP:
 * - High-concurrency event fan-out (thousands of dashboard clients)
 * - Low-overhead event streaming (no scientific computation here)
 * - Native WebSocket support via ws library
 * - Bridges Kafka topics → WebSocket clients
 * - Bridges Prometheus metrics → WebSocket clients
 *
 * What this server does NOT do:
 * - No Zarr reads (Python's job)
 * - No climate data computation (Python/Dask's job)
 * - No HPC orchestration (SLURM/Python's job)
 * - No database writes (this is read-only telemetry)
 */

'use strict';

const { WebSocketServer, WebSocket } = require('ws');
const { Kafka } = require('kafkajs');
const http = require('http');
const https = require('https');
const url = require('url');
const EventEmitter = require('events');
const winston = require('winston');

// ─── LOGGER ────────────────────────────────────────────────────────────────────
const log = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  transports: [new winston.transports.Console()],
});

// ─── CONFIG ────────────────────────────────────────────────────────────────────
const CONFIG = {
  port: parseInt(process.env.WS_PORT || '8080', 10),
  kafkaBrokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
  kafkaGroupId: 'pcmip-realtime-ws',
  prometheusUrl: process.env.PROMETHEUS_URL || 'http://prometheus:9090',
  metricsInterval: parseInt(process.env.METRICS_INTERVAL_MS || '5000', 10),
  pingInterval: parseInt(process.env.PING_INTERVAL_MS || '30000', 10),
  maxClientsPerChannel: parseInt(process.env.MAX_CLIENTS_PER_CHANNEL || '500', 10),
  apiKeyHeader: 'x-api-key',
};

// ─── CHANNEL DEFINITIONS ───────────────────────────────────────────────────────
// Each channel maps to a set of Kafka topics or Prometheus queries
const CHANNELS = {
  'ingest.live':     { type: 'kafka', topics: ['validated.records', 'dead.letter'] },
  'compute.jobs':    { type: 'kafka', topics: ['slurm.job.events'] },
  'api.metrics':     { type: 'prometheus', query: 'pcmip_api_requests_total' },
  'validation.live': { type: 'kafka', topics: ['validation.results'] },
  'system.health':   { type: 'prometheus', query: 'up{job=~"pcmip.*"}' },
  'storage.metrics': { type: 'prometheus', query: 'pcmip_storage_tier_bytes' },
};

// ─── EVENT BUS ─────────────────────────────────────────────────────────────────
// Internal fan-out: sources emit events, clients subscribe to channels
const eventBus = new EventEmitter();
eventBus.setMaxListeners(1000); // High client counts

// ─── CLIENT REGISTRY ───────────────────────────────────────────────────────────
class ClientRegistry {
  constructor() {
    /** @type {Map<string, { ws: WebSocket, channels: Set<string>, connectedAt: Date, tier: string }>} */
    this.clients = new Map();
  }

  register(clientId, ws, tier = 'research') {
    this.clients.set(clientId, {
      ws,
      channels: new Set(),
      connectedAt: new Date(),
      tier,
    });
    log.info('client_connected', { clientId, tier, total: this.clients.size });
  }

  unregister(clientId) {
    this.clients.delete(clientId);
    log.info('client_disconnected', { clientId, remaining: this.clients.size });
  }

  subscribe(clientId, channel) {
    const client = this.clients.get(clientId);
    if (!client) return false;

    // Enforce per-channel client limit
    const channelClients = [...this.clients.values()].filter(c => c.channels.has(channel));
    if (channelClients.length >= CONFIG.maxClientsPerChannel) {
      log.warn('channel_client_limit_reached', { channel, limit: CONFIG.maxClientsPerChannel });
      return false;
    }

    client.channels.add(channel);
    return true;
  }

  unsubscribe(clientId, channel) {
    const client = this.clients.get(clientId);
    if (client) client.channels.delete(channel);
  }

  /** Broadcast an event to all clients subscribed to a channel */
  broadcast(channel, event) {
    const payload = JSON.stringify({ channel, ...event });
    let delivered = 0;

    for (const [clientId, client] of this.clients) {
      if (!client.channels.has(channel)) continue;
      if (client.ws.readyState !== WebSocket.OPEN) {
        this.unregister(clientId);
        continue;
      }
      try {
        client.ws.send(payload);
        delivered++;
      } catch (err) {
        log.warn('send_failed', { clientId, error: err.message });
        this.unregister(clientId);
      }
    }
    return delivered;
  }

  stats() {
    const channelCounts = {};
    for (const client of this.clients.values()) {
      for (const ch of client.channels) {
        channelCounts[ch] = (channelCounts[ch] || 0) + 1;
      }
    }
    return { totalClients: this.clients.size, channelCounts };
  }
}

const registry = new ClientRegistry();

// ─── KAFKA BRIDGE ──────────────────────────────────────────────────────────────
class KafkaBridge {
  constructor() {
    this.kafka = new Kafka({
      clientId: 'pcmip-ws-server',
      brokers: CONFIG.kafkaBrokers,
      retry: { retries: 5, initialRetryTime: 300, factor: 2 },
    });
    this.consumers = new Map(); // topic → consumer
  }

  async startConsuming(topics) {
    const consumer = this.kafka.consumer({ groupId: CONFIG.kafkaGroupId });
    await consumer.connect();
    await consumer.subscribe({ topics, fromBeginning: false });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const value = message.value?.toString();
          if (!value) return;

          const parsed = JSON.parse(value);
          const channel = this._topicToChannel(topic);

          if (!channel) return;

          const event = {
            type: 'kafka_event',
            topic,
            timestamp: new Date().toISOString(),
            data: this._sanitiseForBroadcast(parsed, topic),
          };

          const delivered = registry.broadcast(channel, event);
          log.debug('kafka_event_broadcast', { topic, channel, delivered });
        } catch (err) {
          log.warn('kafka_message_parse_error', { topic, error: err.message });
        }
      },
    });

    log.info('kafka_consumer_started', { topics });
    return consumer;
  }

  _topicToChannel(topic) {
    for (const [channel, config] of Object.entries(CHANNELS)) {
      if (config.type === 'kafka' && config.topics.includes(topic)) {
        return channel;
      }
    }
    return null;
  }

  _sanitiseForBroadcast(record, topic) {
    /**
     * Strip PII and sensitive fields before broadcasting.
     * API keys, raw hashes, and internal IDs must not reach browser clients.
     */
    const safe = { ...record };
    delete safe.api_key;
    delete safe.raw_hash;  // SHA-256 of raw record — internal only
    delete safe.raw_payload_b64;  // dead letter raw bytes — internal only

    // For validated records, include only summary fields
    if (topic === 'validated.records') {
      return {
        source_id: safe._provenance?.source_id,
        dataset_id: safe._provenance?.dataset_id,
        variable: safe.variable,
        quality_flags: safe._provenance?.quality_flags,
        ingest_ts: safe._provenance?.ingest_ts,
        lat: safe.lat,
        lon: safe.lon,
      };
    }

    // For dead letter, include reason but not raw payload
    if (topic === 'dead.letter') {
      return {
        source_id: safe.source_id,
        error_type: safe.error_type,
        error_detail: safe.error_detail?.slice(0, 200), // truncate
        failed_at: safe.failed_at,
      };
    }

    return safe;
  }

  async startAll() {
    const allTopics = Object.values(CHANNELS)
      .filter(c => c.type === 'kafka')
      .flatMap(c => c.topics);

    await this.startConsuming([...new Set(allTopics)]);
  }
}

// ─── PROMETHEUS BRIDGE ─────────────────────────────────────────────────────────
class PrometheusBridge {
  constructor() {
    this._timers = new Map();
  }

  _query(promQuery) {
    return new Promise((resolve, reject) => {
      const queryUrl = `${CONFIG.prometheusUrl}/api/v1/query?query=${encodeURIComponent(promQuery)}`;
      const client = queryUrl.startsWith('https') ? https : http;

      client.get(queryUrl, (res) => {
        let data = '';
        res.on('data', chunk => data += chunk);
        res.on('end', () => {
          try {
            resolve(JSON.parse(data));
          } catch (e) {
            reject(e);
          }
        });
      }).on('error', reject);
    });
  }

  async startPolling(channel, promQuery, intervalMs) {
    const poll = async () => {
      try {
        const result = await this._query(promQuery);
        if (result.status === 'success') {
          registry.broadcast(channel, {
            type: 'metrics_update',
            timestamp: new Date().toISOString(),
            query: promQuery,
            data: result.data.result,
          });
        }
      } catch (err) {
        log.warn('prometheus_poll_error', { channel, error: err.message });
      }
    };

    const timer = setInterval(poll, intervalMs);
    this._timers.set(channel, timer);
    poll(); // immediate first poll
    log.info('prometheus_polling_started', { channel, query: promQuery, intervalMs });
  }

  async startAll() {
    for (const [channel, config] of Object.entries(CHANNELS)) {
      if (config.type === 'prometheus') {
        await this.startPolling(channel, config.query, CONFIG.metricsInterval);
      }
    }
  }

  stop() {
    for (const timer of this._timers.values()) clearInterval(timer);
  }
}

// ─── MESSAGE PROTOCOL ──────────────────────────────────────────────────────────
/**
 * Client → Server messages:
 *   { "action": "subscribe",   "channel": "ingest.live" }
 *   { "action": "unsubscribe", "channel": "ingest.live" }
 *   { "action": "ping" }
 *
 * Server → Client messages:
 *   { "channel": "ingest.live", "type": "kafka_event", "timestamp": "...", "data": {...} }
 *   { "channel": "api.metrics", "type": "metrics_update", "timestamp": "...", "data": [...] }
 *   { "type": "subscribed",   "channel": "..." }
 *   { "type": "unsubscribed", "channel": "..." }
 *   { "type": "error",        "code": "...", "message": "..." }
 *   { "type": "pong" }
 */

function handleClientMessage(clientId, raw) {
  let msg;
  try {
    msg = JSON.parse(raw);
  } catch {
    return { type: 'error', code: 'INVALID_JSON', message: 'Message must be valid JSON' };
  }

  const { action, channel } = msg;

  if (action === 'ping') {
    return { type: 'pong', ts: new Date().toISOString() };
  }

  if (action === 'subscribe') {
    if (!CHANNELS[channel]) {
      return { type: 'error', code: 'UNKNOWN_CHANNEL', message: `Channel '${channel}' does not exist` };
    }
    const ok = registry.subscribe(clientId, channel);
    if (!ok) {
      return { type: 'error', code: 'CHANNEL_FULL', message: `Channel '${channel}' has reached its client limit` };
    }
    log.info('client_subscribed', { clientId, channel });
    return { type: 'subscribed', channel, available_channels: Object.keys(CHANNELS) };
  }

  if (action === 'unsubscribe') {
    registry.unsubscribe(clientId, channel);
    return { type: 'unsubscribed', channel };
  }

  return { type: 'error', code: 'UNKNOWN_ACTION', message: `Unknown action '${action}'` };
}

// ─── SERVER SETUP ──────────────────────────────────────────────────────────────
async function main() {
  const httpServer = http.createServer((req, res) => {
    const { pathname } = url.parse(req.url);

    if (pathname === '/health') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'ok', ...registry.stats() }));
      return;
    }

    res.writeHead(404);
    res.end('Not found');
  });

  const wss = new WebSocketServer({ server: httpServer });

  wss.on('connection', (ws, req) => {
    const clientId = `client_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    const apiKey = req.headers[CONFIG.apiKeyHeader];
    // In production: validate apiKey against Redis, determine tier
    const tier = apiKey ? 'research' : 'anonymous';

    registry.register(clientId, ws, tier);

    // Send welcome message with available channels
    ws.send(JSON.stringify({
      type: 'welcome',
      client_id: clientId,
      available_channels: Object.keys(CHANNELS),
      server_version: '1.0.0',
    }));

    // Keepalive ping
    const pingTimer = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.ping();
      }
    }, CONFIG.pingInterval);

    ws.on('message', (raw) => {
      const response = handleClientMessage(clientId, raw.toString());
      if (response && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(response));
      }
    });

    ws.on('close', () => {
      clearInterval(pingTimer);
      registry.unregister(clientId);
    });

    ws.on('error', (err) => {
      log.warn('ws_client_error', { clientId, error: err.message });
      clearInterval(pingTimer);
      registry.unregister(clientId);
    });
  });

  // Start Kafka and Prometheus bridges
  const kafkaBridge = new KafkaBridge();
  const promBridge = new PrometheusBridge();

  await kafkaBridge.startAll();
  await promBridge.startAll();

  // Stats broadcast every 60s
  setInterval(() => {
    registry.broadcast('system.health', {
      type: 'server_stats',
      timestamp: new Date().toISOString(),
      data: registry.stats(),
    });
  }, 60_000);

  httpServer.listen(CONFIG.port, () => {
    log.info('pcmip_ws_server_started', { port: CONFIG.port, channels: Object.keys(CHANNELS) });
  });

  // Graceful shutdown
  const shutdown = async (signal) => {
    log.info('shutdown_signal', { signal });
    promBridge.stop();
    wss.close();
    httpServer.close(() => {
      log.info('server_stopped');
      process.exit(0);
    });
  };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT',  () => shutdown('SIGINT'));
}

main().catch((err) => {
  log.error('startup_failed', { error: err.message, stack: err.stack });
  process.exit(1);
});
