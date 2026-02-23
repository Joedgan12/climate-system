import express from 'express';
import cors from 'cors';

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

// ─── MOCK DATASTORE ──────────────────────────────────────────────────────────

const tickerItems = [
    "🟢 ERA5 ingestion nominal · 2.2 TB/h",
    "🟡 MODIS-Terra health degraded · 61%",
    "🟢 CESM2 JOB-04821 · 68% complete",
    "🔵 GraphCast validation cleared · SHORT-MEDIUM range approved",
    "🟢 CMIP7 compliance · 100% on new records",
    "🔴 Dead-letter queue: 12 physics constraint violations · under review",
    "🟢 API p99 latency · 340ms",
    "🟢 ARGO Float array · 4,000 active floats ingesting",
    "🟢 STAC catalog · 847,291 datasets indexed",
    "🟡 Pangu-Weather validation · drift check running",
];

const ingestionSources = [
    { name: "ERA5 Reanalysis", org: "ECMWF", volume: "2.2 TB/h", status: "online", rating: "compatible", latency: "4 min" },
    { name: "GOES-16/17/18", org: "NOAA", volume: "840 GB/h", status: "online", rating: "compatible", latency: "6 min" },
    { name: "SENTINEL-6 MF", org: "Copernicus", volume: "120 GB/h", status: "online", rating: "compatible", latency: "12 min" },
    { name: "ARGO Float Array", org: "Argo International", volume: "8 GB/h", status: "online", rating: "almost", latency: "22 min" },
    { name: "NEXRAD Radar", org: "NOAA NWS", volume: "480 GB/h", status: "online", rating: "compatible", latency: "5 min" },
    { name: "MODIS Terra/Aqua", org: "NASA", volume: "360 GB/h", status: "warning", rating: "insufficient", latency: "48 min" },
];

const computeJobs = [
    { id: "JOB-04821", model: "CESM2.1", type: "Physics", progress: 68, status: "running", cores: "16,384", eta: "23h 14m", rating: "compatible" },
    { id: "JOB-04822", model: "IFS CY48r1", type: "Physics", progress: 31, status: "running", cores: "8,192", eta: "33h 2m", rating: "compatible" },
    { id: "JOB-04823", model: "GraphCast v2", type: "AI", progress: 84, status: "running", cores: "512 GPU", eta: "38m", rating: "almost" },
    { id: "JOB-04824", model: "CESM2 VAL", type: "PostProc", progress: 0, status: "queued", cores: "256", eta: "—", rating: "insufficient" },
];

const validationModels = [
    { name: "IFS CY48r1", org: "ECMWF", type: "Physics", rmse: 119.4, consistency: 99.8, safeRange: "All ranges", rating: "compatible", warnings: 0 },
    { name: "AIFS v1.4", org: "ECMWF", type: "AI-Hybrid", rmse: 128.1, consistency: 97.1, safeRange: "Medium (120h)", rating: "almost", warnings: 0 },
    { name: "GraphCast v2", org: "Google DeepMind", type: "AI", rmse: 142.3, consistency: 94.2, safeRange: "Short-Medium (72h)", rating: "almost", warnings: 2 },
    { name: "Pangu-Weather", org: "Huawei", type: "AI", rmse: 156.8, consistency: 91.7, safeRange: "Short (48h)", rating: "insufficient", warnings: 3 },
    { name: "Fuxi v1.0", org: "Fudan Univ.", type: "AI", rmse: "—", consistency: "—", safeRange: "Suspended", rating: "critical", warnings: "—" },
];

// ─── DASHBOARD ENDPOINTS ─────────────────────────────────────────────────────

app.get('/api/ticker', (req, res) => {
    res.json({ items: tickerItems });
});

app.get('/api/ingestion/sources', (req, res) => {
    res.json({ sources: ingestionSources });
});

app.get('/api/compute/jobs', (req, res) => {
    res.json({ jobs: computeJobs });
});

app.get('/api/validation/models', (req, res) => {
    res.json({ models: validationModels });
});

// ─── LIVE THERMOMETER ENDPOINT ───────────────────────────────────────────────

app.get('/api/thermometer', (req, res) => {
    // Return a slightly oscillating value around 1.42 C
    const baseTemp = 1.42;
    const time = Date.now() / 2000;
    const fluctuation = Math.sin(time) * 0.03 + Math.sin(time * 0.4) * 0.02;
    res.json({ current: Number((baseTemp + fluctuation).toFixed(3)) });
});

// ─── API EXPLORER 'LIVE' ENDPOINTS ───────────────────────────────────────────

app.get('/v2/climate/variable', (req, res) => {
    // Mock response for the API explorer
    setTimeout(() => {
        res.json({
            variable: req.query.variable || "air_temperature",
            value: 288.42,
            unit: "K",
            uncertainty: { p05: 287.1, p95: 289.8 },
            provenance: { dataset_id: "ds_c4f8a2", cmip_standard: "CMIP7" }
        });
    }, 150); // Simulate network latency
});

app.get('/v2/ensemble/stats', (req, res) => {
    setTimeout(() => {
        res.json({
            ensemble_size: 48,
            mean_warming: 2.14,
            p10: 1.87,
            p90: 2.61,
            models_agreeing: "83%",
            physically_consistent: "91%"
        });
    }, 200);
});

app.post('/v2/models/validate', (req, res) => {
    setTimeout(() => {
        res.json({
            rmse: 142.3,
            bias: -2.1,
            physical_consistency: "94.2%",
            drift_detected: false,
            recommendation: "SAFE_FOR_MEDIUM_RANGE"
        });
    }, 350);
});

// ─── SERVER START ────────────────────────────────────────────────────────────

app.listen(PORT, () => {
    console.log(`PCMIP Backend API Gateway listening on port ${PORT}`);
});
