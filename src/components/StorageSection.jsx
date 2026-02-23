import RatingBadge from "./RatingBadge";

const StorageSection = () => (
    <section id="storage" style={{ padding: "96px 0", background: "white" }}>
        <div style={{ maxWidth: 1280, margin: "0 auto", padding: "0 24px" }}>
            <div style={{ textAlign: "center", marginBottom: 56 }}>
                <div style={{ fontSize: 12, color: "var(--teal-700)", fontWeight: 600, letterSpacing: "0.1em", textTransform: "uppercase", marginBottom: 12 }}>
                    Layer 3 — Storage Architecture
                </div>
                <h2 style={{ fontFamily: "var(--font-serif)", fontSize: 40, fontWeight: 400, lineHeight: 1.2 }}>Cloud-native Zarr archive</h2>
                <p style={{ fontSize: 15, color: "var(--gray-600)", lineHeight: 1.8, maxWidth: 600, margin: "16px auto 0" }}>
                    Tiered object storage with Zarr v3 chunking, STAC cataloguing, and Dask-powered distributed queries. 361 PB under management across three tiers.
                </p>
            </div>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 20, marginBottom: 48 }}>
                {[
                    { tier: "HOT", desc: "Active analyses & API cache", size: "2.4 PB", cap: "5 PB", pct: 48, latency: "12ms p99", color: "var(--teal-700)", bg: "var(--teal-50)" },
                    { tier: "WARM", desc: "Recent reanalysis & model runs", size: "18.7 PB", cap: "50 PB", pct: 37, latency: "85ms p99", color: "var(--teal-600)", bg: "#f0fbf7" },
                    { tier: "COLD", desc: "Historical archive · Permanent", size: "340 PB", cap: "1 EB", pct: 33, latency: "4–12 hours", color: "var(--gray-600)", bg: "var(--gray-50)" },
                ].map(t => (
                    <div key={t.tier} style={{ background: t.bg, border: `1px solid ${t.color}22`, borderTop: `4px solid ${t.color}`, borderRadius: 10, padding: 28 }}>
                        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 20 }}>
                            <div>
                                <div style={{ fontFamily: "var(--font-serif)", fontSize: 22, color: t.color, marginBottom: 4 }}>{t.tier}</div>
                                <div style={{ fontSize: 13, color: "var(--gray-600)" }}>{t.desc}</div>
                            </div>
                            <div style={{ textAlign: "right" }}>
                                <div style={{ fontFamily: "var(--font-serif)", fontSize: 26, color: t.color }}>{t.size}</div>
                                <div style={{ fontSize: 11, color: "var(--gray-400)" }}>of {t.cap}</div>
                            </div>
                        </div>
                        <div style={{ height: 8, background: `${t.color}22`, borderRadius: 4, overflow: "hidden", marginBottom: 12 }}>
                            <div style={{ height: "100%", width: `${t.pct}%`, background: t.color, borderRadius: 4 }} />
                        </div>
                        <div style={{ display: "flex", justifyContent: "space-between" }}>
                            <span style={{ fontSize: 12, color: "var(--gray-500)", fontFamily: "var(--font-mono)" }}>Latency: {t.latency}</span>
                            <span style={{ fontSize: 12, color: t.color, fontWeight: 600 }}>{t.pct}% used</span>
                        </div>
                    </div>
                ))}
            </div>
            <div style={{ background: "var(--gray-50)", border: "1px solid var(--gray-200)", borderRadius: 10, padding: 32 }}>
                <h3 style={{ fontFamily: "var(--font-serif)", fontSize: 22, marginBottom: 20 }}>Format Compatibility</h3>
                <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 12 }}>
                    {[
                        { fmt: "Zarr v3", status: "PRIMARY", note: "Cloud-native, parallel reads, Xarray native", rating: "compatible" },
                        { fmt: "NetCDF-4", status: "SUPPORTED", note: "Legacy compatibility layer, read/write", rating: "almost" },
                        { fmt: "GRIB2", status: "SUPPORTED", note: "Operational forecasting bridge (ECMWF)", rating: "almost" },
                        { fmt: "HDF5", status: "LEGACY", note: "Read-only, migration queue active", rating: "insufficient" },
                    ].map(f => (
                        <div key={f.fmt} style={{ background: "white", border: "1px solid var(--gray-200)", borderRadius: 8, padding: 16 }}>
                            <div style={{ fontFamily: "var(--font-mono)", fontSize: 14, fontWeight: 500, color: "var(--gray-900)", marginBottom: 8 }}>{f.fmt}</div>
                            <RatingBadge rating={f.rating} size="sm" />
                            <div style={{ fontSize: 12, color: "var(--gray-500)", marginTop: 10, lineHeight: 1.5 }}>{f.note}</div>
                        </div>
                    ))}
                </div>
            </div>
        </div>
    </section>
);

export default StorageSection;
