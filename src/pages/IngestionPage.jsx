import { useState, useEffect } from "react";
import RatingBadge from "../components/RatingBadge";

const IngestionPage = () => {
    const [sources, setSources] = useState([]);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        fetch('/api/ingestion/sources')
            .then(res => res.json())
            .then(data => {
                setSources(data.sources);
                setLoading(false);
            })
            .catch(err => console.error(err));
    }, []);

    const statusColors = { online: "var(--teal-600)", warning: "var(--amber)", error: "var(--red)" };

    return (
        <div style={{ paddingTop: 120, paddingBottom: 96, background: "var(--white)", minHeight: "100vh" }}>
            <div style={{ maxWidth: 1280, margin: "0 auto", padding: "0 24px" }}>
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 64, alignItems: "start", marginBottom: 64 }}>
                    <div>
                        <div style={{ fontSize: 12, color: "var(--teal-700)", fontWeight: 600, letterSpacing: "0.1em", textTransform: "uppercase", marginBottom: 12 }}>Layer 1 — Data Ingestion</div>
                        <h2 style={{ fontFamily: "var(--font-serif)", fontSize: 40, fontWeight: 400, lineHeight: 1.2, color: "var(--gray-900)", marginBottom: 20 }}>Planetary-scale observation<br />pipeline</h2>
                        <p style={{ fontSize: 15, color: "var(--gray-600)", lineHeight: 1.8 }}>
                            Nine live data sources feeding a Kafka-backed event-driven pipeline with real-time schema validation, physical constraint checking, and SHA-256 provenance fingerprinting. Every record is CMIP7-normalised before storage.
                        </p>
                    </div>
                    <div style={{ background: "var(--gray-50)", border: "1px solid var(--gray-200)", borderRadius: 24, padding: 32 }}>
                        <div style={{ fontSize: 12, color: "var(--gray-400)", marginBottom: 16, fontFamily: "var(--font-mono)", textTransform: "uppercase", letterSpacing: "0.08em" }}>Pipeline flow</div>
                        {["INGEST → Kafka Raw", "VALIDATE → Schema + Physics", "NORMALISE → CMIP7 + CF-1.10", "FINGERPRINT → SHA-256", "ROUTE → Zarr Storage"].map((step, i, arr) => (
                            <div key={step} style={{ display: "flex", alignItems: "center" }}>
                                <div style={{ display: "flex", flexDirection: "column", alignItems: "center", marginRight: 16, minWidth: 32 }}>
                                    <div style={{
                                        width: 32, height: 32, background: i === 0 ? "var(--teal-700)" : "var(--teal-100)",
                                        border: `2px solid ${i === 0 ? "var(--teal-700)" : "var(--teal-400)"}`,
                                        borderRadius: "50%", display: "flex", alignItems: "center", justifyContent: "center",
                                        fontFamily: "var(--font-mono)", fontSize: 11,
                                        color: i === 0 ? "white" : "var(--teal-700)", fontWeight: 600, flexShrink: 0,
                                    }}>{i + 1}</div>
                                    {i < arr.length - 1 && <div style={{ width: 2, height: 20, background: "var(--teal-200)" }} />}
                                </div>
                                <div style={{
                                    fontFamily: "var(--font-mono)", fontSize: 12, color: "var(--gray-700)",
                                    padding: "8px 12px", background: "white", border: "1px solid var(--gray-200)",
                                    borderRadius: 30, flex: 1, marginBottom: i < arr.length - 1 ? 4 : 0,
                                }}>{step}</div>
                            </div>
                        ))}
                    </div>
                </div>

                {loading ? (
                    <div style={{ padding: 48, textAlign: "center", color: "var(--gray-500)", fontFamily: "var(--font-mono)" }}>Loading sources from API...</div>
                ) : (
                    <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 16 }}>
                        {sources.map((s) => (
                            <div key={s.name} style={{
                                background: "white", border: "1px solid var(--gray-200)",
                                borderTop: `4px solid ${statusColors[s.status]}`, borderRadius: 16, padding: 24,
                                transition: "box-shadow 0.2s, transform 0.2s", cursor: "default",
                            }}
                                onMouseEnter={e => { e.currentTarget.style.boxShadow = "0 8px 32px rgba(0,0,0,0.1)"; e.currentTarget.style.transform = "translateY(-2px)"; }}
                                onMouseLeave={e => { e.currentTarget.style.boxShadow = "none"; e.currentTarget.style.transform = "translateY(0)"; }}
                            >
                                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 12 }}>
                                    <div>
                                        <div style={{ fontWeight: 600, fontSize: 14, color: "var(--gray-900)", marginBottom: 2 }}>{s.name}</div>
                                        <div style={{ fontSize: 12, color: "var(--gray-400)" }}>{s.org}</div>
                                    </div>
                                    <div style={{ display: "flex", alignItems: "center", gap: 5 }}>
                                        <span style={{ width: 7, height: 7, borderRadius: "50%", background: statusColors[s.status], display: "inline-block" }} />
                                        <span style={{ fontSize: 11, color: statusColors[s.status], fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.06em" }}>{s.status}</span>
                                    </div>
                                </div>
                                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                                    <div>
                                        <div style={{ fontFamily: "var(--font-mono)", fontSize: 16, color: "var(--teal-700)", fontWeight: 500 }}>{s.volume}</div>
                                        <div style={{ fontSize: 11, color: "var(--gray-400)", marginTop: 2 }}>Lag: {s.latency}</div>
                                    </div>
                                    <RatingBadge rating={s.rating} size="sm" />
                                </div>
                            </div>
                        ))}
                    </div>
                )}

                <div style={{
                    marginTop: 32, background: "var(--teal-900)", borderRadius: 24, padding: "32px 40px",
                    display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 32,
                }}>
                    {[
                        { label: "Schema validation pass", value: "99.71%", sub: "last 24h" },
                        { label: "Physics constraint pass", value: "98.34%", sub: "last 24h" },
                        { label: "CMIP7 compliant", value: "100%", sub: "new records" },
                        { label: "Dead-letter rejects", value: "0.29%", sub: "under review" },
                    ].map(({ label, value, sub }) => (
                        <div key={label} style={{ textAlign: "center" }}>
                            <div style={{ fontFamily: "var(--font-serif)", fontSize: 32, color: "var(--teal-400)", lineHeight: 1 }}>{value}</div>
                            <div style={{ fontSize: 13, color: "rgba(255,255,255,0.6)", marginTop: 6 }}>{label}</div>
                            <div style={{ fontSize: 11, color: "rgba(255,255,255,0.35)", marginTop: 3, fontFamily: "var(--font-mono)" }}>{sub}</div>
                        </div>
                    ))}
                </div>
            </div>
        </div>
    );
};

export default IngestionPage;
