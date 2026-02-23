import { useState } from "react";

const APISection = () => {
    const [activeEndpoint, setActiveEndpoint] = useState(0);
    const [showResponse, setShowResponse] = useState(false);

    const endpoints = [
        {
            method: "GET", path: "/v2/climate/variable",
            desc: "Query a climate variable at a point location, time, and pressure level",
            params: `lat=51.5\nlon=-0.1\nvariable=air_temperature\ntime=2024-06-01\nlevel=surface`,
            response: `{\n  "variable": "air_temperature",\n  "value": 288.42,\n  "unit": "K",\n  "uncertainty": {\n    "p05": 287.1,\n    "p95": 289.8\n  },\n  "provenance": {\n    "dataset_id": "ds_c4f8a2",\n    "cmip_standard": "CMIP7"\n  }\n}`,
        },
        {
            method: "GET", path: "/v2/ensemble/stats",
            desc: "Ensemble statistics with uncertainty metadata for a region and scenario",
            params: `dataset=CMIP7-ScenarioMIP\nscenario=ssp245\nvariable=tasmax\nregion=AFR\nhorizon=2050-2100`,
            response: `{\n  "ensemble_size": 48,\n  "mean_warming": 2.14,\n  "p10": 1.87,\n  "p90": 2.61,\n  "models_agreeing": "83%",\n  "physically_consistent": "91%"\n}`,
        },
        {
            method: "POST", path: "/v2/models/validate",
            desc: "Submit an AI model forecast for physics-aware validation",
            params: `ai_model=graphcast-v2\nphysics_baseline=IFS-cy48\nvariable=z500\nregion=GLOBAL`,
            response: `{\n  "rmse": 142.3,\n  "bias": -2.1,\n  "physical_consistency": "94.2%",\n  "drift_detected": false,\n  "recommendation": "SAFE_FOR_MEDIUM_RANGE"\n}`,
        },
    ];

    const ep = endpoints[activeEndpoint];

    return (
        <section id="api" style={{ padding: "96px 0", background: "var(--gray-50)" }}>
            <div style={{ maxWidth: 1280, margin: "0 auto", padding: "0 24px" }}>
                <div style={{ display: "grid", gridTemplateColumns: "360px 1fr", gap: 48, alignItems: "start" }}>
                    <div>
                        <div style={{ fontSize: 12, color: "var(--teal-700)", fontWeight: 600, letterSpacing: "0.1em", textTransform: "uppercase", marginBottom: 12 }}>Layer 4 — API Gateway</div>
                        <h2 style={{ fontFamily: "var(--font-serif)", fontSize: 38, fontWeight: 400, lineHeight: 1.2, marginBottom: 20 }}>Versioned, uncertainty-aware API</h2>
                        <p style={{ fontSize: 15, color: "var(--gray-600)", lineHeight: 1.8, marginBottom: 28 }}>
                            Every response carries uncertainty bounds, provenance IDs, and CMIP7 metadata. The API most climate institutions don't have yet.
                        </p>
                        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                            {endpoints.map((ep, i) => (
                                <button key={ep.path} onClick={() => { setActiveEndpoint(i); setShowResponse(false); }} style={{
                                    background: activeEndpoint === i ? "var(--teal-900)" : "white",
                                    border: `1px solid ${activeEndpoint === i ? "var(--teal-900)" : "var(--gray-200)"}`,
                                    borderRadius: 8, padding: "12px 16px", textAlign: "left", cursor: "pointer", transition: "all 0.2s",
                                }}>
                                    <div style={{ display: "flex", gap: 8, alignItems: "center", marginBottom: 4 }}>
                                        <span style={{
                                            fontFamily: "var(--font-mono)", fontSize: 10, fontWeight: 700,
                                            color: ep.method === "POST" ? (activeEndpoint === i ? "#ffa07a" : "var(--orange)") : (activeEndpoint === i ? "var(--teal-400)" : "var(--teal-700)"),
                                            background: activeEndpoint === i ? "rgba(255,255,255,0.1)" : `${ep.method === "POST" ? "#fff0e8" : "var(--teal-50)"}`,
                                            padding: "1px 6px", borderRadius: 2,
                                        }}>{ep.method}</span>
                                        <span style={{ fontFamily: "var(--font-mono)", fontSize: 12, color: activeEndpoint === i ? "white" : "var(--gray-700)" }}>{ep.path}</span>
                                    </div>
                                    <div style={{ fontSize: 12, color: activeEndpoint === i ? "rgba(255,255,255,0.6)" : "var(--gray-400)" }}>{ep.desc}</div>
                                </button>
                            ))}
                        </div>
                        <div style={{ marginTop: 28, padding: "16px", background: "var(--teal-50)", border: "1px solid var(--teal-200)", borderRadius: 8 }}>
                            <div style={{ fontSize: 13, color: "var(--teal-800)", lineHeight: 1.6 }}>
                                <strong>What's missing elsewhere:</strong> Most climate institutions still distribute raw files via FTP. No API versioning. No uncertainty metadata. No provenance IDs.
                            </div>
                        </div>
                    </div>
                    <div style={{ background: "var(--gray-900)", borderRadius: 12, overflow: "hidden" }}>
                        <div style={{ background: "#1a1a1a", padding: "12px 20px", display: "flex", alignItems: "center", gap: 8, borderBottom: "1px solid #2a2a2a" }}>
                            {["#ff5f56", "#ffbd2e", "#27c93f"].map(c => (
                                <div key={c} style={{ width: 12, height: 12, borderRadius: "50%", background: c }} />
                            ))}
                            <span style={{ fontFamily: "var(--font-mono)", fontSize: 12, color: "#555", marginLeft: 8 }}>PCMIP API Explorer</span>
                        </div>
                        <div style={{ padding: 24 }}>
                            <div style={{ marginBottom: 20 }}>
                                <div style={{ fontFamily: "var(--font-mono)", fontSize: 11, color: "#555", marginBottom: 8, textTransform: "uppercase", letterSpacing: "0.08em" }}>Request</div>
                                <div style={{ background: "#111", borderRadius: 6, padding: 16 }}>
                                    <div style={{ fontFamily: "var(--font-mono)", fontSize: 13, color: "#4dcba0", marginBottom: 12 }}>
                                        <span style={{ color: "#ffa07a" }}>{ep.method}</span>{" "}
                                        <span style={{ color: "#87ceeb" }}>https://api.pcmip.earth{ep.path}</span>
                                    </div>
                                    {ep.params.split("\n").map((line, i) => (
                                        <div key={i} style={{ fontFamily: "var(--font-mono)", fontSize: 12, color: "#888", lineHeight: 1.8 }}>
                                            <span style={{ color: "#a0c4a0" }}>{line.split("=")[0]}</span>
                                            <span style={{ color: "#555" }}>=</span>
                                            <span style={{ color: "#f0c070" }}>{line.split("=")[1]}</span>
                                        </div>
                                    ))}
                                </div>
                            </div>
                            <button onClick={() => setShowResponse(true)} style={{
                                width: "100%", background: "var(--teal-600)", color: "white", border: "none",
                                borderRadius: 6, padding: "10px", fontFamily: "var(--font-sans)", fontWeight: 600,
                                fontSize: 14, cursor: "pointer", marginBottom: 20, transition: "background 0.2s",
                            }}>▶ Execute Request</button>
                            {showResponse && (
                                <div style={{ animation: "fadeIn 0.3s ease" }}>
                                    <div style={{ fontFamily: "var(--font-mono)", fontSize: 11, color: "#555", marginBottom: 8, textTransform: "uppercase", letterSpacing: "0.08em", display: "flex", justifyContent: "space-between" }}>
                                        <span>Response</span>
                                        <span style={{ color: "#27c93f" }}>200 OK · 142ms</span>
                                    </div>
                                    <div style={{ background: "#111", borderRadius: 6, padding: 16 }}>
                                        {ep.response.split("\n").map((line, i) => {
                                            const isKey = line.includes('"') && line.includes(':');
                                            const isNum = line.match(/: [\d.]+,?$/);
                                            const isBool = line.match(/: (true|false)/);
                                            return (
                                                <div key={i} style={{ fontFamily: "var(--font-mono)", fontSize: 12, lineHeight: 1.8 }}>
                                                    {isKey ? (
                                                        <>
                                                            <span style={{ color: "#a0c4a0" }}>{line.substring(0, line.indexOf(':') + 1)}</span>
                                                            <span style={{ color: isNum ? "#f0c070" : isBool ? "#ff9eb5" : "#87ceeb" }}>{line.substring(line.indexOf(':') + 1)}</span>
                                                        </>
                                                    ) : (<span style={{ color: "#555" }}>{line}</span>)}
                                                </div>
                                            );
                                        })}
                                    </div>
                                </div>
                            )}
                        </div>
                    </div>
                </div>
            </div>
        </section>
    );
};

export default APISection;
