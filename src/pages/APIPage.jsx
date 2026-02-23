import { useState } from "react";

const APIPage = () => {
    const [activeEndpoint, setActiveEndpoint] = useState(0);
    const [showResponse, setShowResponse] = useState(false);
    const [responsePayload, setResponsePayload] = useState(null);
    const [loading, setLoading] = useState(false);
    const [executeTime, setExecuteTime] = useState(0);

    const endpoints = [
        {
            method: "GET", path: "/v2/climate/variable",
            desc: "Query a climate variable at a point location, time, and pressure level",
            params: `lat=51.5\nlon=-0.1\nvariable=air_temperature\ntime=2024-06-01\nlevel=surface`,
            fetchOptions: { method: "GET" }
        },
        {
            method: "GET", path: "/v2/ensemble/stats",
            desc: "Ensemble statistics with uncertainty metadata for a region and scenario",
            params: `dataset=CMIP7-ScenarioMIP\nscenario=ssp245\nvariable=tasmax\nregion=AFR\nhorizon=2050-2100`,
            fetchOptions: { method: "GET" }
        },
        {
            method: "POST", path: "/v2/models/validate",
            desc: "Submit an AI model forecast for physics-aware validation",
            params: `ai_model=graphcast-v2\nphysics_baseline=IFS-cy48\nvariable=z500\nregion=GLOBAL`,
            fetchOptions: { method: "POST" }
        },
    ];

    const ep = endpoints[activeEndpoint];

    const executeRequest = async () => {
        setLoading(true);
        setShowResponse(false);

        // In a real scenario we would pass the params properly, 
        // but here we just hit our actual Express mock endpoint
        const startTime = performance.now();
        try {
            const res = await fetch(ep.path, ep.fetchOptions);
            const data = await res.json();
            setResponsePayload(JSON.stringify(data, null, 2));
        } catch (err) {
            setResponsePayload(JSON.stringify({ error: err.message }, null, 2));
        } finally {
            const endTime = performance.now();
            setExecuteTime(Math.round(endTime - startTime));
            setLoading(false);
            setShowResponse(true);
        }
    };

    return (
        <div style={{ paddingTop: 120, paddingBottom: 96, background: "var(--gray-50)", minHeight: "100vh" }}>
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
                            <button onClick={executeRequest} disabled={loading} style={{
                                width: "100%", background: loading ? "var(--teal-800)" : "var(--teal-600)", color: "white", border: "none",
                                borderRadius: 6, padding: "10px", fontFamily: "var(--font-sans)", fontWeight: 600,
                                fontSize: 14, cursor: loading ? "wait" : "pointer", marginBottom: 20, transition: "background 0.2s",
                            }}>{loading ? "Executing Request..." : "▶ Execute Request"}</button>

                            {showResponse && responsePayload && (
                                <div style={{ animation: "fadeIn 0.3s ease" }}>
                                    <div style={{ fontFamily: "var(--font-mono)", fontSize: 11, color: "#555", marginBottom: 8, textTransform: "uppercase", letterSpacing: "0.08em", display: "flex", justifyContent: "space-between" }}>
                                        <span>Response</span>
                                        <span style={{ color: "#27c93f" }}>200 OK · {executeTime}ms</span>
                                    </div>
                                    <div style={{ background: "#111", borderRadius: 6, padding: 16 }}>
                                        {responsePayload.split("\n").map((line, i) => {
                                            const isKey = line.includes('"') && line.includes(':');
                                            const isNum = line.match(/: [\d.]+,?$/);
                                            const isBool = line.match(/: (true|false)/);
                                            return (
                                                <div key={i} style={{ fontFamily: "var(--font-mono)", fontSize: 12, lineHeight: 1.8, whiteSpace: "pre-wrap" }}>
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
        </div>
    );
};

export default APIPage;
