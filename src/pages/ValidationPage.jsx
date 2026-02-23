import { useState, useEffect } from "react";
import RatingBadge from "../components/RatingBadge";

const ValidationPage = () => {
    const [models, setModels] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        fetch('/v2/status/validation/models')
            .then(res => {
                if (!res.ok) throw new Error(`HTTP ${res.status}`);
                return res.json();
            })
            .then(data => {
                setModels(data.models);
                setLoading(false);
            })
            .catch(err => {
                console.error(err);
                setError(err.message);
                setLoading(false);
            });
    }, []);

    return (
        <div style={{ paddingTop: 120, paddingBottom: 96, background: "white", minHeight: "100vh" }}>
            <div style={{ maxWidth: 1280, margin: "0 auto", padding: "0 24px" }}>
                <div style={{ marginBottom: 48 }}>
                    <div style={{ fontSize: 12, color: "var(--teal-700)", fontWeight: 600, letterSpacing: "0.1em", textTransform: "uppercase", marginBottom: 12 }}>Layer 5 — AI Validation Framework</div>
                    <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 48 }}>
                        <h2 style={{ fontFamily: "var(--font-serif)", fontSize: 40, fontWeight: 400, lineHeight: 1.2 }}>No AI output served<br />without validation</h2>
                        <div>
                            <div style={{ padding: "16px 20px", background: "#fdecea", border: "1px solid rgba(192,57,43,0.3)", borderLeft: "4px solid var(--critical)", borderRadius: 12, marginBottom: 16 }}>
                                <div style={{ fontSize: 12, fontWeight: 700, color: "var(--critical)", marginBottom: 6, textTransform: "uppercase", letterSpacing: "0.06em" }}>Non-negotiable control</div>
                                <div style={{ fontSize: 14, color: "var(--gray-800)", lineHeight: 1.6 }}>
                                    The API gateway refuses to serve results from models that have not cleared the full validation pipeline within the preceding 72 hours. This is a hard technical control.
                                </div>
                            </div>
                            <p style={{ fontSize: 14, color: "var(--gray-600)", lineHeight: 1.7 }}>
                                AI models can produce physically inconsistent outputs — states that violate energy conservation — that appear statistically plausible but are not physically possible. Standard RMSE does not detect these violations.
                            </p>
                        </div>
                    </div>
                </div>
                <div style={{ background: "white", border: "1px solid var(--gray-200)", borderRadius: 24, overflow: "hidden" }}>
                    <div style={{ background: "var(--teal-900)", padding: "14px 24px" }}>
                        <span style={{ fontSize: 14, fontWeight: 600, color: "white", fontFamily: "var(--font-sans)" }}>Model Validation Registry</span>
                    </div>
                    <table style={{ width: "100%", borderCollapse: "collapse" }}>
                        <thead>
                            <tr style={{ background: "var(--gray-50)", borderBottom: "2px solid var(--gray-200)" }}>
                                {["Model", "Organisation", "Type", "RMSE Z500", "Consistency", "Safe Range", "Validation Status"].map(h => (
                                    <th key={h} style={{ padding: "10px 16px", textAlign: "left", fontSize: 11, fontWeight: 600, color: "var(--gray-400)", letterSpacing: "0.06em", textTransform: "uppercase", fontFamily: "var(--font-sans)" }}>{h}</th>
                                ))}
                            </tr>
                        </thead>
                        <tbody>
                            {loading ? (
                                <tr><td colSpan="7" style={{ padding: 32, textAlign: "center", color: "var(--gray-500)" }}>Loading registry from validation server...</td></tr>
                            ) : error ? (
                                <tr><td colSpan="7" style={{ padding: 32, textAlign: "center", color: "var(--red)" }}>Error: {error}</td></tr>
                            ) : models.map((m, i) => (
                                <tr key={m.name} style={{ borderBottom: "1px solid var(--gray-100)", background: i % 2 === 0 ? "white" : "var(--gray-50)" }}
                                    onMouseEnter={e => e.currentTarget.style.background = "var(--teal-50)"}
                                    onMouseLeave={e => e.currentTarget.style.background = i % 2 === 0 ? "white" : "var(--gray-50)"}
                                >
                                    <td style={{ padding: "14px 16px", fontSize: 14, fontWeight: 600, color: "var(--gray-900)" }}>{m.name}</td>
                                    <td style={{ padding: "14px 16px", fontSize: 13, color: "var(--gray-500)" }}>{m.org}</td>
                                    <td style={{ padding: "14px 16px" }}>
                                        <span style={{
                                            fontSize: 11, fontWeight: 600,
                                            background: m.type === "Physics" ? "var(--teal-100)" : m.type === "AI-Hybrid" ? "#f0eaff" : "var(--gray-100)",
                                            color: m.type === "Physics" ? "var(--teal-700)" : m.type === "AI-Hybrid" ? "#6b4caf" : "var(--gray-600)",
                                            padding: "2px 8px", borderRadius: 3,
                                        }}>{m.type}</span>
                                    </td>
                                    <td style={{ padding: "14px 16px", fontFamily: "var(--font-mono)", fontSize: 13, color: typeof m.rmse === "number" && m.rmse < 130 ? "var(--teal-700)" : typeof m.rmse === "number" ? "var(--orange)" : "var(--red)" }}>
                                        {typeof m.rmse === "number" ? `${m.rmse}m` : m.rmse}
                                    </td>
                                    <td style={{ padding: "14px 16px" }}>
                                        {typeof m.consistency === "number" ? (
                                            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                                                <div style={{ width: 60, height: 6, background: "var(--gray-100)", borderRadius: 3, overflow: "hidden" }}>
                                                    <div style={{ height: "100%", width: `${m.consistency}%`, background: m.consistency > 97 ? "var(--teal-600)" : m.consistency > 93 ? "var(--amber)" : "var(--red)", borderRadius: 3 }} />
                                                </div>
                                                <span style={{ fontFamily: "var(--font-mono)", fontSize: 12, color: "var(--gray-600)" }}>{m.consistency}%</span>
                                            </div>
                                        ) : <span style={{ color: "var(--red)", fontFamily: "var(--font-mono)", fontSize: 12 }}>—</span>}
                                    </td>
                                    <td style={{ padding: "14px 16px", fontSize: 13, color: m.safeRange === "Suspended" ? "var(--red)" : "var(--gray-700)" }}>
                                        {m.safeRange === "Suspended" ? <strong>{m.safeRange}</strong> : m.safeRange}
                                    </td>
                                    <td style={{ padding: "14px 16px" }}><RatingBadge rating={m.rating} size="sm" /></td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
                <div style={{ display: "grid", gridTemplateColumns: "repeat(5, 1fr)", gap: 12, marginTop: 32 }}>
                    {[
                        { step: "1", title: "Statistical Eval", desc: "RMSE, MAE, bias, ACC against ERA5", status: "PASSING", color: "var(--teal-600)" },
                        { step: "2", title: "Physics Checks", desc: "Conservation, hydrostatic balance", status: "PASSING", color: "var(--teal-600)" },
                        { step: "3", title: "Uncertainty QC", desc: "Calibration of ensemble bounds", status: "PASSING", color: "var(--teal-600)" },
                        { step: "4", title: "Bias Correction", desc: "Quantile mapping vs ERA5", status: "RUNNING", color: "var(--amber)" },
                        { step: "5", title: "Drift Detection", desc: "30-day RMSE trend monitoring", status: "QUEUED", color: "var(--gray-400)" },
                    ].map(s => (
                        <div key={s.step} style={{ background: "var(--gray-50)", border: `1px solid ${s.color}44`, borderTop: `3px solid ${s.color}`, borderRadius: 16, padding: 20, textAlign: "center" }}>
                            <div style={{ fontFamily: "var(--font-serif)", fontSize: 28, color: s.color, marginBottom: 8 }}>{s.step}</div>
                            <div style={{ fontSize: 13, fontWeight: 600, color: "var(--gray-900)", marginBottom: 6 }}>{s.title}</div>
                            <div style={{ fontSize: 12, color: "var(--gray-500)", marginBottom: 10, lineHeight: 1.5 }}>{s.desc}</div>
                            <span style={{ fontFamily: "var(--font-mono)", fontSize: 10, color: s.color, background: `${s.color}11`, padding: "2px 8px", border: `1px solid ${s.color}33`, borderRadius: 10, fontWeight: 600, letterSpacing: "0.06em" }}>{s.status}</span>
                        </div>
                    ))}
                </div>
            </div>
        </div>
    );
};

export default ValidationPage;
