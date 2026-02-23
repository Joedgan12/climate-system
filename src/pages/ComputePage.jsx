import { useState, useEffect } from "react";

const ComputePage = () => {
    const [jobs, setJobs] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        fetch('/v2/status/compute/jobs')
            .then(res => {
                if (!res.ok) throw new Error(`HTTP ${res.status}`);
                return res.json();
            })
            .then(data => {
                setJobs(data.jobs);
                setLoading(false);
            })
            .catch(err => {
                console.error(err);
                setError(err.message);
                setLoading(false);
            });
    }, []);

    const statusColor = (s) => ({ running: "var(--teal-600)", queued: "var(--amber)", complete: "var(--gray-400)", failed: "var(--red)" }[s]);

    return (
        <div style={{ paddingTop: 120, paddingBottom: 96, background: "var(--gray-50)", minHeight: "100vh" }}>
            <div style={{ maxWidth: 1280, margin: "0 auto", padding: "0 24px" }}>
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 64, marginBottom: 56 }}>
                    <div>
                        <div style={{ fontSize: 12, color: "var(--teal-700)", fontWeight: 600, letterSpacing: "0.1em", textTransform: "uppercase", marginBottom: 12 }}>Layer 2 — HPC Compute</div>
                        <h2 style={{ fontFamily: "var(--font-serif)", fontSize: 40, fontWeight: 400, lineHeight: 1.2, marginBottom: 20 }}>24,576-core hybrid<br />compute cluster</h2>
                        <p style={{ fontSize: 15, color: "var(--gray-600)", lineHeight: 1.8 }}>
                            SLURM-managed physics runs on bare-metal HPC with InfiniBand interconnects. Kubernetes-managed AI serving and post-processing. These are not interchangeable — see the engineering note below.
                        </p>
                        <div style={{
                            marginTop: 24, padding: "14px 18px",
                            background: "rgba(243,156,18,0.08)", border: "1px solid rgba(243,156,18,0.3)",
                            borderLeft: "4px solid var(--amber)", borderRadius: 16,
                        }}>
                            <div style={{ fontSize: 12, fontWeight: 600, color: "var(--amber)", marginBottom: 4, textTransform: "uppercase", letterSpacing: "0.06em" }}>Engineering note</div>
                            <div style={{ fontSize: 13, color: "var(--gray-700)", lineHeight: 1.6 }}>
                                Kubernetes does not replace SLURM for MPI-coupled physics workloads. Physics models require InfiniBand, LUSTRE, and NUMA-aware scheduling. This distinction is non-negotiable.
                            </div>
                        </div>
                    </div>
                    <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
                        {[
                            { partition: "PHYSICS", used: 14890, total: 16384, color: "var(--teal-600)", type: "SLURM · InfiniBand HDR" },
                            { partition: "AI-GPU", used: 3712, total: 4096, color: "var(--teal-500)", type: "Kubernetes · NVLink" },
                            { partition: "POSTPROC", used: 2048, total: 4096, color: "var(--teal-400)", type: "Kubernetes · 25GbE" },
                        ].map(p => (
                            <div key={p.partition} style={{ background: "white", border: "1px solid var(--gray-200)", borderRadius: 16, padding: 20 }}>
                                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 10 }}>
                                    <div>
                                        <span style={{ fontSize: 14, fontWeight: 600, color: "var(--gray-900)" }}>{p.partition}</span>
                                        <span style={{ fontSize: 11, color: "var(--gray-400)", marginLeft: 10, fontFamily: "var(--font-mono)" }}>{p.type}</span>
                                    </div>
                                    <span style={{ fontFamily: "var(--font-serif)", fontSize: 20, color: p.color }}>{Math.round((p.used / p.total) * 100)}%</span>
                                </div>
                                <div style={{ height: 8, background: "var(--gray-100)", borderRadius: 4, overflow: "hidden" }}>
                                    <div style={{ height: "100%", width: `${(p.used / p.total) * 100}%`, background: p.color, borderRadius: 4, transition: "width 1.5s cubic-bezier(0.23, 1, 0.32, 1)" }} />
                                </div>
                                <div style={{ fontSize: 12, color: "var(--gray-400)", marginTop: 6, fontFamily: "var(--font-mono)" }}>{p.used.toLocaleString()} / {p.total.toLocaleString()} cores</div>
                            </div>
                        ))}
                    </div>
                </div>

                <div style={{ background: "white", border: "1px solid var(--gray-200)", borderRadius: 24, overflow: "hidden" }}>
                    <div style={{ padding: "16px 24px", background: "var(--teal-900)", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                        <span style={{ fontFamily: "var(--font-sans)", fontSize: 14, fontWeight: 600, color: "white" }}>Active Job Queue</span>
                        <span style={{ fontFamily: "var(--font-mono)", fontSize: 11, color: "rgba(255,255,255,0.5)" }}>SLURM 23.11 · Updated live</span>
                    </div>
                    <table style={{ width: "100%", borderCollapse: "collapse" }}>
                        <thead>
                            <tr style={{ background: "var(--gray-50)", borderBottom: "1px solid var(--gray-200)" }}>
                                {["Job ID", "Model", "Type", "Cores", "Progress", "ETA", "Status"].map(h => (
                                    <th key={h} style={{ padding: "10px 16px", textAlign: "left", fontSize: 11, fontWeight: 600, color: "var(--gray-400)", letterSpacing: "0.06em", textTransform: "uppercase", fontFamily: "var(--font-sans)" }}>{h}</th>
                                ))}
                            </tr>
                        </thead>
                        <tbody>
                            {loading ? (
                                <tr><td colSpan="7" style={{ padding: 32, textAlign: "center", color: "var(--gray-500)" }}>Loading jobs from cluster API...</td></tr>
                            ) : error ? (
                                <tr><td colSpan="7" style={{ padding: 32, textAlign: "center", color: "var(--red)" }}>Error: {error}</td></tr>
                            ) : jobs.map((j) => (
                                <tr key={j.id} style={{ borderBottom: "1px solid var(--gray-100)", transition: "background 0.15s" }}
                                    onMouseEnter={e => e.currentTarget.style.background = "var(--teal-50)"}
                                    onMouseLeave={e => e.currentTarget.style.background = "transparent"}
                                >
                                    <td style={{ padding: "12px 16px", fontFamily: "var(--font-mono)", fontSize: 13, color: "var(--teal-700)", fontWeight: 500 }}>{j.id}</td>
                                    <td style={{ padding: "12px 16px", fontSize: 14, fontWeight: 600, color: "var(--gray-900)" }}>{j.model}</td>
                                    <td style={{ padding: "12px 16px" }}>
                                        <span style={{
                                            fontSize: 11, fontWeight: 600,
                                            background: j.type === "Physics" ? "var(--teal-100)" : j.type === "AI" ? "#f0eaff" : "var(--gray-100)",
                                            color: j.type === "Physics" ? "var(--teal-700)" : j.type === "AI" ? "#6b4caf" : "var(--gray-600)",
                                            padding: "2px 8px", borderRadius: 10,
                                        }}>{j.type}</span>
                                    </td>
                                    <td style={{ padding: "12px 16px", fontFamily: "var(--font-mono)", fontSize: 12, color: "var(--gray-600)" }}>{j.cores}</td>
                                    <td style={{ padding: "12px 16px", minWidth: 120 }}>
                                        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                                            <div style={{ flex: 1, height: 6, background: "var(--gray-100)", borderRadius: 3, overflow: "hidden" }}>
                                                <div style={{ height: "100%", width: `${j.progress}%`, background: statusColor(j.status), borderRadius: 3 }} />
                                            </div>
                                            <span style={{ fontFamily: "var(--font-mono)", fontSize: 11, color: "var(--gray-400)", minWidth: 32 }}>{j.progress}%</span>
                                        </div>
                                    </td>
                                    <td style={{ padding: "12px 16px", fontFamily: "var(--font-mono)", fontSize: 12, color: "var(--gray-600)" }}>{j.eta}</td>
                                    <td style={{ padding: "12px 16px" }}>
                                        <div style={{ display: "flex", alignItems: "center", gap: 5 }}>
                                            <span style={{ width: 7, height: 7, borderRadius: "50%", background: statusColor(j.status), display: "inline-block" }} />
                                            <span style={{ fontSize: 12, color: statusColor(j.status), fontWeight: 500, textTransform: "capitalize" }}>{j.status}</span>
                                        </div>
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    );
};

export default ComputePage;
