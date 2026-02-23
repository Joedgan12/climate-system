const GovernancePage = () => (
    <div style={{ paddingTop: 120, paddingBottom: 96, background: "var(--teal-900)", minHeight: "100vh" }}>
        <div style={{ maxWidth: 1280, margin: "0 auto", padding: "0 24px" }}>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 64, alignItems: "start" }}>
                <div>
                    <div style={{ fontSize: 12, color: "var(--teal-400)", fontWeight: 600, letterSpacing: "0.1em", textTransform: "uppercase", marginBottom: 12 }}>Layer 6 — Governance</div>
                    <h2 style={{ fontFamily: "var(--font-serif)", fontSize: 40, fontWeight: 400, color: "white", lineHeight: 1.2, marginBottom: 20 }}>CMIP7 compliance<br />&amp; full data lineage</h2>
                    <p style={{ fontSize: 15, color: "rgba(255,255,255,0.65)", lineHeight: 1.8, marginBottom: 36 }}>
                        Every dataset carries an auditable lineage chain from raw satellite observation to final API response. FAIR data compliance enforced at every stage.
                    </p>
                    <div style={{ display: "grid", gridTemplateColumns: "repeat(2, 1fr)", gap: 12 }}>
                        {[
                            { letter: "F", label: "Findable", desc: "STAC catalog + DataCite DOI" },
                            { letter: "A", label: "Accessible", desc: "REST API + open metadata" },
                            { letter: "I", label: "Interoperable", desc: "CF-1.10 · CMIP7 · Zarr v3" },
                            { letter: "R", label: "Reusable", desc: "CC BY 4.0 · Full provenance" },
                        ].map(f => (
                            <div key={f.letter} style={{ background: "rgba(255,255,255,0.05)", border: "1px solid rgba(255,255,255,0.1)", borderRadius: 16, padding: 18, display: "flex", gap: 14, alignItems: "flex-start" }}>
                                <div style={{ width: 40, height: 40, borderRadius: 12, background: "var(--teal-600)", display: "flex", alignItems: "center", justifyContent: "center", fontFamily: "var(--font-serif)", fontSize: 22, color: "white", flexShrink: 0 }}>{f.letter}</div>
                                <div>
                                    <div style={{ fontSize: 14, fontWeight: 600, color: "white", marginBottom: 4 }}>{f.label}</div>
                                    <div style={{ fontSize: 12, color: "rgba(255,255,255,0.5)" }}>{f.desc}</div>
                                </div>
                            </div>
                        ))}
                    </div>
                </div>
                <div>
                    <div style={{ fontSize: 13, color: "rgba(255,255,255,0.5)", marginBottom: 20, fontFamily: "var(--font-mono)", textTransform: "uppercase", letterSpacing: "0.08em" }}>Sample lineage chain · ds_f8a3</div>
                    {[
                        { id: "obs_goes16_c4f8", label: "Raw Observation", sub: "GOES-16 · sha256:c4f8a2...", icon: "🛰", color: "var(--teal-400)" },
                        { id: "ds_b2c9", label: "Validated Record", sub: "Schema ✓ · Physics ✓ · CMIP7 ✓", icon: "✓", color: "var(--teal-500)" },
                        { id: "ds_d4e1", label: "Zarr Dataset", sub: "HOT tier · Chunked · STAC registered", icon: "📦", color: "var(--teal-400)" },
                        { id: "ds_e9b3", label: "Bias-Corrected", sub: "ERA5 reference · xclim v0.50", icon: "⚖", color: "var(--teal-300)" },
                        { id: "api_f8a3", label: "API Response", sub: "v2/climate/variable · +uncertainty", icon: "→", color: "white" },
                    ].map((node, i, arr) => (
                        <div>
                            <div key={node.id} style={{ background: "rgba(255,255,255,0.05)", border: "1px solid rgba(255,255,255,0.1)", borderRadius: 16, padding: "14px 18px", display: "flex", gap: 14, alignItems: "center" }}>
                                <div style={{ fontSize: 20, width: 32, textAlign: "center", flexShrink: 0 }}>{node.icon}</div>
                                <div style={{ flex: 1 }}>
                                    <div style={{ fontSize: 14, fontWeight: 600, color: node.color }}>{node.label}</div>
                                    <div style={{ fontSize: 11, color: "rgba(255,255,255,0.45)", fontFamily: "var(--font-mono)", marginTop: 2 }}>{node.id}</div>
                                    <div style={{ fontSize: 12, color: "rgba(255,255,255,0.5)", marginTop: 2 }}>{node.sub}</div>
                                </div>
                            </div>
                            {i < arr.length - 1 && (
                                <div style={{ display: "flex", justifyContent: "flex-start", paddingLeft: 32, margin: "4px 0" }}>
                                    <div style={{ fontFamily: "var(--font-mono)", fontSize: 16, color: "rgba(255,255,255,0.2)" }}>↓</div>
                                </div>
                            )}
                        </div>
                    ))}
                </div>
            </div>
            <div style={{ marginTop: 64, background: "rgba(255,255,255,0.04)", border: "1px solid rgba(255,255,255,0.08)", borderRadius: 24, padding: 32 }}>
                <h3 style={{ fontFamily: "var(--font-serif)", fontSize: 24, color: "white", marginBottom: 24 }}>CMIP7 Compliance Status</h3>
                <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 16 }}>
                    {[
                        ["Variable naming", "CF-1.10 compliant", true],
                        ["Calendar encoding", "proleptic_gregorian", true],
                        ["Grid conventions", "CMIP7 standard", true],
                        ["Coordinate attributes", "CF-1.10 axis attrs", true],
                        ["Global attributes", "25 required fields", true],
                        ["Tracking ID", "UUID per file/run", true],
                        ["Data citation", "DataCite DOI", true],
                        ["ESGF access", "v3 node compatible", true],
                    ].map(([label, value, ok]) => (
                        <div key={label} style={{ background: "rgba(255,255,255,0.04)", border: `1px solid ${ok ? "rgba(26,153,112,0.3)" : "rgba(192,57,43,0.3)"}`, borderRadius: 12, padding: "12px 14px", display: "flex", gap: 10, alignItems: "flex-start" }}>
                            <span style={{ color: ok ? "var(--teal-400)" : "var(--red)", fontSize: 14, marginTop: 1 }}>{ok ? "✓" : "✗"}</span>
                            <div>
                                <div style={{ fontSize: 12, fontWeight: 600, color: "rgba(255,255,255,0.8)", marginBottom: 3 }}>{label}</div>
                                <div style={{ fontSize: 11, color: "rgba(255,255,255,0.4)", fontFamily: "var(--font-mono)" }}>{value}</div>
                            </div>
                        </div>
                    ))}
                </div>
            </div>
        </div>
    </div>
);

export default GovernancePage;
