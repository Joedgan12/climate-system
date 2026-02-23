const Footer = () => (
    <footer style={{ background: "#061209", borderTop: "1px solid rgba(255,255,255,0.05)", padding: "48px 0 32px" }}>
        <div style={{ maxWidth: 1280, margin: "0 auto", padding: "0 24px" }}>
            <div style={{ display: "grid", gridTemplateColumns: "2fr 1fr 1fr 1fr", gap: 48, marginBottom: 48 }}>
                <div>
                    <div style={{ fontFamily: "var(--font-serif)", fontSize: 20, color: "white", marginBottom: 12 }}>PCMIP</div>
                    <p style={{ fontSize: 13, color: "rgba(255,255,255,0.45)", lineHeight: 1.7, maxWidth: 280 }}>
                        Planetary Climate Modeling Infrastructure Platform. The foundational data infrastructure that makes climate science operationally useful at scale.
                    </p>
                    <div style={{ marginTop: 20, fontSize: 11, color: "rgba(255,255,255,0.3)", fontFamily: "var(--font-mono)" }}>v1.0.0 · CMIP7 · FAIR Data</div>
                </div>
                {[
                    { title: "Platform", links: ["Data Ingestion", "HPC Compute", "Zarr Storage", "API Gateway", "AI Validation", "Governance"] },
                    { title: "Resources", links: ["PRD Documentation", "API Reference", "STAC Catalog", "Data Explorer", "Methodology"] },
                    { title: "Community", links: ["Pangeo Integration", "Open Source", "Publications", "Contact", "Privacy Policy"] },
                ].map(col => (
                    <div key={col.title}>
                        <div style={{ fontSize: 12, color: "var(--teal-400)", fontWeight: 600, letterSpacing: "0.1em", textTransform: "uppercase", marginBottom: 16 }}>{col.title}</div>
                        {col.links.map(link => (
                            <div key={link} style={{ marginBottom: 10 }}>
                                <a href="#" style={{ fontSize: 13, color: "rgba(255,255,255,0.5)", textDecoration: "none", transition: "color 0.2s" }}
                                    onMouseEnter={e => e.target.style.color = "var(--teal-400)"}
                                    onMouseLeave={e => e.target.style.color = "rgba(255,255,255,0.5)"}
                                >{link}</a>
                            </div>
                        ))}
                    </div>
                ))}
            </div>
            <div style={{ borderTop: "1px solid rgba(255,255,255,0.07)", paddingTop: 24, display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                <div style={{ fontSize: 12, color: "rgba(255,255,255,0.3)" }}>© 2026 PCMIP · Planetary Climate Modeling Infrastructure Platform · All data FAIR-compliant</div>
                <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                    <span style={{ width: 7, height: 7, borderRadius: "50%", background: "var(--teal-500)", display: "inline-block", animation: "pulse 2s infinite" }} />
                    <span style={{ fontSize: 12, color: "var(--teal-400)", fontFamily: "var(--font-mono)" }}>All systems nominal</span>
                </div>
            </div>
        </div>
    </footer>
);

export default Footer;
