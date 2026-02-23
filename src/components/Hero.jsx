import Thermometer from "./Thermometer";

const Hero = () => (
    <section id="platform" style={{
        background: `linear-gradient(135deg, var(--teal-900) 0%, #0a2a1f 50%, #061a13 100%)`,
        paddingTop: 128, paddingBottom: 80, position: "relative", overflow: "hidden",
    }}>
        <div style={{
            position: "absolute", inset: 0,
            backgroundImage: "radial-gradient(circle at 1px 1px, rgba(255,255,255,0.04) 1px, transparent 0)",
            backgroundSize: "32px 32px", pointerEvents: "none",
        }} />
        <div style={{
            position: "absolute", top: -200, right: -200, width: 600, height: 600,
            background: "radial-gradient(circle, rgba(26,153,112,0.15) 0%, transparent 70%)", pointerEvents: "none",
        }} />

        <div style={{ maxWidth: 1280, margin: "0 auto", padding: "0 24px" }}>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 380px", gap: 80, alignItems: "center" }}>
                <div>
                    <div className="animate-fadeUp stagger-1" style={{
                        display: "inline-flex", alignItems: "center", gap: 8,
                        background: "rgba(26,153,112,0.2)", border: "1px solid rgba(26,153,112,0.4)",
                        borderRadius: 30, padding: "6px 16px", marginBottom: 28,
                    }}>
                        <span style={{ width: 6, height: 6, borderRadius: "50%", background: "var(--teal-400)", animation: "pulse 2s infinite" }} />
                        <span style={{ fontFamily: "var(--font-mono)", fontSize: 11, color: "var(--teal-400)", letterSpacing: "0.08em" }}>
                            CMIP7 COMPLIANT · ALL SYSTEMS NOMINAL
                        </span>
                    </div>

                    <h1 className="animate-fadeUp stagger-2" style={{
                        fontFamily: "var(--font-serif)", fontSize: 58, fontWeight: 400,
                        color: "white", lineHeight: 1.12, marginBottom: 24,
                    }}>
                        The Infrastructure<br /><em style={{ color: "var(--teal-400)" }}>Beneath</em> the Science
                    </h1>

                    <p className="animate-fadeUp stagger-3" style={{
                        fontSize: 18, color: "rgba(255,255,255,0.7)", lineHeight: 1.7, marginBottom: 36, maxWidth: 540,
                    }}>
                        PCMIP is the planetary computation layer that makes climate science operationally useful — from raw satellite observation to validated, uncertainty-quantified API response.
                    </p>

                    <div className="animate-fadeUp stagger-4" style={{ display: "flex", gap: 12, flexWrap: "wrap", marginBottom: 56 }}>
                        <button style={{
                            background: "var(--teal-600)", color: "white", border: "none",
                            fontFamily: "var(--font-sans)", fontWeight: 600, fontSize: 15,
                            padding: "12px 32px", borderRadius: 30, cursor: "pointer", transition: "background 0.2s",
                        }}>Explore Platform →</button>
                        <button style={{
                            background: "transparent", color: "rgba(255,255,255,0.85)",
                            border: "1.5px solid rgba(255,255,255,0.25)",
                            fontFamily: "var(--font-sans)", fontWeight: 500, fontSize: 15,
                            padding: "12px 32px", borderRadius: 30, cursor: "pointer",
                        }}>Read the PRD</button>
                    </div>

                    <div className="animate-fadeUp stagger-5" style={{
                        display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 1,
                        background: "rgba(255,255,255,0.06)", borderRadius: 16, overflow: "hidden",
                        border: "1px solid rgba(255,255,255,0.08)",
                    }}>
                        {[
                            { value: "361 PB", label: "Archive size" },
                            { value: "9 sources", label: "Live ingestion" },
                            { value: "47 jobs", label: "Active compute" },
                            { value: "99.9%", label: "API uptime" },
                        ].map(({ value, label }) => (
                            <div key={label} style={{
                                padding: "20px 16px", textAlign: "center",
                                background: "rgba(255,255,255,0.03)", borderRight: "1px solid rgba(255,255,255,0.06)",
                            }}>
                                <div style={{ fontFamily: "var(--font-serif)", fontSize: 26, color: "var(--teal-400)", lineHeight: 1 }}>{value}</div>
                                <div style={{ fontFamily: "var(--font-sans)", fontSize: 12, color: "rgba(255,255,255,0.5)", marginTop: 6 }}>{label}</div>
                            </div>
                        ))}
                    </div>
                </div>

                <div className="animate-fadeUp stagger-6" style={{
                    background: "rgba(255,255,255,0.04)", border: "1px solid rgba(255,255,255,0.1)",
                    borderRadius: 24, padding: 32, backdropFilter: "blur(8px)",
                }}>
                    <div style={{ fontFamily: "var(--font-sans)", fontSize: 12, color: "rgba(255,255,255,0.5)", marginBottom: 4, textTransform: "uppercase", letterSpacing: "0.08em" }}>
                        Projected warming
                    </div>
                    <div style={{ fontFamily: "var(--font-serif)", fontSize: 22, color: "white", marginBottom: 24 }}>
                        Global Temperature Outlook
                    </div>
                    <Thermometer current={2.7} height={240} />
                    <div style={{
                        marginTop: 24, padding: "16px 20px",
                        background: "rgba(192,57,43,0.15)", border: "1px solid rgba(192,57,43,0.3)", borderRadius: 16,
                    }}>
                        <div style={{ fontFamily: "var(--font-mono)", fontSize: 11, color: "#e74c3c", marginBottom: 4 }}>CURRENT TRAJECTORY</div>
                        <div style={{ fontFamily: "var(--font-serif)", fontSize: 18, color: "white" }}>+2.7°C by 2100</div>
                        <div style={{ fontSize: 12, color: "rgba(255,255,255,0.5)", marginTop: 4 }}>Based on current climate policies · Updated Nov 2025</div>
                    </div>
                    <div style={{ display: "flex", gap: 12, marginTop: 12 }}>
                        {[{ label: "1.5°C target", color: "#1a9970" }, { label: "2.0°C target", color: "#f39c12" }].map(({ label, color }) => (
                            <div key={label} style={{ display: "flex", alignItems: "center", gap: 6 }}>
                                <div style={{ width: 20, height: 2, background: color }} />
                                <span style={{ fontSize: 11, color: "rgba(255,255,255,0.5)" }}>{label}</span>
                            </div>
                        ))}
                    </div>
                </div>
            </div>
        </div>
    </section>
);

export default Hero;
