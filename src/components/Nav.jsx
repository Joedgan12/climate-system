import { Link, useLocation } from "react-router-dom";
import { useState, useEffect } from "react";

const Nav = () => {
    const [scrolled, setScrolled] = useState(false);
    const location = useLocation();
    const activeSection = location.pathname.substring(1) || "platform";

    useEffect(() => {
        const handler = () => setScrolled(window.scrollY > 40);
        window.addEventListener("scroll", handler);
        return () => window.removeEventListener("scroll", handler);
    }, []);

    const links = [
        { name: "Platform", path: "/" },
        { name: "Ingestion", path: "/ingestion" },
        { name: "Compute", path: "/compute" },
        { name: "Storage", path: "/storage" },
        { name: "API", path: "/api-gateway" },
        { name: "Validation", path: "/validation" },
        { name: "Governance", path: "/governance" }
    ];

    return (
        <nav style={{
            position: "fixed", top: 0, left: 0, right: 0, zIndex: 100,
            background: scrolled ? "rgba(255,255,255,0.97)" : "var(--teal-900)",
            borderBottom: scrolled ? "1px solid var(--gray-200)" : "none",
            backdropFilter: scrolled ? "blur(12px)" : "none",
            transition: "all 0.3s ease",
            boxShadow: scrolled ? "0 2px 20px rgba(0,0,0,0.08)" : "none",
        }}>
            <div style={{ maxWidth: 1280, margin: "0 auto", padding: "0 24px", display: "flex", alignItems: "center", height: 64, gap: 32 }}>
                <Link to="/" style={{ display: "flex", alignItems: "center", gap: 12, flexShrink: 0, textDecoration: "none" }}>
                    <div style={{
                        width: 36, height: 36,
                        background: scrolled ? "var(--teal-700)" : "rgba(255,255,255,0.2)",
                        borderRadius: 8, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 18,
                    }}>🌍</div>
                    <div>
                        <div style={{ fontFamily: "var(--font-serif)", fontSize: 16, color: scrolled ? "var(--teal-900)" : "white", lineHeight: 1.2 }}>PCMIP</div>
                        <div style={{ fontSize: 9, color: scrolled ? "var(--gray-400)" : "rgba(255,255,255,0.6)", letterSpacing: "0.1em", textTransform: "uppercase" }}>Planetary Climate Infrastructure</div>
                    </div>
                </Link>
                <div style={{ display: "flex", gap: 2, flex: 1 }}>
                    {links.map(link => {
                        const isActive = location.pathname === link.path;
                        return (
                            <Link key={link.name} to={link.path} style={{
                                background: "none", border: "none", cursor: "pointer",
                                fontFamily: "var(--font-sans)", fontSize: 14,
                                fontWeight: isActive ? 600 : 400,
                                color: scrolled
                                    ? (isActive ? "var(--teal-700)" : "var(--gray-600)")
                                    : (isActive ? "white" : "rgba(255,255,255,0.75)"),
                                padding: "8px 14px",
                                borderBottom: isActive ? `2px solid ${scrolled ? "var(--teal-700)" : "white"}` : "2px solid transparent",
                                transition: "all 0.2s",
                                textDecoration: "none"
                            }}>{link.name}</Link>
                        );
                    })}
                </div>
                <div style={{ display: "flex", gap: 10 }}>
                    <button style={{
                        background: "transparent",
                        border: `1.5px solid ${scrolled ? "var(--teal-700)" : "rgba(255,255,255,0.5)"}`,
                        color: scrolled ? "var(--teal-700)" : "white",
                        fontFamily: "var(--font-sans)", fontSize: 13, fontWeight: 500,
                        padding: "7px 16px", borderRadius: 4, cursor: "pointer",
                    }}>Data Explorer</button>
                    <button style={{
                        background: scrolled ? "var(--teal-700)" : "white",
                        border: "none", color: scrolled ? "white" : "var(--teal-900)",
                        fontFamily: "var(--font-sans)", fontSize: 13, fontWeight: 600,
                        padding: "7px 16px", borderRadius: 4, cursor: "pointer",
                    }}>API Access</button>
                </div>
            </div>
        </nav>
    );
};

export default Nav;
