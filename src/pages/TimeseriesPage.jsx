import { useState } from "react";

const TimeseriesPage = () => {
    const [params, setParams] = useState({
        lat: "51.5",
        lon: "-0.1",
        variable: "air_temperature",
        start: "2024-06-01",
        end: "2024-06-05",
        level: "surface",
        aggregate: "NONE",
    });
    const [result, setResult] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);

    const handleChange = (e) => {
        const { name, value } = e.target;
        setParams(p => ({ ...p, [name]: value }));
    };

    const fetchData = async () => {
        setLoading(true);
        setError(null);
        setResult(null);
        const search = new URLSearchParams(params).toString();
        try {
            const res = await fetch(`/v2/climate/timeseries?${search}`);
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            const data = await res.json();
            setResult(data);
        } catch (e) {
            setError(e.message);
        } finally {
            setLoading(false);
        }
    };

    return (
        <div style={{ paddingTop: 120, paddingBottom: 96, minHeight: "100vh", background: "var(--white)" }}>
            <div style={{ maxWidth: 800, margin: "0 auto", padding: "0 24px" }}>
                <h2 style={{ fontFamily: "var(--font-serif)", fontSize: 38, marginBottom: 24 }}>Timeseries Explorer</h2>
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginBottom: 24 }}>
                    {Object.entries(params).map(([k,v]) => (
                        <div key={k} style={{ display: "flex", flexDirection: "column" }}>
                            <label style={{ fontSize: 12, color: "var(--gray-700)", marginBottom: 4 }}>{k}</label>
                            <input
                                name={k}
                                value={v}
                                onChange={handleChange}
                                style={{ padding: 8, borderRadius: 6, border: "1px solid var(--gray-300)" }}
                            />
                        </div>
                    ))}
                </div>
                <button onClick={fetchData} disabled={loading} style={{ padding: "12px 24px", background: "var(--teal-600)", color: "white", border: "none", borderRadius: 6, cursor: loading ? "wait" : "pointer" }}>
                    {loading ? "Loading..." : "Fetch Timeseries"}
                </button>
                {error && <div style={{ marginTop: 20, color: "var(--red)" }}>{error}</div>}
                {result && (
                    <pre style={{ marginTop: 20, background: "#111", color: "#ccc", padding: 16, borderRadius: 6, overflowX: "auto" }}>
                        {JSON.stringify(result, null, 2)}
                    </pre>
                )}
            </div>
        </div>
    );
};

export default TimeseriesPage;