import { useState, useEffect } from "react";

const LiveTicker = () => {
    const [items, setItems] = useState([]);

    useEffect(() => {
        // try WebSocket first
        let ws;
        try {
            ws = new WebSocket((location.protocol === 'https:' ? 'wss' : 'ws') + '://' + location.hostname + ':8080');
            ws.onopen = () => {
                // subscribe to a couple of example channels
                ['system.health', 'ingest.live', 'compute.jobs'].forEach(ch => {
                    ws.send(JSON.stringify({ action: 'subscribe', channel: ch }));
                });
            };
            ws.onmessage = (ev) => {
                try {
                    const msg = JSON.parse(ev.data);
                    // push a compact string representation for display
                    if (msg.channel) {
                        const summary = msg.channel + ": " + JSON.stringify(msg.data);
                        setItems(prev => [...prev.slice(-9), summary]);
                    }
                } catch (e) {
                    console.warn('WS parse error', e);
                }
            };
            ws.onerror = (e) => console.warn('WS error', e);
        } catch (e) {
            console.warn('WebSocket failed, falling back to HTTP', e);
        }

        if (!ws) {
            fetch('/api/ticker')
                .then(res => res.json())
                .then(data => setItems(data.items))
                .catch(err => console.error(err));
        }

        return () => {
            if (ws) ws.close();
        };
    }, []);

    if (!items.length) return null;

    return (
        <div style={{
            background: "var(--teal-900)",
            borderBottom: "1px solid rgba(255,255,255,0.1)",
            padding: "8px 0", overflow: "hidden",
            position: "fixed", top: 64, left: 0, right: 0, zIndex: 99
        }}>
            <div style={{ display: "flex", alignItems: "center" }}>
                <div style={{
                    flexShrink: 0, background: "var(--teal-600)", color: "white",
                    fontFamily: "var(--font-mono)", fontSize: 10, fontWeight: 500,
                    padding: "2px 12px", letterSpacing: "0.1em", textTransform: "uppercase",
                    marginRight: 16, whiteSpace: "nowrap",
                }}>● LIVE</div>
                <div className="ticker-wrap" style={{ flex: 1 }}>
                    <div className="ticker-inner">
                        {items.map((item, i) => (
                            <span key={i} style={{
                                fontFamily: "var(--font-mono)", fontSize: 11,
                                color: "rgba(255,255,255,0.75)", marginRight: 48,
                            }}>{item}</span>
                        ))}
                    </div>
                </div>
            </div>
        </div>
    );
};

export default LiveTicker;
