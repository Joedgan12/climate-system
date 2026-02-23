const RATINGS = {
    compatible: { label: "Compatible", color: "#1a9970", bg: "#e6f7f1", short: "✓" },
    almost: { label: "Almost sufficient", color: "#27ae60", bg: "#edfaed", short: "~" },
    insufficient: { label: "Insufficient", color: "#e67e22", bg: "#fef5ec", short: "!" },
    highly: { label: "Highly insufficient", color: "#e74c3c", bg: "#fdecea", short: "!!" },
    critical: { label: "Critically insufficient", color: "#c0392b", bg: "#f9e0de", short: "✗" },
};

const RatingBadge = ({ rating, size = "md" }) => {
    const r = RATINGS[rating];
    const sizes = { sm: { fontSize: 10, padding: "2px 8px" }, md: { fontSize: 12, padding: "4px 12px" }, lg: { fontSize: 13, padding: "6px 16px" } };
    const s = sizes[size];
    return (
        <span style={{
            display: "inline-block",
            background: r.bg,
            color: r.color,
            border: `1.5px solid ${r.color}`,
            borderRadius: 30,
            fontFamily: "var(--font-sans)",
            fontWeight: 600,
            letterSpacing: "0.03em",
            fontSize: s.fontSize,
            padding: s.padding,
            textTransform: "uppercase",
        }}>{r.label}</span>
    );
};

export { RATINGS };
export default RatingBadge;
