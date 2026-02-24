import { useState, useEffect, useRef, Suspense } from "react";
import { Canvas, useFrame } from "@react-three/fiber";
import { Environment, MeshTransmissionMaterial, Text } from "@react-three/drei";
import * as THREE from "three";

// Pre-define colors outside the render loop for performance
const COLOR_GREEN = new THREE.Color("#1a9970");
const COLOR_YELLOW = new THREE.Color("#f39c12");
const COLOR_RED = new THREE.Color("#c0392b");
const tempColor = new THREE.Color();

// ─── 3D LIQUID COMPONENT ─────────────────────────────────────────────────────

const LiquidFill = ({ currentTemp, minTemp, maxTemp }) => {
    const meshRef = useRef();

    // Calculate percentage fill based on temp limits
    const targetPct = Math.max(0, Math.min(1, (currentTemp - minTemp) / (maxTemp - minTemp)));
    const targetHeight = targetPct * 4; // Max tube height is ~4 units

    // Smoothly interpolate height and color every frame
    useFrame((state, delta) => {
        if (meshRef.current) {
            meshRef.current.scale.y = THREE.MathUtils.lerp(meshRef.current.scale.y, Math.max(targetHeight, 0.05), delta * 3);
            meshRef.current.position.y = meshRef.current.scale.y / 2 - 2;

            // Color interpolation: blue -> yellow -> red
            if (currentTemp <= 1.5) tempColor.lerpColors(COLOR_GREEN, COLOR_YELLOW, currentTemp / 1.5);
            else tempColor.lerpColors(COLOR_YELLOW, COLOR_RED, Math.min(1, (currentTemp - 1.5) / 1.5));

            meshRef.current.material.color.lerp(tempColor, delta * 3);
        }
    });

    return (
        <mesh ref={meshRef} position={[0, -1.9, 0]}>
            <cylinderGeometry args={[0.3, 0.3, 1, 32]} />
            <meshPhysicalMaterial roughness={0.1} transmission={0.2} thickness={0.5} />
        </mesh>
    );
};

// ─── 3D THERMOMETER COMPONENT ────────────────────────────────────────────────

const Thermometer3D = ({ currentTemp, minTemp, maxTemp }) => {
    const bulbRef = useRef();

    // Bulb color syncs with current temp
    useFrame((state, delta) => {
        if (bulbRef.current) {
            if (currentTemp <= 1.5) tempColor.lerpColors(COLOR_GREEN, COLOR_YELLOW, currentTemp / 1.5);
            else tempColor.lerpColors(COLOR_YELLOW, COLOR_RED, Math.min(1, (currentTemp - 1.5) / 1.5));
            bulbRef.current.material.color.lerp(tempColor, delta * 3);
        }
    });

    return (
        <group position={[0, -0.5, 0]}>
            {/* Outer Glass Tube */}
            <mesh position={[0, 0, 0]}>
                <cylinderGeometry args={[0.4, 0.4, 4, 32]} />
                <MeshTransmissionMaterial backside resolution={128} thickness={0.4} roughness={0.1} ior={1.3} color="#f0fcff" />
            </mesh>

            {/* Outer Glass Bulb */}
            <mesh position={[0, -2.4, 0]}>
                <sphereGeometry args={[0.7, 32, 32]} />
                <MeshTransmissionMaterial backside resolution={128} thickness={0.4} roughness={0.1} ior={1.3} color="#f0fcff" />
            </mesh>

            {/* Inner Liquid Bulb */}
            <mesh ref={bulbRef} position={[0, -2.4, 0]}>
                <sphereGeometry args={[0.6, 32, 32]} />
                <meshPhysicalMaterial roughness={0.1} />
            </mesh>

            {/* Inner Liquid Tube */}
            <LiquidFill currentTemp={currentTemp} minTemp={minTemp} maxTemp={maxTemp} />

            {/* Scale Markings */}
            {[...Array(5)].map((_, i) => {
                const val = minTemp + (maxTemp - minTemp) * (i / 4);
                const yPos = -2 + (i / 4) * 4;
                return (
                    <group key={i} position={[0.5, yPos, 0]}>
                        <mesh position={[-0.1, 0, 0]}>
                            <boxGeometry args={[0.2, 0.02, 0.02]} />
                            <meshBasicMaterial color="#333" />
                        </mesh>
                        <Text position={[0.4, 0, 0]} fontSize={0.25} color="#555" anchorX="left" anchorY="middle" font="https://fonts.gstatic.com/s/jetbrainsmono/v18/tDbY2o-flEEny0FZhsfKu5WU4zr3E_BX0PnT8RD8yKwI.woff">
                            {val}°
                        </Text>
                    </group>
                );
            })}
        </group>
    );
};

// ─── MAIN OVERLAY COMPONENT ──────────────────────────────────────────────────

const Thermometer = ({ height = 300, current: propCurrent }) => {
    // initialize with the prop if provided, otherwise a sensible default
    const [current, setCurrent] = useState(propCurrent !== undefined ? propCurrent : 1.42);

    // Poll the API for live data every 1 second, but only when no explicit value is passed
    useEffect(() => {
        if (propCurrent !== undefined) {
            // if the parent is controlling the temperature, keep state in sync
            setCurrent(propCurrent);
            return;
        }

        const fetchTemp = async () => {
            try {
                // Ensure we use the full proxied path or direct IP to be safe
                const res = await fetch('/v2/status/thermometer');
                const data = await res.json();
                setCurrent(data.current);
            } catch (err) {
                console.warn("Thermometer fetch omitted locally:", err);
            }
        };
        fetchTemp();
        const interval = setInterval(fetchTemp, 1000);
        return () => clearInterval(interval);
    }, [propCurrent]);

    return (
        <div style={{ display: "flex", alignItems: "flex-end", gap: 24, height }}>
            {/* The 3D Canvas rendering the thermometer */}
            <div style={{ width: 140, height: "100%", position: "relative" }}>
                <Canvas camera={{ position: [0, 0, 8], fov: 40 }}>
                    <ambientLight intensity={2} />
                    <pointLight position={[10, 10, 10]} intensity={2} />
                    <Suspense fallback={null}>
                        <Environment preset="night" />
                        <Thermometer3D currentTemp={current} minTemp={0} maxTemp={4} />
                    </Suspense>
                </Canvas>
            </div>

            {/* The 2D Dynamic Label pointing to the liquid */}
            <div style={{ flex: 1, height: "100%", position: "relative", minWidth: 100 }}>
                <div style={{
                    position: "absolute",
                    bottom: `${Math.max(10, Math.min(85, (current / 4) * 100))}%`,
                    left: -20, right: 0,
                    display: "flex", alignItems: "center", gap: 8,
                    transform: "translateY(50%)",
                    transition: "bottom 0.8s cubic-bezier(0.4, 0, 0.2, 1)"
                }}>
                    <div style={{ flex: 1, height: 1.5, background: "var(--teal-400)", opacity: 0.8 }} />
                    <div style={{
                        background: "var(--teal-700)", color: "white", fontFamily: "var(--font-mono)",
                        fontSize: 24, fontWeight: 600, padding: "6px 14px", borderRadius: 30, whiteSpace: "nowrap",
                        boxShadow: "0 8px 32px rgba(0,0,0,0.3)", border: "1px solid rgba(255,255,255,0.1)"
                    }}>{current.toFixed(2)}°C</div>
                </div>
            </div>
        </div>
    );
};

export default Thermometer;
