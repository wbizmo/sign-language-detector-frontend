"use client";

import { useState, useEffect, useCallback, useRef } from "react";
import {
    UseSignLanguageStreamReturn,
    FrameData,
    GlossPrediction,
} from "@/types/sign-language";
import {
    getStreamingStartAction,
    hasActiveConnectionAttempt,
} from "@/lib/websocket-lifecycle";

/**
 * Hook to manage WebSocket connection to FastAPI backend
 * Handles frame streaming and receives gloss predictions
 */
export function useSignLanguageStream(
    backendUrl: string = "ws://localhost:8000",
    onPrediction?: (prediction: GlossPrediction) => void
): UseSignLanguageStreamReturn {
    const [isConnected, setIsConnected] = useState(false);
    const [isStreaming, setIsStreaming] = useState(false);
    const [lastPrediction, setLastPrediction] = useState<GlossPrediction | null>(
        null
    );
    const [error, setError] = useState<string | null>(null);
    const [sessionId] = useState(() => generateSessionId());

    const wsRef = useRef<WebSocket | null>(null);
    const pendingStartRef = useRef(false);
    const reconnectTimeoutRef = useRef<NodeJS.Timeout>(undefined);
    const reconnectAttemptsRef = useRef(0);
    const maxReconnectAttempts = 5;

    const connect = useCallback(() => {
        const existingReadyState = wsRef.current?.readyState ?? null;
        if (hasActiveConnectionAttempt(existingReadyState)) {
            console.log(
                existingReadyState === WebSocket.OPEN
                    ? "WebSocket already connected"
                    : "WebSocket connection already in progress"
            );
            return;
        }

        if (reconnectTimeoutRef.current) {
            clearTimeout(reconnectTimeoutRef.current);
            reconnectTimeoutRef.current = undefined;
        }

        try {
            const ws = new WebSocket(`${backendUrl}/ws/stream/${sessionId}`);
            wsRef.current = ws;

            ws.onopen = () => {
                if (wsRef.current !== ws) {
                    ws.close(1000, "Superseded connection");
                    return;
                }

                console.log("✅ WebSocket OPENED successfully:", sessionId);
                setIsConnected(true);
                setError(null);
                reconnectAttemptsRef.current = 0;

                if (pendingStartRef.current) {
                    pendingStartRef.current = false;
                    setIsStreaming(true);
                }
            };

            ws.onmessage = (event) => {
                if (wsRef.current !== ws) return;

                console.log("🔔 ============ ONMESSAGE FIRED ============");

                try {
                    const message = JSON.parse(event.data);

                    if (message.type) {
                        switch (message.type) {
                            case "prediction":
                                if (message.data && typeof message.data === "object") {
                                    const prediction = message.data as GlossPrediction;
                                    setLastPrediction(prediction);
                                    onPrediction?.(prediction);
                                }
                                break;

                            case "error":
                                console.error("Backend error:", message.error);
                                setError(message.error || "Unknown error");
                                break;

                            case "status":
                                console.log("Status:", message.message);
                                break;

                            case "connected":
                                console.log("Connection confirmed:", message.message);
                                break;

                            default:
                                console.log("⚠️ Unknown message type:", message);
                                console.log("Full message object:", JSON.stringify(message, null, 2));
                        }
                    } else if (message.gloss && typeof message.confidence === "number") {
                        const prediction = message as GlossPrediction;
                        setLastPrediction(prediction);
                        onPrediction?.(prediction);
                    } else {
                        console.log("⚠️ Unknown message format:", JSON.stringify(message, null, 2));
                    }
                } catch (err) {
                    console.error("❌ Failed to parse WebSocket message:", err);
                    console.error("Raw data was:", event.data);
                }
            };

            ws.onerror = (event) => {
                if (wsRef.current !== ws) return;

                console.error("❌ WebSocket ERROR event:", event);
                console.error("❌ WebSocket readyState:", ws.readyState);
                setError("WebSocket connection error");
            };

            ws.onclose = (event) => {
                if (wsRef.current !== ws) return;

                console.log("🔴 WebSocket CLOSED");
                wsRef.current = null;
                setIsConnected(false);
                setIsStreaming(false);

                if (
                    event.code !== 1000 &&
                    reconnectAttemptsRef.current < maxReconnectAttempts
                ) {
                    reconnectAttemptsRef.current++;
                    const delay = Math.min(1000 * reconnectAttemptsRef.current, 5000);
                    console.log(
                        `Reconnecting in ${delay}ms (attempt ${reconnectAttemptsRef.current})`
                    );

                    reconnectTimeoutRef.current = setTimeout(() => {
                        reconnectTimeoutRef.current = undefined;
                        connect();
                    }, delay);
                }
            };
        } catch (err: any) {
            console.error("Failed to create WebSocket:", err);
            setError(err.message);
        }
    }, [backendUrl, sessionId, onPrediction]);

    const disconnect = useCallback(() => {
        pendingStartRef.current = false;
        reconnectAttemptsRef.current = 0;

        if (reconnectTimeoutRef.current) {
            clearTimeout(reconnectTimeoutRef.current);
            reconnectTimeoutRef.current = undefined;
        }

        const ws = wsRef.current;
        wsRef.current = null;
        if (ws) {
            ws.close(1000, "Client disconnect");
        }

        setIsConnected(false);
        setIsStreaming(false);
    }, []);

    const sendFrames = useCallback(
        (frames: string[]) => {
            if (!wsRef.current || wsRef.current.readyState !== WebSocket.OPEN) {
                console.error("WebSocket not connected");
                setError("WebSocket not connected");
                return false;
            }

            try {
                const frameData: FrameData = {
                    session_id: sessionId,
                    frames,
                    timestamp: Date.now(),
                    frame_count: frames.length,
                };

                wsRef.current.send(JSON.stringify(frameData));
                return true;
            } catch (err: any) {
                console.error("Failed to send frames:", err);
                setError(err.message);
                return false;
            }
        },
        [sessionId]
    );

    const startStreaming = useCallback(() => {
        const action = getStreamingStartAction(wsRef.current?.readyState ?? null);

        if (action === "start") {
            pendingStartRef.current = false;
            setIsStreaming(true);
            setError(null);
            return;
        }

        pendingStartRef.current = true;
        setError(null);

        if (action === "connect") {
            connect();
        }
    }, [connect]);

    const stopStreaming = useCallback(() => {
        pendingStartRef.current = false;
        setIsStreaming(false);
    }, []);

    useEffect(() => {
        return () => {
            disconnect();
        };
    }, [disconnect]);

    return {
        isConnected,
        isStreaming,
        lastPrediction,
        error,
        sessionId,
        connect,
        disconnect,
        startStreaming,
        stopStreaming,
        sendFrames,
    } as UseSignLanguageStreamReturn & { sendFrames: (frames: string[]) => boolean };
}

function generateSessionId(): string {
    return `session_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}
