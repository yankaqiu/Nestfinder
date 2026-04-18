import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import RankedList from "./components/RankedList";
import ListingsMap from "./components/ListingsMap";

type ListingData = {
  id: string;
  title: string;
  city?: string | null;
  canton?: string | null;
  latitude?: number | null;
  longitude?: number | null;
  image_urls?: string[] | null;
  hero_image_url?: string | null;
  price_chf?: number | null;
  rooms?: number | null;
  features?: string[];
};

type RankedListingResult = {
  listing_id: string;
  score: number;
  reason: string;
  listing: ListingData;
};

type ToolOutput = {
  listings?: RankedListingResult[];
  meta?: Record<string, unknown>;
};

declare global {
  interface Window {
    openai?: {
      toolOutput?: ToolOutput;
    };
    __NESTFINDER_API_BASE__?: string;
  }
}

function generateSessionId(): string {
  return `sess_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`;
}

function trackClick(listingId: string, sessionId: string, query?: string): void {
  const base = window.__NESTFINDER_API_BASE__;
  if (!base) return;
  fetch(`${base}/preferences`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ listing_id: listingId, action: "click", session_id: sessionId, query }),
  }).catch(() => undefined);
}

type UiToolResultMessage = {
  jsonrpc?: string;
  method?: string;
  params?: {
    structuredContent?: ToolOutput;
  };
};

function readToolOutput(): ToolOutput {
  return window.openai?.toolOutput ?? {};
}

function readToolOutputFromMessage(message: unknown): ToolOutput | null {
  if (!message || typeof message !== "object") {
    return null;
  }

  const maybeToolResult = message as UiToolResultMessage;
  if (
    maybeToolResult.jsonrpc !== "2.0" ||
    maybeToolResult.method !== "ui/notifications/tool-result"
  ) {
    return null;
  }

  return maybeToolResult.params?.structuredContent ?? {};
}

export default function App() {
  const [toolOutput, setToolOutput] = useState<ToolOutput>(() => readToolOutput());
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const sessionIdRef = useRef<string>(generateSessionId());
  const lastQueryRef = useRef<string | undefined>(undefined);

  const lastClickRef = useRef<{ id: string; ts: number } | null>(null);
  const handleSelect = useCallback((listingId: string) => {
    setSelectedId(listingId);
    const now = Date.now();
    const last = lastClickRef.current;
    if (!last || last.id !== listingId || now - last.ts > 1000) {
      lastClickRef.current = { id: listingId, ts: now };
      trackClick(listingId, sessionIdRef.current, lastQueryRef.current);
    }
  }, []);

  useEffect(() => {
    const onGlobals = (event: Event) => {
      const customEvent = event as CustomEvent<{ globals?: { toolOutput?: ToolOutput } }>;
      setToolOutput(customEvent.detail?.globals?.toolOutput ?? readToolOutput());
    };

    window.addEventListener("openai:set_globals", onGlobals as EventListener);

    const onMessage = (event: MessageEvent) => {
      if (event.source !== window.parent) {
        return;
      }

      const nextToolOutput = readToolOutputFromMessage(event.data);
      if (nextToolOutput) {
        setToolOutput(nextToolOutput);
      }
    };

    window.addEventListener("message", onMessage, { passive: true });
    return () => {
      window.removeEventListener("openai:set_globals", onGlobals as EventListener);
      window.removeEventListener("message", onMessage);
    };
  }, []);

  const results = toolOutput.listings ?? [];
  lastQueryRef.current = (toolOutput.meta as any)?.query as string | undefined;

  useEffect(() => {
    if (!results.length) {
      setSelectedId(null);
      return;
    }
    setSelectedId((current) =>
      current && results.some((result) => result.listing_id === current)
        ? current
        : results[0].listing_id,
    );
  }, [results]);

  const selectedListing = useMemo(
    () => results.find((result) => result.listing_id === selectedId) ?? null,
    [results, selectedId],
  );

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="sidebar-header">
          <p className="eyebrow">Listings</p>
          <h1>Ranked results</h1>
          <p className="muted">
            {results.length
              ? `${results.length} result${results.length === 1 ? "" : "s"}`
              : "No results yet"}
          </p>
        </div>
        <RankedList
          results={results}
          selectedId={selectedId}
          onSelect={handleSelect}
        />
      </aside>
      <main className="map-panel">
        <ListingsMap
          results={results}
          selectedId={selectedId}
          selectedListing={selectedListing}
          onSelect={handleSelect}
        />
      </main>
    </div>
  );
}
