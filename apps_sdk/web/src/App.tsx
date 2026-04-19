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
  explanation?: string | null;
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

const SESSION_STORAGE_KEY = "nestfinder_session_id";
const FAVORITES_STORAGE_PREFIX = "nestfinder_favorites_";

function generateSessionId(): string {
  return `sess_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`;
}

function getPersistedSessionId(): string {
  if (typeof window === "undefined") {
    return generateSessionId();
  }
  const existing = window.localStorage.getItem(SESSION_STORAGE_KEY);
  if (existing) {
    return existing;
  }
  const generated = generateSessionId();
  window.localStorage.setItem(SESSION_STORAGE_KEY, generated);
  return generated;
}

function favoritesStorageKey(sessionId: string): string {
  return `${FAVORITES_STORAGE_PREFIX}${sessionId}`;
}

function loadFavoriteIds(sessionId: string): string[] {
  if (typeof window === "undefined") {
    return [];
  }
  try {
    const raw = window.localStorage.getItem(favoritesStorageKey(sessionId));
    if (!raw) {
      return [];
    }
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed.filter((value): value is string => typeof value === "string") : [];
  } catch {
    return [];
  }
}

function persistFavoriteIds(sessionId: string, listingIds: string[]): void {
  if (typeof window === "undefined") {
    return;
  }
  window.localStorage.setItem(favoritesStorageKey(sessionId), JSON.stringify(listingIds));
}

function trackPreference(
  listingId: string,
  sessionId: string,
  action: "click" | "favorite",
  query?: string,
): void {
  const base = window.__NESTFINDER_API_BASE__;
  if (!base) {
    return;
  }
  fetch(`${base}/preferences`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      listing_id: listingId,
      action,
      session_id: sessionId,
      query,
    }),
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
  const sessionIdRef = useRef<string>(getPersistedSessionId());
  const [favoriteIds, setFavoriteIds] = useState<string[]>(() =>
    loadFavoriteIds(sessionIdRef.current),
  );
  const lastQueryRef = useRef<string | undefined>(undefined);
  const lastClickRef = useRef<{ id: string; ts: number } | null>(null);

  const handleSelect = useCallback((listingId: string) => {
    setSelectedId(listingId);
    const now = Date.now();
    const lastClick = lastClickRef.current;
    if (!lastClick || lastClick.id !== listingId || now - lastClick.ts > 1000) {
      lastClickRef.current = { id: listingId, ts: now };
      trackPreference(listingId, sessionIdRef.current, "click", lastQueryRef.current);
    }
  }, []);

  const handleFavorite = useCallback((listingId: string) => {
    if (favoriteIds.includes(listingId)) {
      return;
    }
    const next = [listingId, ...favoriteIds];
    setFavoriteIds(next);
    persistFavoriteIds(sessionIdRef.current, next);
    trackPreference(listingId, sessionIdRef.current, "favorite", lastQueryRef.current);
  }, [favoriteIds]);

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
  const meta = toolOutput.meta ?? {};
  const metaSessionId = typeof meta.session_id === "string" ? meta.session_id : null;
  lastQueryRef.current = typeof meta.query === "string" ? meta.query : undefined;

  useEffect(() => {
    if (!metaSessionId || metaSessionId === sessionIdRef.current) {
      return;
    }
    sessionIdRef.current = metaSessionId;
    window.localStorage.setItem(SESSION_STORAGE_KEY, metaSessionId);
    setFavoriteIds(loadFavoriteIds(metaSessionId));
  }, [metaSessionId]);

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
          favoriteIds={favoriteIds}
          onFavorite={handleFavorite}
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
