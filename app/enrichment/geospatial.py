"""Geospatial enrichment via geo.admin.ch Swiss federal API (async).

Enriches listings with:
- Municipality name and BFS number (backfills city/canton for SRED)
- Lake proximity (distance to nearest major Swiss lake)

Uses async concurrency for throughput. Progress is committed to DB
in batches so a crash never loses more than one batch.
"""
from __future__ import annotations

import asyncio
import logging
import math
import sqlite3
import time
from typing import Any

import httpx

logger = logging.getLogger(__name__)

GEO_ADMIN_BASE = "https://api3.geo.admin.ch/rest/services/api/MapServer"
MUNICIPALITY_LAYER = "ch.swisstopo.swissboundaries3d-gemeinde-flaeche.fill"

SWISS_LAKES: list[tuple[str, float, float]] = [
    ("Zürichsee", 47.2667, 8.65),
    ("Genfersee", 46.45, 6.55),
    ("Vierwaldstättersee", 47.0, 8.43),
    ("Thunersee", 46.68, 7.72),
    ("Brienzersee", 46.73, 7.97),
    ("Bielersee", 47.08, 7.17),
    ("Neuenburgersee", 46.9, 6.85),
    ("Zugersee", 47.12, 8.48),
    ("Bodensee", 47.55, 9.37),
    ("Greifensee", 47.35, 8.68),
    ("Sempachersee", 47.13, 8.15),
    ("Hallwilersee", 47.28, 8.22),
    ("Walensee", 47.12, 9.17),
    ("Murtensee", 46.93, 7.08),
    ("Lago Maggiore", 46.15, 8.75),
    ("Lago di Lugano", 46.0, 9.0),
    ("Baldeggerseee", 47.2, 8.27),
]

MAX_RETRIES = 4
BASE_BACKOFF = 1.0


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def nearest_lake_distance_m(lat: float, lon: float) -> int:
    return int(min(_haversine_m(lat, lon, lk_lat, lk_lon) for _, lk_lat, lk_lon in SWISS_LAKES))


async def _lookup_municipality(
    lat: float, lon: float,
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
) -> dict[str, Any] | None:
    async with sem:
        for attempt in range(MAX_RETRIES):
            try:
                resp = await client.get(
                    f"{GEO_ADMIN_BASE}/identify",
                    params={
                        "geometry": f"{lon},{lat}",
                        "geometryType": "esriGeometryPoint",
                        "layers": f"all:{MUNICIPALITY_LAYER}",
                        "tolerance": "0",
                        "sr": "4326",
                        "returnGeometry": "false",
                        "lang": "de",
                    },
                    timeout=15.0,
                )
                resp.raise_for_status()
                data = resp.json()
                results = data.get("results", [])
                if not results:
                    return None
                attrs = results[0].get("attributes", {})
                return {
                    "name": attrs.get("gemname"),
                    "bfs_nr": attrs.get("gde_nr"),
                    "canton": attrs.get("kanton"),
                }
            except (httpx.HTTPStatusError, httpx.ConnectError, httpx.ReadTimeout) as exc:
                wait = BASE_BACKOFF * (2 ** attempt)
                logger.debug("Retry %d for (%.4f,%.4f): %s", attempt + 1, lat, lon, exc)
                await asyncio.sleep(wait)
            except Exception as exc:
                logger.debug("geo.admin.ch lookup failed (%.4f,%.4f): %s", lat, lon, exc)
                return None
    return None


async def _process_one(
    lid: str, lat: float, lon: float,
    existing_city: str | None, existing_canton: str | None,
    client: httpx.AsyncClient, sem: asyncio.Semaphore,
) -> tuple | None:
    """Process a single listing. Returns update tuple or None if out-of-bounds."""
    if lat == 0.0 or lon == 0.0 or not (45.5 < lat < 48.0 and 5.5 < lon < 10.5):
        return None

    lake_dist = nearest_lake_distance_m(lat, lon)
    muni = await _lookup_municipality(lat, lon, client, sem)

    if muni:
        city_backfill = muni["name"] if not existing_city else None
        canton_backfill = muni["canton"] if not existing_canton else None
        return (muni["name"], muni["bfs_nr"], lake_dist, city_backfill, canton_backfill, lid)
    else:
        return (None, None, lake_dist, None, None, lid)


def _flush_updates(conn: sqlite3.Connection, updates: list[tuple]) -> None:
    conn.executemany(
        "UPDATE listings SET "
        "municipality = COALESCE(?, municipality), "
        "bfs_number = COALESCE(?, bfs_number), "
        "lake_distance_m = ?, "
        "city = COALESCE(?, city), "
        "canton = COALESCE(?, canton) "
        "WHERE listing_id = ?",
        updates,
    )
    conn.commit()


async def _run_async(
    rows: list,
    conn: sqlite3.Connection,
    concurrency: int,
    batch_size: int,
) -> dict[str, int]:
    stats = {"municipality_filled": 0, "lake_filled": 0,
             "city_backfilled": 0, "canton_backfilled": 0, "errors": 0, "skipped": 0}
    total = len(rows)

    sem = asyncio.Semaphore(concurrency)
    updates: list[tuple] = []
    t_start = time.time()

    async with httpx.AsyncClient(
        limits=httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency),
    ) as client:
        wave_size = concurrency * 3
        for wave_start in range(0, total, wave_size):
            wave = rows[wave_start : wave_start + wave_size]

            tasks = [
                _process_one(lid, lat, lon, city, canton, client, sem)
                for lid, lat, lon, city, canton in wave
            ]
            results = await asyncio.gather(*tasks)

            for result in results:
                if result is None:
                    stats["skipped"] += 1
                    continue
                muni_name = result[0]
                if muni_name:
                    stats["municipality_filled"] += 1
                else:
                    stats["errors"] += 1
                stats["lake_filled"] += 1
                if result[3]:
                    stats["city_backfilled"] += 1
                if result[4]:
                    stats["canton_backfilled"] += 1
                updates.append(result)

            if len(updates) >= batch_size:
                _flush_updates(conn, updates)
                updates.clear()

            done = wave_start + len(wave)
            elapsed = time.time() - t_start
            rate = done / elapsed if elapsed > 0 else 0
            eta_s = (total - done) / rate if rate > 0 else 0
            logger.info(
                "  [%d/%d] %.0f%%  |  %.1f req/s  |  ETA %.1f min  |  ok=%d err=%d",
                done, total, done / total * 100, rate, eta_s / 60,
                stats["municipality_filled"], stats["errors"],
            )

    if updates:
        _flush_updates(conn, updates)

    elapsed = time.time() - t_start
    logger.info("Geospatial done in %.1f min: %s", elapsed / 60, stats)
    return stats


def enrich_geospatial(
    conn: sqlite3.Connection,
    *,
    concurrency: int = 25,
    batch_size: int = 100,
) -> dict[str, int]:
    """Enrich listings with municipality + lake distance (async).

    Only processes listings that still have municipality IS NULL,
    so it's safe to re-run after a crash.
    """
    rows = conn.execute(
        "SELECT listing_id, latitude, longitude, city, canton "
        "FROM listings "
        "WHERE latitude IS NOT NULL AND longitude IS NOT NULL "
        "AND municipality IS NULL"
    ).fetchall()

    if not rows:
        logger.info("All listings already have municipality data")
        return {"municipality_filled": 0, "lake_filled": 0, "errors": 0}

    total = len(rows)
    logger.info("Enriching geospatial for %d listings (concurrency=%d)", total, concurrency)

    return asyncio.run(_run_async(rows, conn, concurrency, batch_size))
