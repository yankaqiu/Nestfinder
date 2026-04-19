"""Standalone pipeline debugger web UI.

Run:
    .venv/bin/python -m benchmarks.debug_server

Then open http://127.0.0.1:8899/debug
"""
from __future__ import annotations

import sys
import time
import traceback
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
from starlette.staticfiles import StaticFiles
import uvicorn

from app.config import get_settings
from app.core.hard_filters import search_listings
from app.db import get_connection
from app.harness.search_service import to_hard_filter_params
from app.preferences import build_user_profile, get_events, get_recent_sessions, get_search_history
from app.participant.hard_fact_extraction import extract_hard_facts
from app.participant.ranking import _SIGNAL_MATCHERS, _build_rank_breakdowns, _image_bonus_cap
from app.participant.soft_fact_extraction import extract_soft_facts
from app.participant.soft_filtering import filter_soft_facts

settings = get_settings()
DB_PATH = settings.db_path

app = FastAPI(title="NestFinder Pipeline Debugger")

_sred_images_dir = settings.raw_data_dir / "sred_images"
if _sred_images_dir.exists():
    app.mount(
        "/raw-data-images",
        StaticFiles(directory=str(_sred_images_dir)),
        name="raw-data-images",
    )


class TraceRequest(BaseModel):
    query: str = Field(min_length=1)
    top_n: int = Field(default=20, ge=1, le=100)


class PreferenceTraceRequest(BaseModel):
    session_id: str | None = None
    query: str | None = None
    top_n: int = Field(default=10, ge=1, le=50)


def _total_listings() -> int:
    with get_connection(DB_PATH) as conn:
        return conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]


def _candidate_summary(c: dict[str, Any]) -> dict[str, Any]:
    return {
        "listing_id": c.get("listing_id"),
        "title": c.get("title"),
        "description": (c.get("description") or "")[:300],
        "city": c.get("city"),
        "postal_code": c.get("postal_code"),
        "canton": c.get("canton"),
        "street": c.get("street"),
        "price": c.get("price"),
        "rooms": c.get("rooms"),
        "area": c.get("area"),
        "available_from": c.get("available_from"),
        "offer_type": c.get("offer_type"),
        "object_category": c.get("object_category"),
        "features": c.get("features", []),
        "hero_image_url": c.get("hero_image_url"),
        "original_url": c.get("original_url"),
        "distance_public_transport": c.get("distance_public_transport"),
        "distance_shop": c.get("distance_shop"),
        "distance_kindergarten": c.get("distance_kindergarten"),
        "distance_school_1": c.get("distance_school_1"),
        # --- enrichment data ---
        "floor_level": c.get("floor_level"),
        "year_built": c.get("year_built"),
        "renovation_year": c.get("renovation_year"),
        "is_furnished": c.get("is_furnished"),
        "price_per_sqm": c.get("price_per_sqm"),
        "price_vs_city_median": c.get("price_vs_city_median"),
        "municipality": c.get("municipality"),
        "lake_distance_m": c.get("lake_distance_m"),
        "is_urban": c.get("is_urban"),
        # --- v3.1 data ---
        "nearest_stop_name": c.get("nearest_stop_name"),
        "nearest_stop_distance_m": c.get("nearest_stop_distance_m"),
        "nearest_train_name": c.get("nearest_train_name"),
        "nearest_train_distance_m": c.get("nearest_train_distance_m"),
        "nearest_hb_name": c.get("nearest_hb_name"),
        "nearest_hb_distance_m": c.get("nearest_hb_distance_m"),
        "municipality_name": c.get("municipality_name"),
        "district_name": c.get("district_name"),
        "canton_name": c.get("canton_name"),
        "population_total": c.get("population_total"),
        "population_density": c.get("population_density"),
        "population_density_bucket": c.get("population_density_bucket"),
        "price_per_m2": c.get("price_per_m2"),
        "avg_price_per_m2_municipality": c.get("avg_price_per_m2_municipality"),
        "price_per_m2_vs_municipality": c.get("price_per_m2_vs_municipality"),
        "price_per_m2_vs_municipality_label": c.get("price_per_m2_vs_municipality_label"),
    }


@app.post("/trace")
def trace_pipeline(req: TraceRequest) -> dict[str, Any]:
    result: dict[str, Any] = {"query": req.query, "stages": []}
    total = _total_listings()
    result["total_listings_in_db"] = total

    t0 = time.perf_counter()
    try:
        hard_facts = extract_hard_facts(req.query)
        hard_dict = {
            k: v for k, v in hard_facts.__dict__.items()
            if v is not None and k not in ("limit", "offset", "sort_by")
        }
        result["stages"].append({
            "name": "Hard Fact Extraction",
            "status": "ok",
            "duration_ms": round((time.perf_counter() - t0) * 1000),
            "extracted": hard_dict,
        })
    except Exception as e:
        result["stages"].append({
            "name": "Hard Fact Extraction",
            "status": "error",
            "error": str(e),
            "traceback": traceback.format_exc(),
        })
        return result

    t0 = time.perf_counter()
    try:
        params = to_hard_filter_params(hard_facts)
        params.limit = 999_999
        params.offset = 0
        candidates = search_listings(DB_PATH, params)
        result["stages"].append({
            "name": "Hard Filtering (DB)",
            "status": "ok",
            "duration_ms": round((time.perf_counter() - t0) * 1000),
            "count_before": total,
            "count_after": len(candidates),
            "removed": total - len(candidates),
        })
    except Exception as e:
        result["stages"].append({
            "name": "Hard Filtering (DB)",
            "status": "error",
            "error": str(e),
        })
        return result

    t0 = time.perf_counter()
    try:
        soft_facts = extract_soft_facts(req.query)
        signals = soft_facts.get("signals", {})
        extras = {k: v for k, v in soft_facts.items() if k not in ("signals", "raw_query")}
        result["stages"].append({
            "name": "Soft Fact Extraction",
            "status": "ok",
            "duration_ms": round((time.perf_counter() - t0) * 1000),
            "signals": signals,
            "extras": extras,
        })
    except Exception:
        soft_facts = {"signals": {}, "raw_query": req.query}
        signals = {}
        result["stages"].append({
            "name": "Soft Fact Extraction",
            "status": "fallback",
            "signals": {},
            "extras": {},
        })

    t0 = time.perf_counter()
    after_soft = filter_soft_facts(candidates, soft_facts)
    removed_by_soft = len(candidates) - len(after_soft)
    removed_items = []
    if removed_by_soft > 0:
        after_ids = {c["listing_id"] for c in after_soft}
        removed_items = [
            {"listing_id": c.get("listing_id"), "title": c.get("title"), "price": c.get("price")}
            for c in candidates if c["listing_id"] not in after_ids
        ][:20]
    result["stages"].append({
        "name": "Soft Filtering (Junk Removal)",
        "status": "ok",
        "duration_ms": round((time.perf_counter() - t0) * 1000),
        "count_before": len(candidates),
        "count_after": len(after_soft),
        "removed": removed_by_soft,
        "removed_samples": removed_items,
    })

    t0 = time.perf_counter()
    rank_breakdowns = _build_rank_breakdowns(after_soft, soft_facts)
    top_n = rank_breakdowns[:req.top_n]
    ranked_output = []
    for rank, rank_breakdown in enumerate(top_n, 1):
        entry = _candidate_summary(rank_breakdown.candidate)
        entry["rank"] = rank
        entry["score"] = round(rank_breakdown.final_score, 3)
        entry["soft_score"] = round(rank_breakdown.soft_score, 3)
        entry["global_score_bonus"] = round(rank_breakdown.global_score_bonus, 3)
        entry["global_scores"] = {
            key: round(value, 3) for key, value in rank_breakdown.global_scores.items()
        }
        entry["explanation"] = rank_breakdown.explanation
        entry["image_score"] = round(rank_breakdown.image_score, 3)
        entry["image_bonus"] = round(rank_breakdown.image_bonus, 3)
        entry["image_bonus_cap"] = round(rank_breakdown.image_bonus_cap, 3)
        entry["matched_signals"] = rank_breakdown.matched
        entry["best_image_url"] = rank_breakdown.best_image_url
        signal_breakdown = {}
        for sig_name in entry["matched_signals"]:
            if sig_name == "area_pref":
                signal_breakdown["area_pref"] = 0.3
            else:
                weight = signals.get(sig_name, 0)
                matcher = _SIGNAL_MATCHERS.get(sig_name)
                strength = 0.0
                if matcher:
                    try:
                        strength = matcher(rank_breakdown.candidate)
                    except Exception:
                        strength = 1.0
                signal_breakdown[sig_name] = round(weight * strength, 3)
        entry["signal_breakdown"] = signal_breakdown
        ranked_output.append(entry)

    score_distribution: dict[str, int] = {}
    for rank_breakdown in rank_breakdowns:
        bucket = str(round(rank_breakdown.final_score, 1))
        score_distribution[bucket] = score_distribution.get(bucket, 0) + 1

    result["stages"].append({
        "name": "Ranking",
        "status": "ok",
        "duration_ms": round((time.perf_counter() - t0) * 1000),
        "total_scored": len(rank_breakdowns),
        "max_score": round(rank_breakdowns[0].final_score, 3) if rank_breakdowns else 0,
        "min_score": round(rank_breakdowns[-1].final_score, 3) if rank_breakdowns else 0,
        "image_bonus_cap": round(_image_bonus_cap(soft_facts), 3),
        "boosted_count": sum(1 for item in rank_breakdowns if item.image_bonus > 0),
        "max_image_bonus": round(max((item.image_bonus for item in rank_breakdowns), default=0.0), 3),
        "score_distribution": score_distribution,
        "top_results": ranked_output,
    })

    return result


@app.post("/preferences-trace")
def trace_preferences(req: PreferenceTraceRequest) -> dict[str, Any]:
    session_id = (req.session_id or "").strip() or None
    profile = build_user_profile(session_id=session_id)
    result: dict[str, Any] = {
        "session_id": session_id,
        "recent_sessions": get_recent_sessions(limit=15),
        "profile": profile,
        "events": get_events(session_id=session_id, limit=100) if session_id else [],
        "recent_searches": get_search_history(session_id=session_id, limit=20) if session_id else [],
        "query_preview": None,
    }

    query = (req.query or "").strip()
    if not query:
        return result

    hard_facts = extract_hard_facts(query)
    params = to_hard_filter_params(hard_facts)
    params.limit = max(100, req.top_n * 8)
    params.offset = 0
    candidates = search_listings(DB_PATH, params)
    soft_facts = extract_soft_facts(query)
    filtered = filter_soft_facts(candidates, soft_facts)
    rank_breakdowns = _build_rank_breakdowns(filtered, soft_facts, user_profile=profile)

    top_results: list[dict[str, Any]] = []
    for rank, rank_breakdown in enumerate(rank_breakdowns[: req.top_n], 1):
        entry = _candidate_summary(rank_breakdown.candidate)
        entry["rank"] = rank
        entry["final_score"] = round(rank_breakdown.final_score, 3)
        entry["soft_score"] = round(rank_breakdown.soft_score, 3)
        entry["global_score_bonus"] = round(rank_breakdown.global_score_bonus, 3)
        entry["global_scores"] = {
            key: round(value, 3) for key, value in rank_breakdown.global_scores.items()
        }
        entry["explanation"] = rank_breakdown.explanation
        entry["preference_bonus"] = round(rank_breakdown.preference_bonus, 3)
        entry["preference_reasons"] = rank_breakdown.preference_reasons
        entry["image_bonus"] = round(rank_breakdown.image_bonus, 3)
        entry["image_score"] = round(rank_breakdown.image_score, 3)
        entry["matched_signals"] = rank_breakdown.matched
        entry["best_image_url"] = rank_breakdown.best_image_url
        top_results.append(entry)

    result["query_preview"] = {
        "query": query,
        "candidate_count": len(candidates),
        "filtered_count": len(filtered),
        "total_scored": len(rank_breakdowns),
        "boosted_by_preferences": sum(1 for item in rank_breakdowns if item.preference_bonus > 0),
        "max_preference_bonus": round(max((item.preference_bonus for item in rank_breakdowns), default=0.0), 3),
        "top_results": top_results,
    }
    return result


# ---------------------------------------------------------------------------
# Inline HTML — fully self-contained, no external files needed
# ---------------------------------------------------------------------------

_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>NestFinder Pipeline Debugger</title>
<style>
  :root {
    --bg: #0f1117; --surface: #1a1d27; --surface2: #222535; --border: #2d3148;
    --text: #e1e4ed; --muted: #8b8fa7; --accent: #6c8cff; --accent2: #9f7aea;
    --green: #4ade80; --red: #f87171; --orange: #fbbf24; --cyan: #22d3ee;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'Inter',-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; background:var(--bg); color:var(--text); line-height:1.5; min-height:100vh; }
  .container { max-width:1280px; margin:0 auto; padding:24px; }
  header { margin-bottom:32px; }
  header h1 { font-size:1.5rem; font-weight:600; color:var(--accent); }
  header p { color:var(--muted); font-size:.875rem; margin-top:4px; }

  .query-box { background:var(--surface); border:1px solid var(--border); border-radius:12px; padding:20px; margin-bottom:24px; }
  .query-box label { font-size:.75rem; text-transform:uppercase; letter-spacing:.05em; color:var(--muted); display:block; margin-bottom:8px; }
  .input-row { display:flex; gap:12px; }
  .input-row input[type="text"] { flex:1; background:var(--surface2); border:1px solid var(--border); border-radius:8px; padding:12px 16px; color:var(--text); font-size:.95rem; outline:none; transition:border-color .2s; }
  .input-row input:focus { border-color:var(--accent); }
  .input-row select { background:var(--surface2); border:1px solid var(--border); border-radius:8px; padding:12px 14px; color:var(--text); font-size:.85rem; outline:none; cursor:pointer; }
  .input-row button { background:var(--accent); color:#fff; border:none; border-radius:8px; padding:12px 28px; font-size:.95rem; font-weight:600; cursor:pointer; transition:opacity .2s; white-space:nowrap; }
  .input-row button:hover { opacity:.85; }
  .input-row button:disabled { opacity:.4; cursor:not-allowed; }

  .presets { margin-top:12px; display:flex; flex-wrap:wrap; gap:6px; }
  .presets button { background:var(--surface2); color:var(--muted); border:1px solid var(--border); border-radius:6px; padding:4px 10px; font-size:.75rem; cursor:pointer; transition:all .15s; }
  .presets button:hover { color:var(--text); border-color:var(--accent); }

  .spinner { display:none; text-align:center; padding:48px; color:var(--muted); }
  .spinner.active { display:block; }
  .spinner .dot { display:inline-block; width:8px; height:8px; border-radius:50%; background:var(--accent); margin:0 3px; animation:pulse 1.4s infinite ease-in-out both; }
  .spinner .dot:nth-child(2) { animation-delay:.16s; }
  .spinner .dot:nth-child(3) { animation-delay:.32s; }
  @keyframes pulse { 0%,80%,100%{transform:scale(0);opacity:.5} 40%{transform:scale(1);opacity:1} }

  #results { display:none; }
  #results.active { display:block; }

  .pipeline-flow { display:flex; align-items:center; gap:6px; margin-bottom:28px; flex-wrap:wrap; }
  .pipeline-node { background:var(--surface); border:1px solid var(--border); border-radius:8px; padding:10px 16px; text-align:center; min-width:100px; }
  .pipeline-node .count { font-size:1.25rem; font-weight:700; color:var(--accent); }
  .pipeline-node .label { font-size:.7rem; color:var(--muted); margin-top:2px; }
  .pipeline-node .delta { font-size:.7rem; color:var(--red); }
  .pipeline-arrow { color:var(--muted); font-size:1.2rem; }

  .stage-card { background:var(--surface); border:1px solid var(--border); border-radius:12px; margin-bottom:16px; overflow:hidden; }
  .stage-header { padding:14px 20px; display:flex; justify-content:space-between; align-items:center; cursor:pointer; user-select:none; }
  .stage-header:hover { background:var(--surface2); }
  .stage-header h3 { font-size:.95rem; font-weight:600; }
  .stage-header .badge { font-size:.7rem; padding:2px 8px; border-radius:4px; font-weight:600; }
  .badge-ok { background:rgba(74,222,128,.15); color:var(--green); }
  .badge-error { background:rgba(248,113,113,.15); color:var(--red); }
  .badge-ms { background:rgba(108,140,255,.1); color:var(--accent); margin-left:8px; }
  .stage-body { padding:0 20px 16px; display:none; }
  .stage-card.open .stage-body { display:block; }
  .stage-card.open .stage-header { border-bottom:1px solid var(--border); }

  .kv-grid { display:grid; grid-template-columns:auto 1fr; gap:4px 16px; font-size:.85rem; margin:12px 0; }
  .kv-grid .key { color:var(--muted); }
  .kv-grid .val { color:var(--text); font-family:'JetBrains Mono','Fira Code',monospace; font-size:.8rem; }

  .signal-chips { display:flex; flex-wrap:wrap; gap:6px; margin:8px 0; }
  .signal-chip { background:var(--surface2); border:1px solid var(--border); border-radius:6px; padding:4px 10px; font-size:.78rem; }
  .signal-chip .weight { color:var(--accent); font-weight:600; margin-left:4px; }

  .distribution-bar { display:flex; height:24px; border-radius:6px; overflow:hidden; margin:8px 0; gap:1px; }
  .distribution-bar .seg { display:flex; align-items:center; justify-content:center; font-size:.65rem; font-weight:600; color:#fff; min-width:20px; padding:0 4px; }

  .results-table { width:100%; border-collapse:collapse; font-size:.82rem; }
  .results-table th { text-align:left; padding:8px 10px; color:var(--muted); font-weight:500; font-size:.72rem; text-transform:uppercase; letter-spacing:.04em; border-bottom:1px solid var(--border); position:sticky; top:0; background:var(--surface); z-index:1; }
  .results-table td { padding:10px; border-bottom:1px solid var(--border); vertical-align:top; }
  .results-table tr:hover { background:var(--surface2); }
  .results-table .score-cell { font-weight:700; font-family:'JetBrains Mono',monospace; font-size:.9rem; }
  .score-high { color:var(--green); }
  .score-mid { color:var(--orange); }
  .score-zero { color:var(--muted); }

  .match-tag { display:inline-block; font-size:.68rem; background:rgba(108,140,255,.12); color:var(--accent); border-radius:4px; padding:1px 6px; margin:1px 2px; }
  .match-tag .mw { opacity:.6; margin-left:3px; }

  .expand-btn { background:none; border:1px solid var(--border); color:var(--muted); border-radius:4px; padding:2px 8px; font-size:.7rem; cursor:pointer; margin-top:4px; }
  .expand-btn:hover { color:var(--text); border-color:var(--accent); }

  .listing-detail { display:none; background:var(--surface2); padding:12px; border-radius:8px; margin-top:8px; font-size:.8rem; }
  .listing-detail.open { display:block; }
  .listing-detail .desc { color:var(--muted); line-height:1.4; margin-bottom:6px; }
  .listing-detail .meta-row { display:flex; gap:12px; flex-wrap:wrap; }
  .listing-detail .meta-item { color:var(--muted); }
  .listing-detail .meta-item span { color:var(--text); }
  .photo-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:10px; margin-top:10px; }
  .photo-card { background:rgba(255,255,255,.02); border:1px solid var(--border); border-radius:8px; overflow:hidden; }
  .photo-card img { display:block; width:100%; height:150px; object-fit:cover; background:#0b0d12; }
  .photo-card .photo-meta { display:flex; justify-content:space-between; gap:8px; align-items:center; padding:8px 10px; font-size:.72rem; color:var(--muted); }
  .photo-card .photo-meta a { color:var(--accent); text-decoration:none; }
  .photo-card .photo-meta a:hover { text-decoration:underline; }

  .removed-list { list-style:none; font-size:.8rem; margin:8px 0; }
  .removed-list li { padding:4px 0; border-bottom:1px solid var(--border); display:flex; justify-content:space-between; }
  .removed-list .r-title { color:var(--muted); flex:1; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
  .removed-list .r-price { color:var(--red); min-width:80px; text-align:right; }

  .error-box { background:rgba(248,113,113,.08); border:1px solid rgba(248,113,113,.3); border-radius:8px; padding:12px; color:var(--red); font-size:.85rem; margin:12px 0; white-space:pre-wrap; font-family:monospace; }
</style>
</head>
<body>
<div class="container">
  <header>
    <h1>NestFinder Pipeline Debugger</h1>
    <p>Step-by-step visualization of the search pipeline. Enter a query to trace each stage. <a href="/debug/preferences" style="color:var(--accent)">Preference Debugger →</a></p>
  </header>
  <div class="query-box">
    <label>Search Query</label>
    <div class="input-row">
      <input type="text" id="queryInput" placeholder="e.g. Ruhige 3.5-Zimmer Wohnung in Zürich unter CHF 2500" />
      <select id="topN">
        <option value="10">Top 10</option>
        <option value="20" selected>Top 20</option>
        <option value="50">Top 50</option>
      </select>
      <button id="runBtn" onclick="runTrace()">Run Trace</button>
    </div>
    <div class="presets">
      <div style="width:100%;font-size:.65rem;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;margin-top:4px;margin-bottom:2px">🏠 General</div>
      <button onclick="setQ('Ruhige 3.5-Zimmer Wohnung in Zürich unter CHF 2500')">DE: Zürich quiet 3.5Z</button>
      <button onclick="setQ('2 bedroom flat in Basel under CHF 3500')">EN: Basel 2-bed</button>
      <button onclick="setQ('Je cherche un appartement 3 pièces à Genève, lumineux, moins de 2500 CHF')">FR: Genève 3 pièces</button>
      <button onclick="setQ('Cerco un appartamento a Lugano, 3 locali, massimo 2000 CHF')">IT: Lugano 3 locali</button>
      <button onclick="setQ('Studio in Lausanne under CHF 1500')">EN: Studio Lausanne</button>

      <div style="width:100%;font-size:.65rem;color:var(--cyan);text-transform:uppercase;letter-spacing:.06em;margin-top:10px;margin-bottom:2px">🧪 Enrichment: Claude Text Extraction</div>
      <button onclick="setQ('Wohnung mit Lift und Geschirrspüler in Zürich, 3 Zimmer')">🆕 elevator + dishwasher</button>
      <button onclick="setQ('apartment in Bern with washing machine, cellar, and garden, 3 rooms under 2500 CHF')">🆕 washing machine + cellar + garden</button>
      <button onclick="setQ('Ich brauche eine Wohnung mit Balkon, Waschmaschine und Parkplatz in Zürich')">🆕 balcony + laundry + parking</button>
      <button onclick="setQ('pet-friendly apartment in Basel with elevator, at least 3 rooms')">🆕 pets + elevator</button>
      <button onclick="setQ('Minergie-Wohnung mit Cheminée in Luzern')">🆕 minergie + fireplace</button>
      <button onclick="setQ('möblierte Wohnung in Genf mit Geschirrspüler und Kellerabteil')">🆕 furnished + dishwasher + cellar</button>

      <div style="width:100%;font-size:.65rem;color:var(--cyan);text-transform:uppercase;letter-spacing:.06em;margin-top:10px;margin-bottom:2px">🧪 Enrichment: Geospatial (lake + municipality)</div>
      <button onclick="setQ('Wohnung nahe am Zürichsee, 3.5 Zimmer, max 3500 CHF')">🆕 near Zürichsee</button>
      <button onclick="setQ('apartment near lake Geneva with a view, 2 bedrooms')">🆕 near lake + views</button>
      <button onclick="setQ('Seenähe in Luzern, ruhige Lage, mindestens 80m²')">🆕 near lake + quiet + spacious</button>
      <button onclick="setQ('3 Zimmer in Thalwil oder Kilchberg, nahe am See, mit Balkon')">🆕 lakeside + balcony</button>

      <div style="width:100%;font-size:.65rem;color:var(--cyan);text-transform:uppercase;letter-spacing:.06em;margin-top:10px;margin-bottom:2px">🧪 Enrichment: Backfill (year/floor/price)</div>
      <button onclick="setQ('modern apartment in Zurich built after 2015, bright, high floor')">🆕 modern + bright (year + floor)</button>
      <button onclick="setQ('Neubau in Bern, Erstvermietung, mindestens 3.5 Zimmer')">🆕 new_build (year ≥ 2022)</button>
      <button onclick="setQ('günstige Wohnung in Zürich, 2.5 Zimmer, gutes Preis-Leistungs-Verhältnis')">🆕 affordable (price vs median)</button>
      <button onclick="setQ('recently renovated apartment in Basel, well maintained, 3 rooms')">🆕 well_maintained (renovation)</button>
      <button onclick="setQ('helle Dachwohnung in Zürich, oberste Etage, mit Aussicht')">🆕 bright + views (high floor)</button>

      <div style="width:100%;font-size:.65rem;color:var(--cyan);text-transform:uppercase;letter-spacing:.06em;margin-top:10px;margin-bottom:2px">🧪 Enrichment: Neighborhood (urban/rural)</div>
      <button onclick="setQ('lively apartment in Zurich city center, near restaurants and nightlife')">🆕 lively (is_urban=1)</button>
      <button onclick="setQ('ruhige Wohnung auf dem Land, Kanton Zürich, Natur, mindestens 4 Zimmer')">🆕 quiet + rural (is_urban=0)</button>
      <button onclick="setQ('vibrant neighborhood in Basel, close to cafés and bars, 2 rooms')">🆕 lively + urban</button>

      <div style="width:100%;font-size:.65rem;color:var(--cyan);text-transform:uppercase;letter-spacing:.06em;margin-top:10px;margin-bottom:2px">🧪 Combo: multiple enrichment sources</div>
      <button onclick="setQ('Wir suchen als Familie eine moderne 4.5-Zimmer-Wohnung nahe am See in der Region Zürich, mit Lift, Balkon, Waschmaschine, unter 4000 CHF, guter Zustand')">🆕 FULL: family lakeside modern</button>
      <button onclick="setQ('I need a bright, affordable studio in Bern near public transport, with an elevator and washing machine, move in by June')">🆕 FULL: student-friendly combo</button>
      <button onclick="setQ('Neubau-Wohnung in Luzern, Minergie, mit Garten und Parkplatz, ruhige Lage, 3.5 Zimmer, nahe am See')">🆕 FULL: new build + green + lake</button>
      <button onclick="setQ('Appartement moderne à Lausanne, proche du lac, avec ascenseur, lave-vaisselle, et balcon, maximum 3000 CHF')">🆕 FR: modern + lake + enriched features</button>
      <button onclick="setQ('Cerco un appartamento moderno a Lugano vicino al lago, con ascensore, lavastoviglie, balcone, massimo 2500 CHF')">🆕 IT: modern + lake + features</button>

      <div style="width:100%;font-size:.65rem;color:var(--cyan);text-transform:uppercase;letter-spacing:.06em;margin-top:10px;margin-bottom:2px">🚆 v3.1: Transit + Demographics + Price Benchmarking</div>
      <button onclick="setQ('well-connected apartment near a train station in Zurich, 3 rooms')">🆕 well_connected + near_train</button>
      <button onclick="setQ('Wohnung nahe Hauptbahnhof Zürich, gut angebunden, 2.5 Zimmer unter 2500 CHF')">🆕 near_hauptbahnhof + well_connected</button>
      <button onclick="setQ('affordable apartment in a small town, rural area, good value')">🆕 small_town + low_density + good_value</button>
      <button onclick="setQ('Wohnung in einer grossen Stadt, lebhaft, zentral, mindestens 3 Zimmer')">🆕 large_municipality + high_density + lively</button>
      <button onclick="setQ('good value apartment near train station, quiet village, 3.5 rooms')">🆕 good_value_local + near_train + small_town</button>
      <button onclick="setQ('modern apartment in Zürich, well connected, near lake, with balcony, under 4000 CHF')">🆕 FULL v3.1: modern + transit + lake</button>

      <div style="width:100%;font-size:.65rem;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;margin-top:10px;margin-bottom:2px">📋 Original presets</div>
      <button onclick="setQ('Ich suche eine Wohnung im Raum Zürich, Dübendorf oder Wallisellen, idealerweise 2.5 bis 3.5 Zimmer, ab 70 m², Budget bis 3100 CHF, max 25 Minuten mit dem ÖV bis Stadelhofen')">DE: Multi-city commute</button>
      <button onclick="setQ('Wir suchen als Familie zu dritt etwas im Raum Kilchberg, Rüschlikon oder Thalwil, am liebsten nahe am See oder mit schneller Verbindung nach Zürich, mindestens 3.5 Zimmer, ab 90 m², Budget bis 4300 CHF, gern mit Balkon / Terrasse, Lift, Keller')">DE: Family lakeside</button>
      <button onclick="setQ('Ich suche etwas Kleineres in Lausanne, möglichst in der Nähe von EPFL, gern möbliert, unter 2100 CHF, mit guter Anbindung')">DE: Lausanne EPFL</button>
      <button onclick="setQ('Wohnung in 8001 Zürich, 3 Zimmer')">DE: postal code 8001</button>
      <button onclick="setQ('Haus zu kaufen in Zürich, 5 Zimmer')">DE: Zürich house sale</button>
      <button onclick="setQ('something nice and central')">Vague: nice &amp; central</button>
    </div>
  </div>
  <div class="spinner" id="spinner"><div><span class="dot"></span><span class="dot"></span><span class="dot"></span></div><p style="margin-top:12px">Running pipeline trace&hellip;</p></div>
  <div id="results">
    <div class="pipeline-flow" id="pipelineFlow"></div>
    <div id="stageCards"></div>
  </div>
</div>
<script>
function setQ(q){document.getElementById('queryInput').value=q}
async function runTrace(){
  const query=document.getElementById('queryInput').value.trim();
  if(!query)return;
  const topN=parseInt(document.getElementById('topN').value);
  const btn=document.getElementById('runBtn'), spinner=document.getElementById('spinner'), results=document.getElementById('results');
  btn.disabled=true; spinner.classList.add('active'); results.classList.remove('active');
  try{
    const resp=await fetch('/trace',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({query,top_n:topN})});
    renderResults(await resp.json());
  }catch(e){alert('Error: '+e.message)}
  finally{btn.disabled=false;spinner.classList.remove('active')}
}
document.getElementById('queryInput').addEventListener('keydown',e=>{if(e.key==='Enter')runTrace()});

function renderResults(data){
  document.getElementById('results').classList.add('active');
  renderFlow(data); renderCards(data);
}
function renderFlow(data){
  const el=document.getElementById('pipelineFlow'),s=data.stages,t=data.total_listings_in_db;
  let n=[{c:t.toLocaleString(),l:'Total in DB',d:''}];
  for(const st of s){
    if(st.name==='Hard Filtering (DB)') n.push({c:(st.count_after??'?').toLocaleString(),l:'After Hard',d:'-'+(st.removed??0).toLocaleString()});
    else if(st.name==='Soft Filtering (Junk Removal)') n.push({c:(st.count_after??'?').toLocaleString(),l:'After Soft',d:st.removed>0?'-'+st.removed:''});
    else if(st.name==='Ranking') n.push({c:(st.total_scored??'?').toLocaleString(),l:'Scored',d:''});
  }
  el.innerHTML=n.map((x,i)=>(i>0?'<span class="pipeline-arrow">→</span>':'')+`<div class="pipeline-node"><div class="count">${x.c}</div><div class="label">${x.l}</div>${x.d?`<div class="delta">${x.d}</div>`:''}</div>`).join('');
}
function renderCards(data){
  const el=document.getElementById('stageCards'); el.innerHTML='';
  for(const s of data.stages){
    const card=document.createElement('div'); card.className='stage-card open';
    const ok=s.status==='ok'||s.status==='fallback';
    let hr=`<span class="badge ${ok?'badge-ok':'badge-error'}">${s.status==='fallback'?'FALLBACK':s.status.toUpperCase()}</span>`;
    if(s.duration_ms!=null) hr+=`<span class="badge badge-ms">${s.duration_ms}ms</span>`;
    card.innerHTML=`<div class="stage-header" onclick="this.parentElement.classList.toggle('open')"><h3>${s.name}</h3><div>${hr}</div></div><div class="stage-body">${renderBody(s)}</div>`;
    el.appendChild(card);
  }
}
function renderBody(s){
  if(s.status==='error') return `<div class="error-box">${esc(s.error||'')}\\n${esc(s.traceback||'')}</div>`;
  switch(s.name){
    case 'Hard Fact Extraction': return renderHard(s);
    case 'Hard Filtering (DB)': return renderHardF(s);
    case 'Soft Fact Extraction': return renderSoft(s);
    case 'Soft Filtering (Junk Removal)': return renderSoftF(s);
    case 'Ranking': return renderRank(s);
    default: return `<pre>${JSON.stringify(s,null,2)}</pre>`;
  }
}
function renderHard(s){
  const e=s.extracted||{}; if(!Object.keys(e).length) return '<p style="color:var(--muted);font-size:.85rem">No hard constraints extracted.</p>';
  let h='<div class="kv-grid">';
  for(const[k,v]of Object.entries(e)) h+=`<div class="key">${k}</div><div class="val">${JSON.stringify(v)}</div>`;
  return h+'</div>';
}
function renderHardF(s){
  const p=s.count_before>0?((s.removed/s.count_before)*100).toFixed(1):0;
  return `<div class="kv-grid"><div class="key">Before</div><div class="val">${s.count_before.toLocaleString()}</div><div class="key">After</div><div class="val">${s.count_after.toLocaleString()}</div><div class="key">Removed</div><div class="val" style="color:var(--red)">${s.removed.toLocaleString()} (${p}%)</div></div><div class="distribution-bar"><div class="seg" style="flex:${s.count_after};background:var(--green)">${s.count_after}</div><div class="seg" style="flex:${Math.max(s.removed,1)};background:var(--red);opacity:.6">${s.removed}</div></div>`;
}
function renderSoft(s){
  const sig=s.signals||{},ext=s.extras||{};
  let h='<h4 style="font-size:.8rem;color:var(--muted);margin:8px 0 4px">Signals</h4>';
  if(!Object.keys(sig).length) h+='<p style="color:var(--muted);font-size:.82rem">No soft signals detected.</p>';
  else{h+='<div class="signal-chips">';for(const[n,w]of Object.entries(sig)) h+=`<span class="signal-chip">${n}<span class="weight">${w}</span></span>`;h+='</div>';}
  if(Object.keys(ext).length){h+='<h4 style="font-size:.8rem;color:var(--muted);margin:12px 0 4px">Extras</h4><div class="kv-grid">';for(const[k,v]of Object.entries(ext)) h+=`<div class="key">${k}</div><div class="val">${v??'—'}</div>`;h+='</div>';}
  return h;
}
function renderSoftF(s){
  let h=`<div class="kv-grid"><div class="key">Before</div><div class="val">${s.count_before}</div><div class="key">After</div><div class="val">${s.count_after}</div><div class="key">Junk removed</div><div class="val" style="color:${s.removed>0?'var(--red)':'var(--green)'}">${s.removed}</div></div>`;
  if(s.removed_samples&&s.removed_samples.length){h+='<h4 style="font-size:.78rem;color:var(--muted);margin:10px 0 4px">Removed listings</h4><ul class="removed-list">';for(const r of s.removed_samples) h+=`<li><span class="r-title">${esc(r.title||'(untitled)')}</span><span class="r-price">CHF ${r.price??'?'}</span></li>`;h+='</ul>';}
  return h;
}
function renderRank(s){
  let h='';
  h+=`<div class="kv-grid"><div class="key">Image bonus cap</div><div class="val">${(s.image_bonus_cap??0).toFixed(3)}</div><div class="key">Listings boosted by image</div><div class="val">${s.boosted_count??0}</div><div class="key">Max image bonus</div><div class="val">${(s.max_image_bonus??0).toFixed(3)}</div></div>`;
  const dist=s.score_distribution||{};
  if(Object.keys(dist).length){
    const colors=['#6c8cff','#9f7aea','#22d3ee','#4ade80','#fbbf24','#f87171'];
    const entries=Object.entries(dist).sort((a,b)=>parseFloat(b[0])-parseFloat(a[0]));
    h+='<h4 style="font-size:.78rem;color:var(--muted);margin:8px 0 4px">Score distribution across all '+s.total_scored+' candidates</h4><div class="distribution-bar">';
    entries.forEach(([b,c],i)=>{h+=`<div class="seg" style="flex:${c};background:${colors[i%colors.length]}" title="Score ${b}: ${c}">${b} (${c})</div>`;});
    h+='</div>';
  }
  h+='<div style="max-height:700px;overflow:auto;margin-top:12px"><table class="results-table"><thead><tr><th>#</th><th>Final</th><th>Soft</th><th>Q+</th><th>Img+</th><th>City</th><th>Rooms</th><th>CHF</th><th>Area</th><th>Matched Signals</th><th>Title</th></tr></thead><tbody>';
  for(const r of(s.top_results||[])){
    const sc=r.score, cls=sc>1?'score-high':sc>0?'score-mid':'score-zero', did='d-'+r.listing_id;
    let sh='';
    if(r.matched_signals&&r.matched_signals.length){const bd=r.signal_breakdown||{};sh=r.matched_signals.map(s=>`<span class="match-tag">${s}${bd[s]!=null?`<span class="mw">+${bd[s]}</span>`:''}</span>`).join('');}
    else sh='<span style="color:var(--muted);font-size:.75rem">—</span>';
    const enrich=enrichBlock(r);
    const explainHtml=r.explanation?`<div style="margin-top:8px;padding:8px 10px;background:rgba(80,200,120,.07);border:1px solid rgba(80,200,120,.2);border-radius:6px;font-size:.82rem;line-height:1.5;color:#c8e6c9"><span style="font-size:.68rem;color:rgba(80,200,120,.8);text-transform:uppercase;letter-spacing:.05em;display:block;margin-bottom:3px">Why this score</span>${esc(r.explanation)}</div>`:'';
    const gsHtml=globalScoreBlock(r.global_scores);
    h+=`<tr><td>${r.rank}</td><td class="score-cell ${cls}">${sc.toFixed(2)}</td><td class="score-cell">${(r.soft_score??0).toFixed(2)}</td><td class="score-cell">${(r.global_score_bonus??0).toFixed(2)}</td><td class="score-cell">${(r.image_bonus??0).toFixed(2)}</td><td>${esc(r.city||'?')}</td><td>${r.rooms??'—'}</td><td>${r.price!=null?r.price.toLocaleString():'—'}</td><td>${r.area!=null?r.area+' m²':'—'}</td><td>${sh}</td><td>${esc((r.title||'').substring(0,60))}<button class="expand-btn" onclick="document.getElementById('${did}').classList.toggle('open')">details</button><div class="listing-detail" id="${did}">${explainHtml}${gsHtml}<div class="desc">${esc(r.description||'No description')}</div><div class="meta-row">${r.street?`<div class="meta-item">Street: <span>${esc(r.street)}</span></div>`:''}${r.postal_code?`<div class="meta-item">PLZ: <span>${r.postal_code}</span></div>`:''}${r.canton?`<div class="meta-item">Canton: <span>${r.canton}</span></div>`:''}${r.available_from?`<div class="meta-item">Available: <span>${r.available_from}</span></div>`:''}${r.offer_type?`<div class="meta-item">Type: <span>${r.offer_type}</span></div>`:''}${r.object_category?`<div class="meta-item">Category: <span>${r.object_category}</span></div>`:''}</div><div class="meta-row" style="margin-top:6px">${`<div class="meta-item">Soft score: <span>${(r.soft_score??0).toFixed(3)}</span></div><div class="meta-item">Quality bonus: <span>${(r.global_score_bonus??0).toFixed(3)}</span></div><div class="meta-item">Image score: <span>${(r.image_score??0).toFixed(3)}</span></div><div class="meta-item">Image bonus: <span>${(r.image_bonus??0).toFixed(3)}</span></div><div class="meta-item">Bonus cap: <span>${(r.image_bonus_cap??0).toFixed(3)}</span></div>`}${r.best_image_url?`<div class="meta-item">Best image: <a href="${r.best_image_url}" target="_blank" style="color:var(--accent)">open</a></div>`:''}</div>${renderPhotos(r)}${enrich}${r.features&&r.features.length?`<div style="margin-top:6px"><span style="color:var(--muted)">Features:</span> ${r.features.map(f=>`<span class="match-tag">${esc(f)}</span>`).join(' ')}</div>`:''}${r.distance_public_transport!=null?`<div style="margin-top:4px;color:var(--muted);font-size:.78rem">Public transport: ${r.distance_public_transport}m</div>`:''}${r.original_url?`<div style="margin-top:4px"><a href="${r.original_url}" target="_blank" style="color:var(--accent);font-size:.78rem">View original →</a></div>`:''}</div></td></tr>`;
  }
  h+='</tbody></table></div>';
  return h;
}
function globalScoreBlock(gs){
  if(!gs) return '';
  const g=gs.global_score!=null?gs.global_score:0;
  const dims=[
    ['Value',gs.score_value,0.20,'#66bb6a'],
    ['Amenity',gs.score_amenity,0.18,'#42a5f5'],
    ['Location',gs.score_location,0.17,'#ab47bc'],
    ['Building',gs.score_building,0.13,'#ffa726'],
    ['Complete',gs.score_completeness,0.10,'#26c6da'],
    ['Fresh',gs.score_freshness,0.10,'#ef5350'],
    ['Transit',gs.score_transit,0.12,'#29b6f6'],
  ];
  let h=`<div style="margin-top:6px;padding:8px 10px;background:rgba(108,140,255,.06);border:1px solid rgba(108,140,255,.15);border-radius:6px"><div style="display:flex;align-items:center;gap:8px;margin-bottom:6px"><span style="font-size:.68rem;color:var(--accent);text-transform:uppercase;letter-spacing:.05em">Global Score</span><span style="font-size:.95rem;font-weight:700;color:${g>=0.6?'var(--green)':g>=0.4?'var(--orange)':'var(--red)'}">${g.toFixed(3)}</span></div><div style="display:flex;gap:4px;height:10px;border-radius:4px;overflow:hidden;background:rgba(255,255,255,.05)">`;
  for(const[name,val,weight,col]of dims){
    const w=(val*weight/1)*100;
    h+=`<div title="${name}: ${(val*100).toFixed(0)}% × ${(weight*100).toFixed(0)}%w" style="width:${w.toFixed(1)}%;background:${col};min-width:1px"></div>`;
  }
  h+=`</div><div style="display:flex;gap:6px;flex-wrap:wrap;margin-top:6px">`;
  for(const[name,val,weight,col]of dims){
    const pct=(val*100).toFixed(0);
    h+=`<span style="font-size:.7rem;color:var(--muted);white-space:nowrap"><span style="display:inline-block;width:6px;height:6px;border-radius:50%;background:${col};margin-right:2px;vertical-align:middle"></span>${name} <span style="color:var(--text)">${pct}%</span><span style="opacity:.5">×${(weight*100).toFixed(0)}%</span></span>`;
  }
  h+=`</div></div>`;
  return h;
}
function renderPhotos(r){
  const cards=[];
  if(r.hero_image_url){
    const sameAsBest=r.best_image_url&&r.best_image_url===r.hero_image_url;
    cards.push({
      label:sameAsBest?'Listing / matched image':'Listing hero image',
      url:r.hero_image_url,
    });
  }
  if(r.best_image_url && r.best_image_url!==r.hero_image_url){
    cards.push({
      label:'Image RAG matched image',
      url:r.best_image_url,
    });
  }
  if(!cards.length) return '';
  return `<div style="margin-top:10px"><div style="font-size:.72rem;color:var(--muted);text-transform:uppercase;letter-spacing:.05em;margin-bottom:4px">Photos</div><div class="photo-grid">${cards.map(card=>`<div class="photo-card"><img src="${card.url}" alt="${esc(card.label)}" loading="lazy" referrerpolicy="no-referrer" /><div class="photo-meta"><span>${esc(card.label)}</span><a href="${card.url}" target="_blank">open</a></div></div>`).join('')}</div></div>`;
}
function enrichBlock(r){
  const items=[];
  if(r.floor_level!=null) items.push(['Floor',r.floor_level]);
  if(r.year_built!=null) items.push(['Built',r.year_built]);
  if(r.renovation_year!=null) items.push(['Renovated',r.renovation_year]);
  if(r.is_furnished!=null) items.push(['Furnished',r.is_furnished?'Yes':'No']);
  if(r.price_per_sqm!=null) items.push(['CHF/m² (old)',r.price_per_sqm.toFixed(1)]);
  if(r.price_per_m2!=null) items.push(['CHF/m²',r.price_per_m2.toFixed(1)]);
  if(r.price_vs_city_median!=null){const pct=((r.price_vs_city_median-1)*100).toFixed(0);const col=r.price_vs_city_median<0.85?'var(--green)':r.price_vs_city_median>1.15?'var(--red)':'var(--text)';items.push(['vs City Median',`<span style="color:${col}">${(r.price_vs_city_median*100).toFixed(0)}% (${pct>0?'+'+pct:pct}%)</span>`]);}
  if(r.price_per_m2_vs_municipality!=null){const pct=((r.price_per_m2_vs_municipality-1)*100).toFixed(0);const col=r.price_per_m2_vs_municipality<0.85?'var(--green)':r.price_per_m2_vs_municipality>1.15?'var(--red)':'var(--text)';items.push(['vs Municipality',`<span style="color:${col}">${(r.price_per_m2_vs_municipality*100).toFixed(0)}% (${pct>0?'+'+pct:pct}%)</span>`]);}
  if(r.price_per_m2_vs_municipality_label) items.push(['Value Label',`<span style="font-weight:600">${esc(r.price_per_m2_vs_municipality_label)}</span>`]);
  if(r.municipality_name) items.push(['Municipality',esc(r.municipality_name)]);
  else if(r.municipality) items.push(['Municipality',esc(r.municipality)]);
  if(r.district_name) items.push(['District',esc(r.district_name)]);
  if(r.canton_name) items.push(['Canton (v3.1)',esc(r.canton_name)]);
  if(r.lake_distance_m!=null){const col=r.lake_distance_m<2000?'var(--green)':r.lake_distance_m<5000?'var(--orange)':'var(--muted)';items.push(['Lake distance',`<span style="color:${col}">${(r.lake_distance_m/1000).toFixed(1)} km</span>`]);}
  if(r.nearest_stop_name||r.nearest_stop_distance_m!=null){const n=r.nearest_stop_name||'stop';const d=r.nearest_stop_distance_m!=null?Math.round(r.nearest_stop_distance_m)+'m':'?';const col=(r.nearest_stop_distance_m||9999)<300?'var(--green)':(r.nearest_stop_distance_m||9999)<600?'var(--orange)':'var(--muted)';items.push(['Nearest stop',`<span style="color:${col}">${d}</span> – ${esc(n)}`]);}
  if(r.nearest_train_name||r.nearest_train_distance_m!=null){const n=r.nearest_train_name||'train';const d=r.nearest_train_distance_m!=null?Math.round(r.nearest_train_distance_m)+'m':'?';const col=(r.nearest_train_distance_m||9999)<800?'var(--green)':(r.nearest_train_distance_m||9999)<2000?'var(--orange)':'var(--muted)';items.push(['Nearest train',`<span style="color:${col}">${d}</span> – ${esc(n)}`]);}
  if(r.nearest_hb_name||r.nearest_hb_distance_m!=null){const n=r.nearest_hb_name||'HB';const d=r.nearest_hb_distance_m!=null?Math.round(r.nearest_hb_distance_m)+'m':'?';const col=(r.nearest_hb_distance_m||9999)<2000?'var(--green)':(r.nearest_hb_distance_m||9999)<5000?'var(--orange)':'var(--muted)';items.push(['Nearest HB',`<span style="color:${col}">${d}</span> – ${esc(n)}`]);}
  if(r.population_density!=null) items.push(['Pop. density',`${r.population_density.toFixed(0)} /km²`]);
  if(r.population_density_bucket) items.push(['Density bucket',esc(r.population_density_bucket)]);
  if(r.population_total!=null) items.push(['Population',r.population_total.toLocaleString()]);
  if(r.is_urban!=null) items.push(['Urban/Rural',r.is_urban?'🏙️ Urban':'🌿 Rural']);
  if(!items.length) return '';
  let h='<div style="margin-top:8px;padding:8px 10px;background:rgba(108,140,255,.06);border:1px solid rgba(108,140,255,.15);border-radius:6px"><div style="font-size:.68rem;color:var(--accent);text-transform:uppercase;letter-spacing:.05em;margin-bottom:4px">Enrichment Data</div><div style="display:grid;grid-template-columns:auto 1fr;gap:2px 12px;font-size:.78rem">';
  for(const[k,v]of items) h+=`<div style="color:var(--muted)">${k}</div><div>${v}</div>`;
  return h+'</div></div>';
}
function esc(s){const d=document.createElement('div');d.textContent=s;return d.innerHTML}
</script>
</body>
</html>
"""


@app.get("/debug", response_class=HTMLResponse)
def debug_page():
    return _HTML


_PREFERENCES_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>NestFinder Preference Debugger</title>
<style>
  :root {
    --bg: #0f1117; --surface: #1a1d27; --surface2: #222535; --border: #2d3148;
    --text: #e1e4ed; --muted: #8b8fa7; --accent: #6c8cff; --green: #4ade80; --orange: #fbbf24;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'Inter',-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; background:var(--bg); color:var(--text); line-height:1.5; min-height:100vh; }
  .container { max-width:1280px; margin:0 auto; padding:24px; }
  header { margin-bottom:24px; }
  header h1 { font-size:1.5rem; font-weight:600; color:var(--accent); }
  header p { color:var(--muted); font-size:.875rem; margin-top:4px; }
  .card { background:var(--surface); border:1px solid var(--border); border-radius:12px; padding:18px; margin-bottom:18px; }
  .label { font-size:.75rem; text-transform:uppercase; letter-spacing:.05em; color:var(--muted); display:block; margin-bottom:8px; }
  .input-row { display:flex; gap:12px; flex-wrap:wrap; }
  .input-row input, .input-row select { background:var(--surface2); border:1px solid var(--border); border-radius:8px; padding:12px 14px; color:var(--text); font-size:.9rem; outline:none; }
  .input-row input[type="text"] { flex:1; min-width:240px; }
  .input-row button { background:var(--accent); color:#fff; border:none; border-radius:8px; padding:12px 20px; font-size:.92rem; font-weight:600; cursor:pointer; }
  .input-row button:hover { opacity:.88; }
  .muted { color:var(--muted); }
  .section-title { font-size:.95rem; font-weight:600; margin-bottom:10px; }
  .chips { display:flex; flex-wrap:wrap; gap:6px; margin-top:8px; }
  .chip { background:var(--surface2); border:1px solid var(--border); border-radius:999px; padding:4px 10px; font-size:.78rem; }
  .kv-grid { display:grid; grid-template-columns:auto 1fr; gap:6px 14px; font-size:.85rem; }
  .kv-grid .key { color:var(--muted); }
  .kv-grid .val { color:var(--text); font-family:'JetBrains Mono','Fira Code',monospace; font-size:.8rem; }
  .session-list { display:flex; flex-wrap:wrap; gap:8px; }
  .session-btn { background:var(--surface2); border:1px solid var(--border); color:var(--text); border-radius:8px; padding:8px 10px; cursor:pointer; font-size:.8rem; }
  .session-btn:hover { border-color:var(--accent); }
  .table-wrap { overflow:auto; }
  table { width:100%; border-collapse:collapse; font-size:.82rem; }
  th { text-align:left; padding:8px 10px; color:var(--muted); font-weight:500; font-size:.72rem; text-transform:uppercase; letter-spacing:.04em; border-bottom:1px solid var(--border); background:var(--surface); position:sticky; top:0; }
  td { padding:10px; border-bottom:1px solid var(--border); vertical-align:top; }
  tr:hover { background:var(--surface2); }
  .score { font-family:'JetBrains Mono','Fira Code',monospace; font-weight:700; }
  .score-good { color:var(--green); }
  .score-mid { color:var(--orange); }
  .photo { width:64px; height:64px; border-radius:8px; object-fit:cover; background:#0b0d12; border:1px solid var(--border); }
  .empty { color:var(--muted); font-size:.84rem; }
</style>
</head>
<body>
<div class="container">
  <header>
    <h1>NestFinder Preference Debugger</h1>
    <p>Inspect recent sessions, derived user profiles, and the preference bonus applied to ranking. <a href="/debug" style="color:var(--accent)">Pipeline Debugger →</a></p>
  </header>

  <div class="card">
    <label class="label">Session and Query</label>
    <div class="input-row">
      <input type="text" id="sessionInput" placeholder="session id, e.g. sess_..." />
      <input type="text" id="queryInput" placeholder="optional query to preview preference bonus on candidates" />
      <select id="topN">
        <option value="5">Top 5</option>
        <option value="10" selected>Top 10</option>
        <option value="20">Top 20</option>
      </select>
      <button id="runBtn" onclick="runPreferenceTrace()">Inspect</button>
    </div>
    <p class="muted" style="margin-top:8px">Pick a recent session below or paste one directly. Add a query if you want to preview the ranking boost.</p>
  </div>

  <div class="card">
    <div class="section-title">Recent Sessions</div>
    <div id="recentSessions" class="session-list"></div>
  </div>

  <div class="card">
    <div class="section-title">Derived Profile</div>
    <div id="profileSummary" class="empty">Run an inspection to see the derived user profile.</div>
  </div>

  <div class="card">
    <div class="section-title">Recent Events</div>
    <div id="eventsTable" class="empty">No events loaded yet.</div>
  </div>

  <div class="card">
    <div class="section-title">Recent Searches</div>
    <div id="searchesTable" class="empty">No searches loaded yet.</div>
  </div>

  <div class="card">
    <div class="section-title">Preference Bonus Preview</div>
    <div id="queryPreview" class="empty">Add a query above to see how the user profile nudges candidate scores.</div>
  </div>
</div>

<script>
function esc(s){const d=document.createElement('div');d.textContent=String(s ?? '');return d.innerHTML}
async function runPreferenceTrace(){
  const sessionId=document.getElementById('sessionInput').value.trim() || null;
  const query=document.getElementById('queryInput').value.trim() || null;
  const topN=parseInt(document.getElementById('topN').value, 10);
  const btn=document.getElementById('runBtn');
  btn.disabled=true;
  try{
    const resp=await fetch('/preferences-trace',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({session_id:sessionId,query,top_n:topN})});
    renderPreferenceData(await resp.json());
  }catch(e){
    alert('Error: '+e.message);
  }finally{
    btn.disabled=false;
  }
}
function selectSession(sessionId){
  document.getElementById('sessionInput').value=sessionId;
  runPreferenceTrace();
}
function renderPreferenceData(data){
  if(!data.session_id && (data.recent_sessions || []).length){
    const latestSession = data.recent_sessions[0].session_id;
    document.getElementById('sessionInput').value = latestSession;
    selectSession(latestSession);
    return;
  }
  renderRecentSessions(data.recent_sessions || []);
  renderProfile(data.profile || {});
  renderEvents(data.events || []);
  renderSearches(data.recent_searches || []);
  renderQueryPreview(data.query_preview);
}
function renderRecentSessions(sessions){
  const el=document.getElementById('recentSessions');
  if(!sessions.length){el.innerHTML='<div class="empty">No session activity recorded yet.</div>';return;}
  el.innerHTML=sessions.map(s=>`<button class="session-btn" onclick="selectSession('${esc(s.session_id)}')">${esc(s.session_id)} <span class="muted">(${s.activity_count})</span></button>`).join('');
}
function renderProfile(profile){
  const el=document.getElementById('profileSummary');
  const hasProfile=(profile.preferred_cities&&profile.preferred_cities.length)||(profile.preferred_features&&profile.preferred_features.length)||(profile.favorite_listing_ids&&profile.favorite_listing_ids.length)||(profile.clicked_listing_ids&&profile.clicked_listing_ids.length);
  let h='<div class="kv-grid">';
  h+=`<div class="key">Session</div><div class="val">${esc(profile.session_id || '—')}</div>`;
  h+=`<div class="key">Clicked listings</div><div class="val">${(profile.clicked_listing_ids||[]).length}</div>`;
  h+=`<div class="key">Favorited listings</div><div class="val">${(profile.favorite_listing_ids||[]).length}</div>`;
  h+=`<div class="key">Dismissed listings</div><div class="val">${(profile.dismissed_listing_ids||[]).length}</div>`;
  h+=`<div class="key">Price range</div><div class="val">${profile.price_range ? `CHF ${profile.price_range.min}–${profile.price_range.max}` : '—'}</div>`;
  h+='</div>';
  h+='<div style="margin-top:12px"><div class="label">Preferred Cities</div>';
  h+=(profile.preferred_cities||[]).length?`<div class="chips">${profile.preferred_cities.map(v=>`<span class="chip">${esc(v)}</span>`).join('')}</div>`:'<div class="empty">No city preference deduced yet.</div>';
  h+='</div>';
  h+='<div style="margin-top:12px"><div class="label">Preferred Features</div>';
  h+=(profile.preferred_features||[]).length?`<div class="chips">${profile.preferred_features.map(v=>`<span class="chip">${esc(v)}</span>`).join('')}</div>`:'<div class="empty">No feature preference deduced yet.</div>';
  h+='</div>';
  h+='<div style="margin-top:12px"><div class="label">Liked Listing IDs</div>';
  h+=(profile.favorite_listing_ids||[]).length|| (profile.clicked_listing_ids||[]).length
    ? `<div class="chips">${[...(profile.favorite_listing_ids||[]), ...(profile.clicked_listing_ids||[])].slice(0,12).map(v=>`<span class="chip">${esc(v)}</span>`).join('')}</div>`
    : '<div class="empty">No liked listing ids yet.</div>';
  h+='</div>';
  if(!hasProfile){h+='<div class="empty" style="margin-top:12px">No meaningful profile yet for this session.</div>';}
  el.innerHTML=h;
}
function renderEvents(events){
  const el=document.getElementById('eventsTable');
  if(!events.length){el.innerHTML='<div class="empty">No events for this session.</div>';return;}
  let h='<div class="table-wrap"><table><thead><tr><th>Time</th><th>Action</th><th>Listing</th><th>Query</th></tr></thead><tbody>';
  for(const event of events){
    h+=`<tr><td>${esc(event.ts || '—')}</td><td>${esc(event.action || '—')}</td><td>${esc(event.listing_id || '—')}</td><td>${esc(event.query || '—')}</td></tr>`;
  }
  h+='</tbody></table></div>';
  el.innerHTML=h;
}
function renderSearches(searches){
  const el=document.getElementById('searchesTable');
  if(!searches.length){el.innerHTML='<div class="empty">No searches recorded for this session.</div>';return;}
  let h='<div class="table-wrap"><table><thead><tr><th>Time</th><th>Query</th><th>Results</th></tr></thead><tbody>';
  for(const item of searches){
    h+=`<tr><td>${esc(item.ts || '—')}</td><td>${esc(item.query || '—')}</td><td>${esc(item.result_count ?? '—')}</td></tr>`;
  }
  h+='</tbody></table></div>';
  el.innerHTML=h;
}
function renderQueryPreview(preview){
  const el=document.getElementById('queryPreview');
  if(!preview){el.innerHTML='<div class="empty">Add a query above to see the preference bonus preview.</div>';return;}
  let h='<div class="kv-grid">';
  h+=`<div class="key">Query</div><div class="val">${esc(preview.query || '—')}</div>`;
  h+=`<div class="key">Candidates after hard filters</div><div class="val">${preview.candidate_count}</div>`;
  h+=`<div class="key">After soft filtering</div><div class="val">${preview.filtered_count}</div>`;
  h+=`<div class="key">Boosted by preferences</div><div class="val">${preview.boosted_by_preferences}</div>`;
  h+=`<div class="key">Max preference bonus</div><div class="val">${preview.max_preference_bonus}</div>`;
  h+='</div>';
  if(!(preview.top_results||[]).length){el.innerHTML=h+'<div class="empty" style="margin-top:12px">No ranked results available for this query.</div>';return;}
  h+='<div class="table-wrap" style="margin-top:12px"><table><thead><tr><th>#</th><th>Final</th><th>Soft</th><th>Pref+</th><th>Img+</th><th>Listing</th><th>Reasons</th><th>Photo</th></tr></thead><tbody>';
  for(const item of preview.top_results){
    const photo=item.best_image_url || item.hero_image_url;
    h+=`<tr><td>${item.rank}</td><td class="score score-good">${Number(item.final_score || 0).toFixed(2)}</td><td class="score">${Number(item.soft_score || 0).toFixed(2)}</td><td class="score score-mid">${Number(item.preference_bonus || 0).toFixed(2)}</td><td class="score">${Number(item.image_bonus || 0).toFixed(2)}</td><td><strong>${esc(item.title || '—')}</strong><div class="muted">${esc(item.city || '—')} · ${item.price != null ? 'CHF ' + item.price.toLocaleString() : '—'}</div></td><td>${(item.preference_reasons||[]).length ? item.preference_reasons.map(v=>`<span class="chip">${esc(v)}</span>`).join(' ') : '<span class="empty">—</span>'}</td><td>${photo ? `<img class="photo" src="${photo}" alt="${esc(item.title || 'listing')}" loading="lazy" referrerpolicy="no-referrer" />` : '<span class="empty">—</span>'}</td></tr>`;
  }
  h+='</tbody></table></div>';
  el.innerHTML=h;
}
runPreferenceTrace();
</script>
</body>
</html>
"""


@app.get("/debug/preferences", response_class=HTMLResponse)
def debug_preferences_page():
    return _PREFERENCES_HTML


if __name__ == "__main__":
    print("\\n  Pipeline Debugger → http://127.0.0.1:8899/debug\\n")
    uvicorn.run(app, host="127.0.0.1", port=8899)
