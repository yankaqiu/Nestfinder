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
import uvicorn

from app.config import get_settings
from app.core.hard_filters import search_listings
from app.db import get_connection
from app.harness.search_service import to_hard_filter_params
from app.participant.hard_fact_extraction import extract_hard_facts
from app.participant.ranking import _score_candidate, _SIGNAL_MATCHERS
from app.participant.soft_fact_extraction import extract_soft_facts
from app.participant.soft_filtering import filter_soft_facts

settings = get_settings()
DB_PATH = settings.db_path

app = FastAPI(title="NestFinder Pipeline Debugger")


class TraceRequest(BaseModel):
    query: str = Field(min_length=1)
    top_n: int = Field(default=20, ge=1, le=100)


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
    scored_results = []
    for c in after_soft:
        score, matched = _score_candidate(c, soft_facts)
        scored_results.append((score, matched, c))

    has_soft = bool(signals) or bool(soft_facts.get("preferred_min_area_sqm"))
    if has_soft:
        scored_results.sort(key=lambda x: -x[0])

    top_n = scored_results[:req.top_n]
    ranked_output = []
    for rank, (score, matched, c) in enumerate(top_n, 1):
        entry = _candidate_summary(c)
        entry["rank"] = rank
        entry["score"] = round(score, 3)
        entry["matched_signals"] = matched
        breakdown = {}
        for sig_name in matched:
            if sig_name == "area_pref":
                breakdown["area_pref"] = 0.3
            else:
                breakdown[sig_name] = signals.get(sig_name, 0)
        entry["signal_breakdown"] = breakdown
        ranked_output.append(entry)

    score_distribution: dict[str, int] = {}
    for sc, _, _ in scored_results:
        bucket = str(round(sc, 1))
        score_distribution[bucket] = score_distribution.get(bucket, 0) + 1

    result["stages"].append({
        "name": "Ranking",
        "status": "ok",
        "duration_ms": round((time.perf_counter() - t0) * 1000),
        "total_scored": len(scored_results),
        "max_score": round(scored_results[0][0], 3) if scored_results else 0,
        "min_score": round(scored_results[-1][0], 3) if scored_results else 0,
        "score_distribution": score_distribution,
        "top_results": ranked_output,
    })

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
    <p>Step-by-step visualization of the search pipeline. Enter a query to trace each stage.</p>
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
  const dist=s.score_distribution||{};
  if(Object.keys(dist).length){
    const colors=['#6c8cff','#9f7aea','#22d3ee','#4ade80','#fbbf24','#f87171'];
    const entries=Object.entries(dist).sort((a,b)=>parseFloat(b[0])-parseFloat(a[0]));
    h+='<h4 style="font-size:.78rem;color:var(--muted);margin:8px 0 4px">Score distribution across all '+s.total_scored+' candidates</h4><div class="distribution-bar">';
    entries.forEach(([b,c],i)=>{h+=`<div class="seg" style="flex:${c};background:${colors[i%colors.length]}" title="Score ${b}: ${c}">${b} (${c})</div>`;});
    h+='</div>';
  }
  h+='<div style="max-height:700px;overflow:auto;margin-top:12px"><table class="results-table"><thead><tr><th>#</th><th>Score</th><th>City</th><th>Rooms</th><th>CHF</th><th>Area</th><th>Matched Signals</th><th>Title</th></tr></thead><tbody>';
  for(const r of(s.top_results||[])){
    const sc=r.score, cls=sc>1?'score-high':sc>0?'score-mid':'score-zero', did='d-'+r.listing_id;
    let sh='';
    if(r.matched_signals&&r.matched_signals.length){const bd=r.signal_breakdown||{};sh=r.matched_signals.map(s=>`<span class="match-tag">${s}${bd[s]!=null?`<span class="mw">+${bd[s]}</span>`:''}</span>`).join('');}
    else sh='<span style="color:var(--muted);font-size:.75rem">—</span>';
    const enrich=enrichBlock(r);
    h+=`<tr><td>${r.rank}</td><td class="score-cell ${cls}">${sc.toFixed(2)}</td><td>${esc(r.city||'?')}</td><td>${r.rooms??'—'}</td><td>${r.price!=null?r.price.toLocaleString():'—'}</td><td>${r.area!=null?r.area+' m²':'—'}</td><td>${sh}</td><td>${esc((r.title||'').substring(0,60))}<button class="expand-btn" onclick="document.getElementById('${did}').classList.toggle('open')">details</button><div class="listing-detail" id="${did}"><div class="desc">${esc(r.description||'No description')}</div><div class="meta-row">${r.street?`<div class="meta-item">Street: <span>${esc(r.street)}</span></div>`:''}${r.postal_code?`<div class="meta-item">PLZ: <span>${r.postal_code}</span></div>`:''}${r.canton?`<div class="meta-item">Canton: <span>${r.canton}</span></div>`:''}${r.available_from?`<div class="meta-item">Available: <span>${r.available_from}</span></div>`:''}${r.offer_type?`<div class="meta-item">Type: <span>${r.offer_type}</span></div>`:''}${r.object_category?`<div class="meta-item">Category: <span>${r.object_category}</span></div>`:''}</div>${enrich}${r.features&&r.features.length?`<div style="margin-top:6px"><span style="color:var(--muted)">Features:</span> ${r.features.map(f=>`<span class="match-tag">${esc(f)}</span>`).join(' ')}</div>`:''}${r.distance_public_transport!=null?`<div style="margin-top:4px;color:var(--muted);font-size:.78rem">Public transport: ${r.distance_public_transport}m</div>`:''}${r.original_url?`<div style="margin-top:4px"><a href="${r.original_url}" target="_blank" style="color:var(--accent);font-size:.78rem">View original →</a></div>`:''}</div></td></tr>`;
  }
  h+='</tbody></table></div>';
  return h;
}
function enrichBlock(r){
  const items=[];
  if(r.floor_level!=null) items.push(['Floor',r.floor_level]);
  if(r.year_built!=null) items.push(['Built',r.year_built]);
  if(r.renovation_year!=null) items.push(['Renovated',r.renovation_year]);
  if(r.is_furnished!=null) items.push(['Furnished',r.is_furnished?'Yes':'No']);
  if(r.price_per_sqm!=null) items.push(['CHF/m²',r.price_per_sqm.toFixed(1)]);
  if(r.price_vs_city_median!=null){const pct=((r.price_vs_city_median-1)*100).toFixed(0);const col=r.price_vs_city_median<0.85?'var(--green)':r.price_vs_city_median>1.15?'var(--red)':'var(--text)';items.push(['vs City Median',`<span style="color:${col}">${(r.price_vs_city_median*100).toFixed(0)}% (${pct>0?'+'+pct:pct}%)</span>`]);}
  if(r.municipality) items.push(['Municipality',esc(r.municipality)]);
  if(r.lake_distance_m!=null){const col=r.lake_distance_m<2000?'var(--green)':r.lake_distance_m<5000?'var(--orange)':'var(--muted)';items.push(['Lake distance',`<span style="color:${col}">${(r.lake_distance_m/1000).toFixed(1)} km</span>`]);}
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


if __name__ == "__main__":
    print("\\n  Pipeline Debugger → http://127.0.0.1:8899/debug\\n")
    uvicorn.run(app, host="127.0.0.1", port=8899)
