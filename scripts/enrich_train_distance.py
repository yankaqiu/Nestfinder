import math
from pathlib import Path
import time
import pandas as pd
from agents.sbb_tool import tool_find_locations
from tqdm import tqdm

# ── Haversine ──────────────────────────────────────────────────────────────
def haversine(lat1, lng1, lat2, lng2) -> float:
    R = 6_371_000
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lng2 - lng1)
    a = math.sin(dp/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return 2 * R * math.asin(math.sqrt(a))


# ── Fetch HB coordinates ONCE before the loop ─────────────────────────────
MAIN_STATION_NAMES = [
    "Zürich HB", "Bern", "Basel SBB", "Genève", "Lausanne",
    "Luzern", "St. Gallen", "Winterthur", "Lugano", "Biel/Bienne",
]

print("Fetching main station coordinates...")
HB_STATIONS = []  # list of {name, lat, lng}
for name in MAIN_STATION_NAMES:
    try:
        r = tool_find_locations(query=name, location_type="station")
        stations = r.get("stations", [])
        if stations:
            s = stations[0]
            coord = s.get("coordinate") or {}
            slat, slng = coord.get("x"), coord.get("y")
            if slat and slng:
                HB_STATIONS.append({"name": s.get("name"), "lat": float(slat), "lng": float(slng)})
                print(f"  ✓ {s.get('name')} ({slat}, {slng})")
    except Exception as e:
        print(f"  ✗ {name}: {e}")
    time.sleep(0.2)

print(f"Loaded {len(HB_STATIONS)} main stations.\n")


# ── Pure Python — no API call ──────────────────────────────────────────────
def find_closest_hb(lat: float, lng: float) -> dict:
    best = min(HB_STATIONS, key=lambda s: haversine(lat, lng, s["lat"], s["lng"]))
    return {
        "name": best["name"],
        "distance_m": round(haversine(lat, lng, best["lat"], best["lng"]))
    }


# ── Load data ──────────────────────────────────────────────────────────────
raw_data_dir = Path("raw_data")
csv_files = sorted(raw_data_dir.glob("*.csv"))
if not csv_files:
    raise FileNotFoundError(f"No .csv files found in {raw_data_dir.resolve()}")

frames = []
for csv_path in csv_files:
    df = pd.read_csv(csv_path)
    df["_source_file"] = csv_path.name
    frames.append(df)

output_path = Path("data/listings_enriched.csv")
checkpoint_path = output_path.with_suffix(".partial.csv")
output_path.parent.mkdir(exist_ok=True)

if checkpoint_path.exists():
    all_data = pd.read_csv(checkpoint_path)
    print(f"Resuming from checkpoint: {checkpoint_path}")
else:
    all_data = pd.concat(frames, ignore_index=True)

for col in [
    "nearest_stop_name",
    "nearest_stop_distance_m",
    "nearest_train_name",
    "nearest_train_distance_m",
    "nearest_hb_name",
    "nearest_hb_distance_m",
    "_processed",
]:
    if col not in all_data.columns:
        all_data[col] = None

total = len(all_data)
save_every = 25


def find_nearest(stations, filter_fn=None):
    candidates = [s for s in stations if filter_fn(s)] if filter_fn else stations
    return candidates[0] if candidates else None


# ── Main loop ──────────────────────────────────────────────────────────────
for i, row in tqdm(all_data.iterrows(), total=total, desc="Processing locations"):
    if row.get("_processed") is True:
        continue
    lat, lng = row.geo_lat, row.geo_lng

    if pd.isna(lat) or pd.isna(lng):
        print(f"[{i+1}/{total}] Skipping — missing coordinates")
        continue

    lat, lng = float(lat), float(lng)

    try:
        # 1. Closest stop of any type
        result_all = tool_find_locations(lat=lat, lng=lng, location_type="all")
        all_stops = result_all.get("stations", [])
        nearest_stop = find_nearest(all_stops)
        if nearest_stop:
            all_data.at[i, "nearest_stop_name"]       = nearest_stop.get("name")
            all_data.at[i, "nearest_stop_distance_m"] = nearest_stop.get("distance_m")

        # 2. Closest train station
        result_train = tool_find_locations(lat=lat, lng=lng, location_type="station")
        # for s in result_train.get("stations", [])[:5]:
        #     print(s.get("id"), s.get("name"))
        train_stops = result_train.get("stations", [])
        nearest_train = find_nearest(
            train_stops,
            filter_fn=lambda s: str(s.get("id", "")).startswith("850")
        ) or find_nearest(train_stops)
        if nearest_train:
            all_data.at[i, "nearest_train_name"]       = nearest_train.get("name")
            all_data.at[i, "nearest_train_distance_m"] = nearest_train.get("distance_m")

        # 3. Closest HB — pure math, no API call
        hb = find_closest_hb(lat, lng)
        all_data.at[i, "nearest_hb_name"]       = hb["name"]
        all_data.at[i, "nearest_hb_distance_m"] = hb["distance_m"]

        # print(
        #     f"[{i+1}/{total}] "
        #     f"stop={all_data.at[i, 'nearest_stop_name']} ({all_data.at[i, 'nearest_stop_distance_m']}m) | "
        #     f"train={all_data.at[i, 'nearest_train_name']} ({all_data.at[i, 'nearest_train_distance_m']}m) | "
        #     f"hb={hb['name']} ({hb['distance_m']}m)"
        # )

    except Exception as e:
        print(f"[{i+1}/{total}] Error: {e}")

    all_data.at[i, "_processed"] = True

    if (i + 1) % save_every == 0:
        all_data.to_csv(checkpoint_path, index=False)
        print(f"Checkpoint saved to {checkpoint_path}")

    time.sleep(0.2)

# ── Save ───────────────────────────────────────────────────────────────────
all_data.to_csv(checkpoint_path, index=False)
final_data = all_data.drop(columns=["_processed"], errors="ignore")
final_data.to_csv(output_path, index=False)
print(f"\nDone. Saved to {output_path} ({len(final_data)} rows)")