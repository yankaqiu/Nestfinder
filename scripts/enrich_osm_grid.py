import math
import time
import json
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import requests

# ============================================================
# Config
# ============================================================

INPUT_CSV = "data/listings_enriched.csv"
OUTPUT_CSV = "data/listings_enriched_with_osm.csv"
CACHE_DIR = Path("data/osm_grid_cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Grid size in degrees.
# ~0.01 deg lat ~= 1.11 km
GRID_SIZE_DEG = 0.01

# Add a buffer around each grid cell so houses near cell boundaries
# still see nearby POIs just outside the cell.
CELL_BUFFER_DEG = 0.01

# Radii for count features
RADIUS_SMALL_M = 500
RADIUS_LARGE_M = 1000

# Overpass endpoints (fallbacks)
OVERPASS_URLS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.nchc.org.tw/api/interpreter",
]

# Delay between API calls to be polite to public servers
REQUEST_SLEEP_SEC = 1.0

# Retry settings
MAX_RETRIES = 4
TIMEOUT_SEC = 120

# ============================================================
# OSM tag mapping
# ============================================================

# Each logical feature group maps to OSM tag filters.
# The query will pull all of these in one request per grid cell.
OSM_GROUPS = {
    # Grocery / daily shopping
    "supermarket": [
        ("shop", "supermarket"),
    ],
    "grocery_small": [
        ("shop", "convenience"),
        ("shop", "grocery"),
        ("shop", "greengrocer"),
    ],
    "bakery": [
        ("shop", "bakery"),
    ],

    # Health
    "pharmacy": [
        ("amenity", "pharmacy"),
    ],
    "healthcare": [
        ("amenity", "hospital"),
        ("amenity", "clinic"),
        ("amenity", "doctors"),
        ("amenity", "dentist"),
    ],

    # Education
    "kindergarten": [
        ("amenity", "kindergarten"),
    ],
    "school": [
        ("amenity", "school"),
    ],
    "higher_education": [
        ("amenity", "college"),
        ("amenity", "university"),
    ],

    # Food / lifestyle
    "restaurant": [
        ("amenity", "restaurant"),
        ("amenity", "cafe"),
        ("amenity", "fast_food"),
        ("amenity", "food_court"),
        ("amenity", "biergarten"),
    ],
    "nightlife": [
        ("amenity", "bar"),
        ("amenity", "pub"),
        ("amenity", "nightclub"),
    ],

    # Optional extras that are often useful in Switzerland
    "childcare": [
        ("amenity", "kindergarten"),
    ],
    "market": [
        ("amenity", "marketplace"),
    ],
}

# ============================================================
# Helpers
# ============================================================

def haversine_m(lon1, lat1, lon2, lat2):
    """
    Great-circle distance in meters.
    Supports scalars or numpy arrays.
    """
    R = 6371000.0

    lon1 = np.radians(lon1)
    lat1 = np.radians(lat1)
    lon2 = np.radians(lon2)
    lat2 = np.radians(lat2)

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    c = 2 * np.arcsin(np.sqrt(a))
    return R * c


def floor_to_grid(value: float, grid_size: float) -> float:
    return math.floor(value / grid_size) * grid_size


def build_grid_cell(lat: float, lon: float, grid_size: float) -> Tuple[float, float]:
    """
    Return lower-left corner of the grid cell containing (lat, lon).
    """
    return floor_to_grid(lat, grid_size), floor_to_grid(lon, grid_size)


def expand_bbox(cell_lat: float, cell_lon: float, grid_size: float, buffer_deg: float):
    """
    Return south, west, north, east for buffered cell bbox.
    """
    south = cell_lat - buffer_deg
    west = cell_lon - buffer_deg
    north = cell_lat + grid_size + buffer_deg
    east = cell_lon + grid_size + buffer_deg
    return south, west, north, east


def cache_path_for_cell(cell_lat: float, cell_lon: float) -> Path:
    safe_name = f"cell_lat_{cell_lat:.5f}_lon_{cell_lon:.5f}.json".replace("-", "m")
    return CACHE_DIR / safe_name


def build_overpass_query(south: float, west: float, north: float, east: float) -> str:
    """
    Build one Overpass query for all desired POI categories in a bbox.
    Uses out center so ways/relations get a representative point.
    """
    parts = []
    for group_name, filters in OSM_GROUPS.items():
        for key, value in filters:
            parts.append(f'nwr["{key}"="{value}"]({south},{west},{north},{east});')

    union_block = "\n  ".join(parts)

    query = f"""
    [out:json][timeout:60];
    (
      {union_block}
    );
    out center tags;
    """
    return query.strip()


def fetch_overpass_with_retry(query: str) -> dict:
    """
    POST query to Overpass with retries and endpoint fallbacks.
    """
    last_err = None
    for url in OVERPASS_URLS:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = requests.post(
                    url,
                    data={"data": query},
                    timeout=TIMEOUT_SEC,
                    headers={"User-Agent": "house-enrichment-script/1.0"}
                )
                response.raise_for_status()
                return response.json()
            except Exception as e:
                last_err = e
                wait_s = min(30, attempt * 5)
                print(
                    f"  Attempt {attempt}/{MAX_RETRIES} failed at {url}: {e}. "
                    f"Retrying in {wait_s}s..."
                )
                time.sleep(wait_s)

    raise RuntimeError(f"Overpass request failed after {MAX_RETRIES} retries: {last_err}")


def infer_group_from_tags(tags: Dict[str, str]) -> List[str]:
    """
    A POI can belong to one or more logical groups.
    """
    found = []
    for group_name, filters in OSM_GROUPS.items():
        for key, value in filters:
            if tags.get(key) == value:
                found.append(group_name)
                break
    return found


def parse_overpass_elements(payload: dict) -> pd.DataFrame:
    """
    Convert Overpass JSON into a flat dataframe with lat/lon and logical groups.
    """
    rows = []

    for el in payload.get("elements", []):
        tags = el.get("tags", {})

        if "lat" in el and "lon" in el:
            lat = el["lat"]
            lon = el["lon"]
        elif "center" in el:
            lat = el["center"]["lat"]
            lon = el["center"]["lon"]
        else:
            continue

        groups = infer_group_from_tags(tags)
        if not groups:
            continue

        rows.append(
            {
                "osm_id": el.get("id"),
                "osm_type": el.get("type"),
                "lat": lat,
                "lon": lon,
                "groups": groups,
                "name": tags.get("name"),
            }
        )

    if not rows:
        return pd.DataFrame(columns=["osm_id", "osm_type", "lat", "lon", "groups", "name"])

    df = pd.DataFrame(rows)

    # Deduplicate exact same OSM object
    df = df.drop_duplicates(subset=["osm_type", "osm_id"]).reset_index(drop=True)
    return df


def load_or_fetch_cell_pois(cell_lat: float, cell_lon: float) -> pd.DataFrame:
    """
    Cache POIs per grid cell on disk.
    """
    cache_file = cache_path_for_cell(cell_lat, cell_lon)

    if cache_file.exists():
        with open(cache_file, "r", encoding="utf-8") as f:
            payload = json.load(f)
        return parse_overpass_elements(payload)

    south, west, north, east = expand_bbox(cell_lat, cell_lon, GRID_SIZE_DEG, CELL_BUFFER_DEG)
    query = build_overpass_query(south, west, north, east)

    print(f"Fetching POIs for cell ({cell_lat:.5f}, {cell_lon:.5f})...")
    payload = fetch_overpass_with_retry(query)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(payload, f)

    time.sleep(REQUEST_SLEEP_SEC)
    return parse_overpass_elements(payload)


def compute_features_for_chunk(houses_df: pd.DataFrame, pois_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute nearest distances and counts for one chunk of houses against that cell's POIs.
    """
    out = houses_df.copy()

    feature_groups = list(OSM_GROUPS.keys())

    # initialize outputs
    for group in feature_groups:
        out[f"dist_{group}_m"] = np.nan
        out[f"count_{group}_{RADIUS_SMALL_M}m"] = 0
        out[f"count_{group}_{RADIUS_LARGE_M}m"] = 0

    if pois_df.empty:
        return out

    house_lats = out["geo_lat"].to_numpy()
    house_lons = out["geo_lng"].to_numpy()

    # explode groups so one POI can contribute to multiple logical categories
    pois_exploded = pois_df.explode("groups").rename(columns={"groups": "group"}).reset_index(drop=True)

    for group in feature_groups:
        group_pois = pois_exploded[pois_exploded["group"] == group]

        if group_pois.empty:
            continue

        poi_lats = group_pois["lat"].to_numpy()
        poi_lons = group_pois["lon"].to_numpy()

        # Broadcast distances: shape = [n_houses, n_pois]
        dists = haversine_m(
            house_lons[:, None],
            house_lats[:, None],
            poi_lons[None, :],
            poi_lats[None, :]
        )

        out[f"dist_{group}_m"] = dists.min(axis=1)
        out[f"count_{group}_{RADIUS_SMALL_M}m"] = (dists <= RADIUS_SMALL_M).sum(axis=1)
        out[f"count_{group}_{RADIUS_LARGE_M}m"] = (dists <= RADIUS_LARGE_M).sum(axis=1)

    return out


# ============================================================
# Main
# ============================================================

def main():
    raw_data_dir = Path("raw_data")

    csv_files = sorted(raw_data_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No .csv files found in {raw_data_dir.resolve()}")

    frames = []
    for csv_path in csv_files:
        df = pd.read_csv(csv_path)
        df["_source_file"] = csv_path.name
        frames.append(df)

    all_data = pd.concat(frames, ignore_index=True)
    df = all_data.iloc[:50]  # for testing, remove or increase for full run

    required_cols = {"geo_lat", "geo_lng"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Keep only rows with valid coordinates
    df = df.copy()
    df = df[df["geo_lat"].notna() & df["geo_lng"].notna()].reset_index(drop=True)

    # Assign each house to a grid cell
    cell_keys = df.apply(
        lambda r: build_grid_cell(r["geo_lat"], r["geo_lng"], GRID_SIZE_DEG),
        axis=1
    )
    df["cell_lat"] = [x[0] for x in cell_keys]
    df["cell_lon"] = [x[1] for x in cell_keys]

    unique_cells = (
        df[["cell_lat", "cell_lon"]]
        .drop_duplicates()
        .sort_values(["cell_lat", "cell_lon"])
        .itertuples(index=False, name=None)
    )
    unique_cells = list(unique_cells)

    print(f"Total houses: {len(df)}")
    print(f"Unique grid cells: {len(unique_cells)}")

    enriched_parts = []

    for idx, (cell_lat, cell_lon) in enumerate(unique_cells, start=1):
        print(f"\n[{idx}/{len(unique_cells)}] Processing cell ({cell_lat:.5f}, {cell_lon:.5f})")

        house_chunk = df[(df["cell_lat"] == cell_lat) & (df["cell_lon"] == cell_lon)].copy()
        pois_df = load_or_fetch_cell_pois(cell_lat, cell_lon)
        enriched_chunk = compute_features_for_chunk(house_chunk, pois_df)

        enriched_parts.append(enriched_chunk)

    result = pd.concat(enriched_parts, ignore_index=True)

    # Optional summary scores
    result["score_family"] = (
        -result["dist_school_m"].fillna(5000)
        -result["dist_kindergarten_m"].fillna(5000)
        -result["dist_pharmacy_m"].fillna(5000)
        -result["dist_supermarket_m"].fillna(5000)
        + 100 * result[f"count_school_{RADIUS_LARGE_M}m"].fillna(0)
        + 100 * result[f"count_kindergarten_{RADIUS_LARGE_M}m"].fillna(0)
    )

    result["score_convenience"] = (
        -result["dist_supermarket_m"].fillna(5000)
        -result["dist_pharmacy_m"].fillna(5000)
        -result["dist_healthcare_m"].fillna(5000)
        + 80 * result[f"count_supermarket_{RADIUS_SMALL_M}m"].fillna(0)
        + 60 * result[f"count_pharmacy_{RADIUS_SMALL_M}m"].fillna(0)
    )

    result["score_lifestyle"] = (
        -result["dist_restaurant_m"].fillna(5000)
        -result["dist_nightlife_m"].fillna(5000)
        + 40 * result[f"count_restaurant_{RADIUS_SMALL_M}m"].fillna(0)
        + 50 * result[f"count_nightlife_{RADIUS_LARGE_M}m"].fillna(0)
    )

    # Cleanup helper columns if you do not want them
    result = result.drop(columns=["cell_lat", "cell_lon"])

    result.to_csv(OUTPUT_CSV, index=False)
    print(f"\nSaved enriched dataset to: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()