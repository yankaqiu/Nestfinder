#!/usr/bin/env python3
"""
Deep data profiling for all raw CSV data sources in the Nestfinder project.

Usage:
    source venv/bin/activate
    python scripts/analyze_data.py

Produces a detailed report on:
  - Schema per data source (columns, dtypes)
  - Row counts, duplicates
  - Null / empty rates per column
  - Value distributions for key fields
  - Cross-source column comparison matrix
  - Feature availability heatmap
  - JSON field depth sampling (orig_data, images, location_address)
  - Geolocation coverage
  - Price / rooms / area statistics
"""

from __future__ import annotations

import csv
import json
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path
from textwrap import dedent

import numpy as np
import pandas as pd

RAW_DATA_DIR = Path(__file__).resolve().parents[1] / "raw_data"

CSV_FILES = {
    "robinreal": RAW_DATA_DIR / "robinreal_data_withimages-1776461278845.csv",
    "sred": RAW_DATA_DIR / "sred_data_withmontageimages_latlong.csv",
    "structured_with_images": RAW_DATA_DIR / "structured_data_withimages-1776412361239.csv",
    "structured_without_images": RAW_DATA_DIR / "structured_data_withoutimages-1776412361239.csv",
}

SEPARATOR = "=" * 100
SUB_SEP = "-" * 80


def load_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False, on_bad_lines="warn")


def is_empty(val: str) -> bool:
    if not isinstance(val, str):
        return pd.isna(val)
    return val.strip() == "" or val.strip().upper() == "NULL"


def null_rate(series: pd.Series) -> float:
    return series.apply(is_empty).mean()


def safe_float(val: str) -> float | None:
    if is_empty(val):
        return None
    try:
        return float(val.replace("'", "").replace(",", "."))
    except (ValueError, AttributeError):
        return None


def safe_json(val: str) -> dict | list | None:
    if is_empty(val):
        return None
    try:
        return json.loads(val)
    except (json.JSONDecodeError, TypeError):
        return None


def print_section(title: str) -> None:
    print(f"\n{SEPARATOR}")
    print(f"  {title}")
    print(SEPARATOR)


def print_subsection(title: str) -> None:
    print(f"\n{SUB_SEP}")
    print(f"  {title}")
    print(SUB_SEP)


def analyze_basic_info(name: str, df: pd.DataFrame, path: Path) -> None:
    print_subsection(f"[{name}] Basic Info")
    file_size_mb = path.stat().st_size / (1024 * 1024)
    print(f"  File:     {path.name}")
    print(f"  Size:     {file_size_mb:.2f} MB")
    print(f"  Rows:     {len(df):,}")
    print(f"  Columns:  {len(df.columns)}")

    id_col = "id"
    if id_col in df.columns:
        n_unique = df[id_col].nunique()
        n_dup = len(df) - n_unique
        n_empty_id = df[id_col].apply(is_empty).sum()
        print(f"  Unique IDs:   {n_unique:,}")
        print(f"  Duplicate IDs: {n_dup:,}")
        print(f"  Empty IDs:     {n_empty_id:,}")


def analyze_null_rates(name: str, df: pd.DataFrame) -> None:
    print_subsection(f"[{name}] Column Null/Empty Rates")
    rates = []
    for col in df.columns:
        rate = null_rate(df[col])
        rates.append((col, rate, len(df) - int(rate * len(df))))
    rates.sort(key=lambda x: x[1])
    print(f"  {'Column':<40} {'Populated':>12} {'Empty %':>10}")
    print(f"  {'------':<40} {'---------':>12} {'-------':>10}")
    for col, rate, populated in rates:
        print(f"  {col:<40} {populated:>12,} {rate*100:>9.1f}%")


def analyze_numeric_field(name: str, df: pd.DataFrame, col: str, label: str) -> None:
    if col not in df.columns:
        print(f"  {label}: column '{col}' not present")
        return
    values = df[col].apply(safe_float).dropna()
    if len(values) == 0:
        print(f"  {label}: no valid values")
        return
    values = values[values > 0]
    if len(values) == 0:
        print(f"  {label}: no positive values")
        return
    print(f"\n  {label} (n={len(values):,}, {len(values)/len(df)*100:.1f}% coverage):")
    print(f"    Min:    {values.min():>12,.1f}")
    print(f"    P5:     {values.quantile(0.05):>12,.1f}")
    print(f"    P25:    {values.quantile(0.25):>12,.1f}")
    print(f"    Median: {values.median():>12,.1f}")
    print(f"    Mean:   {values.mean():>12,.1f}")
    print(f"    P75:    {values.quantile(0.75):>12,.1f}")
    print(f"    P95:    {values.quantile(0.95):>12,.1f}")
    print(f"    Max:    {values.max():>12,.1f}")


def analyze_key_numerics(name: str, df: pd.DataFrame) -> None:
    print_subsection(f"[{name}] Key Numeric Fields")

    price_cols = [("price", "Price (raw)"), ("rent_gross", "Rent Gross"), ("rent_net", "Rent Net"), ("rent_extra", "Rent Extra")]
    for col, label in price_cols:
        analyze_numeric_field(name, df, col, label)

    analyze_numeric_field(name, df, "number_of_rooms", "Rooms")
    analyze_numeric_field(name, df, "area", "Area (sqm)")
    analyze_numeric_field(name, df, "floor", "Floor")
    analyze_numeric_field(name, df, "year_built", "Year Built")

    dist_cols = [
        ("distance_public_transport", "Dist. Public Transport (m)"),
        ("distance_shop", "Dist. Shop (m)"),
        ("distance_kindergarten", "Dist. Kindergarten (m)"),
        ("distance_school_1", "Dist. School 1 (m)"),
        ("distance_school_2", "Dist. School 2 (m)"),
    ]
    for col, label in dist_cols:
        analyze_numeric_field(name, df, col, label)


def analyze_categorical(name: str, df: pd.DataFrame, col: str, label: str, top_n: int = 15) -> None:
    if col not in df.columns:
        return
    non_empty = df[col][~df[col].apply(is_empty)]
    if len(non_empty) == 0:
        print(f"\n  {label}: all empty")
        return
    coverage = len(non_empty) / len(df) * 100
    counts = non_empty.value_counts()
    print(f"\n  {label} ({len(non_empty):,} values, {coverage:.1f}% coverage, {counts.nunique()} unique):")
    for val, count in counts.head(top_n).items():
        pct = count / len(non_empty) * 100
        print(f"    {val:<45} {count:>8,} ({pct:>5.1f}%)")
    if len(counts) > top_n:
        print(f"    ... and {len(counts) - top_n} more unique values")


def analyze_categoricals(name: str, df: pd.DataFrame) -> None:
    print_subsection(f"[{name}] Key Categorical Fields")
    analyze_categorical(name, df, "offer_type", "Offer Type")
    analyze_categorical(name, df, "object_category", "Object Category")
    analyze_categorical(name, df, "object_type", "Object Type")
    analyze_categorical(name, df, "object_type_text", "Object Type Text", top_n=20)
    analyze_categorical(name, df, "scrape_source", "Scrape Source")
    analyze_categorical(name, df, "object_state", "Canton (object_state)")
    analyze_categorical(name, df, "object_city", "City (object_city)", top_n=20)
    analyze_categorical(name, df, "price_type", "Price Type")
    analyze_categorical(name, df, "status", "Status")


def analyze_boolean_fields(name: str, df: pd.DataFrame) -> None:
    print_subsection(f"[{name}] Boolean / Feature Flag Fields")
    bool_cols = [
        "prop_balcony", "prop_elevator", "prop_parking", "prop_garage",
        "prop_fireplace", "prop_child_friendly", "animal_allowed",
        "maybe_temporary", "is_new_building",
    ]
    existing = [c for c in bool_cols if c in df.columns]
    if not existing:
        print("  No boolean fields found.")
        return

    print(f"  {'Field':<30} {'True':>8} {'False':>8} {'Empty':>8} {'Coverage':>10}")
    print(f"  {'-----':<30} {'----':>8} {'-----':>8} {'-----':>8} {'--------':>10}")
    for col in existing:
        series = df[col].str.strip().str.lower()
        n_true = series.isin(["true", "1", "yes", "y"]).sum()
        n_false = series.isin(["false", "0", "no", "n"]).sum()
        n_empty = df[col].apply(is_empty).sum()
        coverage = (len(df) - n_empty) / len(df) * 100
        print(f"  {col:<30} {n_true:>8,} {n_false:>8,} {n_empty:>8,} {coverage:>9.1f}%")


def analyze_geo(name: str, df: pd.DataFrame) -> None:
    print_subsection(f"[{name}] Geolocation Coverage")
    lat_col = "geo_lat"
    lng_col = "geo_lng"
    if lat_col not in df.columns or lng_col not in df.columns:
        print("  geo_lat/geo_lng columns not present.")
        return
    lat = df[lat_col].apply(safe_float)
    lng = df[lng_col].apply(safe_float)
    both_valid = (~lat.isna()) & (~lng.isna())
    n_both = both_valid.sum()
    print(f"  Rows with valid lat+lng: {n_both:,} / {len(df):,} ({n_both/len(df)*100:.1f}%)")

    valid_lat = lat[both_valid]
    valid_lng = lng[both_valid]
    if n_both > 0:
        print(f"  Latitude  range: [{valid_lat.min():.4f}, {valid_lat.max():.4f}]")
        print(f"  Longitude range: [{valid_lng.min():.4f}, {valid_lng.max():.4f}]")

        in_switzerland = (
            (valid_lat >= 45.8) & (valid_lat <= 47.9) &
            (valid_lng >= 5.9) & (valid_lng <= 10.5)
        )
        n_ch = in_switzerland.sum()
        print(f"  Within Switzerland bounds: {n_ch:,} ({n_ch/n_both*100:.1f}%)")


def analyze_text_fields(name: str, df: pd.DataFrame) -> None:
    print_subsection(f"[{name}] Text Field Coverage & Length")
    text_cols = ["title", "object_description", "remarks"]
    existing = [c for c in text_cols if c in df.columns]

    for col in existing:
        non_empty = df[col][~df[col].apply(is_empty)]
        coverage = len(non_empty) / len(df) * 100
        if len(non_empty) > 0:
            lengths = non_empty.str.len()
            print(f"\n  {col} ({len(non_empty):,} populated, {coverage:.1f}% coverage):")
            print(f"    Avg length:    {lengths.mean():>8.0f} chars")
            print(f"    Median length: {lengths.median():>8.0f} chars")
            print(f"    Max length:    {lengths.max():>8,} chars")
            print(f"    Min length:    {lengths.min():>8,} chars")
        else:
            print(f"\n  {col}: all empty (0% coverage)")


def analyze_json_field(name: str, df: pd.DataFrame, col: str, sample_n: int = 200) -> None:
    if col not in df.columns:
        return
    print(f"\n  JSON field: {col}")
    non_empty = df[col][~df[col].apply(is_empty)]
    print(f"    Populated: {len(non_empty):,} / {len(df):,} ({len(non_empty)/len(df)*100:.1f}%)")

    if len(non_empty) == 0:
        return

    sample = non_empty.sample(min(sample_n, len(non_empty)), random_state=42)
    parsed_count = 0
    all_top_keys: Counter = Counter()
    nested_keys: Counter = Counter()
    parse_errors = 0

    for val in sample:
        obj = safe_json(val)
        if obj is None:
            parse_errors += 1
            continue
        parsed_count += 1
        if isinstance(obj, dict):
            for k, v in obj.items():
                all_top_keys[k] += 1
                if isinstance(v, list) and len(v) > 0:
                    if isinstance(v[0], dict):
                        for nested_k in v[0].keys():
                            if isinstance(nested_k, str):
                                nested_keys[f"{k}[*].{nested_k}"] += 1
                elif isinstance(v, dict):
                    for nested_k in v.keys():
                        if isinstance(nested_k, str):
                            nested_keys[f"{k}.{nested_k}"] += 1

    if parse_errors > 0:
        print(f"    Parse errors in sample: {parse_errors}/{len(sample)}")
    print(f"    Successfully parsed in sample: {parsed_count}/{len(sample)}")

    if all_top_keys:
        print(f"    Top-level keys (from {parsed_count} parsed objects):")
        for key, cnt in all_top_keys.most_common(25):
            print(f"      {key:<40} {cnt:>4}/{parsed_count} ({cnt/parsed_count*100:.0f}%)")
    if nested_keys:
        print(f"    Nested keys (sampled):")
        for key, cnt in sorted(nested_keys.items(), key=lambda x: -x[1])[:30]:
            print(f"      {key:<50} {cnt:>4}/{parsed_count} ({cnt/parsed_count*100:.0f}%)")


def analyze_json_fields(name: str, df: pd.DataFrame) -> None:
    print_subsection(f"[{name}] JSON Field Analysis")
    for col in ["orig_data", "images", "location_address"]:
        analyze_json_field(name, df, col)


def analyze_images(name: str, df: pd.DataFrame) -> None:
    print_subsection(f"[{name}] Image Availability")
    if "images" not in df.columns:
        print("  No 'images' column.")
        return

    non_empty = df["images"][~df["images"].apply(is_empty)]
    total_with_at_least_one = 0
    image_counts = []

    sample = non_empty.sample(min(500, len(non_empty)), random_state=42)
    for val in sample:
        obj = safe_json(val)
        if obj is None:
            continue
        imgs = []
        if isinstance(obj, dict):
            imgs = obj.get("images", [])
            if not isinstance(imgs, list):
                imgs = []
        n = len(imgs)
        image_counts.append(n)
        if n > 0:
            total_with_at_least_one += 1

    if image_counts:
        arr = np.array(image_counts)
        pct_with_images = total_with_at_least_one / len(image_counts) * 100
        print(f"  Sample size: {len(image_counts)}")
        print(f"  With at least 1 image: {total_with_at_least_one} ({pct_with_images:.1f}%)")
        print(f"  Avg images per listing:    {arr.mean():.1f}")
        print(f"  Median images per listing: {np.median(arr):.0f}")
        print(f"  Max images per listing:    {arr.max()}")


def cross_source_comparison(datasets: dict[str, pd.DataFrame]) -> None:
    print_section("CROSS-SOURCE COLUMN COMPARISON MATRIX")
    all_cols = sorted(set(col for df in datasets.values() for col in df.columns))

    header = f"  {'Column':<40}"
    for name in datasets:
        header += f" {name[:18]:>18}"
    print(header)
    print(f"  {'------':<40}" + "".join(f" {'------':>18}" for _ in datasets))

    for col in all_cols:
        row = f"  {col:<40}"
        for name, df in datasets.items():
            if col in df.columns:
                rate = null_rate(df[col])
                populated_pct = (1 - rate) * 100
                if populated_pct > 90:
                    marker = f"{populated_pct:.0f}%"
                elif populated_pct > 0:
                    marker = f"{populated_pct:.0f}%"
                else:
                    marker = "0%"
            else:
                marker = "MISSING"
            row += f" {marker:>18}"
        print(row)


def feature_coverage_heatmap(datasets: dict[str, pd.DataFrame]) -> None:
    print_section("FEATURE AVAILABILITY HEATMAP (Challenge-Critical Fields)")

    critical_fields = {
        "Core Identity": ["id", "platform_id", "scrape_source", "title"],
        "Pricing": ["price", "rent_gross", "rent_net", "rent_extra", "price_type"],
        "Property Specs": ["number_of_rooms", "area", "floor", "year_built", "object_category", "object_type", "offer_type"],
        "Location - Address": ["object_street", "object_city", "object_zip", "object_state"],
        "Location - Geo": ["geo_lat", "geo_lng"],
        "Location - JSON": ["location_address"],
        "Distances": ["distance_public_transport", "distance_shop", "distance_kindergarten", "distance_school_1", "distance_school_2"],
        "Features / Booleans": ["prop_balcony", "prop_elevator", "prop_parking", "prop_garage", "prop_fireplace", "prop_child_friendly", "animal_allowed", "maybe_temporary", "is_new_building"],
        "Text Content": ["object_description", "remarks", "title"],
        "Rich Data": ["orig_data", "images", "location_address"],
        "Time / Availability": ["available_from", "time_of_creation", "last_scraped", "status"],
        "Agency": ["agency_name", "agency_phone", "agency_email", "partner_name"],
    }

    for group_name, fields in critical_fields.items():
        print(f"\n  {group_name}:")
        header = f"    {'Field':<35}"
        for ds_name in datasets:
            header += f" {ds_name[:16]:>16}"
        print(header)
        print(f"    {'-----':<35}" + "".join(f" {'-----':>16}" for _ in datasets))

        for field in fields:
            row = f"    {field:<35}"
            for ds_name, df in datasets.items():
                if field in df.columns:
                    rate = null_rate(df[field])
                    pct = (1 - rate) * 100
                    if pct >= 90:
                        symbol = f"[###] {pct:.0f}%"
                    elif pct >= 50:
                        symbol = f"[## ] {pct:.0f}%"
                    elif pct > 0:
                        symbol = f"[#  ] {pct:.0f}%"
                    else:
                        symbol = "[   ] 0%"
                else:
                    symbol = "  MISSING"
                row += f" {symbol:>16}"
            print(row)


def id_overlap_analysis(datasets: dict[str, pd.DataFrame]) -> None:
    print_section("ID OVERLAP ANALYSIS BETWEEN SOURCES")
    id_sets = {}
    for name, df in datasets.items():
        if "id" in df.columns:
            ids = set(df["id"][~df["id"].apply(is_empty)])
            id_sets[name] = ids
            print(f"  {name}: {len(ids):,} unique IDs")

    names = list(id_sets.keys())
    for i, n1 in enumerate(names):
        for n2 in names[i+1:]:
            overlap = id_sets[n1] & id_sets[n2]
            print(f"  {n1} ∩ {n2}: {len(overlap):,} shared IDs")


def source_breakdown_within(name: str, df: pd.DataFrame) -> None:
    if "scrape_source" not in df.columns:
        return
    print_subsection(f"[{name}] Breakdown by scrape_source")
    sources = df["scrape_source"][~df["scrape_source"].apply(is_empty)]
    if len(sources) == 0:
        print("  No scrape_source values.")
        return
    for src, count in sources.value_counts().head(20).items():
        pct = count / len(df) * 100
        print(f"  {src:<40} {count:>8,} ({pct:.1f}%)")


def structured_vs_structured_diff(datasets: dict[str, pd.DataFrame]) -> None:
    wi = datasets.get("structured_with_images")
    wo = datasets.get("structured_without_images")
    if wi is None or wo is None:
        return

    print_section("STRUCTURED: WITH-IMAGES vs WITHOUT-IMAGES COMPARISON")

    wi_ids = set(wi["id"][~wi["id"].apply(is_empty)]) if "id" in wi.columns else set()
    wo_ids = set(wo["id"][~wo["id"].apply(is_empty)]) if "id" in wo.columns else set()

    print(f"  with_images:    {len(wi_ids):,} unique IDs")
    print(f"  without_images: {len(wo_ids):,} unique IDs")
    print(f"  Overlap:        {len(wi_ids & wo_ids):,}")
    print(f"  Only in with_images:    {len(wi_ids - wo_ids):,}")
    print(f"  Only in without_images: {len(wo_ids - wi_ids):,}")

    print("\n  Column-level coverage difference (with_images % - without_images %):")
    all_cols = sorted(set(wi.columns) | set(wo.columns))
    diffs = []
    for col in all_cols:
        wi_pct = (1 - null_rate(wi[col])) * 100 if col in wi.columns else 0
        wo_pct = (1 - null_rate(wo[col])) * 100 if col in wo.columns else 0
        diffs.append((col, wi_pct, wo_pct, wi_pct - wo_pct))

    diffs.sort(key=lambda x: abs(x[3]), reverse=True)
    print(f"  {'Column':<40} {'With Imgs':>10} {'No Imgs':>10} {'Diff':>10}")
    print(f"  {'------':<40} {'---------':>10} {'-------':>10} {'----':>10}")
    for col, wi_pct, wo_pct, diff in diffs[:20]:
        print(f"  {col:<40} {wi_pct:>9.1f}% {wo_pct:>9.1f}% {diff:>+9.1f}%")


def orig_data_deep_dive(datasets: dict[str, pd.DataFrame]) -> None:
    print_section("ORIG_DATA JSON DEEP DIVE (per source)")

    for name, df in datasets.items():
        if "orig_data" not in df.columns:
            continue
        print_subsection(f"[{name}] orig_data structure")
        non_empty = df["orig_data"][~df["orig_data"].apply(is_empty)]
        if len(non_empty) == 0:
            print("  All empty")
            continue

        sample = non_empty.sample(min(300, len(non_empty)), random_state=42)
        features_keys_counter: Counter = Counter()
        maindata_keys_counter: Counter = Counter()
        top_level_keys: Counter = Counter()
        has_features_list = 0
        has_maindata_list = 0
        parsed = 0

        for val in sample:
            obj = safe_json(val)
            if not isinstance(obj, dict):
                continue
            parsed += 1
            for k in obj.keys():
                top_level_keys[k] += 1

            features = obj.get("Features")
            if isinstance(features, list):
                has_features_list += 1
                for item in features:
                    if isinstance(item, dict) and "Key" in item:
                        features_keys_counter[item["Key"]] += 1

            maindata = obj.get("MainData")
            if isinstance(maindata, list):
                has_maindata_list += 1
                for item in maindata:
                    if isinstance(item, dict) and "Key" in item:
                        maindata_keys_counter[item["Key"]] += 1

        print(f"  Parsed {parsed}/{len(sample)} sampled records")
        print(f"\n  Top-level keys:")
        for k, c in top_level_keys.most_common(20):
            print(f"    {k:<45} {c:>4}/{parsed}")

        if has_features_list > 0:
            print(f"\n  Features list present in {has_features_list}/{parsed} records")
            print(f"  Feature Keys found:")
            for k, c in features_keys_counter.most_common(30):
                print(f"    {k:<45} {c:>4}/{has_features_list}")

        if has_maindata_list > 0:
            print(f"\n  MainData list present in {has_maindata_list}/{parsed} records")
            print(f"  MainData Keys found:")
            for k, c in maindata_keys_counter.most_common(30):
                print(f"    {k:<45} {c:>4}/{has_maindata_list}")


def sred_images_check() -> None:
    print_section("SRED IMAGE FILES CHECK")
    img_dir = RAW_DATA_DIR / "sred_images"
    if not img_dir.exists():
        print("  sred_images/ directory not found.")
        return
    images = list(img_dir.glob("*.jpeg")) + list(img_dir.glob("*.jpg")) + list(img_dir.glob("*.png"))
    print(f"  Total image files: {len(images):,}")
    if images:
        sizes = [f.stat().st_size for f in images]
        total_mb = sum(sizes) / (1024 * 1024)
        print(f"  Total size: {total_mb:.1f} MB")
        print(f"  Avg size: {np.mean(sizes)/1024:.1f} KB")
        print(f"  Min size: {min(sizes)/1024:.1f} KB")
        print(f"  Max size: {max(sizes)/1024:.1f} KB")


def summary_report(datasets: dict[str, pd.DataFrame]) -> None:
    print_section("EXECUTIVE SUMMARY")
    total_rows = sum(len(df) for df in datasets.values())
    print(f"\n  Total listings across all sources: {total_rows:,}")
    for name, df in datasets.items():
        print(f"    {name}: {len(df):,} rows")

    print("\n  KEY FINDINGS:")
    for name, df in datasets.items():
        issues = []
        if "price" in df.columns and null_rate(df["price"]) > 0.5:
            if "rent_gross" in df.columns:
                has_price = ~df["price"].apply(is_empty)
                has_gross = ~df["rent_gross"].apply(is_empty)
                price_coverage = (has_price | has_gross).mean() * 100
            else:
                price_coverage = (1 - null_rate(df["price"])) * 100
            issues.append(f"price coverage {price_coverage:.0f}%")
        if "geo_lat" in df.columns:
            geo_cov = (1 - null_rate(df["geo_lat"])) * 100
            if geo_cov < 80:
                issues.append(f"geo coverage only {geo_cov:.0f}%")
        if "object_description" in df.columns:
            desc_cov = (1 - null_rate(df["object_description"])) * 100
            if desc_cov < 50:
                issues.append(f"description coverage only {desc_cov:.0f}%")
        if issues:
            print(f"\n    [{name}] Data gaps:")
            for issue in issues:
                print(f"      - {issue}")


def main() -> None:
    print("\n" + "=" * 100)
    print("  NESTFINDER DATA PROFILING REPORT")
    print("  Deep analysis of all raw CSV data sources")
    print("=" * 100)

    datasets: dict[str, pd.DataFrame] = {}
    for name, path in CSV_FILES.items():
        if not path.exists():
            print(f"\n  WARNING: {path} not found, skipping.")
            continue
        print(f"\n  Loading {name} from {path.name}...")
        datasets[name] = load_csv(path)
        print(f"  Loaded {len(datasets[name]):,} rows, {len(datasets[name].columns)} columns.")

    if not datasets:
        print("  No data files found. Exiting.")
        sys.exit(1)

    # Per-source analysis
    for name, df in datasets.items():
        print_section(f"SOURCE: {name.upper()}")
        analyze_basic_info(name, df, CSV_FILES[name])
        analyze_null_rates(name, df)
        analyze_key_numerics(name, df)
        analyze_categoricals(name, df)
        analyze_boolean_fields(name, df)
        analyze_geo(name, df)
        analyze_text_fields(name, df)
        analyze_json_fields(name, df)
        analyze_images(name, df)
        source_breakdown_within(name, df)

    # Cross-source analyses
    cross_source_comparison(datasets)
    feature_coverage_heatmap(datasets)
    id_overlap_analysis(datasets)
    structured_vs_structured_diff(datasets)
    orig_data_deep_dive(datasets)
    sred_images_check()
    summary_report(datasets)

    print(f"\n{'=' * 100}")
    print("  END OF REPORT")
    print(f"{'=' * 100}\n")


if __name__ == "__main__":
    main()
