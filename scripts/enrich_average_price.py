"""Compute price-per-m2 features and area-level comparisons.

Adds:
- price_per_m2 per listing
- avg_price_per_m2 for municipality/district/canton
- price_per_m2_vs_* ratio features
"""

import pandas as pd

INPUT_CSV = "data/listings_enriched_v.3.csv"
OUTPUT_CSV = "data/listings_enriched_v.3.1.csv"

PRICE_COL = "price"
AREA_COL = "area"

df = pd.read_csv(INPUT_CSV)

# Ensure numeric columns for calculation
df[PRICE_COL] = pd.to_numeric(df[PRICE_COL], errors="coerce")
df[AREA_COL] = pd.to_numeric(df[AREA_COL], errors="coerce")

# price per m2
df["price_per_m2"] = pd.NA
valid_area = df[AREA_COL].notna() & (df[AREA_COL] != 0)
df.loc[valid_area, "price_per_m2"] = df.loc[valid_area, PRICE_COL] / df.loc[valid_area, AREA_COL]

# municipality
muni_avg = (
    df.groupby(["municipality_code", "municipality_name"], dropna=False)["price_per_m2"]
    .mean()
    .reset_index()
    .rename(columns={"price_per_m2": "avg_price_per_m2_municipality"})
)
df = df.merge(muni_avg, on=["municipality_code", "municipality_name"], how="left")

# district
district_avg = (
    df.groupby(["district_code", "district_name"], dropna=False)["price_per_m2"]
    .mean()
    .reset_index()
    .rename(columns={"price_per_m2": "avg_price_per_m2_district"})
)
df = df.merge(district_avg, on=["district_code", "district_name"], how="left")

# canton
canton_avg = (
    df.groupby(["canton_code", "canton_name"], dropna=False)["price_per_m2"]
    .mean()
    .reset_index()
    .rename(columns={"price_per_m2": "avg_price_per_m2_canton"})
)
df = df.merge(canton_avg, on=["canton_code", "canton_name"], how="left")

# comparison features
muni_den = df["avg_price_per_m2_municipality"].replace({0: pd.NA})
district_den = df["avg_price_per_m2_district"].replace({0: pd.NA})
canton_den = df["avg_price_per_m2_canton"].replace({0: pd.NA})

df["price_per_m2_vs_municipality"] = df["price_per_m2"] / muni_den
df["price_per_m2_vs_district"] = df["price_per_m2"] / district_den
df["price_per_m2_vs_canton"] = df["price_per_m2"] / canton_den


def price_level_label(ratio: float | int | None) -> str | None:
    if pd.isna(ratio):
        return None
    if ratio < 0.9:
        return "cheaper_than_area"
    if ratio <= 1.1:
        return "similar_to_area"
    return "more_expensive_than_area"


df["price_per_m2_vs_municipality_label"] = df[
    "price_per_m2_vs_municipality"
].apply(price_level_label)

df.to_csv(OUTPUT_CSV, index=False)
print(f"Saved {OUTPUT_CSV}")