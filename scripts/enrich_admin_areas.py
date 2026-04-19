from __future__ import annotations

import pandas as pd
import geopandas as gpd



INPUT_CSV = "data/listing_raw.csv"
BOUNDARIES_GPKG = "data/external/swissBOUNDARIES3D_1_5_LV95_LN02.gpkg"
OUTPUT_CSV = "data/listings_with_admin.csv"

LAT_COL = "geo_lat"
LON_COL = "geo_lng"

# Correct layers from your GPKG
MUNICIPALITY_LAYER = "tlm_hoheitsgebiet"
DISTRICT_LAYER = "tlm_bezirksgebiet"
CANTON_LAYER = "tlm_kantonsgebiet"

# Correct columns from your GPKG
MUNICIPALITY_CODE_COL_SRC = "bfs_nummer"
DISTRICT_CODE_COL_SRC = "bezirksnummer"
CANTON_CODE_COL_SRC = "kantonsnummer"
NAME_COL_SRC = "name"

# Output column names in your final dataset
MUNICIPALITY_CODE_COL = "municipality_code"
MUNICIPALITY_NAME_COL = "municipality_name"

DISTRICT_CODE_COL = "district_code"
DISTRICT_NAME_COL = "district_name"

CANTON_CODE_COL = "canton_code"
CANTON_NAME_COL = "canton_name"


def load_points(csv_path: str) -> gpd.GeoDataFrame:
    df = pd.read_csv(csv_path)

    required = {LAT_COL, LON_COL}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df = df[df[LAT_COL].notna() & df[LON_COL].notna()].copy()

    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df[LON_COL], df[LAT_COL]),
        crs="EPSG:4326",
    )

    # Convert points to same CRS as swissBOUNDARIES3D
    gdf = gdf.to_crs("EPSG:2056")
    return gdf


def load_municipalities(path: str) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(path, layer=MUNICIPALITY_LAYER)
    return gdf[
        [MUNICIPALITY_CODE_COL_SRC, DISTRICT_CODE_COL_SRC, CANTON_CODE_COL_SRC, NAME_COL_SRC, "geometry"]
    ].rename(
        columns={
            MUNICIPALITY_CODE_COL_SRC: MUNICIPALITY_CODE_COL,
            DISTRICT_CODE_COL_SRC: DISTRICT_CODE_COL,
            CANTON_CODE_COL_SRC: CANTON_CODE_COL,
            NAME_COL_SRC: MUNICIPALITY_NAME_COL,
        }
    )


def load_districts(path: str) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(path, layer=DISTRICT_LAYER)
    return gdf[
        [DISTRICT_CODE_COL_SRC, CANTON_CODE_COL_SRC, NAME_COL_SRC, "geometry"]
    ].rename(
        columns={
            DISTRICT_CODE_COL_SRC: DISTRICT_CODE_COL,
            CANTON_CODE_COL_SRC: CANTON_CODE_COL,
            NAME_COL_SRC: DISTRICT_NAME_COL,
        }
    )


def load_cantons(path: str) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(path, layer=CANTON_LAYER)
    return gdf[
        [CANTON_CODE_COL_SRC, NAME_COL_SRC, "geometry"]
    ].rename(
        columns={
            CANTON_CODE_COL_SRC: CANTON_CODE_COL,
            NAME_COL_SRC: CANTON_NAME_COL,
        }
    )


def main() -> None:
    listings = load_points(INPUT_CSV)

    municipalities = load_municipalities(BOUNDARIES_GPKG)
    districts = load_districts(BOUNDARIES_GPKG)
    cantons = load_cantons(BOUNDARIES_GPKG)

    # 1) Municipality join
    out = gpd.sjoin(
        listings,
        municipalities,
        how="left",
        predicate="within",
    ).drop(columns=["index_right"])

    # 2) District join
    district_join = gpd.sjoin(
        listings[["geometry"]],
        districts,
        how="left",
        predicate="within",
    ).drop(columns=["index_right"])

    out[DISTRICT_NAME_COL] = district_join[DISTRICT_NAME_COL].values

    # Use joined district code only if municipality join did not already provide it
    if DISTRICT_CODE_COL in out.columns:
        out[DISTRICT_CODE_COL] = out[DISTRICT_CODE_COL].fillna(district_join[DISTRICT_CODE_COL])
    else:
        out[DISTRICT_CODE_COL] = district_join[DISTRICT_CODE_COL].values

    # 3) Canton join
    canton_join = gpd.sjoin(
        listings[["geometry"]],
        cantons,
        how="left",
        predicate="within",
    ).drop(columns=["index_right"])

    out[CANTON_NAME_COL] = canton_join[CANTON_NAME_COL].values

    if CANTON_CODE_COL in out.columns:
        out[CANTON_CODE_COL] = out[CANTON_CODE_COL].fillna(canton_join[CANTON_CODE_COL])
    else:
        out[CANTON_CODE_COL] = canton_join[CANTON_CODE_COL].values

    # Convert back to normal dataframe
    out = pd.DataFrame(out.drop(columns="geometry"))

    out.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved {OUTPUT_CSV}")


if __name__ == "__main__":
    main()