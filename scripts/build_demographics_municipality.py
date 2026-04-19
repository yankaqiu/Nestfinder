import geopandas as gpd
import pandas as pd

GPKG = "data/external/swissBOUNDARIES3D_1_5_LV95_LN02.gpkg"
OUT = "data/external/demographics_municipality.csv"


def main() -> None:
    gdf = gpd.read_file(GPKG, layer="tlm_hoheitsgebiet")

    df = gdf[["bfs_nummer", "name", "einwohnerzahl", "gem_flaeche"]].copy()
    df = df.rename(
        columns={
            "bfs_nummer": "municipality_code",
            "name": "municipality_name",
            "einwohnerzahl": "population_total",
            "gem_flaeche": "area_ha",
        }
    )

    df["area_km2"] = df["area_ha"] / 100
    df["population_density"] = df["population_total"] / df["area_km2"]

    df.to_csv(OUT, index=False)
    print(f"Saved {OUT}")


if __name__ == "__main__":
    main()
