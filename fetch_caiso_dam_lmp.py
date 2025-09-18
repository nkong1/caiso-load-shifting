# fetch_caiso_dayahead.py
import requests
import pandas as pd
from datetime import datetime
from datetime import timedelta
from pathlib import Path
import geopandas as gpd
import time
import shapely
from zoneinfo import ZoneInfo


def fetch(url):
    for attempt in range(2):  # give it up to 2 tries
        try:
            r = requests.get(url, timeout=60)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"Attempt {attempt+1} failed: {e}")
            if attempt < 2:  # don’t sleep after the last try
                time.sleep(1200)  # wait 20 min then try again
    raise RuntimeError("CAISO API failed after 3 attempts")


def fetch_lmps(outdir):
    # Request CAISO Day-Ahead price contour map
    url = "https://wwwmobile.caiso.com/Web.Service.Chart/api/v3/ChartService/PriceContourMap1"
    contour = fetch(url)

    # Extract metadata
    current_date = contour.get("dd").split("T")[0]  # e.g., "2025-09-13"
    current_hour_ending = int(contour.get("dh"))  # e.g., 23

    # Parse nodes
    nodes = []
    for layer in contour.get("l", []):
        for item in layer.get("m", []):
            if item.get("t") == "Node":
                nodes.append(
                    {
                        "node_id": item.get("n"),
                        "lat": item["c"][0],
                        "lon": item["c"][1],
                        "type": item.get("p"),  # LOAD or GEN
                        "area": item.get("a"),  # e.g., PGE, SCE
                        "price_dp": float(item.get("dp")),  # LMP price
                    }
                )

    df = pd.DataFrame(nodes)

    # Filter for just LOAD nodes
    df = df[df["type"] == "LOAD"]

    # Clip just to nodes within CA
    ca_shapefile = "ca_state/CA_State.shp"  # path to California shapefile

    gdf_lmps = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df["lon"], df["lat"]), crs="EPSG:4326"  # WGS84
    )

    gdf_ca = gpd.read_file(ca_shapefile).to_crs("EPSG:4326")
    ca_polygon = gdf_ca.union_all()
    gdf_lmps = gdf_lmps[gdf_lmps.within(ca_polygon)]

    # Remove unnecessary columns for storage savings
    gdf_lmps = gdf_lmps.drop(columns=["type", "lat", "lon", "area"])

    # Compute the timestamp of the latest file just saved
    latest_dt = datetime.strptime(current_date, "%Y-%m-%d").replace(
        tzinfo=ZoneInfo("US/Pacific")
    ) + timedelta(hours=current_hour_ending)

    window_start = latest_dt - timedelta(hours=23)  # 24 consecutive hours

    # Delete old files outside this window
    for f in outdir.glob("*csv"):
        parts = f.stem.split("_")
        file_date = parts[2]
        file_hour = int(parts[3][2:])
        file_dt = datetime.strptime(file_date, "%Y-%m-%d").replace(
            tzinfo=ZoneInfo("US/Pacific")
        ) + timedelta(hours=file_hour)

        if file_dt < window_start:
            f.unlink()
            print(f"Deleted old file: {f.name}")

    # Save to timestamped CSV
    safe_date = current_date.replace(":", "_")
    outfile = outdir / f"caiso_lmps_{safe_date}_HE{current_hour_ending:02d}.csv"
    gdf_lmps.to_csv(outfile, index=False)
    print(f"Saved {len(gdf_lmps)} nodes to {outfile}")


def combine_lmps(lmp_dir):
    """Make a combined df of hourly CAISO LMP prices across the next 24 hours.
    Each row is an lmp node and each lmp price column is a datetime string"""

    combined_df = pd.DataFrame()

    for f in lmp_dir.glob("*csv"):
        parts = f.stem.split("_")
        file_date = parts[2]
        file_hour = int(parts[3][2:])

        file_dt = datetime.strptime(file_date, "%Y-%m-%d").replace() + timedelta(
            hours=file_hour
        )

        single_hour_df = pd.read_csv(f)
        single_hour_df = single_hour_df.rename(columns={"price_dp": str(file_dt)})

        single_hour_df = single_hour_df.drop(
            columns=["geometry", "type", "lat", "lon", "area"], errors="ignore"
        )

        if combined_df.empty:
            combined_df = single_hour_df
        else:
            combined_df = pd.merge(
                combined_df, single_hour_df, on="node_id", how="inner"
            )

        combined_df = combined_df.sort_index(axis=1, ascending=False)

    # Drop rows where all LMP values are zero
    price_cols = combined_df.columns.drop("node_id")
    combined_df = combined_df[(combined_df[price_cols] != 0).any(axis=1)]

    return combined_df


def score_lmps(dam_lmp_df_hourly):
    """Assign each node a score at every hour, representing how favorable it is to deploy
    a flexible load relative to that node’s most expensive (highest LMP) hour within
    the 24-hour window. The score is calculated as the relative difference between the
    LMP at a given hour and the node’s maximum LMP over the period. A score of 0
    corresponds to the highest-price (least favorable) hour. Higher scores indicate
    lower-price (more favorable) hours.
    """

    # Get the price-only sub-df
    price_df = dam_lmp_df_hourly.drop(columns="node_id", errors="ignore")

    # Row-wise max (worst price) as numpy array
    max_vals = price_df.max(axis=1).to_numpy().reshape(-1, 1)

    # Compute relative improvement vs worst
    percent_better = (max_vals - price_df.to_numpy()) / max_vals 

    # Wrap back into a DataFrame
    percent_better_df = pd.DataFrame(
        percent_better,
        index=price_df.index,
        columns=price_df.columns
    )

    # Handle edge case: if all prices are equal, set scores to 0
    percent_better_df = percent_better_df.fillna(0)

    # Reattach node_id
    percent_better_df = pd.concat([dam_lmp_df_hourly[["node_id"]], percent_better_df], axis=1)

    return percent_better_df


def main():
    # Make a directory to store the retrieved CAISO data
    lmp_dir = Path("caiso_data")
    lmp_dir.mkdir(exist_ok=True)
    fetch_lmps(lmp_dir)

    dam_lmps_hourly = combine_lmps(lmp_dir)
    scored_lmps = score_lmps(dam_lmps_hourly)


if __name__ == "__main__":
    main()
