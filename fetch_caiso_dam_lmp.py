import requests
import pandas as pd
from datetime import datetime
from datetime import timedelta
from pathlib import Path
import geopandas as gpd
import time
import shapely
from zoneinfo import ZoneInfo
from process_data import run


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
    filter_and_save_lmps(df, current_hour_ending, current_date, outdir)

def filter_and_save_lmps(lmp_df, current_hour_ending, current_date, outdir):
    # Filter for just LOAD nodes
    lmp_df = lmp_df[lmp_df["type"] == "LOAD"]

    # Clip just to nodes within CA
    ca_shapefile = "ca_state/CA_State.shp"  # path to California shapefile

    gdf_lmps = gpd.GeoDataFrame(
        lmp_df, geometry=gpd.points_from_xy(lmp_df["lon"], lmp_df["lat"]), crs="EPSG:4326"  # WGS84
    )

    gdf_ca = gpd.read_file(ca_shapefile).to_crs("EPSG:4326")
    ca_polygon = gdf_ca.union_all()
    gdf_lmps = gdf_lmps[gdf_lmps.within(ca_polygon)]

    # Remove unnecessary columns for storage savings and reset index
    gdf_lmps = gdf_lmps.drop(columns=["type", "geometry", "area"]).reset_index(drop=True)

    # Compute the timestamp of the latest file just saved
    latest_dt = datetime.strptime(current_date, "%Y-%m-%d").replace(
        tzinfo=ZoneInfo("US/Pacific")
    ) + timedelta(hours=current_hour_ending)

    window_start = latest_dt - timedelta(hours=23)  # 24 consecutive hours

    # Delete old files outside this window and check if the previous hour is missing
    has_previous_hour = False
    previous_hour = current_hour_ending - 1
    if previous_hour == 0:
        previous_hour = 24

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


        if file_hour == previous_hour:
            has_previous_hour = True

    # Save to timestamped CSV
    safe_date = current_date.replace(":", "_")
    outfile = outdir / f"caiso_lmps_{safe_date}_HE{current_hour_ending:02d}.csv"
    gdf_lmps.to_csv(outfile, index=False)
    print(f"Saved {len(gdf_lmps)} nodes to {outfile}")

    # Fill missing previous hour if needed
    # if not has_previous_hour:
        # fill_previous_hour(current_date, current_hour_ending, outdir, gdf_lmps)
        
        
def fill_previous_hour(current_date, current_hour_ending, outdir, gdf_lmps):
    print("Previous hour missing. Filling with average of current and 2 hours prior...")

    # compute the datetime of the prior hour (2 hours before the current hour ending)
    two_hours_prior_dt = datetime.strptime(current_date, "%Y-%m-%d").replace(
        tzinfo=ZoneInfo("US/Pacific")
    ) + timedelta(hours=current_hour_ending - 2)

    two_hours_prior_hour = two_hours_prior_dt.hour
    two_hours_prior_date = two_hours_prior_dt.strftime("%Y-%m-%d")

    if two_hours_prior_hour == 0:
        two_hours_prior_hour = 24
        two_hours_prior_date = (two_hours_prior_dt + timedelta(hours=-1)).strftime("%Y-%m-%d")

    two_hours_hour_file = outdir / f"caiso_lmps_{two_hours_prior_date}_HE{two_hours_prior_hour:02d}.csv"

    df_prior = pd.read_csv(two_hours_hour_file)
    df_filled = df_prior.copy()

    print(df_prior["price_dp"])
    print(gdf_lmps["price_dp"])

    df_filled["price_dp"] = (df_prior["price_dp"] + gdf_lmps["price_dp"]) / 2
    
    one_hours_prior_dt =  two_hours_prior_dt + timedelta(hours=1)

    one_hour_prior_hour = one_hours_prior_dt.hour
    one_hour_prior_date = one_hours_prior_dt.strftime("%Y-%m-%d")

    if one_hour_prior_hour == 0:
        one_hour_prior_hour = 24
        one_hour_prior_date = (one_hours_prior_dt + timedelta(hours=-1)).strftime("%Y-%m-%d")

    filled_file = outdir / f"caiso_lmps_{one_hour_prior_date}_HE{one_hour_prior_hour:02d}.csv"
    df_filled.to_csv(filled_file, index=False)
    print(f"Filled missing previous hour saved to {filled_file}")


def main():
    # Create an output path
    lmp_dir = Path("caiso_data")
    lmp_dir.mkdir(exist_ok=True)

    # Fetch the latest LMP data from CAISO
    fetch_lmps(lmp_dir)

    # Call the processing module
    run(lmp_dir)

if __name__ == "__main__":
    main()
