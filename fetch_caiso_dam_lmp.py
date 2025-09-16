# fetch_caiso_dayahead.py
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
import geopandas as gpd
import shapely

def main():
    # Request CAISO Day-Ahead price contour map
    url = "https://wwwmobile.caiso.com/Web.Service.Chart/api/v3/ChartService/PriceContourMap1"
    resp = requests.get(url)
    resp.raise_for_status()
    contour = resp.json()

    # Extract metadata
    current_date = contour.get("dd").split('T')[0]     # e.g., "2025-09-13"
    current_hour_ending = int(contour.get("dh"))       # e.g., 23

    # Parse nodes
    nodes = []
    for layer in contour.get("l", []):
        for item in layer.get("m", []):
            if item.get("t") == "Node":
                nodes.append({
                    "node_id": item.get("n"),
                    "lat": item["c"][0] if item.get("c") else None,
                    "lon": item["c"][1] if item.get("c") else None,
                    "type": item.get("p"),   # LOAD or GEN
                    "area": item.get("a"),   # e.g., PGE, SCE
                    "price_dp": float(item.get("dp")) if item.get("dp") else None
                })

    df = pd.DataFrame(nodes)

    # Filter for just LOAD nodes
    df = df[df['type'] == 'LOAD']

    # Clip just to nodes within CA
    ca_shapefile = "ca_state/CA_State.shp"                   # path to California shapefile

    gdf_lmps = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["lon"], df["lat"]),
        crs="EPSG:4326"  # WGS84
    )

    gdf_ca = gpd.read_file(ca_shapefile).to_crs("EPSG:4326")
    ca_polygon = gdf_ca.union_all()
    gdf_lmps = gdf_lmps[gdf_lmps.within(ca_polygon)]

    # Drop unnesseary columns for storage savings
    # gdf_lmps = gdf_lmps.drop(columns=['lat', 'lon', 'area'])

    # Save to timestamped CSV
    outdir = Path("outputs")
    outdir.mkdir(exist_ok=True)
    safe_date = current_date.replace(":", "_")
    outfile = outdir / f"caiso_lmps_{safe_date}_HE{current_hour_ending:02d}.csv"
    gdf_lmps.to_csv(outfile, index=False)
    print(f"Saved {len(gdf_lmps)} nodes → {outfile}")

if __name__ == "__main__":
    main()
