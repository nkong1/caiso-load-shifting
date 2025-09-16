import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

# === Inputs ===
matched_csv = "outputs/caiso_lmps_2025-09-16_HE12.csv"   # your output file
ca_shapefile = "ca_state/CA_State.shp"                   # path to California shapefile

# === Load node data ===
df = pd.read_csv(matched_csv)
gdf_points = gpd.GeoDataFrame(
    df,
    geometry=gpd.points_from_xy(df["lon"], df["lat"]),
    crs="EPSG:4326"  # WGS84
)

# === Load California polygon ===
gdf_ca = gpd.read_file(ca_shapefile).to_crs("EPSG:4326")
ca_polygon = gdf_ca.unary_union   # dissolve into one shape

# === Clip points to CA ===
gdf_points_ca = gdf_points[gdf_points.within(ca_polygon)]

print(f"Kept {len(gdf_points_ca)} nodes inside California (out of {len(gdf_points)})")

# === Plot ===
fig, ax = plt.subplots(figsize=(8, 10))
gdf_ca.boundary.plot(ax=ax, color="black", linewidth=0.8)
gdf_ca.plot(ax=ax, color="lightgrey", alpha=0.5)

gdf_points_ca.plot(ax=ax, color="red", markersize=10, alpha=0.7)

plt.title("CAISO Node Locations (Clipped to California)", fontsize=14)
plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.show()
