#%% Imports
import requests
import pandas as pd
from datetime import date, timedelta
import zipfile, io, xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path

output_path = Path.cwd() / 'outputs' / (str(datetime.now()).replace(':', '-'))
output_path.mkdir()

#%% Function to fetch OASIS data
def fetch_oasis(queryname, start, end, market="DAM", node=None, extra_params=None):
    base_url = "https://oasis.caiso.com/oasisapi/SingleZip"
    params = {
        "queryname": queryname,
        "startdatetime": start,
        "enddatetime": end,
        "market_run_id": market,
        "version": 1
    }
    if node:
        params["node"] = node
    if extra_params:
        params.update(extra_params)

    r = requests.get(base_url, params=params)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    print("Files in ZIP for", queryname, ":", z.namelist())

    # Look for CSV first
    csv_files = [f for f in z.namelist() if f.endswith(".csv")]
    if csv_files:
        return pd.read_csv(z.open(csv_files[0]))
    
    # Otherwise parse XML fallback
    xml_file = z.namelist()[0]
    tree = ET.parse(z.open(xml_file))
    root = tree.getroot()
    ns = {"oasis": "http://www.caiso.com/soa/OASISReport_v1.xsd"}
    rows = []
    for elem in root.findall(".//oasis:REPORT_DATA", ns):
        row = {child.tag.replace(f"{{{ns['oasis']}}}", ""): child.text for child in elem}
        rows.append(row)
    return pd.DataFrame(rows)

#%% Pull contour map from CAISO
url = "https://wwwmobile.caiso.com/Web.Service.Chart/api/v3/ChartService/PriceContourMap1"
resp = requests.get(url)
resp.raise_for_status()
contour = resp.json()

# Get trade date and hour from keys
current_date = contour.get("dd").split('T')[0]  # e.g., "2025-09-13"
current_hour_ending = int(contour.get("dh"))    # e.g., 23

#%% Parse nodes from contour map
nodes = []
for layer in contour.get("l", []):
    for item in layer.get("m", []):
        if item.get("t") == "Node":
            nodes.append({
                "node_id": item.get("n"),
                "lat": item["c"][0] if item.get("c") else None,
                "lon": item["c"][1] if item.get("c") else None,
                "type": item.get("p"),       # LOAD or GEN
                "area": item.get("a"),       # balancing area (e.g., AVA, PGE)
                "price_dp": float(item.get("dp")) if item.get("dp") else None
            })

df_nodes = pd.DataFrame(nodes)

#%% Save contour nodes CSV
safe_current_date = current_date.replace(":", "_")
out_nodes_csv = output_path / f"caiso_nodes_{safe_current_date}_HE{current_hour_ending:02d}.csv"
df_nodes.to_csv(out_nodes_csv, index=False)
print(f"Saved {len(df_nodes)} contour nodes → {out_nodes_csv}")

#%% Get tomorrow's day-ahead start/end for OASIS (GMT)
today = date.today()
tomorrow = str(today + timedelta(days=1)).replace('-', '')
day_after = str(today + timedelta(days=2)).replace('-', '')

today = str(today).replace('-', '')
# Fetch OASIS datasets
df_lmp = fetch_oasis(
    "PRC_LMP",
    f"{today}T07:00-0000",
    f"{tomorrow}T07:00-0000",
    market="DAM",
    extra_params={"grp_type": "ALL_APNODES"}
)

"""df_load = fetch_oasis(
    "SLD_FCST",
    f"{tomorrow}T07:00-0000",
    f"{day_after}T07:00-0000",
    market="DAM"
)

df_ren = fetch_oasis(
    "SLD_REN_FCST",
    f"{tomorrow}T07:00-0000",
    f"{day_after}T07:00-0000",
    market="DAM"
)
"""
#%% Save OASIS CSVs with date/hour stamp from contour map
"""
out_lmp_csv = output_path / f"oasis_lmp_{today}_full.csv"
out_load_csv = output_path / f"oasis_load_{tomorrow}_full.csv"
out_ren_csv = output_path / f"oasis_renewables_{tomorrow}_full.csv"

df_lmp.to_csv(out_lmp_csv, index=False)

print("Saved OASIS files:")
print(" LMPs →", out_lmp_csv)
"""

# Convert contour local hour to GMT
print(f'current date: {current_date}')
"""contour_current_local_dt = datetime.strptime(current_date_only, "%Y-%m-%d") + timedelta(days=1)
print(f'contour dt local: {contour_current_local_dt}')"""

contour_gmt_dt = datetime.strptime(current_date, "%Y-%m-%d") + timedelta(hours=current_hour_ending) + timedelta(hours=7)  # PDT → GMT

# Remove tz info and filter
df_lmp["INTERVAL_END_GMT"] = pd.to_datetime(df_lmp["INTERVAL_END_GMT"]).dt.tz_localize(None)
df_lmp_hour = df_lmp[df_lmp["INTERVAL_END_GMT"] == contour_gmt_dt]
print(df_lmp["INTERVAL_END_GMT"].iloc[0])

print("Converted GMT:", contour_gmt_dt)
print(f"Filtered LMPs: {len(df_lmp_hour)} rows")
df_lmp_hour.to_csv(output_path / f'filtered_oasis_lmp_{safe_current_date}_HE{current_hour_ending:02d}.csv')