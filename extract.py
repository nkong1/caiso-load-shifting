#%%
import requests, zipfile, io, pandas as pd
import xml.etree.ElementTree as ET
from datetime import date, timedelta

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
    print("Files in zip:", z.namelist())


    # Look for CSV first
    csv_files = [f for f in z.namelist() if f.endswith(".csv")]
    if csv_files:
        return pd.read_csv(z.open(csv_files[0]))

    # Otherwise parse XML
    xml_file = z.namelist()[0]
    tree = ET.parse(z.open(xml_file))
    root = tree.getroot()
    print(root)
    rows = []
    ns = {"oasis": "http://www.caiso.com/soa/OASISReport_v1.xsd"}

    for elem in root.findall(".//oasis:REPORT_DATA", ns):
        row = {child.tag.replace(f"{{{ns['oasis']}}}", ""): child.text for child in elem}
        rows.append(row)

    return pd.DataFrame(rows)

# Get tomorrow's date
today = date.today()
tomorrow = str(today + timedelta(days=1)).replace('-', '')
day_after = str(today + timedelta(days=2)).replace('-', '')

df_all_nodes = fetch_oasis(
    "PRC_LMP",
    f"{tomorrow}T07:00-0000",
    f"{day_after}T07:00-0000",  
    market="DAM",
    extra_params={"grp_type": "ALL_APNODES"}
)

df_load = fetch_oasis(
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

#%%
df_all_nodes.to_csv('lmps.csv')
df_load.to_csv('load_forecast.csv')
df_ren.to_csv('renewables_forecast.csv')
# %%
