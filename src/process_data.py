import pandas as pd
import json
from datetime import datetime
from datetime import timedelta
from pathlib import Path


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
            columns=["geometry", "type", "area"], errors="ignore"
        )

        if combined_df.empty:
            combined_df = single_hour_df
        else:
            combined_df = pd.merge(
                combined_df, single_hour_df, on=["node_id", "lat", "lon"], how="inner"
            )

        combined_df = combined_df.sort_index(axis=1, ascending=False)

    # Drop rows where all price columns are zero
    price_cols = combined_df.columns.difference(["node_id", "lat", "lon"])
    combined_df = combined_df[(combined_df[price_cols] != 0).any(axis=1)]

    # Save to the intermediate outputs folder
    outpath = Path('data/intermediate_outputs/caiso_dam_lmps.csv') 
    combined_df.to_csv(outpath, index=False)

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
    price_df = dam_lmp_df_hourly.drop(columns=["node_id", "lat", "lon"], errors="ignore")

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
    percent_better_df = pd.concat([dam_lmp_df_hourly[["node_id", "lat", "lon"]], percent_better_df], axis=1)

    # Save to the intermediate outputs folder
    outpath = Path('data/intermediate_outputs/caiso_dam_lmp_scores.csv')  
    percent_better_df.to_csv(outpath, index=False)

    return percent_better_df

def save_scores_json(scores_df):
    """Saves a JSON structured in the following way:
       [{node_id: node_id, node_data: {lat, lon, best_hour, best_score, worst_hour}}, ...]
    """
    nodes = []
    hour_cols = [c for c in scores_df.columns if c not in {"node_id", "lat", "lon"}]

    for _, row in scores_df.iterrows():
        # Extract only the hourly score series
        scores = row[hour_cols]

        # Best = max score (lowest-price hours)
        best_hour = scores.idxmax()
        best_score = scores.max()

        # Worst = min score (highest-price hour, should be 0)
        worst_hour = scores.idxmin()

        node_data = {
            "lat": row["lat"],
            "lon": row["lon"],
            "best_hour": best_hour,
            'best_hour_price': row[best_hour],
            "best_score": float(best_score),
            "worst_hour": worst_hour,
        }
        nodes.append({"node_id": row["node_id"], "node_data": node_data})

    # write to JSON
    out_path = Path("front_end/lmp_scores.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(nodes, f, indent=2)  


def save_prices_json(prices_df):
    """Saves a JSON-friendly list of dicts:
       [
         {
           "time": <datetime string>,
           "records": [
             {"node_id": ..., "lat": ..., "lon": ..., "price": ...},
             ...
           ]
         },
         ...
       ]
    """
    # Identify hourly columns (everything except identifiers)
    hour_cols = [c for c in prices_df.columns if c not in {"node_id", "lat", "lon"}]

    out = []
    for col in hour_cols:
        records = []
        for _, row in prices_df.iterrows():
            records.append({
                "node_id": row["node_id"],
                "lat": row["lat"],
                "lon": row["lon"],
                "price": float(row[col])
            })
        out.append({"time": col, "records": records})

    # write to JSON
    out_path = Path("front_end/lmp_prices.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(out, f, indent=2)  



def run(lmp_dir):

    dam_lmps_hourly = combine_lmps(lmp_dir)
    scored_lmps_hourly = score_lmps(dam_lmps_hourly)
    
    save_prices_json(dam_lmps_hourly)
    save_scores_json(scored_lmps_hourly)




