import pandas as pd
import os

lmps_path = 'outputs'

subfolders = [
     os.path.join(lmps_path, f) for f in os.listdir(lmps_path)
    if os.path.isdir(os.path.join(lmps_path, f))
]

matches = {} # match each oasis lmp node to a list of potential caiso lmp node matches

iteration = 1;
for subfolder_path in subfolders:
    subfolder_files = [os.path.join(subfolder_path, f) for f in os.listdir(subfolder_path)]
    subfolder_files.sort() # the caiso nodes come first, then the oasis nodes

    caiso_lmp_df =  pd.read_csv(subfolder_files[0])
    oasis_lmp_df =  pd.read_csv(subfolder_files[1])

    # filter the oasis lmp df for the total lmp, not the sub-lmps
    oasis_lmp_df = oasis_lmp_df[oasis_lmp_df['DATA_ITEM'] == 'LMP_PRC']

    # filter the caiso LMPs just for the ones in caiso and also filter out aggregate nodes
    caiso_lmp_df = caiso_lmp_df[caiso_lmp_df['area'] == 'CA']
    oasis_lmp_df = oasis_lmp_df[~oasis_lmp_df['RESOURCE_NAME'].str.contains('TOT_GEN')]


    print(f'length caiso lmp df: {len(caiso_lmp_df)}')
    print(f'length oasis lmp df: {len(oasis_lmp_df)}')

    """# merge on the LMP price ('price_dp' for the caiso df, 'VALUE' for the oasis df), with a specified tolerance
    tolerance = 0.001

    merged_df = oasis_lmp_df.assign(key=1).merge(
    caiso_lmp_df.assign(key=1), on="key"
    ).drop("key", axis=1)

    merged_df = merged_df[abs(merged_df["price_dp"] - merged_df["VALUE"]) <= tolerance]"""

    # merge on the LMP price ('price_dp' for the caiso df, 'VALUE' for the oasis df)
    merged_df = pd.merge(oasis_lmp_df, caiso_lmp_df, left_on='VALUE', right_on='price_dp', how='left')

    print(merged_df.iloc[0])

    # Group by the oasis node to begin matching
    grouped_by_oasis_node = merged_df.groupby(by='RESOURCE_NAME')

    for oasis_node, group in grouped_by_oasis_node:
        matching_caiso_nodes = list(group['node_id'].dropna())
        if iteration == 1:
             matches[oasis_node] = matching_caiso_nodes
        else:
            previous_matches = matches.get(oasis_node, [])
            """if len(previous_matches) == 0:
                matches[oasis_node] = matching_caiso_nodes
            else:"""
            overlapping_matches = [caiso_node for caiso_node in previous_matches if caiso_node in matching_caiso_nodes]
            matches[oasis_node] = overlapping_matches

    nonempty_matches = {k: v for k, v in matches.items() if len(v) > 0}
    print(f'number of matched oasis nodes (nonempty): {len(nonempty_matches)}')

    # Create a lookup dict from CAISO node_id to (lat, lon)
    node_to_latlon = (
        caiso_lmp_df[['node_id', 'lat', 'lon']]
        .dropna()
        .drop_duplicates('node_id')
        .set_index('node_id')
        .T
        .to_dict('list')  # maps node_id -> [lat, lon]
    )

    # Build the output dataframe with lat/lon lists added
    output_rows = []
    for oasis, caiso_list in matches.items():
        if len(caiso_list) > 0:
            latlon_list = [node_to_latlon.get(node_id) for node_id in caiso_list if node_id in node_to_latlon]
            output_rows.append((oasis, caiso_list, latlon_list))

    output_df = pd.DataFrame(output_rows, columns=["oasis_node", "caiso_matches", "lat/lon"])

    output_df.to_csv(f"matched_nodes_iteration_{iteration}.csv", index=False)

    iteration += 1

