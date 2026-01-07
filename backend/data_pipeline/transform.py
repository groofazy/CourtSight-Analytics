import pandas as pd
import json

def filter_raw_data():
    raw_df = pd.read_json('raw_nba_stats.json', orient='records')

    print(f"Loaded {len(raw_df)} players from raw data.")

    # boolean mask, keep players who play more than 10 minutes a game
    filtered_df = raw_df[raw_df['MIN'] > 10].copy()

    filtered_df['PROD_SCORE'] = (
    (filtered_df['PTS'] + filtered_df['REB'] + filtered_df['AST']) / filtered_df['MIN']
    ).round(2)

    json_df = filtered_df.to_json(orient='records')
    data = json.loads(json_df)
    print(data)

filter_raw_data()