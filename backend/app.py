from data_pipeline.extract import extract_past_game_ids, extract_play_by_play, extract_shot_chart
from data_pipeline.transform import get_home_team_won, transform_pbp_to_snapshots, transform_shots
from data_pipeline.load import load_snapshots_to_supabase, load_shots_to_supabase
from supabase import create_client
import os
import joblib
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from dotenv import load_dotenv

load_dotenv()

def run_pipeline(n_games=200, season='2024-25'):
    print(f"Starting pipeline for {n_games} games...")
    game_ids, games_df = extract_past_game_ids(season=season, n_games=n_games)

    for i, game_id in enumerate(game_ids):
        print(f"Processing {i+1}/{len(game_ids)}: {game_id}")

        # play by play for win probability
        pbp_df = extract_play_by_play(game_id)
        if pbp_df is not None and not pbp_df.empty:
            home_won = get_home_team_won(game_id, games_df)
            if home_won != -1:
                snapshots = transform_pbp_to_snapshots(pbp_df, game_id, home_won)
                if snapshots:
                    load_snapshots_to_supabase(snapshots)
                    print(f"Loaded {len(snapshots)} snapshots for {game_id}")

        # shot charts for both teams
        game_rows = games_df[games_df['GAME_ID'] == game_id]
        team_ids = game_rows['TEAM_ID'].unique()
        
        for team_id in team_ids:
            shot_df = extract_shot_chart(game_id, team_id)
            if shot_df is not None and not shot_df.empty:
                shots = transform_shots(shot_df, game_id, team_id)
                if shots:
                    load_shots_to_supabase(shots)
                    print(f"Loaded {len(shots)} shots for team {team_id} in game {game_id}")

def train_model():
    print("Fetching snapshots from Supabase...")
    supabase = create_client(
        os.environ.get("SUPABASE_URL"),
        os.environ.get("SUPABASE_KEY")
    )
    all_data = []
    offset = 0
    batch_size = 1000

    while True:
        response = supabase.table("game_snapshots")\
            .select("*")\
            .range(offset, offset + batch_size - 1)\
            .execute()
        if not response.data:
            break
        all_data.extend(response.data)
        offset += batch_size
        if len(response.data) < batch_size:
            break

    print(f"Fetched {len(all_data)} snapshots")
    df = pd.DataFrame(all_data)  # build from all_data, not response.data

    if df.empty:
        print("No data found — run pipeline first")
        return

    print(f"Training on {len(df)} snapshots...")
    X = df[["score_differential", "seconds_remaining", "is_home"]]
    y = df["home_wins"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    model = LogisticRegression()
    model.fit(X_train, y_train)

    accuracy = accuracy_score(y_test, model.predict(X_test))
    print(f"Accuracy: {accuracy:.3f}")

    os.makedirs("model", exist_ok=True)
    joblib.dump(model, "model/win_probability.pkl")
    print("Model saved to model/win_probability.pkl")
    return accuracy

if __name__ == "__main__":
    # run_pipeline(n_games=200)
    train_model()