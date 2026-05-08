import pandas as pd

def get_home_team_won(game_id: str, games_df: pd.DataFrame) -> int:
    game_rows = games_df[games_df['GAME_ID'] == game_id]
    if game_rows.empty:
        return -1
    home_row = game_rows[~game_rows['MATCHUP'].str.contains('@')]
    if home_row.empty:
        return -1
    return 1 if home_row.iloc[0]['WL'] == 'W' else 0
def transform_pbp_to_snapshots(pbp_df, game_id: str, home_team_won: int) -> list:
    snapshots = []
    for period in range(1, 5):
        for minute in range(0, 12, 2):
            seconds_remaining_in_period = (12 - minute) * 60
            periods_remaining = max(0, 4 - period)
            total_seconds_remaining = (periods_remaining * 720) + seconds_remaining_in_period

            period_plays = pbp_df[pbp_df['period'] <= period]
            scored_plays = period_plays[
                period_plays['scoreHome'].notna() & 
                period_plays['scoreAway'].notna()
            ]
            if scored_plays.empty:
                continue

            latest = scored_plays.iloc[-1]
            try:
                home_score = int(latest['scoreHome'])
                away_score = int(latest['scoreAway'])
                score_differential = home_score - away_score
            except:
                continue

            snapshots.append({
                'game_id': game_id,
                'period': period,
                'seconds_remaining': total_seconds_remaining,
                'score_differential': score_differential,
                'is_home': 1,
                'home_wins': home_team_won
            })

    return snapshots

def transform_shots(df, game_id: str, team_id: int) -> list:
    shots = []
    for _, row in df.iterrows():
        shots.append({
            "game_id": game_id,
            "team_id": str(team_id),
            "player": row["PLAYER_NAME"],
            "x": int(row["LOC_X"]),
            "y": int(row["LOC_Y"]),
            "made": bool(row["SHOT_MADE_FLAG"]),
            "shot_type": row["SHOT_TYPE"],
            "distance": int(row["SHOT_DISTANCE"]),
            "description": row["ACTION_TYPE"],
        })
    return shots