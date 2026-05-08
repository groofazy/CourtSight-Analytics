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

def transform_games(games_df, label: str = '') -> list:
    def split_name(full_name: str):
        parts = str(full_name).rsplit(' ', 1)
        return (parts[0], parts[1]) if len(parts) == 2 else ('', full_name)

    records = []
    for game_id in games_df['GAME_ID'].unique():
        rows = games_df[games_df['GAME_ID'] == game_id]
        home_rows = rows[~rows['MATCHUP'].str.contains('@')]
        away_rows = rows[rows['MATCHUP'].str.contains('@')]
        if home_rows.empty or away_rows.empty:
            continue

        home = home_rows.iloc[0]
        away = away_rows.iloc[0]

        home_city, home_name = split_name(home['TEAM_NAME'])
        away_city, away_name = split_name(away['TEAM_NAME'])

        game_date = str(home.get('GAME_DATE', ''))
        game_time = game_date + 'T00:00:00+00:00' if game_date else None

        records.append({
            'game_id':      str(game_id),
            'game_time':    game_time,
            'status':       3,
            'home_tricode': str(home['TEAM_ABBREVIATION']),
            'home_team_id': int(home['TEAM_ID']),
            'home_city':    home_city,
            'home_name':    home_name,
            'home_score':   int(home.get('PTS') or 0),
            'home_wins':    int(home.get('W') or 0),
            'home_losses':  int(home.get('L') or 0),
            'away_tricode': str(away['TEAM_ABBREVIATION']),
            'away_team_id': int(away['TEAM_ID']),
            'away_city':    away_city,
            'away_name':    away_name,
            'away_score':   int(away.get('PTS') or 0),
            'away_wins':    int(away.get('W') or 0),
            'away_losses':  int(away.get('L') or 0),
            'series_text':  '',
            'series_game':  '',
            'label':        label,
        })
    return records


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