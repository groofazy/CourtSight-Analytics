import pytest 
import pandas as pd
import os
from data_pipeline.extract import extract_all_active_players

def test_extract_returns_df():
    df = extract_all_active_players(season='2024-25')
    assert isinstance(df, pd.DataFrame)
    assert not df.empty

def test_req_cols():
    df = extract_all_active_players(season='2024-25')
    required_cols = ['PLAYER_ID', 'PLAYER_NAME', 'TEAM_ABBREVIATION', 
                   'AGE', 'GP', 'MIN', 
                   'FGM', 'FGA', 'FG_PCT', 
                   'FG3M', 'FG3A', 'FG3_PCT',
                   'FTA', 'FT_PCT', 
                   'OREB', 'DREB', 'REB',
                   'AST', 'TOV', 'STL', 'BLK',
                   'PF', 'PFD', 
                   'PTS']
    for col in required_cols:
        assert col in df.columns

def test_raw_file_saved():
    extract_all_active_players(season='2024-25')
    assert os.path.exists('raw_nba_stats.json')