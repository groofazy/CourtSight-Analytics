# batch extraction
import pandas as pd
import requests
from nba_api.stats.endpoints import leaguedashplayerstats
from nba_api.stats.endpoints import leaguedashteamstats

# calling swar_nba_api at the end of each day and updating existing data in our data warehouse

def extract_all_active_players_basic(season='2024-25'):
    """
    Fetches stats for ALL active players in one big request
    """
    print(f"Connecting to NBA API LeagueDashPlayerStats endpoint for {season} player basic stats...")

    req_columns = ['PLAYER_ID', 'PLAYER_NAME', 'TEAM_ABBREVIATION', 
                   'AGE', 'GP', 'MIN', 
                   'FGM', 'FGA', 'FG_PCT', 
                   'FG3M', 'FG3A', 'FG3_PCT',
                   'FTA', 'FT_PCT', 
                   'OREB', 'DREB', 'REB',
                   'AST', 'TOV', 'STL', 'BLK',
                   'PF', 'PFD', 
                   'PTS']
    try:
        basic_player_stats = leaguedashplayerstats.LeagueDashPlayerStats(season=season)
        basic_player_df = basic_player_stats.get_data_frames()[0]

        return basic_player_df[req_columns]
    
    except Exception as e:
        print(f"Extraction failed: {e}")
        return None

def extract_all_active_teams_basic(season='2024-25'):
    print(f"Connecting to NBA API LeagueDashTeamStats endpoint for {season} team basic stats...")

    # KEEP SAME LOGIC AS PREVIOUS FUNCTION, NO DROP
    remove_cols = ['GP', 'MIN', 'GP_RANK', 'W_RANK', 'L_RANK', 
                   'MIN_RANK', 'FGM_RANK', 'FG_PCT_RANK', 'FG3M_RANK', 
                   'FG3_PCT_RANK', 'FTM_RANK','FTA_RANK', 'FT_PCT_RANK',
                   'OREB_RANK', 'DREB_RANK', 'REB_RANK', 'AST_RANK', 
                   'TOV_RANK', 'STL_RANK', 'BLK_RANK', 'BLKA_RANK',
                   'PF_RANK', 'PFD_RANK', 'PTS_RANK', 'PLUS_MINUS_RANK']
    try:
        basic_team_stats = leaguedashteamstats.LeagueDashTeamStats(season=season)
        df = basic_team_stats.get_data_frames()

        basic_team_df = df.drop(columns=remove_cols, errors='ignore')
        
        return basic_team_df
    
    except Exception as e:
        print(f"Extraction failed: {e}")
        return None


if __name__ == "__main__":
    extract_all_active_players_basic()
    extract_all_active_teams_basic()
        


