# batch extraction
import pandas as pd
from nba_api.stats.endpoints import leaguedashplayerstats, leaguedashteamstats, leaguehustlestatsplayer, playerdashptshots
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.endpoints import playbyplayv3
from nba_api.stats.endpoints import shotchartdetail
import time


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
        player_basic_stats = leaguedashplayerstats.LeagueDashPlayerStats(season=season)
        player_basic_df = player_basic_stats.get_data_frames()[0]

        return player_basic_df[req_columns]
    
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
        df = basic_team_stats.get_data_frames()[0]

        basic_team_df = df.drop(columns=remove_cols, axis=1, errors='ignore')
        
        return basic_team_df
    
    except Exception as e:
        print(f"Extraction failed: {e}")
        return None

def extract_all_active_players_hustle_stats(season='2024-25'):
    print(f"Connecting to NBA API LeagueHustleStatsPlayer endpoint for {season} player hustle stats...")

    req_columns = ['PLAYER_ID', 'PLAYER_NAME', 'TEAM_ABBREVIATION', 
                   'CONTESTED_SHOTS', 'CONTESTED_SHOTS_2PT', 'CONTESTED_SHOTS_3PT', 
                   'DEFLECTIONS', 'CHARGES_DRAWN',
                   'SCREEN_ASSISTS', 'SCREEN_AST_PTS',
                   'OFF_LOOSE_BALLS_RECOVERED', 'DEF_LOOSE_BALLS_RECOVERED', 'LOOSE_BALLS_RECOVERED',
                   'OFF_BOXOUTS', 'DEF_BOXOUTS', 'BOX_OUTS'
                   ]

    try:
        player_hustle_stats = leaguehustlestatsplayer.LeagueHustleStatsPlayer(season='2024-25')
        player_hustle_df = player_hustle_stats.get_data_frames()[0]

        return player_hustle_df[req_columns]
    
    except Exception as e:
        print(f"Extraction failed: {e}")
        return None
    

# MAYBE INCLUDE PLAYERDASHPTREB FOR REBOUNDING DATA (LOCATIONS, CONT/UNCONT, ETC)

def extract_rotation_players_general_shooting(target_id, season='2024-25'):
    print(f"Connecting to NBA API PlayerDashPTShots endpoint for {season} player shot data...")

    try:
        rotation_players_general_shooting = playerdashptshots.PlayerDashPtShots(season='2024-25')
        rotation_players_general_shooting_df = rotation_players_general_shooting.get_data_frames[0]

        return rotation_players_general_shooting_df
    except Exception as e:
        print(f"Extraction failed: {e}")
        return None

def rotation_players_df():
    all_players_df = extract_all_active_players_basic()

    rotation_players = all_players_df[all_players_df['MIN'] >= 10]

    return rotation_players
    
# NEXUS ENDPOINTS
def extract_past_game_ids(season='2024-25', n_games=200):
    print(f"Fetching game IDs for {season}...")
    try:
        gamefinder = leaguegamefinder.LeagueGameFinder(
            season_nullable=season,
            league_id_nullable='00',
            season_type_nullable='Regular Season'
        )
        games_df = gamefinder.get_data_frames()[0]
        game_ids = games_df['GAME_ID'].unique()[:n_games]
        print(f"Found {len(game_ids)} game IDs")
        return list(game_ids), games_df
    except Exception as e:
        print(f"Failed to fetch game IDs: {e}")
        return [], None

def extract_play_by_play(game_id: str):
    try:
        time.sleep(1.0)
        pbp = playbyplayv3.PlayByPlayV3(
            game_id=game_id,
            start_period=1,
            end_period=4,
            timeout=60
        )
        df = pbp.get_data_frames()[0]
        if df.empty:
            return None
        return df
    except Exception as e:
        print(f"Failed PBP for {game_id}: {e}")
        return None 

def extract_shot_chart(game_id: str, team_id: int):
    try:
        time.sleep(0.6)
        shot_chart = shotchartdetail.ShotChartDetail(
            team_id=team_id,
            player_id=0,
            game_id_nullable=game_id,
            context_measure_simple="FGA"
        )
        df = shot_chart.get_data_frames()[0]
        return df
    except Exception as e:
        print(f"Failed shot chart for {game_id}: {e}")
        return None


if __name__ == "__main__":
    extract_all_active_players_basic()
    extract_all_active_teams_basic()


        


