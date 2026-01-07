# batch extraction
# calling swar_nba_api at the end of each day and updating existing data in our data warehouse

import pandas as pd
from nba_api.stats.endpoints import leaguedashplayerstats
from nba_api.stats.static import teams

def extract_all_active_players(season='2024-25'):
    """
    Fetches stats for ALL active players in one big request
    """
    print(f"Connecting to NBA API for {season} stats...")

    try:
        stats = leaguedashplayerstats.LeagueDashPlayerStats(season=season)
        df = stats.get_data_frames()[0]

        # relevant columns needed for data
        req_columns = ['PLAYER_ID', 'PLAYER_NAME', 'TEAM_ABBREVIATION', 
                   'AGE', 'GP', 'MIN', 
                   'FGM', 'FGA', 'FG_PCT', 
                   'FG3M', 'FG3A', 'FG3_PCT',
                   'FTA', 'FT_PCT', 
                   'OREB', 'DREB', 'REB',
                   'AST', 'TOV', 'STL', 'BLK',
                   'PF', 'PFD', 
                   'PTS']
        raw_data = df[req_columns]

        # save to layer
        raw_data.to_json('raw_nba_stats.json', orient='records')
        print(f"Succesfully extracted {len(raw_data)} players to raw_nba_stats.json")
        return raw_data
    
    except Exception as e:
        print(f"Extraction failed: {e}")
        return None

if __name__ == "__main__":
    extract_all_active_players()
        


