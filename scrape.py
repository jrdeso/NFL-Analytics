import requests
import pandas as pd
import datetime
from log_helper import NFL_Logging
import os
import inspect

log = NFL_Logging()

def get_players():
    log.label_log(os.path.basename(__file__), inspect.currentframe().f_code.co_name)

    players_url = "https://api.sleeper.app/v1/players/nfl"
    
    try:
        response = requests.get(players_url)
        response.raise_for_status()  # Will raise an HTTPError for bad responses
        players_data = response.json()
        players_df = pd.DataFrame.from_dict(players_data, orient='index')

        # Filter for active players:
        players_df = players_df[(players_df['status'] == 'Active')]

        # Filter DF columns
        filtered_columns = [
            'player_id',
            'first_name',
            'last_name',
            'team',
            'age',
            'position',
            'depth_chart_order',
            'injury_status',
            'injury_body_part',
            'injury_notes'
        ]
        players_df = players_df[filtered_columns]

        # Filter for skill positions and filter out Inactive players
        positions_to_include = ['QB', 'RB', 'TE', 'WR']
        players_df = players_df[
            (players_df['position'].isin(positions_to_include)) & 
            (players_df['team'].notna())
        ]

        # # For recording data to excel
        # excel_path = 'Excel Exports/nfl_players.xlsx'
        # players_df.to_excel(excel_path, index=False, engine='openpyxl')

        log.info(f"Successfully scraped players dataframe from Sleeper: {players_url}")
        return players_df
        
    except requests.exceptions.RequestException as e:
        log.critical(f"Failed to retrieve data at {players_url}: {str(e)}")

def main():
    get_players()

main()
