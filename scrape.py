import requests
import pandas as pd
import datetime
from log_helper import NFL_Logging
import os
import inspect
from dotenv import load_dotenv
import json

log = NFL_Logging()

class Scrape:

    def __init__(self):
        """
        Initializes the Scrape class with API credentials and base URL.
        """
        load_dotenv()
        self.log = NFL_Logging()
        self.api_base_url = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/"
        self.headers = {'x-rapidapi-key': os.getenv('API_KEY')}
        self.endpoint = ""
        self.params = ""


    def scrape_players(self):
        """
        Scrapes player data from the NFL API and returns it as a pandas DataFrame.

        This method sends a GET request to the NFL API to retrieve a list of players. 
        It then filters the data to include only NFL skill players (QB, RB, TE, WR) 
        and returns a DataFrame with relevant player details.

        Returns:
            pandas.DataFrame: A DataFrame containing information about NFL skill players, 
                              including fields such as player ID, name, position, team, 
                              height, weight, age, experience, college, jersey number, 
                              and injury details.

        Raises:
            requests.exceptions.RequestException: If the API request fails or there is an error in retrieving data.

        Examples:
            >>> scraper = Scrape()
            >>> df = scraper.scrape_players()
            >>> print(df.head(1))
            playerID espnName pos team teamID height weight age exp school jerseyNum injury.designation injury.injDate injury.description
            12345 John Doe QB NE 1 6'4" 230 28 5 CollegeA 12 Questionable 20230810 Shoulder Injury
        """
        self.log.label_log(os.path.basename(__file__), inspect.currentframe().f_code.co_name)

        try: 
            self.endpoint = "getNFLPlayerList"
            query = self.api_base_url + self.endpoint
            # get response and convert to pd dataframe
            response = requests.get(query, headers=self.headers, params=self.params)
            data = response.json().get('body', {})
            players_df = pd.json_normalize(data)

            # Filter dataframe for desired fields and positions (only NFL skill players)
            filtered_fields = [
                'playerID',             # ESPN player ID > Carrying over to DB
                'espnName',             # Name of player
                'pos',                  # Player's position (e.g., QB, RB, WR, TE)
                'team',                 # NFL Team (Abbreviated)
                'teamID',               # NFL Team ID > Carrying over to DB
                'height',               # Player's height
                'weight',               # Player's weight
                'age',                  # Player's age
                'exp',                  # Number of years player has been in league ('R' for Rookies)
                'school',               # Player's college
                'jerseyNum',            # Player's jersey number
                'injury.designation',   # Player's injury designation (e.g., 'Questionable')
                'injury.injDate',       # Date of Player's injury (formatted as '20230810')
                'injury.description',   # Text description of Player's injury
            ]

            skill_positions = ['QB', 'RB', 'TE', 'WR']
            players_df = players_df[filtered_fields]
            players_df = players_df[players_df['pos'].isin(skill_positions)]

            # return scraped and filtered dataframe
            self.log.info(f"Successfully scraped players dataframe from: {query}")
            return players_df

        except requests.exceptions.RequestException as e:
            self.log.critical(f"Failed to retrieve data at {query}: {str(e)}")


    def scrape_nfl_schedule(self, year):
        try:
            self.endpoint = "getNFLGamesForWeek"
            query = self.api_base_url + self.endpoint
            self.params = {
                "week": "all",          # We want all weeks of the year 
                "seasonType": "all",    # get all game types (Preseason, Regular Season, Post Season)
                "season": year,         # Specify what season from year param
            }
            # get response and convert to pd dataframe
            response = requests.get(query, headers=self.headers, params=self.params)
            data = response.json().get('body', {})
            season_games_df = pd.json_normalize(data)

            # Filter dataframe for desired fields
            filtered_fields = [
                'gameID',       # Game ID for API (e.g., 20220804_JAX@LV)
                'season',       # Season Year (e.g., 2022)
                'seasonType',   # Game Type (e.g., Preseason, Regular Season, Postseason)
                'teamIDHome',   # ID of home team
                'home',         # Abbr of home team
                'teamIDAway',   # ID of away team
                'away',         # Abbr of away team
                'gameDate',     # Date of game (formatted as 20220804)
                'gameTime',     # Time of game (EST)
            ]
            season_games_df = season_games_df[filtered_fields]

            # return scraped and filtered dataframe
            self.log.info(f"Successfully scraped {year} season schedule dataframe from: {query}")
            return season_games_df
        
        except requests.exceptions.RequestException as e:
            self.log.critical(f"Failed to retrieve data at {query}: {str(e)}")


    def scrape_game_info(self, game):
        try:
            self.endpoint = "getNFLBoxScore"
            query = self.api_base_url + self.endpoint
            self.params = {
                "gameID": game,
                "playByPlay": "false",
                "fantasyPoints": "false"
            }
            # get response and convert to pd dataframe
            response = requests.get(query, headers=self.headers, params=self.params)
            data = response.json().get('body', {})
            game_info_df = pd.json_normalize(data)

            # return scraped and filtered dataframe
            self.log.info(f"Successfully scraped [{game}] game information dataframe from: {query}")
            return game_info_df
        
        except requests.exceptions.RequestException as e:
            self.log.critical(f"Failed to retrieve data at {query}: {str(e)}")


    def clean_game_info_df(self, game_info_df):
        # Open config.json file to reference defined dataframe structure
        # (Put defined lists/dicts in config.json to declutter scrape.py)
        with open('config.json') as f:
            config = json.load(f)
        game_filtered_fields = config['game_filtered_fields']
        home_team_filtered_fields = config['home_team_filtered_fields']
        home_team_renamed_fields = config['home_team_renamed_fields']
        away_team_filtered_fields = config['away_team_filtered_fields']
        away_team_renamed_fields = config['away_team_renamed_fields']
        player_data_cols = config['player_data_cols']

        # Extract general game info 
        game_data_df = game_info_df[game_filtered_fields].copy()
        # Extract home team data from game > Update names to clean up dataframe
        home_team_data_df = game_info_df[home_team_filtered_fields].copy()
        home_team_data_df.rename(columns=home_team_renamed_fields, inplace=True)
        # Extract away team data from game > Update names to clean up dataframe
        away_team_data_df = game_info_df[away_team_filtered_fields].copy()
        away_team_data_df.rename(columns=away_team_renamed_fields, inplace=True)

        # Extract home team and away team IDs, we assign these to the players game dataframe (helps keep track of who they were playing when they got these stats)
        home_team_id = game_data_df.at[0,'teamIDHome']
        away_team_id = game_data_df.at[0,'teamIDAway']

        # Make dataframe to hold all player stats from game param:
        players_stats_df = pd.DataFrame(columns=player_data_cols)

        # Extract player stats from game_info_df param
        for fieldname in game_info_df:
            # Find a column with player stats in it
            # > disregard any stats related to individual defensive player, kicking, or punting
            if 'playerStats.' in fieldname and not any(substring in fieldname for substring in ['Defense', 'Punting', 'Kicking', 'scoringPlays']):
                # Sample fieldnames that and how they're split:
                # playerStats.3915508.longName          > [playerStats, 3915508, longName]
                # playerStats.2578369.Receiving.targets > [playerStats, 2578369, Receiving, targets]
                parts = fieldname.split('.')
                player_id = parts[1] # playerID is always second index in list
                
                # Extract what stat category we're looking at (depends on size of list)
                if len(parts) == 3:
                    stat_category = parts[2]                  # e.g., longName
                elif len(parts) == 4:
                    stat_category = parts[2] + '.' + parts[3] # e.g., Receiving + . + targets
                
                # If player isn't in our dataframe yet, add them in
                if player_id not in players_stats_df['playerID'].values:
                    new_player = pd.DataFrame({'playerID': [player_id], stat_category: game_info_df[fieldname]})
                    players_stats_df = pd.concat([players_stats_df, new_player], ignore_index=True)

                # Identify stat_value, e.g., category is 'Receiving.targets', value is '8'
                stat_value = game_info_df[fieldname].values[0]

                # When we identify the team the player is on, insert the data on who they're playing against
                if stat_category == 'teamID':
                    if stat_value == home_team_id:
                        players_stats_df.loc[players_stats_df['playerID'] == player_id, 'teamIDPlayedAgainst'] = away_team_id
                        players_stats_df.loc[players_stats_df['playerID'] == player_id, 'homeOrAway'] = 'Home'
                    elif stat_value == away_team_id:
                        players_stats_df.loc[players_stats_df['playerID'] == player_id, 'teamIDPlayedAgainst'] = home_team_id
                        players_stats_df.loc[players_stats_df['playerID'] == player_id, 'homeOrAway'] = 'Away'

                # Insert stat into player's game data
                players_stats_df.loc[players_stats_df['playerID'] == player_id, stat_category] = stat_value

        # Filter out players with 0 stats (i.e., defensive players, kickers, punters, etc.)
        # import list containing player stat columns
        player_stat_columns = config['player_stat_columns']
        # Create a boolean mask for rows where all specified columns are empty or NaN
        mask = players_stats_df[player_stat_columns].isna().all(axis=1) | (players_stats_df[player_stat_columns] == '').all(axis=1)
        # Drop the rows where the mask is True
        players_stats_df = players_stats_df[~mask]


        return game_data_df, home_team_data_df, away_team_data_df, players_stats_df





def main():
    scrape = Scrape()
    df = scrape.scrape_players()
    print(df.head())

main()
