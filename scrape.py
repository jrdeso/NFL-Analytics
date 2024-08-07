import requests
import pandas as pd
from log_helper import NFL_Logging
import os
import inspect
from dotenv import load_dotenv


class Scrape:

    def __init__(self):
        """
        Initializes the Scrape class with API credentials and base URL.
        """
        load_dotenv() ## Load .env file
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
        """
        Scrapes NFL schedule data for a given year from the NFL API and returns it as a pandas DataFrame.

        This method sends a GET request to the NFL API to retrieve the schedule for all weeks of the specified year. 
        It filters the data to include relevant game details and returns a DataFrame.

        Args:
            year (int): The year for which to retrieve the NFL schedule.

        Returns:
            pandas.DataFrame: A DataFrame containing information about NFL games, 
                              including fields such as game ID, season, season type, 
                              team IDs, team abbreviations, game date, and game time.

        Raises:
            requests.exceptions.RequestException: If the API request fails or there is an error in retrieving data.

        Examples:
            >>> scraper = Scrape()
            >>> df = scraper.scrape_nfl_schedule(2023)
            >>> print(df.head(1))
            gameID season seasonType teamIDHome home teamIDAway away gameDate gameTime
            20220804_JAX@LV 2023 Regular Season 1 JAX 2 LV 20220804 20:00
        """
        self.log.label_log(os.path.basename(__file__), inspect.currentframe().f_code.co_name)
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
        """
        Scrapes detailed game information for a specific game from the NFL API and returns it as a pandas DataFrame.

        This method sends a GET request to the NFL API to retrieve detailed information about a specific game. 
        It returns a DataFrame with the game details.

        Args:
            game (str): The game ID for which to retrieve the information.

        Returns:
            pandas.DataFrame: A DataFrame containing detailed information about the specified game.

        Raises:
            requests.exceptions.RequestException: If the API request fails or there is an error in retrieving data.

        Examples:
            >>> scraper = Scrape()
            >>> df = scraper.scrape_game_info("20220804_JAX@LV")
            >>> print(df.head(1))
            gameID gameDate gameTime home away homeScore awayScore
            20220804_JAX@LV 20220804 20:00 JAX LV 14 20
        """
        self.log.label_log(os.path.basename(__file__), inspect.currentframe().f_code.co_name)
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

