import requests
import pandas as pd
import os
import inspect
import json
from dotenv import load_dotenv
from log_helper import NFL_Logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from io import StringIO
import pandas as pd
import json
from datetime import datetime, timedelta
import time


class Scrape:

    def __init__(self):
        """
        Initializes the Scrape class with API credentials and base URL.
        """
        load_dotenv() ## Load .env file
        self.log = NFL_Logging()
        self.api_base_url = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/"
        self.headers = {'x-rapidapi-key': os.getenv('RAPID_API_KEY')}
        self.endpoint = ""
        self.params = ""

        # open json dict defining weather scrape details
        with open('config.json') as f:
            config = json.load(f)
        self.team_data_map = config['Weather_Table_Mapping']

        # Counter to assist in recording API inquirys (to help keep track of limit while pipeline running. )
        self.api_request_count = 0


    def check_api_count(self):
        """
        This function helps prevent overages on API subscription.
        Current plan is 1000 calls/day.
        If we hit 1000 while the pipeline is running, wait until 12:15 AM the next day to continue the process.
        """
        if self.api_request_count < 1000:
            pass
        else:
            # We've hit max API calls for the day > wait to continue script until 12:15 next day
            # get current time
            now = datetime.now()
            # calculate time to wait
            tomorrow = now + timedelta(days=1)
            next_run_time = datetime(year=tomorrow.year, month=tomorrow.month, day=tomorrow.day, hour=0, minute=15) # Resume 12:15AM tomorrow
            # Calculate the seconds to wait
            wait_seconds = (next_run_time - now).total_seconds()
            self.log.warning(f"API limit reached. Waiting {wait_seconds // 3600} hours and {(wait_seconds % 3600) // 60} minutes until 12:15 AM tomorrow.")

            # wait and reset count back to 0 for next day
            time.sleep(wait_seconds)
            self.api_request_count = 0


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
            self.api_request_count += 1 # Increase API Count
            self.check_api_count()  # Check if we've hit max queries for today

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
            self.api_request_count += 1 # Increase API Count
            self.check_api_count()  # Check if we've hit max queries for today

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
            self.api_request_count += 1 # Increase API Count
            self.check_api_count()  # Check if we've hit max queries for today

            data = response.json().get('body', {})
            game_info_df = pd.json_normalize(data)

            # return scraped and filtered dataframe
            self.log.info(f"Successfully scraped [{game}] game information dataframe from: {query}")
            return game_info_df
        
        except requests.exceptions.RequestException as e:
            self.log.critical(f"Failed to retrieve data at {query}: {str(e)}")


    def scrape_game_time(self, game_id):
        """
        Scrapes the start time of a specific NFL game from the NFL API.

        This method sends a GET request to the NFL API to retrieve the start time for the specified game.
        The start time is extracted from the response and returned.

        Args:
            game_id (str): The game ID for which to retrieve the start time.

        Returns:
            str: The start time of the specified NFL game.

        Raises:
            requests.exceptions.RequestException: If the API request fails or there is an error in retrieving data.

        Examples:
            >>> scraper = Scrape()
            >>> game_time = scraper.scrape_game_time("20220804_JAX@LV")
            >>> print(game_time)
            "8:00"
        """
        self.log.label_log(os.path.basename(__file__), inspect.currentframe().f_code.co_name)
        try:
            self.endpoint = "getNFLScoresOnly"
            query = self.api_base_url + self.endpoint
            self.params = {
                "gameID": game_id,
                'topPerformers': "false"
            }
            # get response and convert to pd dataframe
            response = requests.get(query, headers=self.headers, params=self.params)
            self.api_request_count += 1 # Increase API Count
            self.check_api_count()  # Check if we've hit max queries for today
            
            data = response.json().get('body', {})
            temp_df = pd.json_normalize(data)
            game_time = temp_df[game_id+'.gameTime'].iloc[0]

            # Extract the time and period (AM/PM) (it originally returns time with an 'a' or 'p')
            time_str = game_time[:-1].strip()
            period = game_time[-1].lower()

            if time_str.startswith("0:"):
                time_str = "12:" + time_str[2:]  # Convert "0:xx" to "12:xx"

            # Convert period to AM/PM
            if period == 'a':
                period_str = 'AM'
            elif period == 'p':
                period_str = 'PM'

            # Format the time string
            game_time = f"{time_str} {period_str}"
            # return scraped and filtered dataframe
            self.log.info(f"Successfully scraped [{game_id}] start time from: {query}")
            return game_time
        
        except requests.exceptions.RequestException as e:
            self.log.critical(f"Failed to retrieve data at {query}: {str(e)}")


    def scrape_weather_data(self, home_team, game_date, game_id):
        """
        Scrapes weather data from https://www.wunderground.com/ and returns it as a pandas dataframe
        Define the URL from parameters and query using selenium/chrome webdriver. 

        Then scrape HTML returned for the 'daily observation' table holding hourly weather information. 

        Returns:
            pandas.DataFrame: A DataFrame containing information about weather at specific location
        Raises:
            requests.exceptions.RequestException: If there is an error in retrieving data.

        Examples:
            >>> scraper = Scrape()
            >>> df = scraper.scrape_weather_data('LV', '08-04-2022', '20220804_JAX@LV')
            >>> print(df.head(1))
                        GAME_ID      Time Temperature Dew Point Humidity  Wind Wind Speed Wind Gust   Pressure  Precip.                Condition
            0   20220804_JAX@LV  12:53 AM       94 °F     55 °F    27 °%     N     5 °mph    0 °mph  27.54 °in  0.0 °in                     Fair
        """
        self.log.label_log(os.path.basename(__file__), inspect.currentframe().f_code.co_name)
        try:
            # Define parameters for URL query
            state_abbr = self.team_data_map[home_team]['state']
            city = self.team_data_map[home_team]['city']
            formatted_date = datetime.strptime(game_date, '%m-%d-%Y').strftime('%Y-%m-%d')
            url = f"https://www.wunderground.com/history/daily/us/{state_abbr}/{city}/date/{formatted_date}"

            # Define chrome driver and run request (run headless)
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-gpu")  # Disable GPU for headless mode
            chrome_options.add_argument("--no-sandbox")  # Bypass OS security model
            chrome_options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems
            chrome_options.add_argument("window-size=1920x1080")  # Set window size to avoid issues
        
            driver = webdriver.Chrome(options=chrome_options)
            driver.get(url)

            # Scrape for tables, and get second table (daily observations table)
            web_page_tables = WebDriverWait(driver,20).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table")))
            daily_obs_html = web_page_tables[1].get_attribute('outerHTML')
            daily_obs_df = pd.read_html(StringIO(daily_obs_html))[0]
            daily_obs_df.insert(0, 'GAME_ID', game_id)
            self.log.info(f"Successfully scraped weather data for [{game_id}] from: {url}")
            return daily_obs_df
        
        except (requests.exceptions.RequestException, IndexError, AttributeError) as e:
            self.log.critical(f"Failed to retrieve data at {url}: {str(e)}")