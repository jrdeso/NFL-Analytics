import requests
import pandas as pd
from bs4 import BeautifulSoup
import os
import inspect
from datetime import datetime
import json
from dotenv import load_dotenv
from log_helper import NFL_Logging


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
            data = response.json().get('body', {})
            temp_df = pd.json_normalize(data)
            game_time = temp_df[game_id+'.gameTime'].iloc[0]

            # return scraped and filtered dataframe
            self.log.info(f"Successfully scraped [{game_id}] start time from: {query}")
            return game_time
        
        except requests.exceptions.RequestException as e:
            self.log.critical(f"Failed to retrieve data at {query}: {str(e)}")


    def scrape_weather_data(self, year=None, week=None):
        self.log.label_log(os.path.basename(__file__), inspect.currentframe().f_code.co_name)
        years_list = [2022, 2023, 2024]
        weeks_dict = {
            "Preseason 1": "preseason-week-1",
            "Preseason 2": "preseason-week-2",
            "Preseason 3": "preseason-week-3",
            "1": "week-1",
            "2": "week-2",
            "3": "week-3",
            "4": "week-4",
            "5": "week-5",
            "6": "week-6",
            "7": "week-7",
            "8": "week-8",
            "9": "week-9",
            "10": "week-10",
            "11": "week-11",
            "12": "week-12",
            "13": "week-13",
            "14": "week-14",
            "15": "week-15",
            "16": "week-16",
            "17": "week-17",
            "18": "week-18",
            "Wild Card": "wild-card",
            "Divisional Round": "divisional-round",
            "Conference Championship": "conference-championship",
            "Super Bowl": "super-bowl"
        }
        # Check if acceptable parameter given
        if year is not None and year not in years_list:
            self.log.critical("Invalid year given for weather scrape. ")
            return
        if week is not None and week not in weeks_dict:
            self.log.critical("Invalid week given for weather scrape. ")
            return 
        
        # filter years_list and weeks_dict to the specific year/week given, if none leave as the entire set of years/weeks 
        if year in years_list:
            years_list = [year]
        if week in weeks_dict:
            weeks_dict = {week: weeks_dict[week]}

        # Initialize an empty list to store the data
        weather_data = []

        # open config.json to get defined dictionary on team names to their abbreviations (need it to format gameID)
        with open('config.json') as f:
            config = json.load(f)
        team_abbr = config['Weather_Table_Mapping']['Team_Abbreviations']     
        
        base_weather_url = "https://www.nflweather.com/week/"

        try:
            for year in years_list:
                for week in weeks_dict:
                    # query weather data for season and week given
                    response = requests.get(base_weather_url + year + '/' + weeks_dict[week]) 

                    soup = BeautifulSoup(response.text, 'html.parser')
                    # Find all game boxes (that hold weather data for the given response)
                    game_boxes = soup.find_all('div', class_='game-box')

                    # Loop through each game box and extract data
                    for game_box in game_boxes:
                        # Extract game date and time
                        date_time = game_box.find('div', class_='game-kickoff-status').get_text(strip=True).strip('Final')
                        
                        # Extract team names 
                        teams = game_box.find_all('div', class_='team-game-box')
                        away_team_name = teams[0].find('span', class_='fw-bold').get_text(strip=True)
                        home_team_name = teams[1].find('span', class_='fw-bold').get_text(strip=True)

                        # Extract weather condition
                        weather_div = game_box.find('div', class_='text-break')
                        weather_condition = weather_div.find('span').get_text(strip=True) if weather_div else 'No data'

                        # Extract wind speed and direction
                        wind_div = game_box.find('div', class_='text-break col-md-2 mb-1 px-1 flex-centered')
                        wind_speed_span = wind_div.find_all('span')
                        wind_speed = "No wind speed data"
                        wind_direction = "No wind direction data"
                        for span in wind_speed_span:
                            text = span.get_text(strip=True)
                            if 'mph' in text:
                                wind_data = text.split('mph')
                                wind_speed = wind_data[0].strip()
                                if '_' in wind_data[1]: # has dir such as NE, NW, SE, SW
                                    wind_direction = wind_data[1][-2:]
                                elif '' == wind_data[1]: # empty > no wind direction data
                                    wind_direction = "No wind direction data"
                                else: # directions are signular e.g., N, E, S, W
                                    wind_direction = wind_data[1][-1]
                                
                        # Check for dome game
                        dome_img = game_box.find('img', class_='game-box-weather-image')
                        dome_game = dome_img['src'] == '/climates/dome.webp' if dome_img else False

                        # Extract a gameID field (this is used as foreign key for other tables)
                        game_id_date = datetime.strptime(date_time[0:8], '%m/%d/%y')
                        game_id_date = game_id_date.strftime("%Y%m%d")
                        game_id = game_id_date + '_' + team_abbr[away_team_name] + '@' + team_abbr[home_team_name]

                        # Append the data to the list
                        weather_data.append({
                            "GAME_ID": game_id,
                            "AWAY_TEAM": away_team_name,
                            "HOME_TEAM": home_team_name,
                            "WEATHER": weather_condition,
                            "WIND_SPEED_MPH": wind_speed,
                            "WIND_DIRECTION": wind_direction,
                            "DOME_GAME": dome_game
                        })
            weather_df = pd.DataFrame(weather_data)
            self.log.info(f"Successfully scraped weather data.")
            return weather_df
        
        except requests.exceptions.RequestException as e:
            self.log.critical(f"Error scraping weather data: {str(e)}")

