import requests
import pandas as pd
import datetime
from log_helper import NFL_Logging
import os
import inspect
from dotenv import load_dotenv

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

            self.log.info(f"Successfully scraped players dataframe from: {query}")
            return players_df

        except requests.exceptions.RequestException as e:
            self.log.critical(f"Failed to retrieve data at {query}: {str(e)}")


def main():
    scrape = Scrape()
    df = scrape.scrape_players()
    print(df.head())

main()
