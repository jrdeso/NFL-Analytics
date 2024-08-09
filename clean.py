import pandas as pd
import json
from datetime import datetime
from log_helper import NFL_Logging
import os
import inspect

class Clean:
    """
    Class used to clean and organize NFL game data for insertion into a local database.
    """

    def __init__(self):
        """
        Initializes the Clean class.

        Loads configuration settings from 'config.json' to initialize filtering lists, renaming maps, and data type mappings
        used for cleaning and organizing NFL game data.
        """
        self.log = NFL_Logging()
        # Open config.json file to reference defined dataframe structure
        # (Put defined lists/dicts in config.json to declutter files)
        with open('config.json') as f:
            config = json.load(f)

        self.game_filtered_fields = config['game_filtered_fields']              # To filter desired game fields from API
        self.home_team_filtered_fields = config['home_team_filtered_fields']    # To filter desired game fields from API
        self.home_team_renamed_fields = config['home_team_renamed_fields']      # To rename fields filtered from API
        self.away_team_filtered_fields = config['away_team_filtered_fields']    # To filter desired game fields from API
        self.away_team_renamed_fields = config['away_team_renamed_fields']      # To rename fields filtered from API
        self.player_data_cols = config['player_data_cols']                      # To identify fields related to player and their stats from a game
        self.player_stat_columns = config['player_stat_columns']                # To identify fields related particularly to player stats
        
        # Maps from extracted/cleaned table dataframe to corresponding columns in SQL Table
        self.players_df_to_player_table_map = config['players_df_to_player_table_map']
        self.game_data_df_to_game_table_map = config['game_data_df_to_game_table_map']   
        self.team_game_df_to_team_game_table_map = config['team_game_df_to_team_game_table_map']   


        # Maps dataframe types to correct SQL datatypes for each table
        self.players_df_to_player_table_datatypes = config['players_df_to_player_table_datatypes']
        self.game_data_df_to_game_table_datatypes = config['game_data_df_to_game_table_datatypes']
        self.team_game_df_to_team_game_table_datatypes = config['team_game_df_to_team_game_table_datatypes']   


    def organize_game_info_df(self, game_info_df):
        """
        Splits a game DataFrame into four separate DataFrames: general game info, home team info, away team info, and player info.

        Parameters
        ----------
        game_info_df : DataFrame
            The DataFrame containing raw game data.

        Returns
        -------
        tuple of DataFrames
            A tuple containing four DataFrames: game_data_df, home_team_data_df, away_team_data_df, and players_stats_df.
        """
        self.log.label_log(os.path.basename(__file__), inspect.currentframe().f_code.co_name)

        # Extract general game info 
        game_data_df = game_info_df[self.game_filtered_fields].copy()
        # Extract home team data from game > Update names to clean up dataframe
        home_team_data_df = game_info_df[self.home_team_filtered_fields].copy()
        home_team_data_df.rename(columns=self.home_team_renamed_fields, inplace=True)
        home_team_data_df['homeOrAway'] = 'Home'
        # Extract away team data from game > Update names to clean up dataframe
        away_team_data_df = game_info_df[self.away_team_filtered_fields].copy()
        away_team_data_df.rename(columns=self.away_team_renamed_fields, inplace=True)
        away_team_data_df['homeOrAway'] = 'Away'

        # Extract home team and away team IDs, we assign these to the players game dataframe (helps keep track of who they were playing when they got these stats)
        home_team_id = game_data_df.at[0,'teamIDHome']
        away_team_id = game_data_df.at[0,'teamIDAway']

        # Make dataframe to hold all player stats from game param:
        players_stats_df = pd.DataFrame(columns=self.player_data_cols)

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
        # Create a boolean mask for rows where all specified columns are empty or NaN
        mask = players_stats_df[self.player_stat_columns].isna().all(axis=1) | (players_stats_df[self.player_stat_columns] == '').all(axis=1)
        # Drop the rows where the mask is True
        players_stats_df = players_stats_df[~mask]

        return game_data_df, home_team_data_df, away_team_data_df, players_stats_df
    

    def clean_players(self, players_df):
        """
        Cleans the players DataFrame (NFL Players, no stats included) by renaming columns and converting data types.

        Parameters
        ----------
        players_df : DataFrame
            The DataFrame containing player data to be cleaned.

        Returns
        -------
        DataFrame
            The cleaned DataFrame ready for database insertion.
        """
        self.log.label_log(os.path.basename(__file__), inspect.currentframe().f_code.co_name)

        # Rename columns for SQL Player table
        try:
            players_df.rename(columns=self.players_df_to_player_table_map, inplace=True)
            self.log.info("Renamed player_df columns successfully.")
        except Exception as e:
            self.log.critical(f"Error renaming player_df columns: {e}")

        # Remap data types for SQL Player table. 
        players_df = self.convert_column_types(players_df, self.players_df_to_player_table_datatypes)

        self.log.info("Successfully cleaned players_df to load into database. ")
        return players_df


    def clean_game(self, game_data_df):
        """
        Cleans the game DataFrame (general game info) by renaming columns and converting data types. Then wrangle some
        additonal information (ID of winning team, what season game is from, identify if game is a primetime game.)

        Parameters
        ----------
        game_data_df : DataFrame
            The DataFrame containing game data to be cleaned.

        Returns
        -------
        DataFrame
            The cleaned DataFrame ready for database insertion.
        """
        self.log.label_log(os.path.basename(__file__), inspect.currentframe().f_code.co_name)

        # Rename columns for SQL Game table
        try:
            game_data_df.rename(columns=self.game_data_df_to_game_table_map, inplace=True)
            self.log.info("Renamed game_data_df columns successfully.")
        except Exception as e:
            self.log.critical(f"Error renaming game_data_df columns: {e}")

        # Remap data types for SQL Game table. 
        game_data_df = self.convert_column_types(game_data_df, self.game_data_df_to_game_table_datatypes)

        # Wrangle winning team ID > store ID of team who won and add to dataframe (if tie, enter None)
        game_data_df['WINNING_TEAM_ID'] = game_data_df.apply(
            lambda row: row['HOME_TEAM_ID'] if row['HOME_POINTS'] > row['AWAY_POINTS'] else (
                row['AWAY_TEAM_ID'] if row['AWAY_POINTS'] > row['HOME_POINTS'] else None), 
            axis=1
        )

        # Format GAME_DATE to desired format (YYYYMMDD -> MM-DD-YYYY)
        game_data_df['GAME_DATE'] = self.format_date(game_data_df['GAME_DATE'].iloc[0])

        # Assign SEASON_ID into dataframe (pull from date of game). Season ID's are the starting year of season (e.g., 23-24 > 2023)
        game_data_df['SEASON_ID'] = game_data_df['GAME_DATE'].iloc[0][-4:] # pull last four digits of date (YYYY)

        # Record if a game is a 'primetime' game. Defining as starting at 8PM or later. 
        game_data_df['PRIMETIME'] = self.check_if_primetime(game_data_df['GAME_TIME'].iloc[0])

        self.log.info("Successfully cleaned game_data_df to load into database. ")
        return game_data_df
    

    def clean_team_game(self, team_game_df):
        """
        Cleans the team game DataFrame (team stats from particular game) by renaming columns and converting data types. Then wrangle fantasy 
        points allowed by defense on various platforms. 

        Parameters
        ----------
        game_data_df : DataFrame
            The DataFrame containing game data to be cleaned.

        Returns
        -------
        DataFrame
            The cleaned DataFrame ready for database insertion.
        """
        self.log.label_log(os.path.basename(__file__), inspect.currentframe().f_code.co_name)

        # Rename columns for SQL Team Game Stats table
        try:
            team_game_df.rename(columns=self.team_game_df_to_team_game_table_map, inplace=True)
            self.log.info("Renamed team_game_df columns successfully.")
        except Exception as e:
            self.log.critical(f"Error renaming team_game_df columns: {e}")

        # Remap data types for SQL Player table. 
        team_game_df = self.convert_column_types(team_game_df, self.team_game_df_to_team_game_table_datatypes)

        print(team_game_df.head())
        print(team_game_df.dtypes)



    def check_if_primetime(self, time):
        """
        Determines if a given time is 8:00 PM or later and can be considered 'primetime'.

        Parameters:
        time (str):     A string representing the time in the format 'H:MMa' or 'H:MMp',
                        where 'a' denotes AM and 'p' denotes PM.

        Returns:
        str: 'Yes' if the time is 8:00 PM or later, 'No' otherwise.

        Notes:
        - The input time string is expected to be in 12-hour format with a single 'a' or 'p' 
        to denote AM or PM respectively.
        - The function appends 'M' to the 'a' or 'p' to match the expected '%I:%M%p' format
        for datetime parsing.
        """
        print(time)
        # Add 'M' to the end of 'a' or 'p' to form 'AM' or 'PM'
        if time.endswith('a') or time.endswith('p'):
            time = time[:-1] + ('AM' if time.endswith('a') else 'PM')
        # get time object
        time_obj = datetime.strptime(time, '%I:%M%p')
        # Defining threshold of time of a primetime game as 8PM or later. 
        primetime_threshold = datetime.strptime('8:00PM', '%I:%M%p')

        # Check if time is considered 'primetime'
        if time_obj >= primetime_threshold:
            return 'Yes'
        else:
            return 'No'


    def convert_column_types(self, df, column_types):
        """
        Converts the data types of specified columns in a DataFrame to match given SQL data types.

        Handles conversion to INTEGER, REAL, and TEXT types, with error handling and logging for each conversion.

        Parameters
        ----------
        df : DataFrame
            The DataFrame containing columns to be converted.
        column_types : dict
            A dictionary mapping DataFrame columns to their respective SQL data types.

        Returns
        -------
        DataFrame
            The DataFrame with columns converted to the specified data types.
        """
        for column, sql_type in column_types.items():
            try:
                if sql_type == "INTEGER":
                    df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0).astype(int)
                elif sql_type == "REAL":
                    df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0.0).astype(float)
                elif sql_type == "TEXT":
                    df[column] = df[column].fillna('').astype(str)
                else:
                    self.log.warning(f"SQL type {sql_type} for column {column} is not handled.")
            except Exception as e:
                self.log.critical(f"Error converting column {column} to {sql_type}: {e}")
        return df


    def format_date(self, date_str):
        """
        Converts a date string from 'YYYYMMDD' format to 'MM-DD-YYYY' format.

        Parameters:
        date_str (str): A string representing a date in 'YYYYMMDD' format.

        Returns:
        str: A string representing the date in 'MM-DD-YYYY' format.

        Example:
        >>> format_date('20220804')
        '08-04-2022'
        """
        # Extract year, month, and day from the input string
        year = date_str[:4]
        month = date_str[4:6]
        day = date_str[6:]

        # Return the date in MM-DD-YYYY format
        return f"{month}-{day}-{year}"
