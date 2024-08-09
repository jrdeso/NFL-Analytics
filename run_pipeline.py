import pandas as pd
from datetime import datetime
from log_helper import NFL_Logging
from scrape import Scrape
from clean import Clean

# Temp saves df for testing purposes (so I don't need to re-run API call over and over)
def save_df(df, filename):
    df.to_csv('Test-DataFrames\\' + filename + '.csv', index=False)

# Load saved df for testing
def load_df(filename):
    df = pd.read_csv('Test-DataFrames\\' + filename + '.csv', low_memory=False)
    return df




cleaner = Clean()



def run_pipeline(year=None):
    if year is None:
        year = datetime.now().year
        year = 2022 # Temp for debugging

    print(f"Running pipeline for the year: {year}")

    scraper = Scrape()
    cleaner = Clean()

    """ Scrape Players"""
    # players_df = scraper.scrape_players()
    # save_df(players_df, 'players_df')
    players_df = load_df('players_df')
    players_df = cleaner.clean_players(players_df)
    # print(players_df.head())
    # print(players_df.dtypes)

    # """ Scrape 2022 Schedule """
    # # nfl_schedule_2022 = scraper.scrape_nfl_schedule(2022)
    # # save_df(nfl_schedule_2022, 'nfl_schedule_2022')
    nfl_schedule_2022 = load_df('nfl_schedule_2022')
    games_list = nfl_schedule_2022['gameID'].tolist()

    # # Put all season's game IDs in a list


    # """ Scrape game info for 2022 Games """
    # # Note : SHOULD NOT STORE ALL GAMES INTO ONE DATAFRAME. 
    # # Do to the nature of the API every player gets set of their own fieldnames for each of their stats. 
    # # Combining all players games for a whole season gets messy real quick.
    # # E.g., ended up with a final dataframe with 47,000+ columns. 
    # ## >> Process each game individually, game data, team data, player data. 
    # for game in games_list:
    #     # Scrape individual game info
    #     game_info_df = scraper.scrape_game_info(game)

    #     if game_info_df is not None:
    #         # Organize each game into their separate dataframes
    #         game_data_df, home_team_data_df, away_team_data_df, players_stats_df = cleaner.organize_game_info_df(game_info_df)
            
    #         # Scrape game time into game_data_df (originally not included)
    #         game_data_df['gameTime'] = scraper.scrape_game_time(game_data_df['gameID'].iloc[0])

    #         # Clean game data for SQL load
    #         game_data_df = cleaner.clean_game(game_data_df)

    
            
    #         save_df(game_data_df, 'game_data_df')
    #         save_df(home_team_data_df, 'home_team_data_df')
    #         save_df(away_team_data_df, 'away_team_data_df')
    #         save_df(players_stats_df, 'players_stats_df')
    #     break

    home_team_data_df = load_df('home_team_data_df')
    cleaner.clean_team_game(home_team_data_df)

    # game_data_df = load_df('game_data_df')
    # game_data_df['gameTime'] = scraper.scrape_game_time(game_data_df['gameID'].iloc[0])
    # game_data_df = cleaner.clean_game(game_data_df)


    



run_pipeline()

# pipeline = Pipeline()
# pipeline.extract()

# rows, cols = pipeline.games_info_2022_games.shape

# print(f'Rows: {rows}, Columns: {cols}')