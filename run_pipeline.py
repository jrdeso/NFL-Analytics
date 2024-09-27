import pandas as pd
from datetime import datetime
from log_helper import NFL_Logging
from scrape import Scrape
from clean import Clean
import sqlite3

# Temp saves df for testing purposes (so I don't need to re-run API call over and over)
def save_df(df, filename):
    df.to_csv('Test-DataFrames\\' + filename + '.csv', index=False)

# Load saved df for testing
def load_local_df(filename):
    df = pd.read_csv('Test-DataFrames\\' + filename + '.csv', low_memory=False)
    return df




def etl_players(conn, cursor, scraper, cleaner, log):
    """ Scrape Players"""
    players_df = scraper.scrape_players()
    players_df = cleaner.clean_players(players_df)
    # save_df(players_df, 'players_df')
    # players_df = load_local_df('players_df')

    # Drop data in Player table > we do this to get rid of past players out of the league, and also changes to existing players
    cursor.execute("DELETE FROM Player")
    conn.commit() # update data deletion
    log.info("Reset Players table in database")

    players_df.to_sql('Player', conn, if_exists='append', index=False)
    log.info("Completed ETL process for players table. ")
    conn.commit() # update players_df upload


def etl_seasons_game_data(conn, cursor, year, scraper, cleaner, log):
    """ Scrape schedule for the given year"""
    schedule = scraper.scrape_nfl_schedule(year)
    # save_df(schedule, 'schedule')
    # schedule = load_local_df('schedule')
    games_list = schedule['gameID'].tolist()
    # with open('2022-games.txt', 'r') as file:
    #     games_list = file.read().splitlines()

    # Drop entries in the tables from current year (removes possible duplicate entries for re-runs in pipeline and allows us to update throughout the season)
    cursor.execute("DELETE FROM Game WHERE GAME_ID IN ({seq})".format(seq=','.join(['?']*len(games_list))), games_list)
    cursor.execute("DELETE FROM Player_Game_Stats WHERE GAME_ID IN ({seq})".format(seq=','.join(['?']*len(games_list))), games_list)
    cursor.execute("DELETE FROM Team_Game_Stats WHERE GAME_ID IN ({seq})".format(seq=','.join(['?']*len(games_list))), games_list)
    cursor.execute("DELETE FROM Weather WHERE GAME_ID IN ({seq})".format(seq=','.join(['?']*len(games_list))), games_list)

    conn.commit()
    log.info("Reset Game, Player_Game_Stats, Team_Game_Stats, and Weather table in database")

    with open('games_list_subset.txt', 'r') as file:
        games_list = file.read().splitlines()

    """ 
    Scrape game info for the year's games 
    - Cycle through each game in list > then clean up the entire games df (this df contains all player/team/stat/etc. data) 
        - Need to break it down separately as it gets super messy (+10,000 lines)
    - Extract into separate general game info, player stats, and home/away team stats
    """
    for game in games_list:
        # Scrape individual game info
        game_info_df = scraper.scrape_game_info(game)

        if game_info_df is not None:
            # Organize each game into their separate dataframes
            game_data_df, home_team_data_df, away_team_data_df, players_stats_df = cleaner.organize_game_info_df(game_info_df)
            
            # Scrape game time into game_data_df (originally not included), and clean game_data_df
            game_data_df['gameTime'] = scraper.scrape_game_time(game_data_df['gameID'].iloc[0])
            game_data_df = cleaner.clean_game(game_data_df)

            # Clean home & away team game stats for SQL load
            home_team_data_df = cleaner.clean_team_game_stats(home_team_data_df)
            away_team_data_df = cleaner.clean_team_game_stats(away_team_data_df)

            # Clean player game stats
            players_stats_df = cleaner.clean_player_game_stats(players_stats_df)

            # Scrape & clean weather data for particular game
            weather_df = scraper.scrape_weather_data(game_data_df['HOME_TEAM'].iloc[0], game_data_df['GAME_DATE'].iloc[0], game_data_df['GAME_ID'].iloc[0])
            weather_df = cleaner.clean_weather_df(weather_df, game_data_df['GAME_TIME'].iloc[0])
            # No weather data was able to get collected
            if not isinstance(weather_df, pd.DataFrame):
                log.critical(f"No weather data could be collected for {game_data_df["GAME_ID"].iloc[0]}")
                weather_flag = False
            else:
                weather_flag = True

            # save_df(game_data_df, 'game_data_df')
            # save_df(home_team_data_df, 'home_team_data_df')
            # save_df(away_team_data_df, 'away_team_data_df')
            # save_df(players_stats_df, 'players_stats_df')
            # save_df(weather_df, 'weather_df')

            # game_data_df = load_local_df('game_data_df')
            # home_team_data_df = load_local_df('home_team_data_df')
            # away_team_data_df = load_local_df('away_team_data_df')
            # players_stats_df = load_local_df('players_stats_df')
            # weather_df = load_local_df('weather_df')

            # load dfs to individual tables
            game_data_df.to_sql('Game', conn, if_exists='append', index=False)
            home_team_data_df.to_sql('Team_Game_Stats', conn, if_exists='append', index=False)
            away_team_data_df.to_sql('Team_Game_Stats', conn, if_exists='append', index=False)
            players_stats_df.to_sql('Player_Game_Stats', conn, if_exists='append', index=False)
            if weather_flag:
                weather_df.to_sql('Weather', conn, if_exists='append', index=False)
            conn.commit()
            log.info(f"Completed ETL process for {game_data_df["GAME_ID"].iloc[0]}")


def run_pipeline(year=None):
    scraper = Scrape()
    cleaner = Clean()
    log = NFL_Logging()
    log.reset_log_file()
    
    conn = sqlite3.connect('nfl_fantasy.db')
    cursor = conn.cursor()



    # If no year specified, run for current year (season)
    if year is None:
        year = datetime.now().year

    print(f"Running pipeline for the year: {year}")
    log.info(f"Running pipeline for the year: {year}")

    etl_players(conn, cursor, scraper, cleaner, log)
    etl_seasons_game_data(conn, cursor, year, scraper, cleaner, log)

    print(f"Completed pipeline for the year: {year}")
    log.info(f"Completed pipeline for the year: {year}\n\t")


    conn.close()


def main():
    nfl_seasons = [2022]

    for year in nfl_seasons:
        run_pipeline(year)



if __name__ == "__main__":
    main()