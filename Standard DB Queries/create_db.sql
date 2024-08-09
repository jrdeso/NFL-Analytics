CREATE TABLE Team (
    TEAM_ID INTEGER PRIMARY KEY,
    CITY TEXT CHECK(LENGTH(CITY) <= 15),
    NICKNAME TEXT CHECK(LENGTH(NICKNAME) <= 15),
    ABBREVIATION TEXT CHECK(LENGTH(ABBREVIATION) <= 3),
    STADIUM_TYPE TEXT, -- [Dome, Retractable, Outdoor] 
    STATE TEXT CHECK(LENGTH(STATE) <= 2),
    DIVISION TEXT CHECK(LENGTH(STATE) <= 8)
);

CREATE TABLE Player (
    PLAYER_ID INTEGER PRIMARY KEY,
    FULL_NAME TEXT CHECK(LENGTH(FULL_NAME) <= 65),
    POSITION TEXT CHECK(LENGTH(POSITION) <= 2),
    TEAM_ABBR TEXT CHECK(LENGTH(TEAM_ABBR) <= 3),
    TEAM_ID INTEGER,
    HEIGHT TEXT,
    WEIGHT INTEGER,
    AGE INTEGER,
    EXPERIENCE TEXT,
    COLLEGE TEXT,
    JERSEY_NUMBER INTEGER,
    INJURY_DESIGNATION TEXT,
    INJURY_DATE TEXT,
    INJURY_DESCRIPTION TEXT,
    FOREIGN KEY (TEAM_ABBR) REFERENCES Team(ABBREVIATION)
    FOREIGN KEY (TEAM_ID) REFERENCES TEAM(TEAM_ID)
);

CREATE TABLE Game (
    GAME_ID TEXT PRIMARY KEY,
    GAME_DATE TEXT,
    GAME_TIME TEXT,
    GAME_TYPE TEXT,
    HOME_TEAM TEXT CHECK(LENGTH(HOME_TEAM) <= 3),
    HOME_TEAM_ID INTEGER,
    HOME_POINTS INTEGER,
    HOME_RESULT TEXT CHECK(LENGTH(HOME_RESULT) <= 1),
    AWAY_TEAM TEXT CHECK(LENGTH(AWAY_TEAM) <= 3),
    AWAY_TEAM_ID INTEGER,
    AWAY_POINTS INTEGER,
    AWAY_RESULT TEXT CHECK(LENGTH(AWAY_RESULT) <= 1),

    WINNING_TEAM_ID INTEGER,
    SEASON_ID TEXT,
     
    PRIMETIME TEXT, 
    
    FOREIGN KEY (HOME_TEAM_ID) REFERENCES Team(TEAM_ID),
    FOREIGN KEY (AWAY_TEAM_ID) REFERENCES Team(TEAM_ID),
    FOREIGN KEY (WINNING_TEAM_ID) REFERENCES Team(TEAM_ID)
);

CREATE TABLE Player_Game_Stats (
    PLAYER_GAME_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    PLAYER_ID INTEGER,
    GAME_ID TEXT,
    PLAYER_NAME TEXT,
    TEAM TEXT CHECK(LENGTH(TEAM) <= 3),
    TEAM_ID INTEGER,
    TEAM_ID_PLAYED_AGAINST INTEGER,
    HOME_OR_AWAY TEXT CHECK(LENGTH(HOME_OR_AWAY) <= 4),
    PASSING_INTERCEPTIONS INTEGER,
    PASSING_ATTEMPTS INTEGER,
    PASSING_AVG REAL,
    PASSING_COMPLETIONS INTEGER,
    PASSING_TOUCHDOWNS INTEGER,
    PASSING_QBR REAL,
    PASSING_RTG REAL,
    PASSING_SACKED TEXT,
    RECEIVING_LONG_REC INTEGER,
    RECEIVING_REC_AVG REAL,
    RECEIVING_REC_TOUCHDOWNS INTEGER,
    RECEIVING_REC_YARDS INTEGER,
    RECEIVING_RECEPTIONS INTEGER,
    RECEIVING_TARGETS INTEGER,
    RECEIVING_REC_TWO_PT_CNVR INTEGER,
    RUSHING_CARRIES INTEGER,
    RUSHING_LONG_RUSH INTEGER,
    RUSHING_RUSH_AVG REAL,
    RUSHING_RUSH_TOUCHDOWNS INTEGER,
    RUSHING_RUSH_YARDS INTEGER,
    RUSHING_RUSH_TWO_PT_CNVR INTEGER,

    HOME_LEAGUE_PTS REAL,
    DK_PTS REAL,
    FD_PTS REAL,

    FOREIGN KEY (PLAYER_ID) REFERENCES Player(PLAYER_ID),
    FOREIGN KEY (GAME_ID) REFERENCES Game(GAME_ID),
    FOREIGN KEY (TEAM_ID) REFERENCES Team(TEAM_ID),
    FOREIGN KEY (TEAM_ID_PLAYED_AGAINST) REFERENCES Team(TEAM_ID)
);

CREATE TABLE Team_Game_Stats (
    TEAM_GAME_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    GAME_ID TEXT,
    GAME_DATE TEXT,
    GAME_TYPE TEXT,
    TEAM_ABBR TEXT CHECK(LENGTH(TEAM_ABBR) <= 3),
    TEAM_ID INTEGER,
    VERSUS_TEAM TEXT CHECK(LENGTH(VERSUS_TEAM) <= 3),
    VERSUS_TEAM_ID INTEGER,
    HOME_OR_AWAY TEXT CHECK(LENGTH(HOME_OR_AWAY) <= 4),
    DEF_INTERCEPTIONS INTEGER,
    FUMBLES_RECOVERED INTEGER,
    DEF_OR_ST_TOUCHDOWNS INTEGER,
    DEF_TOUCHDOWN INTEGER,
    OFF_FIRST_DOWNS INTEGER,
    OFF_FIRST_DOWNS_FROM_PEN INTEGER,
    OFF_FOURTH_DOWN_EFFICIENCY TEXT,
    FUMBLES_LOST INTEGER,
    INTERCEPTIONS_THROWN INTEGER,
    OFF_PASS_COMPLETIONS_AND_ATTEMPTS TEXT,
    OFF_PASSING_FIRST_DOWNS INTEGER,
    OFF_PASSING_YARDS INTEGER,
    PENALTIES TEXT,
    POSSESSION_TIME TEXT,
    OFF_RED_ZONE_SCORED_AND_ATTEMPTED TEXT,
    OFF_RUSHING_ATTEMPTS INTEGER,
    OFF_RUSHING_FIRST_DOWNS INTEGER,
    OFF_RUSHING_YARDS INTEGER,
    SACKS_TAKEN_AND_YARDS_LOST TEXT,
    SAFETIES INTEGER,
    OFF_THIRD_DOWN_EFFICIENCY TEXT,
    TOTAL_DRIVES INTEGER,
    TOTAL_PLAYS INTEGER,
    TOTAL_YARDS INTEGER,
    TURNOVERS INTEGER,
    YARDS_PER_PASS_PLAY REAL,
    YARDS_PER_PLAY REAL,
    YARDS_PER_RUSH REAL,
    POINTS_ALLOWED INTEGER,
    DEF_SACKS INTEGER,
    DEF_YARDS_ALLOWED INTEGER,

    FRIEND_LEAGUE_QB_PTS_ALLOWED REAL,
    FRIEND_LEAGUE_RB_PTS_ALLOWED REAL,
    FRIEND_LEAGUE_WR_PTS_ALLOWED REAL,
    FRIEND_LEAGUE_TE_PTS_ALLOWED REAL,
    DK_QB_PTS_ALLOWED REAL,
    DK_RB_PTS_ALLOWED REAL,
    DK_WR_PTS_ALLOWED REAL,
    DK_TE_PTS_ALLOWED REAL,
    FD_QB_PTS_ALLOWED REAL,
    FD_RB_PTS_ALLOWED REAL,
    FD_WR_PTS_ALLOWED REAL,
    FD_TE_PTS_ALLOWED REAL,

    FOREIGN KEY (TEAM_ID) REFERENCES Team(TEAM_ID),
    FOREIGN KEY (GAME_ID) REFERENCES Game(GAME_ID),
    FOREIGN KEY (VERSUS_TEAM_ID) REFERENCES Team(TEAM_ID),
);