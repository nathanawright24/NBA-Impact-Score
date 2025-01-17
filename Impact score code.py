# -*- coding: utf-8 -*-
"""
Created on Wed Jul 10 16:28:27 2024

@author: NAWri
"""
!pip install --upgrade nba_api
import nba_api as nbaapi
import requests
from bs4 import BeautifulSoup
import pandas as pd
from nba_api.stats.static import players
from nba_api.stats.endpoints import PlayerDashboardByYearOverYear


# Checking available endpoints
import nba_api.stats.endpoints as endpoints
print(dir(endpoints))

#---------------------------------------------------------------------------------------------------------------
#-------------------------------- Importing, Conditioning, and Merging Data ------------------------------------
#---------------------------------------------------------------------------------------------------------------
data_advanced23_24 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/BaskRefAdvanced23-24.csv", encoding='latin1')
data_box23_24 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/BaskRefBox23-24.csv", encoding='latin1')
data_rtg23_24 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/BaskRefNetRtg23-24.csv", encoding='latin1')
data_advanced22_23 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/BaskRefAdvanced22-23.csv", encoding='latin1')
data_box22_23 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/BaskRefBox22-23.csv", encoding='latin1')
data_rtg22_23 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/BaskRefNetRtg22-23.csv", encoding='latin1')
data_advanced21_22 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/BaskRefAdvanced21-22.csv", encoding='latin1')
data_box21_22 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/BaskRefBox21-22.csv", encoding='latin1')
data_rtg21_22 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/BaskRefNetRtg21-22.csv", encoding='latin1')
#----------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------
def filter_tot(df):
    # Identify players with multiple teams
    multiple_teams = df[df.duplicated(subset='Player', keep=False)]
    
    # Keep only "TOT" rows for those players
    tot_rows = multiple_teams[multiple_teams['Tm'] == 'TOT']
    
    # Keep rows for players who played for only one team
    single_team_rows = df.drop_duplicates(subset='Player', keep=False)
    
    # Combine the two sets of rows
    filtered_df = pd.concat([tot_rows, single_team_rows], ignore_index=True)
    
    return filtered_df

data_advanced23_24_filtered = filter_tot(data_advanced23_24)
data_box23_24_filtered = filter_tot(data_box23_24)
data_rtg23_24_filtered = filter_tot(data_rtg23_24)
data_advanced22_23_filtered = filter_tot(data_advanced22_23)
data_box22_23_filtered = filter_tot(data_box22_23)
data_rtg22_23_filtered = filter_tot(data_rtg22_23)
data_advanced21_22_filtered = filter_tot(data_advanced21_22)
data_box21_22_filtered = filter_tot(data_box21_22)
data_rtg21_22_filtered = filter_tot(data_rtg21_22)

#----------------------------------------------------------------------------------------------
def filter_by_games_minutes(df):
    df['MPG'] = df['MP'] / df['G']
    filtered_df = df[(df['G'] >= 65) & (df['MPG'] >= 20)]
    return filtered_df

data_advanced23_24_filtered = filter_by_games_minutes(data_advanced23_24_filtered)
data_rtg23_24_filtered = filter_by_games_minutes(data_rtg23_24_filtered)
data_advanced22_23_filtered = filter_by_games_minutes(data_advanced22_23_filtered)
data_rtg22_23_filtered = filter_by_games_minutes(data_rtg22_23_filtered)
data_advanced21_22_filtered = filter_by_games_minutes(data_advanced21_22_filtered)
data_rtg21_22_filtered = filter_by_games_minutes(data_rtg21_22_filtered)

# Box dataframes already have MPG in them, column just needs to be renamed and filter by GP
# Function to filter box data by renaming MP to MPG and applying filters
def filter_box_data(df):
    df = df.rename(columns={'MP': 'MPG'})
    df = df[(df['G'] >= 65) & (df['MPG'] >= 20)]
    return df

# Apply the filter function to the box dataframes
data_box23_24_filtered = filter_box_data(data_box23_24_filtered)
data_box22_23_filtered = filter_box_data(data_box22_23_filtered)
data_box21_22_filtered = filter_box_data(data_box21_22_filtered)

# Save the filtered DataFrames back to CSV
data_advanced23_24_filtered.to_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/BaskRefAdvanced23-24_filtered.csv", index=False)
data_box23_24_filtered.to_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/BaskRefBox23-24_filtered.csv", index=False)
data_rtg23_24_filtered.to_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/BaskRefNetRtg23-24_filtered.csv", index=False)
data_advanced22_23_filtered.to_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/BaskRefAdvanced22-23_filtered.csv", index=False)
data_box22_23_filtered.to_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/BaskRefBox22-23_filtered.csv", index=False)
data_rtg22_23_filtered.to_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/BaskRefNetRtg22-23_filtered.csv", index=False)
data_advanced21_22_filtered.to_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/BaskRefAdvanced21-22_filtered.csv", index=False)
data_box21_22_filtered.to_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/BaskRefBox21-22_filtered.csv", index=False)
data_rtg21_22_filtered.to_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/BaskRefNetRtg21-22_filtered.csv", index=False)

#---------------------------------------------------------------------------------------------------------------
#-------------------------------- Merging Basketball Reference Data --------------------------------------------
#---------------------------------------------------------------------------------------------------------------
# Merge DataFrames
merged_23_24 = pd.merge(data_box23_24_filtered, data_advanced23_24_filtered, on='Player', how='left')
merged_23_24 = pd.merge(merged_23_24, data_rtg23_24_filtered, on='Player', how='left')

merged_22_23 = pd.merge(data_box22_23_filtered, data_advanced22_23_filtered, on='Player', how='left')
merged_22_23 = pd.merge(merged_22_23, data_rtg22_23_filtered, on='Player', how='left')

merged_21_22 = pd.merge(data_box21_22_filtered, data_advanced21_22_filtered, on='Player', how='left')
merged_21_22 = pd.merge(merged_21_22, data_rtg21_22_filtered, on='Player', how='left')

# Save merged DataFrames to CSV
merged_23_24.to_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/Merged23-24.csv", index=False)
merged_22_23.to_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/Merged22-23.csv", index=False)
merged_21_22.to_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/Merged21-22.csv", index=False)

#----------------------------------------------------------------------------------------------------------------
# Clean up merged dataframes before merging LEBRON and RAPTOR into them
columns_to_keep = [
    'Player', 'Pos_x', 'Age_x', 'Tm_x', 'G_x', 'GS_x', 'MPG_x', 'FG_x', 'FGA_x',
    'FG%_x', '3P_x', '3PA_x', '3P%_x', '2P_x', '2PA_x', '2P%_x', 'eFG%',
    'FT_x', 'FTA_x', 'FT%_x', 'ORB_x', 'DRB_x', 'TRB_x', 'AST_x', 'STL_x',
    'BLK_x', 'TOV_x', 'PF_x', 'PTS_x', 'Player-additional_x', 'PER', 'TS%',
    '3PAr', 'FTr', 'ORB%', 'DRB%', 'TRB%', 'AST%', 'STL%', 'BLK%', 'TOV%',
    'USG%', 'OWS', 'DWS', 'WS', 'WS/48', 'OBPM', 'DBPM', 'BPM', 'VORP',
    'FG_y', 'FGA_y', 'FG%_y', '3P_y', '3PA_y', '3P%_y', '2P_y', '2PA_y',
    '2P%_y', 'FT_y', 'FTA_y', 'FT%_y', 'ORB_y', 'DRB_y', 'TRB_y', 'AST_y',
    'STL_y', 'BLK_y', 'TOV_y', 'PF_y', 'PTS_y', 'NetRtg', 'ORtg', 'DRtg'
]
merged_21_22 = merged_21_22[columns_to_keep]
merged_22_23 = merged_22_23[columns_to_keep]
merged_23_24 = merged_23_24[columns_to_keep]

# Rename Columns
merged_21_22 = merged_21_22.rename(columns={
    'Player_x': 'Player',
    'Pos_x': 'Pos',
    'Age_x': 'Age',
    'Tm_x': 'Tm',
    'G_x': 'G',
    'GS_x': 'GS',
    'MPG_x': 'MPG',
    'FG_x': 'FGM_pergame',
    'FGA_x': 'FGA_pergame',
    'FG%_x': 'FG%_pergame',
    '3P_x': '3PM_pergame',
    '3PA_x': '3PA_pergame',
    '3P%_x': '3P%_pergame',
    '2P_x': '2PM_pergame',
    '2PA_x': '2PA_pergame',
    '2P%_x': '2P%_pergame',
    'eFG%': 'eFG%_pergame',
    'FT_x': 'FT_pergame',
    'FTA_x': 'FTA_pergame',
    'FT%_x': 'FT%_pergame',
    'ORB_x': 'ORB_pergame',
    'DRB_x': 'DRB_pergame',
    'TRB_x': 'TRB_pergame',
    'AST_x': 'AST_pergame',
    'STL_x': 'STL_pergame',
    'BLK_x': 'BLK_pergame',
    'TOV_x': 'TOV_pergame',
    'PF_x': 'PF_pergame',
    'PTS_x': 'PTS_pergame',
    'Player-additional_x': 'Player-additional',
    'FG_y': 'FG_100',
    'FGA_y': 'FGA_100',
    'FG%_y': 'FG%_100',
    '3P_y': '3P_100',
    '3PA_y': '3PA_100',
    '3P%_y': '3P%_100',
    '2P_y': '2P_100',
    '2PA_y': '2PA_100',
    '2P%_y': '2P%_100',
    'FT_y': 'FT_100',
    'FTA_y': 'FTA_100',
    'FT%_y': 'FT%_100',
    'ORB_y': 'ORB_100',
    'DRB_y': 'DRB_100',
    'TRB_y': 'TRB_100',
    'AST_y': 'AST_100',
    'STL_y': 'STL_100',
    'BLK_y': 'BLK_100',
    'TOV_y': 'TOV_100',
    'PF_y': 'PF_100',
    'PTS_y': 'PTS_100',
})

merged_22_23 = merged_22_23.rename(columns={
    'Player_x': 'Player',
    'Pos_x': 'Pos',
    'Age_x': 'Age',
    'Tm_x': 'Tm',
    'G_x': 'G',
    'GS_x': 'GS',
    'MPG_x': 'MPG',
    'FG_x': 'FGM_pergame',
    'FGA_x': 'FGA_pergame',
    'FG%_x': 'FG%_pergame',
    '3P_x': '3PM_pergame',
    '3PA_x': '3PA_pergame',
    '3P%_x': '3P%_pergame',
    '2P_x': '2PM_pergame',
    '2PA_x': '2PA_pergame',
    '2P%_x': '2P%_pergame',
    'eFG%': 'eFG%_pergame',
    'FT_x': 'FT_pergame',
    'FTA_x': 'FTA_pergame',
    'FT%_x': 'FT%_pergame',
    'ORB_x': 'ORB_pergame',
    'DRB_x': 'DRB_pergame',
    'TRB_x': 'TRB_pergame',
    'AST_x': 'AST_pergame',
    'STL_x': 'STL_pergame',
    'BLK_x': 'BLK_pergame',
    'TOV_x': 'TOV_pergame',
    'PF_x': 'PF_pergame',
    'PTS_x': 'PTS_pergame',
    'Player-additional_x': 'Player-additional',
    'FG_y': 'FG_100',
    'FGA_y': 'FGA_100',
    'FG%_y': 'FG%_100',
    '3P_y': '3P_100',
    '3PA_y': '3PA_100',
    '3P%_y': '3P%_100',
    '2P_y': '2P_100',
    '2PA_y': '2PA_100',
    '2P%_y': '2P%_100',
    'FT_y': 'FT_100',
    'FTA_y': 'FTA_100',
    'FT%_y': 'FT%_100',
    'ORB_y': 'ORB_100',
    'DRB_y': 'DRB_100',
    'TRB_y': 'TRB_100',
    'AST_y': 'AST_100',
    'STL_y': 'STL_100',
    'BLK_y': 'BLK_100',
    'TOV_y': 'TOV_100',
    'PF_y': 'PF_100',
    'PTS_y': 'PTS_100',
})

merged_23_24 = merged_23_24.rename(columns={
   'Player_x': 'Player',
    'Pos_x': 'Pos',
    'Age_x': 'Age',
    'Tm_x': 'Tm',
    'G_x': 'G',
    'GS_x': 'GS',
    'MPG_x': 'MPG',
    'FG_x': 'FGM_pergame',
    'FGA_x': 'FGA_pergame',
    'FG%_x': 'FG%_pergame',
    '3P_x': '3PM_pergame',
    '3PA_x': '3PA_pergame',
    '3P%_x': '3P%_pergame',
    '2P_x': '2PM_pergame',
    '2PA_x': '2PA_pergame',
    '2P%_x': '2P%_pergame',
    'eFG%': 'eFG%_pergame',
    'FT_x': 'FT_pergame',
    'FTA_x': 'FTA_pergame',
    'FT%_x': 'FT%_pergame',
    'ORB_x': 'ORB_pergame',
    'DRB_x': 'DRB_pergame',
    'TRB_x': 'TRB_pergame',
    'AST_x': 'AST_pergame',
    'STL_x': 'STL_pergame',
    'BLK_x': 'BLK_pergame',
    'TOV_x': 'TOV_pergame',
    'PF_x': 'PF_pergame',
    'PTS_x': 'PTS_pergame',
    'Player-additional_x': 'Player-additional',
    'FG_y': 'FG_100',
    'FGA_y': 'FGA_100',
    'FG%_y': 'FG%_100',
    '3P_y': '3P_100',
    '3PA_y': '3PA_100',
    '3P%_y': '3P%_100',
    '2P_y': '2P_100',
    '2PA_y': '2PA_100',
    '2P%_y': '2P%_100',
    'FT_y': 'FT_100',
    'FTA_y': 'FTA_100',
    'FT%_y': 'FT%_100',
    'ORB_y': 'ORB_100',
    'DRB_y': 'DRB_100',
    'TRB_y': 'TRB_100',
    'AST_y': 'AST_100',
    'STL_y': 'STL_100',
    'BLK_y': 'BLK_100',
    'TOV_y': 'TOV_100',
    'PF_y': 'PF_100',
    'PTS_y': 'PTS_100',
})

#----------------------------------------------------------------------------------------------

# Save merged DataFrames to CSV
merged_23_24.to_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/Merged23-24.csv", index=False)
merged_22_23.to_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/Merged22-23.csv", index=False)
merged_21_22.to_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/Merged21-22.csv", index=False)

#----------------------------------------------------------------------------------------------
#-------------------- Conditioning and Merging LEBRON and RAPTOR ------------------------------
#----------------------------------------------------------------------------------------------
data_lebron23_24 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/LEBRON23-24.csv", encoding='latin1')
data_raptor23_24 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/RAPTOR23-24.csv", encoding='latin1')
data_lebron22_23 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/LEBRON22-23.csv", encoding='latin1')
data_raptor22_23 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/RAPTOR22-23.csv", encoding='latin1')
data_lebron21_22 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/LEBRON21-22.csv", encoding='latin1')
data_raptor21_22 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/RAPTOR21-22.csv", encoding='latin1')

#------------------------------------------------------------------------------------------------------------------
data_raptor21_22 = data_raptor21_22.rename(columns={
    'PLAYER':'Player'})
data_raptor22_23 = data_raptor22_23.rename(columns={
    'PLAYER':'Player'})
data_raptor23_24 = data_raptor23_24.rename(columns={
    'PLAYER':'Player'})
#standardize player names
# Standardize player names
data_lebron23_24['Player'] = data_lebron23_24['Player'].str.strip().str.lower()
data_raptor23_24['Player'] = data_raptor23_24['Player'].str.strip().str.lower()

data_lebron22_23['Player'] = data_lebron22_23['Player'].str.strip().str.lower()
data_raptor22_23['Player'] = data_raptor22_23['Player'].str.strip().str.lower()

data_lebron21_22['Player'] = data_lebron21_22['Player'].str.strip().str.lower()
data_raptor21_22['Player'] = data_raptor21_22['Player'].str.strip().str.lower()
import pandas as pd

#remove year from names of players in 21-22 and 22-23 raptor data
# Function to remove year suffix from the Player column
def remove_year_suffix(player_name):
    import re
    return re.sub(r"'\d{2}-'\d{2}", "", player_name)

# Apply the function to the Player column in the 21-22 RAPTOR dataframe
data_raptor21_22['Player'] = data_raptor21_22['Player'].apply(remove_year_suffix)

# Apply the function to the Player column in the 22-23 RAPTOR dataframe
data_raptor22_23['Player'] = data_raptor22_23['Player'].apply(remove_year_suffix)

# Merge the dataframes on 'Player' column
lebronraptor23_24 = pd.merge(data_lebron23_24, data_raptor23_24, on='Player', how='inner')
lebronraptor22_23 = pd.merge(data_lebron22_23, data_raptor22_23, on='Player', how='inner')
lebronraptor21_22 = pd.merge(data_lebron21_22, data_raptor21_22, on='Player', how='inner')
"""

# Players in LEBRON but not in RAPTOR
lebron_not_in_raptor = lebron_players23_24 - raptor_players23_24
# Players in RAPTOR but not in LEBRON
raptor_not_in_lebron = raptor_players23_24 - lebron_players23_24

"""

#----------------------------------------------------------------------------------------------
#----------------------- Merging LEBRON, RAPTOR into bask ref data ----------------------------
#----------------------------------------------------------------------------------------------

# standardize names in Basketball Reference data
merged_23_24['Player'] = merged_23_24['Player'].str.strip().str.lower()
merged_22_23['Player'] = merged_22_23['Player'].str.strip().str.lower()
merged_21_22['Player'] = merged_21_22['Player'].str.strip().str.lower()

# merge dataframes
merged_21_22 = pd.merge(merged_21_22, lebronraptor21_22, on='Player', how= 'inner')
merged_22_23 = pd.merge(merged_22_23, lebronraptor22_23, on='Player', how= 'inner')
merged_23_24 = pd.merge(merged_23_24, lebronraptor23_24, on='Player', how= 'inner')

# Remove redundant columns and rename "pos_x" to "pos" for each merged dataframe
merged_21_22 = merged_21_22.drop(columns=['Pos_y','Position','Minutes']).rename(columns={'Pos_x': 'Pos'})
merged_22_23 = merged_22_23.drop(columns=['Pos_y','Position','Minutes']).rename(columns={'Pos_x': 'Pos','G_x':'G'})
merged_23_24 = merged_23_24.drop(columns=['Pos_y','Position','Minutes']).rename(columns={'Pos_x': 'Pos','G_x':'G'})

merged_21_22 = merged_21_22.drop(columns=['Team(s)','Season','Tm','MP'])
merged_22_23 = merged_22_23.drop(columns=['Team(s)','Season','Tm','MP'])
merged_23_24 = merged_23_24.drop(columns=['Team(s)','Season','Tm','MP'])

# Save merged DataFrames to CSV
merged_23_24.to_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/Merged23-24.csv", index=False)
merged_22_23.to_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/Merged22-23.csv", index=False)
merged_21_22.to_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/Merged21-22.csv", index=False)

#-----------------------------------------------------------------------------------------------
#--------------------- Standardize Units for All Stats -----------------------------------------
#-----------------------------------------------------------------------------------------------

# Filtering dataframes down to only certain columns
columns = [
    'Player','Pos','Age','Team','G','GS','MPG','FG%_pergame','3P%_pergame','2P%_pergame',
    'eFG%_pergame','TS%','ORB_pergame','ORB_100','DRB_pergame','DRB_100','TRB_pergame','TRB_100',
    'TOV_pergame','TOV_100','FTA_pergame','FTA_100','FT%_pergame','PTS_pergame','PTS_100','AST%',
    'STL_pergame','STL_100','BLK_pergame','BLK_100','PER','3PAr','FTr','ORB%','DRB%','TRB%','USG%','OWS','DWS','WS',
    'TOV%','OBPM','DBPM','BPM','VORP','NetRtg','ORtg','DRtg','LEBRON','O LEBRON','D LEBRON','LEBRON WAR',
    'LEBRON Contract Value','Off','Def','Tot','WAR']

merged_21_22 = merged_21_22[columns]
merged_22_23 = merged_22_23[columns]
merged_23_24 = merged_23_24[columns]

# Re-import function following all of above conditioning
import pandas as pd
merged_21_22 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/Merged21-22.csv")
merged_22_23 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/Merged22-23.csv")
merged_23_24 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/Merged23-24.csv")


# Traditional Per Game Stats
traditional_per_game_stats = [
    'FG%_pergame', '3P%_pergame', '2P%_pergame',
    'eFG%_pergame', 'TS%', 'ORB_pergame', 'DRB_pergame',
    'TRB_pergame', 'TOV_pergame', 'FTA_pergame', 'FT%_pergame',
    'PTS_pergame', 'AST%', 'STL_pergame', 'BLK_pergame'
]

# Advanced Per 100 Possessions Stats
advanced_per_100_possessions_stats = [
    'FG%_100', '3P%_100', '2P%_100', 'PTS_100',
    'ORB_100', 'DRB_100', 'TRB_100', 'TOV_100', 'FTA_100',
    'FT%_100', 'STL_100', 'BLK_100', 'TS%', 'eFG%_pergame'
]

# On/Off Court Stats
on_off_court_stats = [
    'BPM', 'OBPM', 'DBPM', 'NetRtg', 'ORtg', 'DRtg',
    'LEBRON', 'O LEBRON', 'D LEBRON',
    'Off', 'Def', 'Tot'
]

# Advanced Metrics and Efficiency Stats
advanced_metrics_and_efficiency_stats = [
    'PER', 'VORP', 'WS', 'OWS', 'DWS', 'TS%','eFG%_pergame','LEBRON WAR','WAR'
]

# Usage and Other Advanced Stats
usage_and_other_advanced_stats = [
    'USG%', 'ORB%', 'DRB%', 'TRB%', 'TOV%'
]

merged_21_22 = merged_21_22.rename(columns={'WAR':'RAPTOR WAR','Off':'RAPTOR Off','Def':'RAPTOR Def','Tot':'RAPTOR Tot'})
merged_22_23 = merged_22_23.rename(columns={'WAR':'RAPTOR WAR','Off':'RAPTOR Off','Def':'RAPTOR Def','Tot':'RAPTOR Tot'})
merged_23_24 = merged_23_24.rename(columns={'WAR':'RAPTOR WAR','Off':'RAPTOR Off','Def':'RAPTOR Def','Tot':'RAPTOR Tot'})
# Rename 'eFG%_pergame' to 'eFG%' in each of the merged dataframes
merged_21_22 = merged_21_22.rename(columns={'FG%_pergame':'FG%'})
merged_22_23 = merged_22_23.rename(columns={'FG%_pergame':'FG%'})
merged_23_24 = merged_23_24.rename(columns={'FG%_pergame':'FG%'})

#-------------------------------------------------------------------------------------------
# Grouping stat categories for weighting
perstats = ['FG%', '3P%', '2P%', 'ORB_pergame', 'DRB_pergame',
'TRB_pergame', 'TOV_pergame', 'FTA_pergame', 'FT%',
'PTS_pergame', 'AST%', 'STL_pergame', 'BLK_pergame','PTS_100',
'ORB_100', 'DRB_100', 'TRB_100', 'TOV_100', 'FTA_100', 'STL_100', 'BLK_100']
advancedstats = ['USG%', 'ORB%', 'DRB%', 'TRB%', 'TOV%',
'PER', 'VORP', 'WS', 'OWS', 'DWS', 'TS%','eFG%','LEBRON WAR','RAPTOR WAR']
onoffstats = ['BPM', 'OBPM', 'DBPM', 'NetRtg', 'ORtg', 'DRtg',
'LEBRON', 'O LEBRON', 'D LEBRON',
'RAPTOR Off', 'RAPTOR Def', 'RAPTOR Tot']

#--------------------------------------------------------------------------------------------
import pandas as pd
# Importing League Averages for "per" and "advanced" stats
boxaverages21_22 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/LeagueAveragesBox21-22.csv")
boxaverages22_23 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/LeagueAveragesBox22-23.csv")
boxaverages23_24 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/LeagueAveragesBox23-24.csv")
advancedaverages21_22 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/LeagueAveragesAdvanced21-22.csv")
advancedaverages22_23 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/LeagueAveragesAdvanced22-23.csv")
advancedaverages23_24 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/LeagueAveragesAdvanced23-24.csv")
per100averages21_22 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/LeagueAveragesNetRtg21-22.csv")
per100averages22_23 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/LeagueAveragesNetRtg22-23.csv")
per100averages23_24 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/LeagueAveragesNetRtg23-24.csv")

perstatsaverages21_22 = pd.concat([boxaverages21_22, per100averages21_22], axis=0)
perstatsaverages22_23 = pd.concat([boxaverages22_23, per100averages22_23], axis=0)
perstatsaverages23_24 = pd.concat([boxaverages23_24, per100averages23_24], axis=0)
# Transpose averages dataframes to match player dataframe formatting
perstatsaverages21_22 = perstatsaverages21_22.set_index('Stat').T
perstatsaverages22_23 = perstatsaverages22_23.set_index('Stat').T
perstatsaverages23_24 = perstatsaverages23_24.set_index('Stat').T
advancedaverages21_22 = advancedaverages21_22.set_index('Stat').T
advancedaverages22_23 = advancedaverages22_23.set_index('Stat').T
advancedaverages23_24 = advancedaverages23_24.set_index('Stat').T

perstatsaverages21_22.to_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/perstatsaverages21_22.csv", index=True)
perstatsaverages22_23.to_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/perstatsaverages22_23.csv", index=True)
perstatsaverages23_24.to_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/perstatsaverages23_24.csv", index=True)


#---------------------------------------------------------------------------------------------
#-------------------- Finding Difference between player stats and League Average -------------
#---------------------------------------------------------------------------------------------
import pandas as pd
# Calculations done in Excel
# Import "Adjusted datasets"
Adjusted21_22 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/Adjusted Data/Merged21-22Adjusted.csv")
Adjusted22_23 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/Adjusted Data/Merged22-23Adjusted.csv")
Adjusted23_24 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/Adjusted Data/Merged23-24Adjusted.csv")
Missing21_22 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/Adjusted Data/21-22MissingPlayers.csv")
Missing23_24 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/Adjusted Data/23-24MissingPlayers.csv")

# Merge missing players into respective datasets
# Merge the datasets on the 'Player' column
# Concatenate the smaller dataset into the larger one
Adjusted21_22 = pd.concat([Adjusted21_22, Missing21_22], ignore_index=True)
Adjusted23_24 = pd.concat([Adjusted23_24, Missing23_24], ignore_index=True)

#-----------------------------------------------------------------------------------------------
#---------------------------- Calculating Impact Score for Datasets ----------------------------
#-----------------------------------------------------------------------------------------------
# Set Stat Categories that count
ImpactScoreColumns = ['FG%', '3P%', '2P%', 'ORB_pergame', 'DRB_pergame',
'TRB_pergame', 'TOV_pergame', 'FTA_pergame', 'FT%',
'PTS_pergame', 'AST%', 'STL_pergame', 'BLK_pergame','PTS_100',
'ORB_100', 'DRB_100', 'TRB_100', 'TOV_100', 'FTA_100', 'STL_100', 'BLK_100',
'USG%', 'ORB%', 'DRB%', 'TRB%', 'TOV%',
'PER', 'VORP', 'WS', 'OWS', 'DWS', 'TS%','eFG%','LEBRON WAR','RAPTOR WAR',
'BPM', 'OBPM', 'DBPM', 'NetRtg','LEBRON', 'O LEBRON', 'D LEBRON',
'RAPTOR Off', 'RAPTOR Def', 'RAPTOR Tot']
Percentages = ['FG%', '3P%', '2P%','FT%', 'TS%','eFG%']

# Function to convert percentages to integers
def convert_percentages_to_integers(df, percentage_columns):
    for col in percentage_columns:
        df[col] = df[col] * 100
    return df

# Convert percentages to integers for all datasets
Adjusted21_22 = convert_percentages_to_integers(Adjusted21_22, Percentages)
Adjusted22_23 = convert_percentages_to_integers(Adjusted22_23, Percentages)
Adjusted23_24 = convert_percentages_to_integers(Adjusted23_24, Percentages)

# Function to calculate the Impact Score
def calculate_impact_score(df, impact_columns):
    df['ImpactScore'] = df[impact_columns].sum(axis=1)
    return df

# Calculate Impact Scores for all datasets
Adjusted21_22 = calculate_impact_score(Adjusted21_22, ImpactScoreColumns)
Adjusted22_23 = calculate_impact_score(Adjusted22_23, ImpactScoreColumns)
Adjusted23_24 = calculate_impact_score(Adjusted23_24, ImpactScoreColumns)
