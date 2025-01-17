# -*- coding: utf-8 -*-
"""
Created on Fri Aug  2 06:59:00 2024

@author: NAWri
"""

import pandas as pd

data_advanced23_24 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/BaskRefAdvanced23-24.csv", encoding='latin1')
data_box23_24 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/BaskRefBox23-24.csv", encoding='latin1')
data_rtg23_24 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/BaskRefNetRtg23-24.csv", encoding='latin1')
data_advanced22_23 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/BaskRefAdvanced22-23.csv", encoding='latin1')
data_box22_23 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/BaskRefBox22-23.csv", encoding='latin1')
data_rtg22_23 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/BaskRefNetRtg22-23.csv", encoding='latin1')
data_advanced21_22 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/BaskRefAdvanced21-22.csv", encoding='latin1')
data_box21_22 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/BaskRefBox21-22.csv", encoding='latin1')
data_rtg21_22 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/BaskRefNetRtg21-22.csv", encoding='latin1')

import pandas as pd

import pandas as pd

def compute_league_averages(df):
    # Select only numeric columns
    numeric_df = df.select_dtypes(include=['number'])
    # Calculate the mean for each numeric column
    league_averages = numeric_df.mean()
    return league_averages

def save_league_averages(file_path, df):
    league_averages = compute_league_averages(df)
    # Convert Series to DataFrame and save to CSV
    league_averages_df = league_averages.reset_index()
    league_averages_df.columns = ['Stat', 'League_Average']
    league_averages_df.to_csv(file_path, index=False)

# Define file paths for the datasets
file_paths = {
    "advanced23_24": "C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/BaskRefAdvanced23-24.csv",
    "box23_24": "C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/BaskRefBox23-24.csv",
    "rtg23_24": "C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/BaskRefNetRtg23-24.csv",
    "advanced22_23": "C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/BaskRefAdvanced22-23.csv",
    "box22_23": "C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/BaskRefBox22-23.csv",
    "rtg22_23": "C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/BaskRefNetRtg22-23.csv",
    "advanced21_22": "C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/BaskRefAdvanced21-22.csv",
    "box21_22": "C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/BaskRefBox21-22.csv",
    "rtg21_22": "C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/BaskRefNetRtg21-22.csv"
}

# Define output file paths for the league averages
output_paths = {
    "advanced23_24": "C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/LeagueAveragesAdvanced23-24.csv",
    "box23_24": "C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/LeagueAveragesBox23-24.csv",
    "rtg23_24": "C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/LeagueAveragesNetRtg23-24.csv",
    "advanced22_23": "C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/LeagueAveragesAdvanced22-23.csv",
    "box22_23": "C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/LeagueAveragesBox22-23.csv",
    "rtg22_23": "C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/LeagueAveragesNetRtg22-23.csv",
    "advanced21_22": "C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/LeagueAveragesAdvanced21-22.csv",
    "box21_22": "C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/LeagueAveragesBox21-22.csv",
    "rtg21_22": "C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/LeagueAveragesNetRtg21-22.csv"
}

# Load datasets, compute league averages, and save results
for key, path in file_paths.items():
    df = pd.read_csv(path, encoding='latin1')
    output_path = output_paths[key]
    save_league_averages(output_path, df)

print("League averages computed and saved.")

import pandas as pd
pergameavg21_22 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/LeagueAveragesBox21-22.csv")
pergameavg22_23 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/LeagueAveragesBox22-23.csv")
pergameavg23_24 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/LeagueAveragesBox23-24.csv")
per100avg21_22 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/LeagueAveragesNetRtg21-22.csv")
per100avg22_23 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/LeagueAveragesNetRtg22-23.csv")
per100avg23_24 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/LeagueAveragesNetRtg23-24.csv")
advancedavg21_22 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/LeagueAveragesAdvanced21-22.csv")
advancedavg22_23 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/LeagueAveragesAdvanced22-23.csv")
advancedavg23_24 = pd.read_csv("C:/Users/NAWri/Documents/BGA/NBA_Impact_Score/LeagueAveragesAdvanced23-24.csv")
