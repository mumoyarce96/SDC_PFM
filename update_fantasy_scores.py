import pandas as pd
from functions.fantasy_scores_update import *

tournament_id = 11653
player_stats_df = pd.read_parquet(f'data/Player Stats/{tournament_id}_player_stats.parquet').fillna(0)
player_positions_df =  pd.read_parquet(f'data/Player Positions/{tournament_id}_player_positions.parquet')
try:
  previous_scores = pd.read_parquet(f'data/Fantasy Scores/{tournament_id}_player_fantasy_scores.parquet')
except:
  previous_scores = pd.DataFrame(columns = ['match_id', 'player_name',	'team','score', 'minmax_score', 'percentile',	'position',	'season_id',	'round'])
importances_df = pd.read_csv('data/importancia_stats_por_posicion.csv')
matches_df = pd.read_parquet(f'data/Matches/{tournament_id}_matches.parquet')
final_output = previous_scores.copy() 
temp_dfs = []
for position in importances_df['position'].unique():
   temp_dfs.append(fantasy_scores_output(player_stats_df, player_positions_df, final_output, importances_df, matches_df, position))

if len(temp_dfs)> 0:
    new_dfs = pd.concat(temp_dfs, ignore_index = True)
    final_output = pd.concat([final_output, new_dfs], ignore_index = True).drop_duplicates(['match_id', 'player_name', 'position'], keep = 'last')
    final_output.to_parquet(f'data/Fantasy Scores/{tournament_id}_player_fantasy_scores.parquet')
