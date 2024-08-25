import pandas as pd
from fantasy_scores_update_functions import *

tournament_id = 11653
player_stats_df = pd.read_parquet(f'data/Player Stats/{tournament_id}_player_stats.parquet')
player_positions_df =  pd.read_parquet(f'data/Player Positions/{tournament_id}_player_positions.parquet')
try:
  previous_scores = pd.read_parquet(f'data/Fantasy Scores/{tournament_id}_fantasy_scores.parquet')
except:
  previous_scores = pd.DataFrame(columns = ['match_id', 'player_name',	'team',	'final_score',	'position',	'season_id',	'round'])
importances_df = pd.read_csv('data/importancia_stats_por_posicion.csv')
matches_df = pd.read_parquet(f'data/Matches/{tournament_id}_matches.parquet')

final_output = previous_scores.copy()

for position in previous_scores['position']:
   temp_df = fantasy_scores_output(player_stats_df, player_positions_df, previous_scores, importances_df, matches_df, position)
   final_output = pd.concat([final_output, temp_df])

final_output.to_parquet(f'data/Fantasy Scores/{tournament_id}_fantasy_scores.parquet')

