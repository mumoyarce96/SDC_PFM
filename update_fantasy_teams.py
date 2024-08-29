import pandas as pd
from functions.fantasy_teams import *

tournament_id = 11653
fantasy_scores = pd.read_parquet(f'data/Fantasy Scores/{tournament_id}_player_fantasy_scores.parquet')
try:
    fantasy_teams = pd.read_parquet(f'data/Fantasy Teams/{tournament_id}_fantasy_teams.parquet')
except:
    fantasy_teams = pd.DataFrame(columns = fantasy_scores.columns)

last_season = fantasy_scores[fantasy_scores['season_id'] == fantasy_scores['season_id'].max()]
last_season_id = last_season['season_id'].unique()[0]
n_teams = last_season['team'].nunique()
matches_played = last_season.groupby('round')['match_id'].nunique().reset_index()
rounds = matches_played[matches_played['match_id'] < int(n_teams/2)]['round'].to_list()
rounds = matches_played['round'].to_list()
fantasy_scores = fantasy_scores[(fantasy_scores['season_id'] == last_season_id) & (fantasy_scores['round'].isin(rounds))]
new_fantasy_teams = get_new_fantasy_teams(fantasy_scores).drop('score', axis = 1)
fantasy_teams = fantasy_teams[((fantasy_teams['season_id'] == last_season_id) & (fantasy_teams['round'].isin(rounds))) == False]
output = pd.concat([fantasy_teams, new_fantasy_teams])
output.to_parquet(f'data/Fantasy Teams/{tournament_id}_fantasy_teams.parquet')

print(output[(output['round'] == 19) & (output['season_id'] == 57883)] )