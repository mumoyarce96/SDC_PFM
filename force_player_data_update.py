from functions.data_update import *

tournament_id = 11653
matches = pd.read_parquet(f'data/Matches/{tournament_id}_matches.parquet')
matches = matches[matches['season_id'] == matches['season_id'].max()]
previous_df = pd.read_parquet(f'data/Player Stats/{tournament_id}_player_stats.parquet').drop_duplicates(subset = ['match_id', 'player_id'])
previous_df_info = previous_df.groupby(['match_id']).size().reset_index().sort_values(by = 0).rename(columns={0: 'n'})
target_match_ids = previous_df_info[previous_df_info['n'] < 30]['match_id'].tolist()
target_matches = matches[matches['match_id'].isin(target_match_ids)]
df = get_player_stats(target_matches, previous_df, reruns = 15).drop_duplicates(subset=['match_id', 'player_id'], keep = 'last')
df.to_parquet(f'data/Player Stats/{tournament_id}_player_stats.parquet')