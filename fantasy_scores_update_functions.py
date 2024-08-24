import pandas as pd

not_stats_columns = ['player_name', 'player_position', 'player_id', 'match_id', 'minutesPlayed', 'home', 'rating']
position_canditates_map = {
  'GK': ['G'],
  'DR': ['M', 'D'],
  'DL': ['M', 'D'],
  'DC': ['D'],
  'DM': ['M'],
  'MC': ['M'],
  'AM': ['F', 'M'],
  'ML': ['F', 'M', 'D'],
  'MR': ['F', 'M', 'D'],
  'RW': ['F', 'M', 'D'],
  'LW': ['F', 'M', 'D'],
  'ST': ['F']
}

def get_player_ids_by_position(positions_df, position):
    """
    Get a list of player_ids based on the provided position.

    Args:
        positions_df (DataFrame): DataFrame containing player positions.
        position (str): Position to filter players by.

    Returns:
        list: List of player_ids who play the given position.
    """
    return positions_df[positions_df['positions'].apply(lambda x: position in x)]['player_id'].to_list()

def get_candidates(player_stats_df, position):
    list_of_positions = position_candidates_map[position]
    return player_stats_df[player_stats_df['player_position'].isin(list_of_positions)]

def get_unscored_rows(player_stats_df, player_positions_df, previous_scores, position):
    match_ids = [match_id for match_id in player_stats_df['match_id'].unique() if match_id not in previous_scores['match_id']]
    player_stats_df = player_stats_df[player_stats_df['match_id'].isin(match_ids)]
    position_ids = get_player_ids_by_position(player_positions_df, position)
    no_position_ids = positions_df[positions_df['positions'].apply(lambda x: len(x) == 0)]['player_id'].to_list()
    player_stats_df = get_candidates(player_stats_df[player_stats_df['player_id'].isin(position_ids + no_position_ids)])
    return player_stats_df

def normalize_df(df):
    for column in df.columns:
      if column not in not_stats_columns:
        df[column] = (df[column] - df[column].mean())/df[column].std()
    return df

def get_fantasy_scores(player_stats_df, player_positions_df, previous_scores, importances_df, position):    
    player_stats_df = normalize_df(player_stats_df)
    unscored_players = get_unscored_rows(player_stats_df, player_positions_df, previous_scores, position).drop('team', axis = 1)
    importances_df = importances_df[importances_df['position'] == position].drop(['position', 'description'], axis = 1)
    scores = []
    for i, player_row in unscored_players.drop(not_stats_columns, axis = 1).iterrows():
          scores.append((player_row * importances).sum(axis = 1).values[0])
    unscored_players['score'] = scores
    return unscored_players    
