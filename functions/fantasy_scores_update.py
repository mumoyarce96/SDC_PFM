import pandas as pd

not_stats_columns = ['player_name', 'player_position', 'player_id', 'match_id', 'home']
position_candidates_map = {
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
    match_ids = [match_id for match_id in player_stats_df['match_id'].unique() if match_id not in previous_scores['match_id'].to_list()]
    player_stats_df = player_stats_df[player_stats_df['match_id'].isin(match_ids)]
    position_ids = get_player_ids_by_position(player_positions_df, position)
    no_position_ids = player_positions_df[player_positions_df['positions'].apply(lambda x: len(x) == 0)]['player_id'].to_list()
    player_stats_df = player_stats_df[player_stats_df['player_id'].isin(position_ids + no_position_ids)]
    player_stats_df = get_candidates(player_stats_df, position)
    return player_stats_df

def normalize_df(df):
    for column in df.columns:
      if column not in not_stats_columns and column != 'team':
        df[column] = (df[column] - df[column].mean())/df[column].std()
    return df

def get_fantasy_scores(player_stats_df, player_positions_df, previous_scores, importances_df, position):    
    unscored_players = get_unscored_rows(player_stats_df, player_positions_df, previous_scores, position).drop('team', axis = 1)
    # normalizacion debe ser con unscored + jugadores de  previous_scores en esa posicion
    unscored_players = normalize_df(unscored_players)
    print(len(unscored_players))
    print(unscored_players[unscored_players['match_id'] == 11986405][['match_id', 'home', 'formation', 'player_name', 'player_position',
       'player_id', 'totalPass', 'accuratePass',
       'totalLongBalls', 'accurateLongBalls']])
    importances_df = importances_df[importances_df['position'] == position].drop(['position', 'description'], axis = 1)
    scores = []
    for _, player_row in unscored_players.drop(not_stats_columns, axis = 1).iterrows():
          # REVISAR QUE LA MULTIPLICACIÓN ESTÁ DANDO BIEN, NO DEBERÍA HABER PROBLEMA
          scores.append((player_row * importances_df).sum(axis = 1).values[0])
    unscored_players['score'] = scores
    return unscored_players    

def calculate_final_scores(player_stats_df, player_positions_df, previous_scores, importances_df, position):
    cols = ['player_name', 'match_id', 'score']
    new_scores = get_fantasy_scores(player_stats_df, player_positions_df, previous_scores, importances_df, position)
    stats_df = pd.concat([previous_scores[cols], new_scores[cols]])
    stats_df['percentile'] = pd.qcut(stats_df['score'], q=100, labels=False)
    min = 1
    max = 99
    stats_df['minmax_score'] = min + ((stats_df['score'] - stats_df['score'].min()) / (stats_df['score'].max() - stats_df['score'].min())) * (max - min)
    stats_df['final_score'] = (stats_df['percentile'] + stats_df['minmax_score'])/2
    stats_df['position'] = position
    return stats_df[stats_df['match_id'].isin(new_scores['match_id'])]

def fantasy_scores_output(player_stats_df, player_positions_df, previous_scores, importances_df, matches_df, position):
    player_stats_df = player_stats_df[['match_id', 'home', 'formation', 'player_name', 'player_position',
       'player_id', 'team', 'totalPass', 'accuratePass',
       'totalLongBalls', 'accurateLongBalls', 'goodHighClaim',
       'savedShotsFromInsideTheBox', 'saves', 'touches', 'possessionLostCtrl',
       'totalCross', 'aerialLost', 'duelLost', 'challengeLost', 'totalContest',
       'interceptionWon', 'aerialWon', 'duelWon', 'totalClearance',
       'outfielderBlock', 'totalTackle', 'wasFouled', 'dispossessed',
       'totalOffside', 'wonContest', 'shotOffTarget', 'fouls', 'keyPass',
       'accurateCross', 'onTargetScoringAttempt', 'blockedScoringAttempt',
       'bigChanceMissed', 'punches', 'bigChanceCreated', 'goalAssist', 'goals',
       'totalKeeperSweeper', 'accurateKeeperSweeper', 'hitWoodwork',
       'clearanceOffLine', 'penaltyConceded', 'errorLeadToAGoal',
       'penaltyMiss', 'ownGoals', 'penaltyWon', 'penaltySave',
       'errorLeadToAShot', 'lastManTackle']]
    
    new_scores = calculate_final_scores(player_stats_df, player_positions_df, previous_scores, importances_df, position)
    output = pd.merge(player_stats_df, new_scores[['match_id', 'player_name', 'score', 'minmax_score', 'percentile', 'final_score', 'position']], on = ['match_id', 'player_name'], how = 'left').dropna(subset = 'score')
    output = pd.merge(output, matches_df[['match_id', 'season_id', 'round']], on = ['match_id'], how = 'left')[['match_id', 'player_name', 'team', 'score',  'minmax_score', 'percentile','final_score', 'position', 'season_id', 'round']]
    return output