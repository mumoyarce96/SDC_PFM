import pandas as pd
import requests
import random
import time

headers = {
    'authority': 'api.sofascore.com',
    'accept': '*/*',
    'accept-encoding': 'gzip, deflate, br, zstd',
    'accept-language': 'es-419,es;q=0.9',
    'cache-control': 'max-age=0',
    'dnt': '1',
    'if-none-match': 'W/"4bebed6144"',
    'origin': 'https://www.sofascore.com',
    'referer': 'https://www.sofascore.com/',
    'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0',
}

def get_matches_info(tournament_id, season_id):
    rounds = []
    match_ids = []
    home_teams = []
    away_teams = []
    home_team_ids = []
    away_team_ids = []
    url = f'https://api.sofascore.com/api/v1/unique-tournament/{tournament_id}/season/{season_id}/rounds' 
    response = requests.request("GET", url, headers={}, data = {}).json()
    n_rounds = len(response['rounds'])
    for round in range(1, n_rounds + 1):
      url = f'https://api.sofascore.com/api/v1/unique-tournament/{tournament_id}/season/{season_id}/events/round/{round}'
      time.sleep(random.uniform(0.7, 1.3))
      fecha = requests.request("GET", url, headers={}, data = {}).json()
      if 'error' not in fecha.keys():
          for match in fecha['events']:
              rounds.append(round)
              home_teams.append(match['homeTeam']['name'])
              home_team_ids.append(match['homeTeam']['id'])
              away_teams.append(match['awayTeam']['name'])
              away_team_ids.append(match['awayTeam']['id'])
              match_ids.append(match['id'])

    matches = pd.DataFrame({
              'round': rounds,
              'match_id': match_ids,
              'home_team': home_teams,
              'away_team': away_teams,
              'home_team_id': home_team_ids,
              'away_team_id': away_team_ids
              })

    matches['season_id'] = season_id
    matches['tournament_id'] = tournament_id
    return matches

def save_matches_info(tournament_id, season_ids):
    for season_id in season_ids:
      try: 
          previous_matches = pd.read_parquet(f"data/Matches/{tournament_id}_matches.parquet")
          matches = get_matches_info(tournament_id, season_id)
          final_matches = pd.concat([matches, previous_matches]).drop_duplicates()
          final_matches.to_parquet(f"data/Matches/{tournament_id}_matches.parquet")
      except:
          matches = get_matches_info(tournament_id, season_id)
          matches.to_parquet(f"data/Matches/{tournament_id}_matches.parquet")

def get_new_matches(tournament_id, season_id):
    matches = pd.read_parquet(f"data/Matches/{tournament_id}_matches.parquet")
    try:
      previous_df = pd.read_parquet(f"data/Player Stats/{tournament_id}_player_stats.parquet")
      previous_matches = previous_df['match_id'].unique()
    except:
      previous_df = pd.DataFrame()
      previous_matches = []
    match_ids = matches[(matches['season_id'] == season_id)]['match_id']
    match_ids = [match_id for match_id in match_ids if match_id not in previous_matches]
    new_matches = matches[matches['match_id'].isin(match_ids)]
    return previous_df, new_matches

def parse_player_info(player_info, home, match_id, team):
    if 'statistics' in player_info.keys():
      stats = player_info['statistics']
      if 'minutesPlayed' in stats.keys() and stats['minutesPlayed'] >= 15:
        if 'ratingVersions' in stats.keys():
          stats.pop('ratingVersions')
        df = pd.DataFrame(stats, index = [0])
        df['player_name'] = player_info['player']['name']
        df['player_id'] = player_info['player']['id']
        df['player_position'] = player_info['position']
        df['home'] = home
        df['match_id'] = match_id
        df['team'] = team
        return df.dropna(subset = 'player_name')
    else:
        return pd.DataFrame()  

def parse_player_data_response(dfs, response, match_id, home_team, away_team):
      if response.status_code == 200:
        home_players = response.json()['home']['players']
        away_players = response.json()['away']['players']
        for player_info in home_players:
            df = parse_player_info(player_info, True, match_id, home_team)
            dfs.append(df)
        for player_info in away_players:
            df = parse_player_info(player_info, False, match_id, away_team)
            dfs.append(df)
      return dfs

def get_match_player_stats(dfs, matches, match_id):
        response = requests.request("GET", f'https://api.sofascore.com/api/v1/event/{match_id}/lineups', headers = headers, data = {})
        match_info = matches[matches['match_id'] == match_id].iloc[0]
        home_team = match_info['home_team']
        away_team = match_info['away_team']
        dfs = parse_player_data_response(dfs, response, match_id, home_team, away_team)
        return dfs

def get_player_stats(matches, previous_df, reruns = 0):
      dfs = []
      match_ids = matches['match_id'].unique()
      for match_id in match_ids:
          get_match_player_stats(dfs, matches, match_id)
          time.sleep(random.uniform(0.8, 1.2))
          for i in range(reruns):
              get_match_player_stats(dfs, matches, match_id)
              time.sleep(random.uniform(0.5, 1))
      if len(dfs) > 0:
        print(len(dfs))
        df = pd.concat(dfs, ignore_index = True)
        cols = ['player_id', 'player_position', 'player_name']
        for col in cols:
            first_column = df.pop(col)
            df.insert(0, col, first_column)
        df = pd.concat([previous_df, df]).drop_duplicates(subset = ['match_id', 'player_name'], keep = 'last').reset_index(drop = True)
        df = df.fillna(0)
        return df
      else: 
        return previous_df

def save_player_stats(tournament_id, season_id):
      previous_df, matches = get_new_matches(tournament_id, season_id)
      df = get_player_stats(matches, previous_df).dropna(subset = 'player_name')
      df.to_parquet(f"data/Player Stats/{tournament_id}_player_stats.parquet")

def save_player_positions(tournament_id, season_id):
      try:
        previous_df = pd.read_parquet(f"data/Player Positions/{tournament_id}_player_positions.parquet")
      except:
        previous_df = pd.DataFrame()
      matches_info = pd.read_parquet(f"data/Matches/{tournament_id}_matches.parquet")
      match_ids = matches_info[matches_info['season_id'] == season_id]['match_id'].unique()
      player_stats = pd.read_parquet(f"data/Player Stats/{tournament_id}_player_stats.parquet")
      player_ids = player_stats[player_stats['match_id'].isin(match_ids)]['player_id'].unique()
      positions = []
      fetched_player_ids = []
      for i, player_id in enumerate(player_ids):
          url = f'https://api.sofascore.com/api/v1/player/{int(player_id)}/characteristics'
          try:
            response = requests.request("GET", url, headers={}, data = {}).json()
            positions.append(response['positions'])
            time.sleep(random.uniform(0.5,1.1))
          except:
            positions.append(None)
          fetched_player_ids.append(player_id)
      df = pd.DataFrame({'player_id': fetched_player_ids,
                        'positions': positions})
      df['fecha_carga'] = pd.to_datetime('today')
      df = pd.concat([previous_df, df]).sort_values(by = 'fecha_carga').drop_duplicates(subset = 'player_id', keep = 'last')
      df.to_parquet(f"data/Player Positions/{tournament_id}_player_positions.parquet")