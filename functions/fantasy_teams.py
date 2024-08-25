import pandas as pd
import json
import re

with open('data/formations_info.json', 'r') as f:
    formations_dict = json.load(f)

def remove_numbers(strings):
    return [re.sub(r'\d+', '', string) for string in strings]

for key in formations_dict.keys():
  formations_dict[key]['sofascore_positions'] = remove_numbers(formations_dict[key]['sofascore_positions'])

def get_best_position_player(df, position):
  df = df[df['position'] == position]
  df = df.sort_values(by = 'final_score', ascending = False).head(1)
  return df

def get_best_11(df, positions):
   df = df[df['position'].isin(positions)]
   df = df.sort_values(by = 'final_score', ascending = False)
   output_df = pd.DataFrame(columns = df.columns)
   for position in positions:
       temp_df = df[df['player_name'].isin(output_df['player_name']) == False]
       output_df = pd.concat([output_df, get_best_position_player(temp_df, position)], ignore_index=True)
   return output_df

def get_best_score(df, formation_name, formations_dict):
   positions = formations_dict[formation_name]['sofascore_positions']
   formation = get_best_11(df, positions)
   score = formation['final_score'].mean()
   return formation, score

def get_best_lineup(df):
   best_score = 0
   best_formation = None
   for formation_name in formations_dict.keys():
     formation, score = get_best_score(df, formation_name, formations_dict)
     if score > best_score:
       best_score = score
       best_formation = formation
       best_formation['formation'] = formation_name
   return best_formation, best_score

def add_suffix_to_duplicates(data):
    df = pd.DataFrame(data)

    # Agrupar por 'season_id' y 'round' y contar las ocurrencias de 'position'
    df['position'] = df.groupby(['season_id', 'round', 'position']).cumcount() + 1

    # Crear una nueva columna para mostrar la posición con sufijo numérico si es necesario
    df['position'] = df['position'].astype(str)
    df['position'] = df['position'].replace('1', '')
    df['position'] = data['position'] + df['position']
    return df

def get_new_fantasy_teams(df):
   fantasy_teams = pd.DataFrame(columns = df.columns)
   for season_id in df['season_id'].unique():
        season_df = df[df['season_id'] == season_id]
   for round in season_df['round'].unique():
        round_df = season_df[season_df['round'] == round]
        temp_df = get_best_lineup(round_df)
        fantasy_teams = pd.concat([fantasy_teams, temp_df[0]], ignore_index=True)
   fantasy_teams = add_suffix_to_duplicates(fantasy_teams)
   return fantasy_teams