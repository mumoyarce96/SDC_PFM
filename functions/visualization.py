import json
import matplotlib.pyplot as plt
from mplsoccer import VerticalPitch
import pandas as pd
import numpy as np 

PITCH_COLOR = '#75d186'
pitch = VerticalPitch(pitch_color = PITCH_COLOR, line_color = 'w')

with open(f'data/formations_info.json', 'r') as f:
    formations_dict = json.load(f)

def get_plot_df(round_df):
    formation_name = round_df['formation'].iloc[0]
    mplsoccer_formation = formations_dict[formation_name]['mplsoccer_formation']
    sofascore_positions = formations_dict[formation_name]['sofascore_positions']
    pitch_positions = formations_dict[formation_name]['pitch_positions']
    positions = []
    x = []
    y = []
    for position in pitch.get_formation(mplsoccer_formation):
        positions.append(position.name)
        x.append(position.x)
        y.append(position.y)

    locations_df = pd.DataFrame({'position': positions, 'x': x, 'y': y})
    positions_map = dict(zip(pitch_positions, sofascore_positions))
    locations_df['sofascore_position'] = locations_df['position'].map(positions_map)
    round_df = round_df.merge(locations_df,left_on = 'position', right_on = 'sofascore_position', how = 'right')
    return round_df 

def plot_lineup(round_df):
    fig, ax = pitch.draw(figsize=(20, 12))
    df = get_plot_df(round_df)
    plt.scatter(df['y'], df['x'], s = 700, c = 'red', edgecolor = 'k')
    for i, player in df.iterrows():
        plt.annotate(player['player_name'], (player['y'], player['x'] - 4), ha = 'center')
        plt.annotate(player['team'], (player['y'], player['x'] - 6), ha = 'center')
        plt.annotate(int(np.round(player['percentile'], 0)), (player['y'], player['x']), ha = 'center', va = 'center', color = 'k', size = 14)
    fig.set_facecolor(PITCH_COLOR)
    return fig