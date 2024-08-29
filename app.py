import pandas as pd
import streamlit as st
import numpy as np
from functions.visualization import plot_lineup

st.set_page_config(page_title='Fantasy Primera División Chile')
st.header('Fantasy')

def max_index(list):
    return list.index(max(list))


df = pd.read_parquet('data/Fantasy Teams/11653_fantasy_teams.parquet')
leagues_information = pd.read_csv('data/informacion_torneos.csv')
season_ids_map = dict(zip(leagues_information['temporada'], leagues_information['season_id']))
seasons = leagues_information['temporada'].unique().tolist()
season = st.selectbox("Temporada", seasons, index = max_index(seasons))
season_id = season_ids_map[season]
rounds = df[df['season_id'] == season_id]['round'].unique().tolist()
round = st.selectbox("Fecha", rounds, index = max_index(rounds))

fantasy_team = df[(df['season_id'] == season_id) & (df['round'] == round)]
if len(fantasy_team) == 0:
    st.write("No hay info")
else:
    # Display the DataFrame in Streamlit
    st.title(f'Equipo de la Fecha {round}')
    st.pyplot(plot_lineup(fantasy_team))