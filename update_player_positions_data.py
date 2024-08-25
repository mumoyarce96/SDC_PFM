from functions.data_update import *

tournament_id = 11653
season_ids = [57883]
for season_id in season_ids:
  save_player_positions(tournament_id, season_id)
