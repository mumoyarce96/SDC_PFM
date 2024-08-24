from functions import *
import time

tournament_id = 11653
season_ids = [57883]

save_matches_info(tournament_id, season_ids)
time.sleep(10)
for season_id in season_ids:
  save_player_stats(tournament_id, season_id)
time.sleep(10)
for season_id in season_ids:
  save_player_positions(tournament_id, season_id)
