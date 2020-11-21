import json
from pathlib import Path
import hashlib
from unidecode import unidecode
import peewee as pw
import numpy as np
import pickle
import re
from playhouse.dataset import DataSet
import pandas as pd
import numpy as np
import sqlite3
from sklearn import linear_model

"""
Class BasePlayer()
Description: Base class for season and player values.
"""
class BasePlayer(pw.Model):
    class Meta:
        database = pw.SqliteDatabase('fifa.db')


"""
Class PlayerStatistics
Description: PlayerStatistics class contains all players, with all of their real life and FIFA statistics.
             Is season specific, and may be duplicated across years.
"""
class PlayerStatistics(BasePlayer):
    # Base player generic information
    # Year
    fifa_year = pw.IntegerField()
    name = pw.CharField()
    first_name = pw.CharField()
    last_name = pw.CharField()
    ws_team_name = pw.CharField()
    ws_tournament_name = pw.CharField()
    ws_player_id = pw.IntegerField()
    ws_season = pw.CharField()
    age = pw.IntegerField()
    height = pw.IntegerField()
    weight = pw.IntegerField()
    
    # Base Player 
    # Category Summary, Subcategory All
    position = pw.CharField()
    appearances = pw.IntegerField()
    subs_on = pw.IntegerField()
    min_played = pw.IntegerField()
    goal_per_game = pw.DoubleField()
    assists_total_per_game = pw.DoubleField()
    yellow_cards_per_game = pw.DoubleField()
    red_cards_per_game = pw.DoubleField()
    shots_per_game = pw.DoubleField()
    aerials_won_per_game = pw.DoubleField()
    man_of_match_per_game = pw.DoubleField()
    pass_success = pw.DoubleField()

    # Base player
    # Category Summary, Subcategory Defensive
    tackles_per_game = pw.DoubleField()
    interceptions_per_game = pw.DoubleField()
    fouls_per_game = pw.DoubleField()
    offsides_won_per_game = pw.DoubleField()
    clearance_per_game = pw.DoubleField()
    was_dribbled_per_game = pw.DoubleField()
    outfielder_blocked_per_game = pw.DoubleField()
    goal_own_per_game = pw.IntegerField()

    # Base player
    # Category Summary, Subcategory Offensive
    key_pass_per_game = pw.DoubleField()
    dribbles_won_per_game = pw.DoubleField()
    fouls_given_per_game = pw.DoubleField()
    offsides_given_per_game = pw.DoubleField()
    dispossessed_per_game = pw.DoubleField()
    turnovers_per_game = pw.DoubleField()

    # Base player
    # Category Summary, Subcategory Passing
    total_passes_per_game = pw.DoubleField()
    accurate_crosses_per_game = pw.DoubleField()
    accurate_long_passes_per_game = pw.DoubleField()
    accurate_through_ball_per_game = pw.DoubleField()

    # Futbin player
    # PACE
    fifa_pace = pw.IntegerField()
    fifa_acceleration = pw.IntegerField()
    fifa_sprint_speed = pw.IntegerField()

    # SHOOTING
    fifa_shooting = pw.IntegerField()
    fifa_positioning = pw.IntegerField()
    fifa_finishing = pw.IntegerField()
    fifa_shot_power = pw.IntegerField()
    fifa_long_shots = pw.IntegerField()
    fifa_volleys = pw.IntegerField()
    fifa_penalties = pw.IntegerField()

    # PASSING
    fifa_passing = pw.IntegerField()
    fifa_vision = pw.IntegerField()
    fifa_crossing = pw.IntegerField()
    fifa_free_kick = pw.IntegerField()
    fifa_short_passing = pw.IntegerField()
    fifa_long_passing = pw.IntegerField()
    fifa_curve = pw.IntegerField()

    # DRIBBLING
    fifa_dribbling = pw.IntegerField()
    fifa_agility = pw.IntegerField()
    fifa_balance = pw.IntegerField()
    fifa_reactions = pw.IntegerField()
    fifa_ball_control = pw.IntegerField()
    fifa_dribbling_min = pw.IntegerField()
    fifa_composure = pw.IntegerField()

    # DEFENSE
    fifa_defense = pw.IntegerField()
    fifa_marking = pw.IntegerField()
    fifa_interceptions = pw.IntegerField()
    fifa_heading_accuracy = pw.IntegerField()
    fifa_standing_tackle = pw.IntegerField()
    fifa_sliding_tackle = pw.IntegerField()

    # PHYSICAL
    fifa_physical = pw.IntegerField()
    fifa_jumping = pw.IntegerField()
    fifa_stamina = pw.IntegerField()
    fifa_strength = pw.IntegerField()
    fifa_aggression = pw.IntegerField()

    # OVERALL
    fifa_overall_score = pw.IntegerField()
    fifa_overall_category = pw.CharField()


"""
Db_Controller(): Actually generates and controls the database, exporting CSV files as needed.
"""
class Db_Controller():
    def __init__(self):
        self.db = pw.SqliteDatabase('fifa.db')
        self.db.connect()
        if not self.db.table_exists('playerstatistics') and not self.db.table_exists('season'):
            self.db.create_tables([PlayerStatistics])
        self.who_trimmed = json.load(open('init_data/trimmed_who_scored.json'))
        self.true_matches = self.get_true_matches()

    def get_true_matches(self):
        season_list = [
            '2021',
            '2020',
            '2019',
            '2018',
        ]
        true_matches = {}
        for season in season_list:
            verified_file = f'init_data/match_verified_fifa_{season}.pickle'
            verified_matches = pickle.load(open(verified_file, mode='rb'))
            true_matches[season] = {}
            for player_id in verified_matches:
                if verified_matches[player_id] is not False and player_id in self.who_trimmed[f"{int(season) - 2}/{int(season) - 1}"]:
                    true_matches[season][player_id] = verified_matches[player_id]
            print(f"Total True Matches: {len(true_matches[season])}")
        return true_matches
    
    def write_csv(self):
        conn = sqlite3.connect('fifa.db')
        x = pd.read_sql("SELECT * from playerstatistics", conn)
        x.to_csv('fifa_final.csv')
    
    def player_build(self):
        season_list = [
            '2021',
            '2020',
            '2019',
            '2018',
        ]
        with self.db.atomic():
            for season in season_list:
                for player_id in self.true_matches[season]:
                    
                    who_player = self.who_trimmed[f"{int(season) - 2}/{int(season) - 1}"][player_id]
                    print(f"Building player {who_player['name']} ...")
                    fifa_player = self.true_matches[season][player_id]

                    if fifa_player['acceleration'] == '' or fifa_player['composure'] == '':
                        print(f"Missing substats ...")
                        continue

                    if 'long_team_name' in who_player:
                        team_name = who_player['long_team_name']
                    else:
                        team_name = who_player['teamName']
                    
                    try:
                        player_height = int(fifa_player['height'])
                    except:
                        player_height = int(who_player['height'])
                    
                    # Hatem Ben Arfa
                    if fifa_player['age'] == '120':
                        fifa_player['age'] = 33
                        fifa_player['weight'] = 65
                    
                    # Tati Tulhuk
                    if int(fifa_player['weight']) == 0:
                        fifa_player['weight'] = 75
                    
                    PlayerStatistics.get_or_create(
                        fifa_year = int(season),
                        name = who_player['name'],
                        first_name = who_player['firstName'],
                        last_name = who_player['lastName'],
                        ws_team_name = team_name,
                        ws_tournament_name = who_player['tournamentName'],
                        ws_player_id = int(who_player['playerId']),
                        ws_season = who_player['seasonName'],
                        age = int(fifa_player['age']),
                        height = player_height,
                        weight = int(fifa_player['weight']),

                        position = fifa_player['player_position'],
                        appearances = int(who_player['apps']),
                        subs_on = int(who_player['subOn']),
                        min_played = int(who_player['minsPlayed']),
                        goal_per_game = float(who_player['goal']) / float(who_player['apps']),
                        assists_total_per_game = float(who_player['assistTotal']) / float(who_player['apps']), 
                        yellow_cards_per_game = float(who_player['yellowCard']) /  float(who_player['apps']),
                        red_cards_per_game = float(who_player['redCard']) / float(who_player['apps']) ,
                        shots_per_game = float(who_player['shotsPerGame']),
                        aerials_won_per_game = float(who_player['aerialWonPerGame']),
                        man_of_match_per_game = float(who_player['manOfTheMatch']) / float(who_player['apps']),
                        pass_success = float(who_player['passSuccess']),

                        tackles_per_game = float(who_player['tacklePerGame']),
                        interceptions_per_game = float(who_player['interceptionPerGame']),
                        fouls_per_game = float(who_player['foulsPerGame']),
                        offsides_won_per_game = float(who_player['offsideWonPerGame']),
                        clearance_per_game = float(who_player['clearancePerGame']),
                        was_dribbled_per_game = float(who_player['wasDribbledPerGame']),
                        outfielder_blocked_per_game = float(who_player['outfielderBlockPerGame']),
                        goal_own_per_game = float(who_player['goalOwn']) / float(who_player['apps']),
                        key_pass_per_game = float(who_player['keyPassPerGame']),
                        dribbles_won_per_game = float(who_player['dribbleWonPerGame']),
                        fouls_given_per_game = float(who_player['foulGivenPerGame']),
                        offsides_given_per_game = float(who_player['offsideGivenPerGame']),
                        dispossessed_per_game = float(who_player['dispossessedPerGame']),
                        turnovers_per_game = float(who_player['turnoverPerGame']),
                        total_passes_per_game = float(who_player['totalPassesPerGame']),
                        accurate_crosses_per_game = float(who_player['accurateCrossesPerGame']),
                        accurate_long_passes_per_game = float(who_player['accurateLongPassPerGame']),
                        accurate_through_ball_per_game = float(who_player['accurateThroughBallPerGame']),

                        # FUTBIN DATA
                        fifa_overall_score = int(fifa_player['overall_rating']),
                        fifa_overall_category = fifa_player['CARD_COLOR_RANK'],
                        fifa_pace = int(fifa_player['pac']),
                        fifa_shooting = int(fifa_player['sho']),
                        fifa_passing = int(fifa_player['pas']),
                        fifa_dribbling = int(fifa_player['dri']),
                        fifa_defense = int(fifa_player['def']),
                        fifa_physical = int(fifa_player['phy']),
                        fifa_acceleration = int(fifa_player['acceleration']),
                        fifa_aggression = int(fifa_player['aggression']),
                        fifa_agility = int(fifa_player['agility']),
                        fifa_balance = int(fifa_player['balance']),
                        fifa_ball_control = int(fifa_player['ball_control']),
                        fifa_crossing = int(fifa_player['crossing']),
                        fifa_curve = int(fifa_player['curve']),
                        fifa_dribbling_min = int(fifa_player['dribbling']),
                        fifa_heading_accuracy = int(fifa_player['heading_accuracy']),
                        fifa_interceptions = int(fifa_player['interceptions']),
                        fifa_jumping = int(fifa_player['jumping']),
                        fifa_long_passing = int(fifa_player['long_passing']),
                        fifa_long_shots = int(fifa_player['long_shots']),
                        fifa_marking = int(fifa_player['marking']),
                        fifa_penalties = int(fifa_player['penalties']),
                        fifa_positioning = int(fifa_player['positioning']),
                        fifa_reactions = int(fifa_player['reactions']),
                        fifa_short_passing = int(fifa_player['short_passing']),
                        fifa_free_kick = int(fifa_player['fk_accuracy']),
                        fifa_shot_power = int(fifa_player['shot_power']),
                        fifa_sliding_tackle = int(fifa_player['sliding_tackle']),
                        fifa_sprint_speed = int(fifa_player['sprint_speed']),
                        fifa_standing_tackle = int(fifa_player['standing_tackle']),
                        fifa_stamina = int(fifa_player['stamina']),
                        fifa_strength = int(fifa_player['strength']),
                        fifa_vision = int(fifa_player['vision']),
                        fifa_volleys = int(fifa_player['volleys']),
                        fifa_finishing = int(fifa_player['finishing']),
                        fifa_composure = int(fifa_player['composure']),
                    )


if __name__ == '__main__':
    # Generate the Db_Controller
    db_manager = Db_Controller()
    db_manager.player_build()
    db_manager.write_csv()
