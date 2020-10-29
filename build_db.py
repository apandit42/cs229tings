from peewee import *
import json
import unidecode

FP = 'init_data/'
REAL_DATA_FP = FP + 'who_scored_data.json'
FIFA_DATA_FP = FP + 'fut_bin_data.json'


db = SqliteDatabase('fifa.db', pragmas={
    'journal_mode': 'wal',
    'cache_size': -1 * 64000,  # 64MB
    'foreign_keys': 1,
    'ignore_check_constraints': 0,
    'synchronous': 0})

db.connect()


class PlayerBase(Model):
    class Meta:
        database = db


class AllYearPlayerStats(PlayerBase):
    # Overall Player, this is info that stays constant
    name = CharField()
    first_name = CharField()
    last_name = CharField()
    player_id = IntegerField()
    simple_league_name = CharField()
    stage_id = IntegerField()


class PlayerStats(PlayerBase):
    # Link back to all years
    base_player = ForeignKeyField(AllYearPlayerStats, backref='base_players')
    base_year = IntegerField()

    # Base player
    # Generic player information
    name = CharField()
    first_name = CharField()
    last_name = CharField()
    player_id = IntegerField()
    season_name = CharField()
    region_name = CharField()
    tournament_name = CharField()
    team_name = CharField()
    team_region = CharField()
    age = IntegerField()
    height = IntegerField()
    weight = IntegerField()

    # Base Player
    # Category Summary, Subcategory All
    rank = IntegerField()
    played_positions = CharField()
    appearances = IntegerField()
    subs_on = IntegerField()
    minutes_played = IntegerField()
    goals = IntegerField()
    assists_total = IntegerField()
    yellow_cards = IntegerField()
    red_cards = IntegerField()
    shots_per_game = DoubleField()
    aerials_won_per_game = DoubleField()
    man_of_match = IntegerField()
    pass_success = DoubleField()

    # Base player
    # Category Summary, Subcategory Defensive
    tackles_per_game = IntegerField()
    interceptions_per_game = DoubleField()
    fouls_per_game = DoubleField()
    offsides_won_per_game = DoubleField()
    was_dribbled_per_game = DoubleField()
    outfielder_blocked_per_game = DoubleField()
    goal_own = IntegerField()

    # Base player
    # Category Summary, Subcategory Offensive
    key_pass_per_game = DoubleField()
    dribbles_won_per_game = DoubleField()
    fouls_given_per_game = DoubleField()
    offsides_given_per_game = DoubleField()
    dispossessed_per_game = DoubleField()
    turnovers_per_game = DoubleField()

    # Base player
    # Category Summary, Subcategory Passing
    total_passes_per_game = DoubleField()
    accurate_crosses_per_game = DoubleField()
    accurate_long_passes_per_game = DoubleField()
    accurate_through_ball_per_game = DoubleField()

    # FUTHEAD player
    # PACE
    fifa_pace = IntegerField(null=True)
    fifa_accleration = IntegerField(null=True)
    fifa_sprint_speed = IntegerField(null=True)

    # SHOOTING
    fifa_shooting = IntegerField(null=True)
    fifa_positioning = IntegerField(null=True)
    fifa_finishing = IntegerField(null=True)
    fifa_shot_power = IntegerField(null=True)
    fifa_long_shots = IntegerField(null=True)
    fifa_volleys = IntegerField(null=True)
    fifa_penalties = IntegerField(null=True)

    # PASSING
    fifa_passing = IntegerField(null=True)
    fifa_vision = IntegerField(null=True)
    fifa_crossing = IntegerField(null=True)
    fifa_free_kick = IntegerField(null=True)
    fifa_short_passing = IntegerField(null=True)
    fifa_long_passing = IntegerField(null=True)
    fifa_curve = IntegerField(null=True)

    # DRIBBLING
    fifa_dribbling = IntegerField(null=True)
    fifa_agility = IntegerField(null=True)
    fifa_balance = IntegerField(null=True)
    fifa_reactions = IntegerField(null=True)
    fifa_ball_control = IntegerField(null=True)
    fifa_dribbling_min = IntegerField(null=True)
    fifa_composure = IntegerField(null=True)

    # DEFENSE
    fifa_defense = IntegerField(null=True)
    fifa_interceptions = IntegerField(null=True)
    fifa_heading = IntegerField(null=True)
    fifa_def_awareness = IntegerField(null=True)
    fifa_standing_tackle = IntegerField(null=True)
    fifa_sliding_tackle = IntegerField(null=True)

    # PHYSICAL
    fifa_physical = IntegerField(null=True)
    fifa_jumping = IntegerField(null=True)
    fifa_stamina = IntegerField(null=True)
    fifa_strength = IntegerField(null=True)
    fifa_aggression = IntegerField(null=True)

    # OVERALL
    fifa_overall_score = IntegerField(null=True)

# Grabs stage IDs from local files


def load_stage_ids(file_path):
    with open(file_path) as file:
        stage_id_dict = {}
        currKey = None
        for line in file.readlines():
            if line[0].isalpha():
                currKey = line.strip()[:-1]
                stage_id_dict[currKey] = {}
            elif line.isspace():
                continue
            else:
                season, stage_id = line.split()
                season = season.strip()[:-1]
                stage_id = stage_id.strip()
                stage_id_dict[currKey][season] = stage_id
    return stage_id_dict

#


def get_fifa_data(player_query, fb_data, year):
    # Searches for a given instance of a player in a given year

    card_rank = {'gold', 'silver', 'bronze'}
    for rank in card_rank:
        player_list = fb_data[str(year)][rank]
        for player in player_list:
            if player['player_name'] == player_query:
                return player

# It is repreated code, but is meant to fulfil an error check while searching the directory
def player_in_db(player_query, fb_data):
    years = {'2018', '2019', '2020', '2021'}
    card_rank = {'gold', 'silver', 'bronze'}
    for year in years:
        for rank in card_rank:
            player_list = fb_data[year][rank]
            for player in player_list:
                if player['player_name'] == player_query:
                    return True
    return False

# A supersummary is <WHOSCOREDDATA>[league][seasonid] ***Take from superscraper v2


def model_build_players(super_summary, season_key, league_key, stageId, fb_data):
    with db.atomic():

        # Create player entry if none exists
        for player in super_summary:
            query = AllYearPlayerStats.select().where(
                AllYearPlayerStats.player_id == player)
            if query.exists():
                all_year_player_model = query.get()
            else:
                name = unidecode.unidecode(
                    super_summary[player]['firstName'] + " " + super_summary[player]['lastName']).strip().lower()
                all_year_player_model = AllYearPlayerStats.create(
                    name=name,
                    first_name=super_summary[player]['firstName'],
                    last_name=super_summary[player]['lastName'],
                    player_id=super_summary[player]['playerId'],
                    simple_league_name=league_key,
                    stage_id=stageId,
                )

            # Get name ready for player lookup (Was an online task, is now offline)
            player_name = unidecode.unidecode(super_summary[player]['firstName'].strip(
            ) + " " + super_summary[player]['lastName'].strip()).strip()
            player_name = player_name.replace('junior', 'jr.')
            if not player_in_db(player_name,fb_data) :
                # raise Exception(
                #     f"We're in trouble bub... Can't find the player {player_name} in the futhead directory")
                print(f"We're in trouble bub... Can't find the player {player_name} in the futhead directory")
                continue

            print(f'Building models for player {player_name}...')

            # Player is disgusting and plz ignore its presense. Its for the dictionary stuff
            player_yr = str(int(season_key.split('/')[1]) + 1)
            fb_player_data = get_fifa_data(player_name, fb_data, player_yr)
            if fb_player_data == None:
                print(f"Oof! Was {player_name} active in {player_yr}?")
                continue
            print(f"{player_name} added Successfully!")
   
            # Stats are added to the table
            player_stat_model = PlayerStats.create(
                base_player=all_year_player_model,
                base_year=int(season_key.split('/')[0]),
                name=player_name,
                first_name=super_summary[player]['firstName'],
                last_name=super_summary[player]['lastName'],
                player_id=super_summary[player]['playerId'],
                season_name=super_summary[player]['seasonName'],
                region_name=super_summary[player]['tournamentRegionName'],
                tournament_name=super_summary[player]['tournamentName'],
                team_name=super_summary[player]['teamName'],
                team_region=super_summary[player]['teamRegionName'],
                age=super_summary[player]['age'],
                height=super_summary[player]['height'],
                weight=super_summary[player]['weight'],
                rank=super_summary[player]['ranking'],
                played_positions=super_summary[player]['playedPositions'],
                appearances=super_summary[player]['apps'],
                subs_on=super_summary[player]['subOn'],
                minutes_played=super_summary[player]['minsPlayed'],
                goals=super_summary[player]['goal'],
                assists_total=super_summary[player]['assistTotal'],
                yellow_cards=super_summary[player]['yellowCard'],
                red_cards=super_summary[player]['redCard'],
                shots_per_game=super_summary[player]['shotsPerGame'],
                aerials_won_per_game=super_summary[player]['aerialWonPerGame'],
                man_of_match=super_summary[player]['manOfTheMatch'],
                pass_success=super_summary[player]['passSuccess'],

                tackles_per_game=super_summary[player]['tacklePerGame'],
                interceptions_per_game=super_summary[player]['interceptionPerGame'],
                fouls_per_game=super_summary[player]['foulsPerGame'],
                offsides_won_per_game=super_summary[player]['offsideWonPerGame'],
                was_dribbled_per_game=super_summary[player]['dribbleWonPerGame'],
                outfielder_blocked_per_game=super_summary[player]['outfielderBlockPerGame'],
                goal_own=super_summary[player]['goalOwn'],

                key_pass_per_game=super_summary[player]['keyPassPerGame'],
                dribbles_won_per_game=super_summary[player]['dribbleWonPerGame'],
                fouls_given_per_game=super_summary[player]['foulGivenPerGame'],
                offsides_given_per_game=super_summary[player]['offsideGivenPerGame'],
                dispossessed_per_game=super_summary[player]['dispossessedPerGame'],
                turnovers_per_game=super_summary[player]['turnoverPerGame'],

                total_passes_per_game=super_summary[player]['totalPassesPerGame'],
                accurate_crosses_per_game=super_summary[player]['accurateCrossesPerGame'],
                accurate_long_passes_per_game=super_summary[player]['accurateLongPassPerGame'],
                accurate_through_ball_per_game=super_summary[player]['accurateThroughBallPerGame'],

                # Fifa Data 
                # *** Strength Stat is missing from webscrape  ***
                # *** Volleys is also missing from webscrape ***
                # *** Vision is missing from webscrape *** 
                # *** Composure is missing from webscrape ***
                # *** Defensice Awareness is missing from webscrape ***
                
                fh_pace=fb_player_data['pac'],
                fh_acceleration=fb_player_data['acceleration'],
                fh_sprint_speed=fb_player_data['sprint_speed'],

                fh_shooting=fb_player_data['sho'],
                fh_positioning=fb_player_data['positioning'],
                fh_shot_power=fb_player_data['shot_power'],
                fh_long_shots=fb_player_data['long_shots'],
                # fh_volleys=fb_player_data['volleys'],
                fh_penalties=fb_player_data['penalties'],

                fh_passing=fb_player_data['pas'],
                # fh_vision=fb_player_data['vision'],
                fh_crossing=fb_player_data['crossing'],
                fh_free_kick_accuracy=fb_player_data['fk_accuracy'],
                fh_short_passing=fb_player_data['short_passing'],
                fh_long_passing=fb_player_data['long_passing'],
                fh_curve=fb_player_data['curve'],

                fh_dribbling=fb_player_data['dri'],
                fh_agility=fb_player_data['agility'],
                fh_balance=fb_player_data['balance'],
                fh_reactions=fb_player_data['reactions'],
                fh_ball_control=fb_player_data['ball_control'],
                fh_dribbling_min=fb_player_data['dribbling'],
                # fh_composure=fb_player_data['composure'],

                fh_defense=fb_player_data['def'],
                fh_interceptions=fb_player_data['interceptions'],
                fh_heading=fb_player_data['heading_accuracy'],
                # fh_def_awareness=fb_player_data['def_awareness'],
                fh_standing_tackle=fb_player_data['standing_tackle'],
                fh_sliding_tackle=fb_player_data['sliding_tackle'],

                fh_physical=fb_player_data['phy'],
                fh_jumping=fb_player_data['jumping'],
                fh_stamina=fb_player_data['stamina'],
                # fh_strength=fb_player_data['strength'],
                fh_aggression=fb_player_data['aggression'],

                # IDK What this stat is
                fh_marking=fb_player_data['marking'],

                fh_overall_score=fb_player_data['overall_rating'],
            )

# Pass in the whoscored data and get the individual super summaries


def build_stage_players(ws_data, fb_data):
    stage_dicts = load_stage_ids(FP + 'stage_ids.txt')
    for league_key in stage_dicts:
        for season_key in stage_dicts[league_key]:
            stageId = stage_dicts[league_key][season_key]
            super_summary = ws_data[league_key][season_key]
            print(len(super_summary.keys()))
            model_build_players(super_summary, season_key,
                                league_key, stageId, fb_data)


# Who Scored Data
ws_data = None
fb_data = None
with open(REAL_DATA_FP, 'r') as f:
    ws_data = json.load(f)

# FutBin Data
with open(FIFA_DATA_FP, 'r') as f:
    fb_data = json.load(f)

db.create_tables([PlayerBase, AllYearPlayerStats, PlayerStats])

build_stage_players(ws_data, fb_data)
