from bs4 import BeautifulSoup
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from peewee import *
import json
import time
import string


driver = webdriver.Firefox()
driver.get('http://google.com')
db = SqliteDatabase('fifa.db')
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
    fh_pace = IntegerField(null=True)
    fh_accleration = IntegerField(null=True)
    fh_sprint_speed = IntegerField(null=True)

    # SHOOTING
    fh_shooting = IntegerField(null=True)
    fh_positioning = IntegerField(null=True)
    fh_finishing = IntegerField(null=True)
    fh_shot_power = IntegerField(null=True)
    fh_long_shots = IntegerField(null=True)
    fh_volleys = IntegerField(null=True)
    fh_penalties = IntegerField(null=True)

    # PASSING
    fh_passing = IntegerField(null=True)
    fh_vision = IntegerField(null=True)
    fh_crossing = IntegerField(null=True)
    fh_free_kick = IntegerField(null=True)
    fh_short_passing = IntegerField(null=True)
    fh_long_passing = IntegerField(null=True)
    fh_curve = IntegerField(null=True)

    # DRIBBLING
    fh_dribbling = IntegerField(null=True)
    fh_agility = IntegerField(null=True)
    fh_balance = IntegerField(null=True)
    fh_reactions = IntegerField(null=True)
    fh_ball_control = IntegerField(null=True)
    fh_dribbling_min = IntegerField(null=True)
    fh_composure = IntegerField(null=True)

    # DEFENSE
    fh_defense = IntegerField(null=True)
    fh_interceptions = IntegerField(null=True)
    fh_heading = IntegerField(null=True)
    fh_def_awareness = IntegerField(null=True)
    fh_standing_tackle = IntegerField(null=True)
    fh_sliding_tackle = IntegerField(null=True)

    # PHYSICAL
    fh_physical = IntegerField(null=True)
    fh_jumping = IntegerField(null=True)
    fh_stamina = IntegerField(null=True)
    fh_strength = IntegerField(null=True)
    fh_aggression = IntegerField(null=True)

    # OVERALL
    fh_overall_score = IntegerField(null=True)


def get_subcat_json(subcategory, stageId):
    payload = {
        'category': 'summary',
        'subcategory': subcategory,
        'statsAccumulationType': '0',
        'isCurrent': 'true',
        'playerId': '',
        'teamIds': '',
        'matchId': '',
        'stageId': stageId,
        'tournamentOptions': '',
        'sortBy': 'Rating',
        'sortAscending': '',
        'age': '',
        'ageComparisonType': '',
        'appearances': '',
        'appearancesComparisonType': '',
        'field': 'Overall',
        'nationality': '',
        'positionOptions': '',
        'timeOfTheGameEnd': '',
        'timeOfTheGameStart': '',
        'isMinApp': 'false',
        'page': '1',
        'includeZeroValues': 'true',
        'numberOfPlayersToPick': '1',
    }
    link = 'https://www.whoscored.com/StatisticsFeed/1/GetPlayerStatistics?'
    for key in payload:
        link += key + '=' + payload[key] + '&'
    link = link[:-1]
    driver.get(link)
    json_body = json.loads(driver.find_element(By.CSS_SELECTOR, 'body').text)
    total_results = json_body['paging']['totalResults']
    link = link[:-1] + str(total_results)
    driver.get(link)
    json_body = json.loads(driver.find_element(By.CSS_SELECTOR, 'body').text)
    return json_body


def load_stageId_files():
    with open('stageID.txt') as fp:
        output_dict = {}
        currKey = None
        for line in fp:
            if line[0].isalpha():
                currKey = line.strip().strip(string.punctuation)
                output_dict[currKey] = {}
            elif line.isspace():
                continue
            else:
                season, stageId = line.split()
                season = season.strip()[:-1]
                stageId = stageId.strip()
                output_dict[currKey][season]= stageId
    return output_dict


def merge_summaries(summary_subcat, summary_def, summary_off, summary_pass):
    summary_subcat = summary_subcat['playerTableStats']
    summary_def = summary_def['playerTableStats']
    summary_off = summary_off['playerTableStats']
    summary_pass = summary_pass['playerTableStats']

    merged_final_dict = {}
    for player in summary_subcat:
        merged_final_dict[player['playerId']] = {key: value for key, value in player.items()}
    for player in summary_def:
        if player['playerId'] in merged_final_dict:
            for key in player:
                if key not in merged_final_dict[player['playerId']]:
                    merged_final_dict[player['playerId']][key] = player[key]
        else:
            raise Exception('Mismatch between players in each table')
    for player in summary_off:
        if player['playerId'] in merged_final_dict:
            for key in player:
                if key not in merged_final_dict[player['playerId']]:
                    merged_final_dict[player['playerId']][key] = player[key]
        else:
            raise Exception('Mismatch between players in each table')
    for player in summary_pass:
        if player['playerId'] in merged_final_dict:
            for key in player:
                if key not in merged_final_dict[player['playerId']]:
                    merged_final_dict[player['playerId']][key] = player[key]
        else:
            raise Exception('Mismatch between players in each table')

    return merged_final_dict
    

def get_actual_player_info(player):
    base_url = 'https://www.futhead.com'
    specific_player_url = base_url + player.a['href']
    specific_page = BeautifulSoup(requests.get(specific_player_url).text, 'lxml')
    player_name = specific_page.select_one('ul.nav.pull-left.hidden-sm.hidden-xs li.dropdown.active').get_text().strip().lower()
    player_info_dict = {}
    print(f'Collecting Futhead data for {player_name}...')
    card_years = specific_page.select('li.media.list-group-item div.row')
    if len(card_years) > 4:
        card_years = card_years[:4]
    for card_row in card_years:
        gold_card = card_row.select_one('a')
        year_card_link = base_url + gold_card['href']
        year_card_page = BeautifulSoup(requests.get(year_card_link).text, 'lxml')

        player_year = int('20' + year_card_page.select('span[itemprop=title]')[1].get_text().strip()) # Should be year
        player_info_dict[player_year] = {}
        player_big_stats = year_card_page.select_one('div.playercard.card-large').get_text()
        
        # Get big stats
        for line in player_big_stats.splitlines():
            if line.strip() == '':
                continue
            else:
                line = line.strip().split()
                if len(line) == 1:
                    if line[0].isnumeric() == True and 'fh_overall_score' not in player_info_dict[player_year]:
                        player_info_dict[player_year]['fh_overall_score'] = int(line[0])
                elif len(line) == 2:
                    if line[0].isalpha() == False:
                        if line[1].strip() == 'PAC':
                            player_info_dict[player_year]['fh_pace'] = int(line[0])
                        elif line[1].strip() == 'SHO':
                            player_info_dict[player_year]['fh_shooting'] = int(line[0])
                        elif line[1].strip() == 'PAS':
                            player_info_dict[player_year]['fh_passing'] = int(line[0])
                        elif line[1].strip() == 'DRI':
                            player_info_dict[player_year]['fh_dribbling'] = int(line[0])
                        elif line[1].strip() == 'DEF':
                            player_info_dict[player_year]['fh_defense'] = int(line[0])
                        elif line[1].strip() == 'PHY':
                            player_info_dict[player_year]['fh_physical'] = int(line[0])
        
        # Get sub stats
        player_sub_stats = year_card_page.select('div.col-lg-2.col-sm-4.col-xs-6')
        pace_sub, sho_sub, pass_sub, dribb_sub, def_sub, phys_sub = player_sub_stats

        pace_sub = pace_sub.get_text().splitlines()
        input_num = 0
        for line in pace_sub:
            if line.strip() == '':
                continue
            else:
                line = line.strip().lower().replace(" ", "")
                if line.isalpha() == False:
                    if input_num == 0:
                        player_info_dict[player_year]['fh_acceleration'] = int(line)
                        input_num += 1
                    elif input_num == 1:
                        player_info_dict[player_year]['fh_sprint_speed'] = int(line)
                        input_num += 1
        
        sho_sub = sho_sub.get_text().splitlines()
        input_num = 0
        for line in sho_sub:
            if line.strip() == '':
                continue
            else:
                line = line.strip().lower().replace(" ", "")
                if line.isalpha() == False:
                    if input_num == 0:
                        player_info_dict[player_year]['fh_positioning'] = int(line)
                        input_num += 1
                    elif input_num == 1:
                        player_info_dict[player_year]['fh_finishing'] = int(line)
                        input_num += 1
                    elif input_num == 2:
                        player_info_dict[player_year]['fh_shot_power'] = int(line)
                        input_num += 1
                    elif input_num == 3:
                        player_info_dict[player_year]['fh_long_shots'] = int(line)
                        input_num += 1
                    elif input_num == 4:
                        player_info_dict[player_year]['fh_volleys'] = int(line)
                        input_num += 1
                    elif input_num == 5:
                        player_info_dict[player_year]['fh_penalties'] = int(line)
                        input_num += 1
        
        pass_sub = pass_sub.get_text().splitlines()
        input_num = 0
        for line in pass_sub:
            if line.strip() == '':
                continue
            else:
                line = line.strip().lower().replace(" ", "")
                if line.isalpha() == False:
                    if input_num == 0:
                        player_info_dict[player_year]['fh_vision'] = int(line)
                        input_num += 1
                    elif input_num == 1:
                        player_info_dict[player_year]['fh_crossing'] = int(line)
                        input_num += 1
                    elif input_num == 2:
                        player_info_dict[player_year]['fh_free_kick'] = int(line)
                        input_num += 1
                    elif input_num == 3:
                        player_info_dict[player_year]['fh_short_passing'] = int(line)
                        input_num += 1
                    elif input_num == 4:
                        player_info_dict[player_year]['fh_long_passing'] = int(line)
                        input_num += 1
                    elif input_num == 5:
                        player_info_dict[player_year]['fh_curve'] = int(line)
                        input_num += 1
        
        dribb_sub = dribb_sub.get_text().splitlines()
        input_num = 0
        for line in dribb_sub:
            if line.strip() == '':
                continue
            else:
                line = line.strip().lower().replace(" ", "")
                if line.isalpha() == False:
                    if input_num == 0:
                        player_info_dict[player_year]['fh_agility'] = int(line)
                        input_num += 1
                    elif input_num == 1:
                        player_info_dict[player_year]['fh_balance'] = int(line)
                        input_num += 1
                    elif input_num == 2:
                        player_info_dict[player_year]['fh_reactions'] = int(line)
                        input_num += 1
                    elif input_num == 3:
                        player_info_dict[player_year]['fh_ball_control'] = int(line)
                        input_num += 1
                    elif input_num == 4:
                        player_info_dict[player_year]['fh_dribbling_min'] = int(line)
                        input_num += 1
                    elif input_num == 5:
                        player_info_dict[player_year]['fh_composure'] = int(line)
                        input_num += 1
        
        def_sub = def_sub.get_text().splitlines()
        input_num = 0
        for line in def_sub:
            if line.strip() == '':
                continue
            else:
                line = line.strip().lower().replace(" ", "").replace(".", "")
                if line.isalpha() == False:
                    if input_num == 0:
                        player_info_dict[player_year]['fh_interceptions'] = int(line)
                        input_num += 1
                    elif input_num == 1:
                        player_info_dict[player_year]['fh_heading'] = int(line)
                        input_num += 1
                    elif input_num == 2:
                        player_info_dict[player_year]['fh_marking'] = int(line)
                        input_num += 1
                    elif input_num == 3:
                        player_info_dict[player_year]['fh_standing_tackle'] = int(line)
                        input_num += 1
                    elif input_num == 4:
                        player_info_dict[player_year]['fh_sliding_tackle'] = int(line)
                        input_num += 1
        
        phys_sub = phys_sub.get_text().splitlines()
        input_num = 0
        for line in phys_sub:
            if line.strip() == '':
                continue
            else:
                line = line.strip().lower().replace(" ", "")
                if line.isalpha() == False:
                    if input_num == 0:
                        player_info_dict[player_year]['fh_jumping'] = int(line)
                        input_num += 1
                    elif input_num == 1:
                        player_info_dict[player_year]['fh_stamina'] = int(line)
                        input_num += 1
                    elif input_num == 2:
                        player_info_dict[player_year]['fh_strength'] = int(line)
                        input_num += 1
                    elif input_num == 3:
                        player_info_dict[player_year]['fh_aggression'] = int(line)
                        input_num += 1
    return player_info_dict


def get_fh_info():
    player_directory = {}
    link = 'https://www.futhead.com/players/?page='
    base_url = 'https://www.futhead.com'
    for i in range(1, 941):
        link_i = link + str(i)
        player_page = requests.get(link_i)
        print(f"Building Futhead directory on link {link_i}...")
        player_page = BeautifulSoup(player_page.text, 'lxml')
        player_elems = player_page.select('.content.player-item.font-24')
        player_elems = {x.select_one('.player-name').get_text().strip().lower(): x for x in player_elems if x.select_one('.player-name').get_text().strip() != ''}
        player_directory.update(player_elems)
    return player_directory


def model_build_players(super_summary, season_key, league_key, stageId, fh_basic_directory):
    with db.atomic():
        for player in super_summary:
            query = AllYearPlayerStats.select().where(User.player_id == player['playerId'])
            if query.exists():
                all_year_player_model = query.get()
            else:
                all_year_player_model = AllYearPlayerStats.create(
                    name = player['name'],
                    first_name = player['firstName'],
                    last_name = player['lastName'],
                    player_id = player['playerId'],
                    simple_league_name = league_key,
                    stage_id = stage_id,
                )
            player_name = player['name'].lower()
            if player_name not in fh_basic_directory:
                raise Exception("We're in trouble bub... Can't find the player in the futhead directory")
            print(f'Building models for player {player_name}...')
            
            fh_specific_player_data = get_actual_player_info(fh_basic_directory[player_name])

            player_stat_model = PlayerStats.create(
                base_player = all_year_player_model,
                base_year = int(season_key.split('/')[0]),
                name = player['name'],
                first_name = player['firstName'],
                last_name = player['lastName'],
                player_id = player['playerId'],
                season_name = player['seasonName'],
                region_name = player['tournamentRegionName'],
                tournament_name = player['tournamentName'],
                team_name = player['teamName'],
                team_region = player['teamRegionName'],
                age = player['age'],
                height = player['height'],
                weight = player['weight'],
                rank = player['ranking'],
                played_positions = player['playedPositions'],
                appearances = player['apps'],
                subs_on = player['subOn'],
                minutes_played = player['minsPlayed'],
                goals = player['goal'],
                assists_total = player['assistsTotal'],
                yellow_cards = player['yellowCard'],
                red_cards = player['redCard'],
                shots_per_game = player['shotsPerGame'],
                aerials_won_per_game = player['aerialWonPerGame'],
                man_of_match = player['manOfTheMatch'],
                pass_success = player['passSuccess'],

                tackles_per_game = player['tacklePerGame'],
                interceptions_per_game = player['interceptionPerGame'],
                fouls_per_game = player['foulsPerGame'],
                offsides_won_per_game = player['offsideWonPerGame'],
                was_dribbled_per_game = player['dribbleWonPerGame'],
                outfielder_blocked_per_game = player['outfielderBlockPerGame'],
                goal_own = player['goalOwn'],

                key_pass_per_game = player['keyPassPerGame'],
                dribbles_won_per_game = player['dribbleWonPerGame'],
                fouls_given_per_game = player['foulGivenPerGame'],
                offsides_given_per_game = player['offsideGivenPerGame'],
                dispossessed_per_game = player['dispossessedPerGame'],
                turnovers_per_game = player['turnoverPerGame'],

                total_passes_per_game = player['totalPassesPerGame'],
                accurate_crosses_per_game = player['accurateCrossesPerGame'],
                accurate_long_passes_per_game = player['accurateLongPassPerGame'],
                accurate_through_ball_per_game = player['accurateThroughBallPerGame'],

                fh_pace = fh_specific_player_data['fh_pace'],
                fh_acceleration = fh_specific_player_data['fh_acceleration'],
                fh_spring_speed = fh_specific_player_data['fh_sprint_speed'],
                
                fh_shooting = fh_specific_player_data['fh_shooting'],
                fh_positioning = fh_specific_player_data['fh_positioning'],
                fh_shot_power = fh_specific_player_data['fh_shot_power'],
                fh_long_shots = fh_specific_player_data['fh_long_shots'],
                fh_volleys = fh_specific_player_data['fh_volleys'],
                fh_penalties = fh_specific_player_data['fh_penalties'],
                
                fh_passing = fh_specific_player_data['fh_passing'],
                fh_vision = fh_specific_player_data['fh_vision'],
                fh_crossing = fh_specific_player_data['fh_crossing'],
                fh_free_kick = fh_specific_player_data['fh_free_kick'],
                fh_short_passing = fh_specific_player_data['fh_short_passing'],
                fh_long_passing = fh_specific_player_data['fh_long_passing'],
                fh_curve = fh_specific_player_data['fh_curve'],

                fh_dribbling = fh_specific_player_data['fh_dribbling'],
                fh_agility = fh_specific_player_data['fh_agility'],
                fh_balance = fh_specific_player_data['fh_balance'],
                fh_reactions = fh_specific_player_data['fh_reactions'],
                fh_ball_control = fh_specific_player_data['fh_ball_control'],
                fh_dribbling_min = fh_specific_player_data['fh_dribbling_min'],
                fh_composure = fh_specific_player_data['fh_composure'],
                
                fh_defense = fh_specific_player_data['fh_defense'],
                fh_interceptions = fh_specific_player_data['fh_interceptions'],
                fh_heading = fh_specific_player_data['fh_heading'],
                fh_def_awareness = fh_specific_player_data['fh_def_awareness'],
                fh_standing_tackle = fh_specific_player_data['fh_standing_tackle'],
                fh_sliding_tackle = fh_specific_player_data['fh_sliding_tackle'],

                fh_physical = fh_specific_player_data['fh_physical'],
                fh_jumping = fh_specific_player_data['fh_jumping'],
                fh_stamina = fh_specific_player_data['fh_stamina'],
                fh_strength = fh_specific_player_data['fh_strength'],
                fh_aggression = fh_specific_player_data['fh_aggression'],

                fh_overall_score = fh_specific_player_data['fh_overall_score'],
            )
            

def build_stage_players():
    fh_basic_directory = get_fh_info()
    stage_dicts = load_stageId_files()
    for league_key in stage_dicts:
        for season_key in stage_dicts[league_key]:
            stageId = stage_dicts[league_key][season_key]
            summary_subcat = get_subcat_json('all', stageId)
            summary_def = get_subcat_json('defensive', stageId)
            summary_off = get_subcat_json('offensive', stageId)
            summary_pass = get_subcat_json('passing', stageId)
            super_summary = merge_summaries(summary_subcat, summary_def, summary_off, summary_pass)
            model_build_players(super_summary, season_key, league_key, stageId, fh_basic_directory)

db.create_tables([PlayerBase, AllYearPlayerStats, PlayerStats])

try:
    build_stage_players()
    # player_fh_dict = get_fh_info()
    # with open('testing_da_dict.txt', mode='w') as fp:
    #     fp.write(str(player_fh_dict))
    # build_stage_players()
finally:
    driver.quit()
    print('ALL DONE')

