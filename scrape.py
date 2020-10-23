from bs4 import BeautifulSoup
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from peewee import *
import json
import string
from pathlib import Path
import hashlib
import unidecode
from multiprocessing.pool import Pool

BEYBLADE_LEVEL = 64


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
    print(link)
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
                output_dict[currKey][season] = stageId
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
    

def get_actual_player_info(player_url):
    base_url = 'https://www.futhead.com'
    specific_player_url = base_url + player_url
    specific_page = BeautifulSoup(requests.get(specific_player_url).text, 'lxml')
    player_name = specific_page.select_one('ul.nav.pull-left.hidden-sm.hidden-xs li.dropdown.active a').get_text().strip().lower()
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
                        player_info_dict[player_year]['fh_def_awareness'] = int(line)
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
    player_elem_file = Path('fh_player_elem_json.json')
    if player_elem_file.is_file():
        player_elems = json.load(player_elem_file.open())
    else:
        dirroot = Path('fh_data/')
        if not dirroot.is_dir():
            dirroot.mkdir()
        player_directory = {}
        link = 'https://www.futhead.com/players/?page='
        base_url = 'https://www.futhead.com'
        player_elems = []
        for i in range(1, 941):
            link_i = link + str(i)
            hasher = hashlib.md5()
            hasher.update(link_i.encode('utf-8'))
            link_file = dirroot / (hasher.hexdigest() + '.txt')
            if link_file.is_file():
                player_page = link_file.read_text()
                print(f'Using cached Futhead directory for link {link_i}...')
            else:
                player_page = requests.get(link_i).text
                link_file.write_text(player_page)
                print(f'Downloading Futhead directory data for link {link_i}...')
            player_page = BeautifulSoup(player_page, 'lxml')
            player_elems += [x['href'] for x in player_page.select('.content.player-item.font-24 a')]
            json.dump(player_elems, player_elem_file.open())
    
    print("BOI, YOU GOT ", len(player_elems), " playing ball right now son. L.")
    b1_file = Path('batch_1.json')
    batch_1 = player_elems[:10000]
    b2_file = Path('batch_2.json')
    batch_2 = player_elems[10000:20000]
    b3_file = Path('batch_3.json')
    batch_3 = player_elems[20000:30000]
    b4_file = Path('batch_4.json')
    batch_4 = player_elems[30000:]

    with Pool(BEYBLADE_LEVEL) as pool:
        if b1_file.is_file():
            player_multi_data_1 = json.load(b1_file.open())
        else:
            player_multi_data_1 = pool.map(get_player_name_data, batch_1)
            player_multi_data_1 = dict(player_multi_data_1)
            json.dump(b1_file.open())
        
        if b2_file.is_file():
            player_multi_data_2 = json.load(b2_file.open())
        else:
            player_multi_data_2 = pool.map(get_player_name_data, batch_2)
            player_multi_data_2 = dict(player_multi_data_2)
            json.dump(b2_file.open())
        
        if b3_file.is_file():
            player_multi_data_3 = json.load(b3_file.open())
        else:
            player_multi_data_3 = pool.map(get_player_name_data, batch_3)
            player_multi_data_3 = dict(player_multi_data_3)
            json.dump(b3_file.open())
        
        if b4_file.is_file():
            player_multi_data_4 = json.load(b4_file.open())
        else:
            player_multi_data_4 = pool.map(get_player_name_data, batch4)
            player_multi_data_4 = dict(player_multi_data_4)
            json.dump(b4_file.open())

    player_directory = {**player_multi_data_1, **player_multi_data_2, **player_multi_data_3, **player_multi_data_4}
    return player_directory


def get_player_name_data(player_url):
    header = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36'}
    base_url = 'https://www.futhead.com'
    player_link = base_url + player_url
    player_link_data = requests.get(player_link, headers=header)
    if player_link_data.status_code != requests.codes.ok:
        return None
    card_page_link = base_url + BeautifulSoup(player_link_data.text, 'lxml').select_one('li.media.list-group-item div.row a')['href']
    print(f'Getting player names while building Futhead directory data for link {card_page_link}...')
    player_name_req = requests.get(card_page_link, headers=header)
    if player_name_req.status_code != requests.codes.ok:
        raise Exception(f'ERROR: URL retrieval failed for player name at {player_name_req.url}, error {player_name_req.status_code}.')
    player_name = BeautifulSoup(player_name_req.text, 'lxml').select_one('.row div.font-16.fh-red a')
    if player_name is None:
        print(BeautifulSoup(player_name_req.text, 'lxml').select_one('.row div.font-16.fh-red'))
        raise Exception(f'ERROR: Page parsing for player failed at {player_name_req.url}')
    player_name = player_name.get_text().strip()
    if player_name is None:
        print(BeautifulSoup(player_name_req.text, 'lxml').select_one('.row div.font-16.fh-red'))
        raise Exception(f'Ya done fucked up the name chief at {player_name_req.url}')
    player_name = unidecode.unidecode(player_name).lower()
    return (player_name, player_link)


def model_build_players(super_summary, season_key, league_key, stageId, fh_basic_directory):
    with db.atomic():
        for player in super_summary:
            query = AllYearPlayerStats.select().where(AllYearPlayerStats.player_id == player)
            if query.exists():
                all_year_player_model = query.get()
            else:
                name = unidecode.unidecode(super_summary[player]['firstName'] + " " + super_summary[player]['lastName']).strip().lower()
                all_year_player_model = AllYearPlayerStats.create(
                    name = name,
                    first_name = super_summary[player]['firstName'],
                    last_name = super_summary[player]['lastName'],
                    player_id = super_summary[player]['playerId'],
                    simple_league_name = league_key,
                    stage_id = stageId,
                )

            player_name = unidecode.unidecode(super_summary[player]['firstName'] + " " + super_summary[player]['lastName']).strip().lower()
            if player_name not in fh_basic_directory:
                raise Exception("We're in trouble bub... Can't find the player in the futhead directory")
            
            print(f'Building models for player {player_name}...')
            
            fh_specific_player_data = get_actual_player_info(fh_basic_directory[player_name])
            print(fh_specific_player_data.keys())
            player_yr = int(season_key.split('/')[1]) + 1

            player_stat_model = PlayerStats.create(
                base_player = all_year_player_model,
                base_year = int(season_key.split('/')[0]),
                name = player_name,
                first_name = super_summary[player]['firstName'],
                last_name = super_summary[player]['lastName'],
                player_id = super_summary[player]['playerId'],
                season_name = super_summary[player]['seasonName'],
                region_name = super_summary[player]['tournamentRegionName'],
                tournament_name = super_summary[player]['tournamentName'],
                team_name = super_summary[player]['teamName'],
                team_region = super_summary[player]['teamRegionName'],
                age = super_summary[player]['age'],
                height = super_summary[player]['height'],
                weight = super_summary[player]['weight'],
                rank = super_summary[player]['ranking'],
                played_positions = super_summary[player]['playedPositions'],
                appearances = super_summary[player]['apps'],
                subs_on = super_summary[player]['subOn'],
                minutes_played = super_summary[player]['minsPlayed'],
                goals = super_summary[player]['goal'],
                assists_total = super_summary[player]['assistTotal'],
                yellow_cards = super_summary[player]['yellowCard'],
                red_cards = super_summary[player]['redCard'],
                shots_per_game = super_summary[player]['shotsPerGame'],
                aerials_won_per_game = super_summary[player]['aerialWonPerGame'],
                man_of_match = super_summary[player]['manOfTheMatch'],
                pass_success = super_summary[player]['passSuccess'],

                tackles_per_game = super_summary[player]['tacklePerGame'],
                interceptions_per_game = super_summary[player]['interceptionPerGame'],
                fouls_per_game = super_summary[player]['foulsPerGame'],
                offsides_won_per_game = super_summary[player]['offsideWonPerGame'],
                was_dribbled_per_game = super_summary[player]['dribbleWonPerGame'],
                outfielder_blocked_per_game = super_summary[player]['outfielderBlockPerGame'],
                goal_own = super_summary[player]['goalOwn'],

                key_pass_per_game = super_summary[player]['keyPassPerGame'],
                dribbles_won_per_game = super_summary[player]['dribbleWonPerGame'],
                fouls_given_per_game = super_summary[player]['foulGivenPerGame'],
                offsides_given_per_game = super_summary[player]['offsideGivenPerGame'],
                dispossessed_per_game = super_summary[player]['dispossessedPerGame'],
                turnovers_per_game = super_summary[player]['turnoverPerGame'],

                total_passes_per_game = super_summary[player]['totalPassesPerGame'],
                accurate_crosses_per_game = super_summary[player]['accurateCrossesPerGame'],
                accurate_long_passes_per_game = super_summary[player]['accurateLongPassPerGame'],
                accurate_through_ball_per_game = super_summary[player]['accurateThroughBallPerGame'],

                fh_pace = fh_specific_player_data[player_yr]['fh_pace'],
                fh_acceleration = fh_specific_player_data[player_yr]['fh_acceleration'],
                fh_spring_speed = fh_specific_player_data[player_yr]['fh_sprint_speed'],
                
                fh_shooting = fh_specific_player_data[player_yr]['fh_shooting'],
                fh_positioning = fh_specific_player_data[player_yr]['fh_positioning'],
                fh_shot_power = fh_specific_player_data[player_yr]['fh_shot_power'],
                fh_long_shots = fh_specific_player_data[player_yr]['fh_long_shots'],
                fh_volleys = fh_specific_player_data[player_yr]['fh_volleys'],
                fh_penalties = fh_specific_player_data[player_yr]['fh_penalties'],
                
                fh_passing = fh_specific_player_data[player_yr]['fh_passing'],
                fh_vision = fh_specific_player_data[player_yr]['fh_vision'],
                fh_crossing = fh_specific_player_data[player_yr]['fh_crossing'],
                fh_free_kick = fh_specific_player_data[player_yr]['fh_free_kick'],
                fh_short_passing = fh_specific_player_data[player_yr]['fh_short_passing'],
                fh_long_passing = fh_specific_player_data[player_yr]['fh_long_passing'],
                fh_curve = fh_specific_player_data[player_yr]['fh_curve'],

                fh_dribbling = fh_specific_player_data[player_yr]['fh_dribbling'],
                fh_agility = fh_specific_player_data[player_yr]['fh_agility'],
                fh_balance = fh_specific_player_data[player_yr]['fh_balance'],
                fh_reactions = fh_specific_player_data[player_yr]['fh_reactions'],
                fh_ball_control = fh_specific_player_data[player_yr]['fh_ball_control'],
                fh_dribbling_min = fh_specific_player_data[player_yr]['fh_dribbling_min'],
                fh_composure = fh_specific_player_data[player_yr]['fh_composure'],
                
                fh_defense = fh_specific_player_data[player_yr]['fh_defense'],
                fh_interceptions = fh_specific_player_data[player_yr]['fh_interceptions'],
                fh_heading = fh_specific_player_data[player_yr]['fh_heading'],
                fh_def_awareness = fh_specific_player_data[player_yr]['fh_def_awareness'],
                fh_standing_tackle = fh_specific_player_data[player_yr]['fh_standing_tackle'],
                fh_sliding_tackle = fh_specific_player_data[player_yr]['fh_sliding_tackle'],

                fh_physical = fh_specific_player_data[player_yr]['fh_physical'],
                fh_jumping = fh_specific_player_data[player_yr]['fh_jumping'],
                fh_stamina = fh_specific_player_data[player_yr]['fh_stamina'],
                fh_strength = fh_specific_player_data[player_yr]['fh_strength'],
                fh_aggression = fh_specific_player_data[player_yr]['fh_aggression'],

                fh_overall_score = fh_specific_player_data[player_yr]['fh_overall_score'],
            )
            

def build_stage_players(fh_basic_directory):
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
    fh_data_file = Path('fh_data_json.json',)
    if fh_data_file.is_file():
        fh_directory = json.load(fh_data_file.open())
    else:
        fh_directory = get_fh_info()
        json.dump(fh_directory, fh_data_file.open())
    # build_stage_players(fh_directory)
finally:
    driver.quit()
    print('ALL DONE')

