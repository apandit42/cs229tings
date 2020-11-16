from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
import json
from pathlib import Path
import hashlib
from unidecode import unidecode
import time
import random
import peewee as pw
import uuid
import numpy as np
import pickle
from multiprocessing import Pool

# Take 2 at this bungus

"""
Class WhoScoredData()
Description: Class that controls scraping for whoscored.com data. Builds out JSON file containing all data.
"""
class WhoScoredData():
    def __init__(self):
        trimmed_data = Path('init_data/trimmed_who_scored.json')
        if trimmed_data.is_file():
            self.trimmed_data = json.load(trimmed_data.open())
        else:
            data_path = Path('init_data/who_scored_data.json')
            if data_path.is_file():
                self.player_data = json.load(data_path.open())
            else:
                self.player_data = self.init_build_player_data()
                json.dump(self.player_data, data_path.open(mode='w'))
            self.trimmed_data = self.init_trim_data()
            json.dump(self.trimmed_data, trimmed_data.open(mode='w'))
            
    def init_build_player_data(self):
        # Selenium setup
        self.driver = webdriver.Firefox()
        self.driver.get('http://google.com')
        all_player_who_scored_data = {}
        stage_id_dict = self.load_stage_ids('init_data/stage_ids.txt')
        team_names = json.load(Path('init_data/team_name_data.json').open())
        for league_key in stage_id_dict:
            all_player_who_scored_data[league_key] = {}
            for season_key in stage_id_dict[league_key]:
                stage_id = stage_id_dict[league_key][season_key]
                summary_all, summary_def, summary_off, summary_pass = self.get_player_json(stage_id)
                team_key_int = abs(2019 - int(season_key.split('/')[0]))
                print(f'Team Key: {team_key_int}, League Key: {league_key}')
                team_names_subset = team_names[league_key][team_key_int]
                summary_dict = self.merge_summaries(summary_all, summary_def, summary_off, summary_pass, team_names_subset)
                all_player_who_scored_data[league_key][season_key] = summary_dict
        self.driver.quit()
        return all_player_who_scored_data
    
    def init_trim_data(self):
        trimmed_data = {}
        for league_key in self.player_data:
            for season_key in self.player_data[league_key]:
                if season_key not in trimmed_data:
                    trimmed_data[season_key] = {}
                for player in self.player_data[league_key][season_key]:
                    if player not in trimmed_data[season_key]:
                        trimmed_data[season_key][player] = self.player_data[league_key][season_key][player]
                    else:
                        duplicate_player = self.player_data[league_key][season_key][player]
                        existing_player = trimmed_data[season_key][player]
                        merged_data = self.merge_duplicate_players(duplicate_player, existing_player)
                        trimmed_data[season_key][player] = merged_data
        return trimmed_data
    
    def merge_duplicate_players(self, duplicate_player, existing_player):
        merged_data = {key: value for key, value in existing_player.items()}
        if duplicate_player['tournamentName'] == 'Champions League':
            merged_data['tournamentName'] = existing_player['tournamentName']
        elif existing_player['tournamentName'] == 'Champions League':
            merged_data['tournamentName'] = duplicate_player['tournamentName']
        if duplicate_player['playedPositions'] != existing_player['playedPositions']:
            merged_data['playedPositions'] += duplicate_player['playedPositions'][1:]
            merged_data['playedPositionsShort'] += '-' + duplicate_player['playedPositions']
        merged_data['apps'] += duplicate_player['apps']
        merged_data['subOn'] += duplicate_player['subOn']
        merged_data['manOfTheMatch'] += duplicate_player['manOfTheMatch']
        merged_data['goal'] += duplicate_player['goal']
        merged_data['assistTotal'] += duplicate_player['assistTotal']
        merged_data['shotsPerGame'] = (existing_player['shotsPerGame'] + duplicate_player['shotsPerGame']) / 2.0
        merged_data['aerialWonPerGame'] = (existing_player['aerialWonPerGame'] + duplicate_player['aerialWonPerGame']) / 2.0
        merged_data['rating'] = (existing_player['ranking'] + duplicate_player['ranking']) / 2.0
        merged_data['minsPlayed'] += duplicate_player['minsPlayed']
        merged_data['yellowCard'] += duplicate_player['yellowCard']
        merged_data['redCard'] += duplicate_player['redCard']
        merged_data['passSuccess'] = (existing_player['passSuccess'] + duplicate_player['passSuccess']) / 2.0
        merged_data['ranking'] = min(existing_player['ranking'], duplicate_player['ranking'])
        merged_data['tacklePerGame'] = (existing_player['tacklePerGame'] + duplicate_player['tacklePerGame']) / 2.0
        merged_data['interceptionPerGame'] = (existing_player['interceptionPerGame'] + duplicate_player['interceptionPerGame']) / 2.0
        merged_data['foulsPerGame'] = (existing_player['foulsPerGame'] + duplicate_player['foulsPerGame']) / 2.0
        merged_data['offsideWonPerGame'] = (existing_player['offsideWonPerGame'] + duplicate_player['offsideWonPerGame']) / 2.0
        merged_data['clearancePerGame'] = (existing_player['clearancePerGame'] + duplicate_player['clearancePerGame']) / 2.0
        merged_data['wasDribbledPerGame'] = (existing_player['wasDribbledPerGame'] + duplicate_player['wasDribbledPerGame']) / 2.0
        merged_data['outfielderBlockPerGame'] = (existing_player['outfielderBlockPerGame'] + duplicate_player['outfielderBlockPerGame']) / 2.0
        merged_data['goalOwn'] += duplicate_player['goalOwn']
        merged_data['keyPassPerGame'] = (existing_player['keyPassPerGame'] + duplicate_player['keyPassPerGame']) / 2.0
        merged_data['dribbleWonPerGame'] = (existing_player['dribbleWonPerGame'] + duplicate_player['dribbleWonPerGame']) / 2.0
        merged_data['foulGivenPerGame'] = (existing_player['foulGivenPerGame'] + duplicate_player['foulGivenPerGame']) / 2.0
        merged_data['offsideGivenPerGame'] = (existing_player['offsideGivenPerGame'] + duplicate_player['offsideGivenPerGame']) / 2.0
        merged_data['dispossessedPerGame'] = (existing_player['dispossessedPerGame'] + duplicate_player['dispossessedPerGame']) / 2.0
        merged_data['turnoverPerGame'] = (existing_player['turnoverPerGame'] + duplicate_player['turnoverPerGame']) / 2.0
        merged_data['totalPassesPerGame'] = (existing_player['totalPassesPerGame'] + duplicate_player['totalPassesPerGame']) / 2.0
        merged_data['accurateCrossesPerGame'] = (existing_player['accurateCrossesPerGame'] + duplicate_player['accurateCrossesPerGame']) / 2.0
        merged_data['accurateLongPassPerGame'] = (existing_player['accurateLongPassPerGame'] + duplicate_player['accurateLongPassPerGame']) / 2.0
        merged_data['accurateThroughBallPerGame'] = (existing_player['accurateThroughBallPerGame'] + duplicate_player['accurateThroughBallPerGame']) / 2.0
        return merged_data

    def load_stage_ids(self, file_path):
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
    
    def get_player_json(self, stage_id):
        subcategory_list = ['all', 'defensive', 'offensive', 'passing']
        payload = {
            'category': 'summary',
            'subcategory': '',
            'statsAccumulationType': '0',
            'isCurrent': 'true',
            'playerId': '',
            'teamIds': '',
            'matchId': '',
            'stageId': stage_id,
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
        all_json_results = []
        for subcategory in subcategory_list:
            payload['subcategory'] = subcategory
            link = 'https://www.whoscored.com/StatisticsFeed/1/GetPlayerStatistics?'
            for key in payload:
                link += key + '=' + payload[key] + '&'
            link = link[:-1]
            self.driver.get(link)
            json_body = json.loads(self.driver.find_element(By.CSS_SELECTOR, 'body').text)
            total_results = json_body['paging']['totalResults']
            link = link[:-1] + str(total_results)
            print(f'Scraping player link {link} ...')
            self.driver.get(link)
            json_body = json.loads(self.driver.find_element(By.CSS_SELECTOR, 'body').text)
            all_json_results.append(json_body)
        return all_json_results

    def merge_summaries(self, summary_all, summary_def, summary_off, summary_pass, team_names_subset):
        summary_all = summary_all['playerTableStats']
        summary_def = summary_def['playerTableStats']
        summary_off = summary_off['playerTableStats']
        summary_pass = summary_pass['playerTableStats']
        team_names = team_names_subset['teamTableStats']

        merged_final_dict = {}
        for player in summary_all:
            player_short_team_name = player['teamName']
            for team in team_names:
                try:
                    if team['name'] == player_short_team_name:
                        player['long_team_name'] = team['teamName']
                        break
                except KeyError:
                    team = team['teamTableStats'][0]
                    if team['name'] == player_short_team_name:
                        player['long_team_name'] = team['teamName']
                        break
            if 'long_team_name' not in player:
                raise Exception(f'No team match found, short ({player_short_team_name}), all teams \n{team_names}')
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
    
    def get_trimmed_player_count(self):
        total_players = 0
        for season in self.trimmed_data:
            total_players += len(self.trimmed_data[season].keys())
        return total_players


"""
Class FutBinData()
Description: Class that controls scraping for futbin.com data. Builds out JSON file containing all 
             data as it is scraped.
"""
class FutBinData():
    def __init__(self):
        data_path = Path('init_data/fut_bin_data.json')
        if data_path.is_file():
            self.player_data = json.load(data_path.open())
        else:
            self.player_data = self.init_build_player_data()
            json.dump(self.player_data, data_path.open(mode='w'))
    
    def init_build_player_data(self):
        self.driver = webdriver.Firefox()
        input('Ready to proceed?')
        # Collecting all of the links from each of the years
        year_list = ['21', '20', '19', '18']
        card_type_list = ['gold', 'silver', 'bronze']
        year_list_dict = {}
        for year in year_list:
            year_list_dict[f'20{year}'] = {}
            for card_type in card_type_list:
                all_page_urls = self.get_all_pages(year, card_type)
                player_data_list = self.generate_player_data(all_page_urls)
                year_list_dict[f'20{year}'][card_type] = player_data_list
        self.driver.quit()
        return year_list_dict

    def get_all_pages(self, year, card_type):
        base_page_url = self.construct_url(year, card_type)
        self.driver.get(base_page_url)
        total_page_num = self.driver.find_elements(By.CSS_SELECTOR, 'li.page-item a.page-link')[-2].text.strip()
        total_page_num = int(total_page_num)
        all_page_urls = []
        for i in range(1, total_page_num + 1):
            curr_page_url = self.construct_url(year, card_type, str(i))
            all_page_urls.append(curr_page_url)
        return all_page_urls
    
    def generate_player_data(self, all_page_urls):
        general_player_data = []
        for url in all_page_urls:
            player_data = self.scrape_page_data(url)
            for player in player_data:
                general_player_data.append(player)
        return general_player_data

    def scrape_page_data(self, page_url):
        print(f'Collecting player Futbin data from {page_url}...')
        hasher = hashlib.md5()
        hasher.update(page_url.encode('utf-8'))
        page_file = Path('futbin/' + hasher.hexdigest() + '.txt')
        if page_file.is_file():
            page = BeautifulSoup(page_file.read_text(),'lxml')
            if len(page.select('#repTb tbody tr')) == 0:
                page_file.unlink()
        if not page_file.is_file():
            time.sleep(random.randint(0, 1) + random.random())
            self.driver.get(page_url)
            page_file.write_text(self.driver.page_source)
            page = BeautifulSoup(self.driver.page_source, 'lxml')
        player_rows = page.select('#repTb tbody tr')
        page_player_data = []
        for player in player_rows:
            player_dict = {}
            player_row_data = player.select('td')
            player_dict['unique_player_id'] = str(uuid.uuid4())
            player_dict['player_name'] = player_row_data[0].get_text().strip()
            player_dict['player_club'] = player_row_data[0].select_one('span.players_club_nation a')['data-original-title'].strip()
            player_dict['overall_rating'] = player_row_data[1].get_text().strip()
            player_dict['player_position'] = player_row_data[2].get_text().strip()
            player_dict['pac'] = player_row_data[8].get_text().strip()
            player_dict['sho'] = player_row_data[9].get_text().strip()
            player_dict['pas'] = player_row_data[10].get_text().strip()
            player_dict['dri'] = player_row_data[11].get_text().strip()
            player_dict['def'] = player_row_data[12].get_text().strip()
            player_dict['phy'] = player_row_data[13].get_text().strip()
            try:
                player_dict['height'] = player_row_data[14].get_text().split()[0].replace('cm', '')
            except:
                player_dict['height'] = ''  # Something to consider when matching players...
            player_dict['age'] = player_row_data[18].get_text().strip()
            player_dict['weight'] = player_row_data[19].get_text().strip().replace('kg', '')
            player_dict['acceleration'] = player_row_data[20].get_text().strip()
            player_dict['aggression'] = player_row_data[21].get_text().strip()
            player_dict['agility'] = player_row_data[22].get_text().strip()
            player_dict['balance'] = player_row_data[23].get_text().strip()
            player_dict['ball_control'] = player_row_data[24].get_text().strip()
            player_dict['crossing'] = player_row_data[25].get_text().strip()
            player_dict['curve'] = player_row_data[26].get_text().strip()
            player_dict['dribbling'] = player_row_data[27].get_text().strip()
            player_dict['heading_accuracy'] = player_row_data[28].get_text().strip()
            player_dict['interceptions'] = player_row_data[29].get_text().strip()
            player_dict['jumping'] = player_row_data[30].get_text().strip()
            player_dict['long_passing'] = player_row_data[31].get_text().strip()
            player_dict['long_shots'] = player_row_data[32].get_text().strip()
            player_dict['marking'] = player_row_data[33].get_text().strip()
            player_dict['penalties'] = player_row_data[34].get_text().strip()
            player_dict['positioning'] = player_row_data[35].get_text().strip()
            player_dict['reactions'] = player_row_data[36].get_text().strip()
            player_dict['short_passing'] = player_row_data[37].get_text().strip()
            player_dict['fk_accuracy'] = player_row_data[38].get_text().strip()
            player_dict['shot_power'] = player_row_data[39].get_text().strip()
            player_dict['sliding_tackle'] = player_row_data[40].get_text().strip()
            player_dict['sprint_speed'] = player_row_data[41].get_text().strip()
            player_dict['standing_tackle'] = player_row_data[42].get_text().strip()
            player_dict['stamina'] = player_row_data[43].get_text().strip()
            player_dict['strength'] = player_row_data[44].get_text().strip()
            player_dict['vision'] = player_row_data[45].get_text().strip()
            player_dict['volleys'] = player_row_data[46].get_text().strip()
            player_dict['finishing'] = player_row_data[47].get_text().strip()
            player_dict['composure'] = player_row_data[48].get_text().strip()
            page_player_data.append(player_dict)
        return page_player_data

    def construct_url(self, year, card_type, page='1'):
        return f'https://www.futbin.com/{year}/players?page={page}&version={card_type}&showStats=Weight,Age,Acceleration,Sprintspeed,Positioning,Finishing,Shotpower,Longshots,Volleys,Penalties,Dribbling,Agility,Balance,Reactions,Ballcontrol,Composure,Interceptions,Headingaccuracy,Marking,Standingtackle,Slidingtackle,Vision,Crossing,Freekickaccuracy,Shortpassing,Longpassing,Curve,Jumping,Stamina,Strength,Aggression'

    def get_player_count(self):
        total_players = 0
        for year in self.player_data:
            for card_type in self.player_data[year]:
                total_players += len(self.player_data[year][card_type])
        return total_players
    
    def club_name_dict(self):
        player_club_names = {}
        for year in self.player_data:
            player_club_names[year] = {}
            for card_type in self.player_data[year]:
                name_set = set()
                for player in self.player_data[year][card_type]:
                    name_set.add(player['player_club'])
                player_club_names[year][card_type] = name_set
        return player_club_names
    
    def get_player_by_name(self, name, year, club, weight=None):
        club = club.strip()
        data = self.player_data[year]
        for card_type in data:
            for player in data[card_type]:
                if player['player_name'] == name and player['player_club'] == club:
                    if weight is None:
                        print(f'Player {name} found in {card_type} for {year}...')
                        return player
                    elif player['weight'] == weight:
                        print(f'Player {name} found in {card_type} for {year}...')
                        return player
        return None


"""
Class BasePlayer()
Description: Base class for season and player values.
"""
class BasePlayer(pw.Model):
    class Meta:
        database = pw.SqliteDatabase('fifa.db')


"""
Class Season()
Description: Season class contains all players from the same season.
"""
class Season(BasePlayer):
    season_name = pw.CharField()
    base_year = pw.IntegerField()


"""
Class PlayerStatistics
Description: PlayerStatistics class contains all players, with all of their real life and FIFA statistics.
             Is season specific, and may be duplicated across years.
"""
class PlayerStatistics(BasePlayer):
    # Season() Connections
    season = pw.ForeignKeyField(model=Season, backref='players')
    base_year = pw.IntegerField()

    # Base player
    # Generic player information
    name = pw.CharField()
    first_name = pw.CharField()
    last_name = pw.CharField()
    player_id = pw.IntegerField()
    season_name = pw.CharField()
    region_name = pw.CharField()
    tournament_name = pw.CharField()
    team_name = pw.CharField()
    team_region = pw.CharField()
    age = pw.IntegerField()
    height = pw.IntegerField()
    weight = pw.IntegerField()
    
    # Base Player 
    # Category Summary, Subcategory All
    rank = pw.IntegerField()    
    played_positions = pw.CharField()
    appearances = pw.IntegerField()
    subs_on = pw.IntegerField()
    minutes_played = pw.IntegerField()
    goals = pw.IntegerField()
    assists_total = pw.IntegerField()
    yellow_cards = pw.IntegerField()
    red_cards = pw.IntegerField()
    shots_per_game = pw.DoubleField()
    aerials_won_per_game = pw.DoubleField()
    man_of_match = pw.IntegerField()
    pass_success = pw.DoubleField()

    # Base player
    # Category Summary, Subcategory Defensive
    tackles_per_game = pw.IntegerField()
    interceptions_per_game = pw.DoubleField()
    fouls_per_game = pw.DoubleField()
    offsides_won_per_game = pw.DoubleField()
    was_dribbled_per_game = pw.DoubleField()
    outfielder_blocked_per_game = pw.DoubleField()
    goal_own = pw.IntegerField()

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
    fifa_accleration = pw.IntegerField()
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
    fifa_interceptions = pw.IntegerField()
    fifa_heading = pw.IntegerField()
    fifa_def_awareness = pw.IntegerField()
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
    fifa_overall_category = pw.IntegerField()


"""
Class DbManager()
Description: Class that manages creating and populating the database. Builds and verifies database integrity.
"""
class DbManager():
    def __init__(self, who_scored_data=None, fifa_card_data=None):
        if who_scored_data is not None:
            self.who_trimmed = who_scored_data.trimmed_data
        if fifa_card_data is not None:
            self.fifa_card_data = fifa_card_data.player_data
        self.team_translation = json.load(open('init_data/team_translation.json'))
        self.team_translation = self.parse_team_translation()
    
    def optimize_db(self):
        season_list = [
            '2019/2020',
            # '2018/2019',
            # '2017/2018',
            # '2016/2017',
        ]
        for season in season_list:
            verified_filename = f'match_verified_fifa_{str(int(season[-4:]) + 1)}'
            verified_filename = Path('init_data/' + verified_filename + '.pickle')
            if verified_filename.is_file():
                verified_match_obj = pickle.load(verified_filename.open('rb'))
            else:
                verified_match_obj = {}
            REAL_RUNTIME = 0
            total_players = len(self.who_trimmed[season])
            for player_id in self.who_trimmed[season]:
                if player_id in verified_match_obj:
                    REAL_RUNTIME += 1
                    continue
                hasher = hashlib.md5()
                file_id = season + player_id
                hasher.update(file_id.encode('utf-8'))
                file_path = Path('match_data/' + hasher.hexdigest() + 'raw.pickle')
                fifa_match_list = pickle.load(file_path.open(mode='rb'))
                if len(fifa_match_list) > 0:
                    init_match_score, init_match = fifa_match_list[0]
                    if init_match_score < -5.0:
                        verified_match_obj[player_id] = init_match
                        REAL_RUNTIME += 1
                print(f'Matched {REAL_RUNTIME} out of {total_players}')
            verified_match_obj['REAL_RUNTIME'] = REAL_RUNTIME
            pickle.dump(verified_match_obj, verified_filename.open(mode='wb'))

    def check_db(self):
        season_list = [
            '2019/2020',
            # '2018/2019',
            # '2017/2018',
            # '2016/2017',
        ]
        for season in season_list:
            verified_filename = f'match_verified_fifa_{str(int(season[-4:]) + 1)}'
            verified_filename = Path('init_data/' + verified_filename + '.pickle')
            verified_match_obj = {}
            if verified_filename.is_file():
                verified_match_obj = pickle.load(verified_filename.open('rb'))
            if 'REAL_RUNTIME' in verified_match_obj:
                verified_match_obj.pop('REAL_RUNTIME', None)

            runtime = 0
            total_players = len(self.who_trimmed[season])
            for player_id in self.who_trimmed[season]:
                if player_id in verified_match_obj:
                    runtime += 1
                    continue

                hasher = hashlib.md5()
                file_id = season + player_id
                hasher.update(file_id.encode('utf-8'))
                file_path = Path('match_data/' + hasher.hexdigest() + 'raw.pickle')
                fifa_match_list = pickle.load(file_path.open(mode='rb'))
                
                if fifa_match_list is None or len(fifa_match_list) == 0:
                    fifa_match_list = []
                    init_match_score = 0
                else:
                    init_match_score = fifa_match_list[0][0]

                if init_match_score > -5.0:
                    final_match = self.get_human_decision(season, player_id, fifa_match_list)
                    if final_match == 'DISCARD':
                        verified_match_obj[player_id] = False
                    elif final_match is False:
                        continue
                    else:
                        verified_match_obj[player_id] = final_match
                else:
                    final_match = fifa_match_list[0][1]
                    verified_match_obj[player_id] = final_match

                pickle.dump(verified_match_obj, verified_filename.open(mode='wb'))
                print(f'Matched {runtime} out of {total_players}')
                runtime += 1
            
            pickle.dump(verified_match_obj, verified_filename.open(mode='wb'))

    def get_human_decision(self, season, player_id, fifa_match_list):
        curr_player = self.who_trimmed[season][player_id]
        decision = False
        while not decision:
            print(f"  {season} WhoScored {curr_player['name']} ({curr_player['firstName']} {curr_player['lastName']}) w/ club {curr_player['long_team_name']}, age {curr_player['age']}, and weight {curr_player['weight']} ...")
            if len(fifa_match_list) == 0:
                print("Error, no possible player matches! Must enter character manually!")
            else:
                best_score, best_match = fifa_match_list[0]
                print(f"  Best match w/ FIFA  {best_match['player_name']} w/ score {best_score}, club {best_match['player_club']}, age {best_match['age']}, and weight {best_match['weight']} ...")
            val = input("Enter y to accept match, n to reject match, d to discard player, and s to specify exact match, k to skip: ")
            if val.strip().lower() == 'y':
                decision = True
                return best_match
            elif val.strip().lower() == 'n':
                if len(fifa_match_list) > 1:
                    fifa_match_list = fifa_match_list[1:]
            elif val.strip().lower() == 'd':
                return 'DISCARD'
            elif val.strip().lower() == 's':
                name = input("Paste exact player name: ")
                year = str(int(season[-4:]) + 1)
                club = input("Paste exact player club: ")
                weight = None
                selected_player = self.get_fifa_player_by_name(name, year, club, weight)
                if selected_player is None:
                    print('Error, please enter valid player information.')
                else:
                    return selected_player
            elif val.strip().lower() == 'k':
                return False
            else:
                print('Error, please enter valid input.')

    def get_fifa_player_by_name(self, name, year, club, weight=None):
        club = club.strip()
        data = self.fifa_card_data[year]
        for card_type in data:
            for player in data[card_type]:
                if player['player_name'] == name and player['player_club'] == club:
                    if weight is None:
                        print(f'Player {name} found in {card_type} for {year}...')
                        player['CARD_COLOR_RANK'] = card_type
                        return player
                    elif player['weight'] == weight:
                        print(f'Player {name} found in {card_type} for {year}...')
                        player['CARD_COLOR_RANK'] = card_type
                        return player
        return None

    def parse_team_translation(self):
        new_team_translation = {}
        for league_key in self.team_translation:
            for season_key in self.team_translation[league_key]:
                if season_key not in new_team_translation:
                    new_team_translation[season_key] = {key: value for key, value in self.team_translation[league_key][season_key].items()}
                else:
                    new_team_translation[season_key].update({key: value for key, value in self.team_translation[league_key][season_key].items()})
        return new_team_translation
            
    def hyper_match(self):
        season_list = [
            '2019/2020',
            #'2018/2019',
            #'2017/2018',
            #'2016/2017',   
        ]
        players_to_match = self.hyper_match_player_list(season_list)
        ### CHUNGUS LEVEL ###
        CHUNGUS_LVL = 16
        with Pool(CHUNGUS_LVL) as big_chungus:
            big_chungus.map(self.hyper_worker, players_to_match)

    def hyper_match_player_list(self, season_list):
        players_to_match = []
        for season in season_list:
            for player_id in self.who_trimmed[season]:
                players_to_match.append((player_id, season))
        return players_to_match
    
    def hyper_worker(self, player_tuple):
        player_id, season = player_tuple
        hasher = hashlib.md5()
        hasher.update((season + player_id).encode('utf-8'))
        match_save_path = Path('match_data/' + hasher.hexdigest() + 'raw.pickle')
        
        # Player already processed by raw matching.
        if match_save_path.is_file():
            return

        who_player = self.who_trimmed[season][player_id]
        fifa_year = str(int(season[-4:]) + 1)
        match_list = []

        for card_type in self.fifa_card_data[fifa_year]:
            for fifa_player in self.fifa_card_data[fifa_year][card_type]:
                if abs(who_player['age'] - int(fifa_player['age'])) > 1:
                    continue
                try:
                    if abs(who_player['height'] - int(fifa_player['height'])) > 4:
                        continue
                except ValueError:
                    print(f"FIFA height missing for {fifa_player['player_name']}")
                if abs(who_player['weight'] - int(fifa_player['weight'])) > 7:
                    continue

                special_team_match = 100.0 # Some random impossibly high value
                if season in self.team_translation:
                    if who_player['long_team_name'] in self.team_translation[season]:
                        special_team_match = self.club_match_threshold(
                            self.team_translation[season][who_player['long_team_name']],
                            fifa_player['player_club'], 
                        )
                curr_match_score = min(
                    special_team_match,
                    self.club_match_threshold(who_player['teamName'], fifa_player['player_club']),
                    self.club_match_threshold(who_player['long_team_name'], fifa_player['player_club']),
                )
                curr_match_score += min(
                    self.name_match_threshold(who_player['name'], fifa_player['player_name']),
                    self.name_match_threshold(who_player['firstName'] + ' ' + who_player['lastName'], fifa_player['player_name']),
                )
                fifa_player['CARD_COLOR_RANK'] = card_type
                match_list.append((curr_match_score, fifa_player))
        
        if len(match_list) != 0:
            match_list.sort(key=lambda x: x[0])        
        pickle.dump(match_list, match_save_path.open(mode="wb"))
    
    def get_levenshtein_score(self, str_a, str_b):
        # Implement Levenshtein Distance Metric (for string matching)
        rows = len(str_a)
        cols = len(str_b)
        score_matrix = np.zeros((rows + 1, cols + 1))
        for i in range(rows + 1):
            for j in range(cols + 1):
                if i == 0:
                    score_matrix[i, j] = j
                elif j == 0:
                    score_matrix[i, j] = i
                else:
                    del_a_b = score_matrix[i - 1, j] + 1
                    ins_a_b = score_matrix[i, j - 1] + 1
                    if str_a[i - 1] == str_b[j - 1]:
                        match_mismatch = score_matrix[i - 1, j - 1]
                    else:
                        match_mismatch = score_matrix[i - 1, j - 1] + 1
                    min_val = min(del_a_b, ins_a_b, match_mismatch)
                    if min_val == del_a_b:
                        score_matrix[i, j] = del_a_b
                    elif min_val == ins_a_b:
                        score_matrix[i, j] = ins_a_b
                    else:
                        score_matrix[i, j] = match_mismatch
        return score_matrix[rows - 1, cols - 1]
    
    def club_match_threshold(self, who_scored_club, fifa_club):
        who_scored_club = unidecode(who_scored_club).lower().replace('.','')
        who_scored_club = who_scored_club.strip()
        fifa_club = unidecode(fifa_club).lower().replace('.','')
        fifa_club = fifa_club.strip()
        levenshtein_score = self.get_levenshtein_score(who_scored_club, fifa_club)
        z_w = levenshtein_score
        for i in who_scored_club.split():
            for j in fifa_club.split():
                if i == j:
                    z_w -= len(i) / 2
                    break
        return z_w

    def name_match_threshold(self, who_scored_name, fifa_name):
        who_scored_name = unidecode(who_scored_name).replace('-',' ').replace('.','').replace('Jr','').replace('Junior','')
        who_scored_name = who_scored_name.strip()
        fifa_name = unidecode(fifa_name).replace('-',' ').replace('.','').replace('Jr','').replace('Junior','')
        fifa_name = fifa_name.strip()
        levenshtein_score = self.get_levenshtein_score(who_scored_name, fifa_name)
        z_w = levenshtein_score * 2
        for i in who_scored_name.split():
            for j in fifa_name.split():
                if i == j:
                    z_w -= len(i) / 2
                    break
        return z_w


if __name__ == '__main__':
    # Build WhoScoredData
    real_athlete_data = WhoScoredData()
    print(f'Collected {real_athlete_data.get_trimmed_player_count()} TRIMMED players\' data from WhoScored.com...')
    # Build Futhead Data
    fifa_card_data = FutBinData()
    print(f'Collected {fifa_card_data.get_player_count()} players\' data from Futbin.com...')
    # Build Db
    db_gen = DbManager(real_athlete_data, fifa_card_data)
    # db_gen.optimize_db()
    db_gen.check_db()
