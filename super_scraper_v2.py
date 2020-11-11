from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import json
from pathlib import Path
import hashlib
from unidecode import unidecode
import time
import random
import peewee as pw
import uuid
import numpy as np

# Take 2 at this bungus

"""
Class WhoScoredData()
Description: Class that controls scraping for whoscored.com data. Builds out JSON file containing all data.
"""
class WhoScoredData():
    def __init__(self):
        data_path = Path('init_data/who_scored_data.json')
        if data_path.is_file():
            self.player_data = json.load(data_path.open())
        else:
            self.player_data = self.init_build_player_data()
            json.dump(self.player_data, data_path.open(mode='w'))
    
    def init_build_player_data(self):
        # Selenium setup
        self.driver = webdriver.Firefox()
        self.driver.get('http://google.com')
        all_player_who_scored_data = {}
        stage_id_dict = self.load_stage_ids('init_data/stage_ids.txt')
        team_names = json.load(Path('init_data/team_bomboclaat.json').open())
        for league_key in stage_id_dict:
            all_player_who_scored_data[league_key] = {}
            for season_key in stage_id_dict[league_key]:
                stage_id = stage_id_dict[league_key][season_key]
                summary_all, summary_def, summary_off, summary_pass = self.get_player_json(stage_id)
                team_key_int = abs(2019 - int(season_key.split('/')[0]))
                print(f'Team Key: {team_key_int}')
                team_names_subset = team_names[league_key][team_key_int]
                print(team_names_subset)
                summary_dict = self.merge_summaries(summary_all, summary_def, summary_off, summary_pass, team_names_subset)
                all_player_who_scored_data[league_key][season_key] = summary_dict
        self.driver.quit()
        return all_player_who_scored_data
    
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
    
    def get_player_count(self):
        total_players = 0
        for league_key in self.player_data:
            for season_key in self.player_data[league_key]:
                total_players += len(self.player_data[league_key][season_key].keys())
        return total_players
    
    def get_player_by_name(self, name):
        for league_key in self.player_data:
            for season_key in self.player_data[league_key]:
                for player in self.player_data[league_key][season_key]:
                    if self.player_data[league_key][season_key][player]['name'] == name:
                        print(f'Player {name} found for {league_key} in {season_key}...')
                        return player
        return None


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
        caps = DesiredCapabilities().FIREFOX
        caps["pageLoadStrategy"] = "eager"
        self.driver = webdriver.Firefox(desired_capabilities=caps)
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
    
    def get_player_by_name(self, name):
        for year in self.player_data:
            for card_type in self.player_data[year]:
                for player in self.player_data[year][card_type]:
                    if player['player_name'] == name:
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
        db_path = Path('fifa.db')
        if not db_path.is_file():
            self.init_db(db_path)
        else:
            self.db = pw.SqliteDatabase(db_path)
            self.db.connect()
        if who_scored_data is not None:
            self.who_scored_data = who_scored_data.player_data
            self.who_scored_summary = self.get_who_scored_summary()
        if fifa_card_data is not None:
            self.fifa_card_data = fifa_card_data.player_data
            self.fifa_card_summary = self.get_fifa_card_summary()
    
    def init_db(self, db_path):
        self.db = pw.SqliteDatabase(db_path)
        self.db.connect()
        self.db.create_tables([Season, PlayerStatistics])
    
    def check_db(self, who_scored_data):
        pass
    
    def get_who_scored_summary(self):
        """
        Get summary information from who_scored_data.
        1. # of players
        2. Average age
        3. Stdev age
        4. Average height
        5. Stdev height
        6. Average weight
        7. Stdev weight
        8. Average Levenshtein (centered at 0, with stdev of 2.5, hard coded).
        """
        total_players = 0
        age_list = []
        height_list = []
        weight_list = []
        for league_key in self.who_scored_data:
            for season_key in self.who_scored_data[league_key]:
                for player_id in self.who_scored_data[league_key][season_key]:
                    total_players += 1
                    curr_player = self.who_scored_data[league_key][season_key][player_id]
                    if curr_player['age'] != 0 and curr_player['age'] != '':
                        age_list.append(int(curr_player['age']))
                    if curr_player['height'] != 0 and curr_player['height'] != '':
                        height_list.append(int(curr_player['height']))
                    if curr_player['weight'] != 0 and curr_player['weight'] != '':
                        weight_list.append(int(curr_player['weight']))
        who_scored_summary = {}
        who_scored_summary['total_players'] = total_players
        age_list = np.array(age_list)
        height_list = np.array(height_list)
        weight_list = np.array(weight_list)
        who_scored_summary['age_avg'] = np.mean(age_list)
        who_scored_summary['age_stdev'] = np.std(age_list)
        who_scored_summary['height_avg'] = np.mean(height_list)
        who_scored_summary['height_stdev'] = np.std(height_list)
        who_scored_summary['weight_avg'] = np.mean(weight_list)
        who_scored_summary['weight_stdev'] = np.std(weight_list)
        who_scored_summary['name_lev_avg'] = 0  # Hard Coded
        who_scored_summary['name_lev_stdev'] = 3  # Hard Coded
        who_scored_summary['club_lev_avg'] = 0 # Hard Coded
        who_scored_summary['club_lev_stdev'] = 2.5 # Hard Coded
        return who_scored_summary
    
    def get_fifa_card_summary(self):
        """
        Get summary information from fifa_card_data.
        1. # of players
        2. Average age
        3. Average height
        4. Average weight
        """
        total_players = 0
        age_list = []
        height_list = []
        weight_list = []
        for fifa_year in self.fifa_card_data:
            for card_type in self.fifa_card_data[fifa_year]:
                for player in self.fifa_card_data[fifa_year][card_type]:
                    total_players += 1
                    try:
                        age = int(player['age'])
                    except ValueError:
                        age = None
                    if age:
                        age_list.append(age)
                    try:
                        height = int(player['height'])
                    except ValueError:
                        height = None
                    if height:
                        height_list.append(height)
                    try:
                        weight = int(player['weight'])
                    except ValueError:
                        weight = None
                    if weight:
                        weight_list.append(weight)
        age_list = np.array(age_list)
        height_list = np.array(height_list)
        weight_list = np.array(weight_list)
        fifa_card_summary = {}
        fifa_card_summary['age_avg'] = np.mean(age_list)
        fifa_card_summary['age_stdev'] = np.std(age_list)
        fifa_card_summary['height_avg'] = np.mean(height_list)
        fifa_card_summary['height_stdev'] = np.std(height_list)
        fifa_card_summary['weight_avg'] = np.mean(weight_list)
        fifa_card_summary['weight_stdev'] = np.std(weight_list)
        fifa_card_summary['name_lev_avg'] = 0   # Hard Coded
        fifa_card_summary['name_lev_stdev'] = 3 # Hard Coded
        fifa_card_summary['club_lev_avg'] = 0   # Hard Coded
        fifa_card_summary['club_lev_stdev'] = 2.5   # Hard Coded
        return fifa_card_summary
    
    def build_db(self, who_scored_data=None, fifa_card_data=None):
        if who_scored_data is not None:
            self.who_scored_data = who_scored_data
            self.who_scored_summary = self.get_who_scored_summary()
        if fifa_card_data is not None:
            self.fifa_card_data = fifa_card_data
            self.fifa_card_summary = self.get_fifa_card_summary()
        for league_key in self.who_scored_data:
            for season_key in self.who_scored_data[league_key]:
                for player_id in self.who_scored_data[league_key][season_key]:
                    curr_player = self.who_scored_data[league_key][season_key][player_id]
                    fifa_year = str(int(season_key.split('/')[1]) + 1)
                    fifa_card_subset = self.fifa_card_data[fifa_year]
                    self.get_fifa_match(curr_player, fifa_card_subset)
                    # break
                break
            break
    
    def get_fifa_match(self, curr_player, fifa_card_subset):
        player_matches = []
        print(f"WhoScored {curr_player['name']} ({curr_player['firstName']} {curr_player['lastName']}), ",end="")
        for card_type in fifa_card_subset:
            for player in fifa_card_subset[card_type]:
                player_match_score = 0
                if curr_player['age'] != int(player['age']) or abs(curr_player['weight'] - int(player['weight'])) >= 6:
                    continue
                # player_match_score += self.height_match_threshold(curr_player['height'], player['height'])
                player_match_score += min(
                    self.club_match_threshold(curr_player['teamName'], player['player_club']),
                    self.club_match_threshold(curr_player['long_team_name'], player['player_club'])
                )
                player_match_score += min(
                    self.name_match_threshold(curr_player['name'], player['player_name']),
                    self.name_match_threshold(curr_player['firstName'] + ' ' + curr_player['lastName'], player['player_name']),
                )
                player_matches.append((player_match_score, player))
        player_matches.sort(key=lambda x:x[0])
        best_player_score, best_player = player_matches[0]
        print(f"matches FIFA {best_player['player_name']} w/ score {best_player_score} ...")
    
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

    def height_match_threshold(self, who_scored_height, fifa_height):
        try:
            fifa_height = int(fifa_height)
        except ValueError:
            fifa_height = self.fifa_card_summary['height_avg']
        z_w = (who_scored_height - self.who_scored_summary['height_avg']) / self.who_scored_summary['height_stdev']
        z_f = (fifa_height - self.fifa_card_summary['height_avg']) / self.fifa_card_summary['height_stdev']
        return np.square(z_w - z_f)
    
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
                    z_w -= len(i)
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
                    z_w -= len(i)
                    break
        return z_w


if __name__ == '__main__':
    # Build WhoScoredData
    real_athlete_data = WhoScoredData()
    print(f'Collected {real_athlete_data.get_player_count()} players\' data from WhoScored.com...')
    # Build Futhead Data
    fifa_card_data = FutBinData()
    print(f'Collected {fifa_card_data.get_player_count()} players\' data from Futbin.com...')
    # db_gen = DbManager(real_athlete_data, fifa_card_data)
    # db_gen.build_db()
    # print(db_gen.who_scored_summary)
    # print(db_gen.fifa_card_summary)
