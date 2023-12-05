import os
import time
import logging
import pandas as pd

import statistics_scraper
import match_scraper
import odds_scraper
import votes_scraper
import tennis_power_scraper
import point_by_point_scraper

from constants import file_path
from utils import parse_args, build_date_possible_informations, build_stats_links, json_to_parquet
from request_data import get_daily_game_ids, get_tennis_responses


from tables_dataclasses import *

def main():
    
    arguments = parse_args()
    
    for key_folder, value_folder in file_path.items():
        if not os.path.exists(os.path.join("data", value_folder)):
            os.makedirs(os.path.join("data", value_folder))
    
    parameters = {
    "statistics": {"functions": [statistics_scraper.statistic_information], "dataclass_structures": [PeriodInfo]},
    "match": {"functions": [match_scraper.match_event_information,
                            match_scraper.match_tournament_information,
                            match_scraper.match_season_information,
                            match_scraper.match_round_information,
                            match_scraper.match_venue_information,
                            match_scraper.match_home_team_information,
                            match_scraper.match_away_team_information,
                            match_scraper.match_home_team_score_information,
                            match_scraper.match_away_team_score_information,
                            match_scraper.match_time_information],
              "dataclass_structures": [MatchEventInfo,
                                       MatchTournamentInfo,
                                       MatchSeasonInfo,
                                       MatchRoundInfo,
                                       MatchVenueInfo,
                                       MatchHomeTeamInfo,
                                       MatchAwayTeamInfo,
                                       MatchHomeScoreInfo,
                                       MatchAwayScoreInfo,
                                       MatchTimeInfo]},
    "votes": {"functions": [votes_scraper.game_votes_information], "dataclass_structures": [MatchVotesInfo]},
    "point_by_point": {"functions": [point_by_point_scraper.game_point_by_point_information], "dataclass_structures": [GameInfo]},
    "odds": {"functions": [odds_scraper.game_odds_information], "dataclass_structures": [OddsInfo]},
    "tennis_power": {"functions": [tennis_power_scraper.game_power_information], "dataclass_structures": [PowerInfo]}}
    
    list_of_game_ids = get_daily_game_ids(games_date=arguments.date)
    logging.info("all game ids of specified date is available now.")
    if not os.path.isfile(os.path.join("data", f"altered_scraper_dataframe_{arguments.date.replace('-', '')}.csv")):
        scraper_dataframe = build_date_possible_informations(list_of_game_ids=list_of_game_ids,
                                                            scrape_date=arguments.date)
        logging.info("dataframe created of all possible scrapes for games and parameters.")
        scraper_dataframe.to_csv(os.path.join("data", f"altered_scraper_dataframe_{arguments.date.replace('-', '')}.csv"), index=False)
    else:
        scraper_dataframe = pd.read_csv(os.path.join("data",
                                                            f"altered_scraper_dataframe_{arguments.date.replace('-', '')}.csv"))
    
    dict_of_active_scrapes = {"statistics": arguments.statistics,
                              "match": arguments.match,
                              "point_by_point": arguments.point_by_point,
                              "votes": arguments.votes,
                              "odds": arguments.odds,
                              "tennis_power": arguments.tennis_power}
    
    for row_num, value in scraper_dataframe.iterrows():
        
        
        valid_links_per_game_id = build_stats_links(game_id=value["game_id"])
        logging.info(f"all links {value['game_id']} are created for scraping.")
        
        # loop through parameters
        for parameter, value_parameter in parameters.items():   
            if value[parameter] == 0 and parameter in dict_of_active_scrapes.keys():
                for function, dataclass in zip(value_parameter["functions"], value_parameter["dataclass_structures"]):
                    try:                                        
                        response_of_parameter = get_tennis_responses(url=valid_links_per_game_id[parameter])
                        logging.info(f"response of {parameter} {value['game_id']} are scraped.")
                        single_parameter_stat = function(match_id=value["game_id"],
                                                        response_json=response_of_parameter,
                                                        data_class_schema=dataclass)
                        logging.info(f"response of {function.__name__.removeprefix('match_').removesuffix('_information')} {value['game_id']} are scraped at json.")
                        json_to_parquet(json_input=single_parameter_stat,
                                        write_path="data/" +
                                        file_path[parameter] +
                                        f"{function.__name__.removeprefix('match_').removesuffix('information')}" +
                                        str(value["game_id"]) +
                                        ".parquet")
                        logging.info(f"json of {value['game_id']} result converted to parquet file.")
                        
                        scraper_dataframe.loc[row_num, parameter] = 1
                        logging.info("value at scrape dataframe corrected.")
                        
                        scraper_dataframe.to_csv(os.path.join("data",
                                                            f"altered_scraper_dataframe_{arguments.date.replace('-', '')}.csv"), index=False)
                    except:
                        scraper_dataframe.loc[row_num, parameter] = 9
        
            else:
                logging.info(f"{parameter} not valid to scrape.")   
            time.sleep(2)
        
    
if __name__ == "__main__":

    logging.basicConfig(filename='tennis_loggs.log',
                        filemode='a',
                        level=logging.INFO,
                        format='%(asctime)s %(message)s')
    main()