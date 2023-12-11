import os
import time
import logging
import pandas as pd


import match_scraper
import game_power_scraper
import odds_scraper
import point_by_point_scraper
import vote_scraper
import statistics_scraper


from constants import file_path
from utils import (
    parse_args,
    build_date_possible_informations,
    build_stats_links,
)
from request_data import get_daily_game_ids, get_tennis_responses



def main():
    arguments = parse_args()

    for key_folder, value_folder in file_path.items():
        if not os.path.exists(value_folder):
            os.makedirs(value_folder)

    dict_of_scrapers = {
        "statistics": statistics_scraper.main,
        "match": match_scraper.main,
        "point_by_point": point_by_point_scraper.main,
        "votes": vote_scraper.main,
        "odds": odds_scraper.main,
        "tennis_power": game_power_scraper.main,
    }

    dict_of_active_scrapes = {
        "statistics": arguments.statistics,
        "match": arguments.match,
        "point_by_point": arguments.point_by_point,
        "votes": arguments.votes,
        "odds": arguments.odds,
        "tennis_power": arguments.tennis_power,
    }

    list_of_game_ids = get_daily_game_ids(games_date=arguments.date)
    logging.info("all game ids of specified date is available now.")
    if not os.path.isfile(os.path.join(file_path["tracking_scrape"], f"{arguments.date.replace('-', '')}.csv")):
        scraper_dataframe = build_date_possible_informations(list_of_game_ids=list_of_game_ids, scrape_date=arguments.date)
        logging.info("dataframe created of all possible scrapes for games and parameters.")
        scraper_dataframe.to_csv(os.path.join(file_path["tracking_scrape"], f"{arguments.date.replace('-', '')}.csv"), index=False,)
    else:
        scraper_dataframe = pd.read_csv(os.path.join(file_path["tracking_scrape"],f"{arguments.date.replace('-', '')}.csv"))

    for row_num, value in scraper_dataframe.iterrows():
        valid_links_per_game_id = build_stats_links(game_id=value["game_id"])
        logging.info(f"all links {value['game_id']} are created for scraping.")

        for parameter, scraper in dict_of_scrapers.items():
            if value[parameter] == "-" and parameter in dict_of_active_scrapes.keys():
                try:
                    response_of_parameter = get_tennis_responses(url=valid_links_per_game_id[parameter])
                    logging.info(f"response of {parameter} {value['game_id']} are scraped.")
                    scraper(match_id=value["game_id"], response_json=response_of_parameter)

                    scraper_dataframe.loc[row_num, parameter] = 1
                    logging.info("value at scrape dataframe corrected.")

                    scraper_dataframe.to_csv(
                        os.path.join(file_path["tracking_scrape"], f"{arguments.date.replace('-', '')}.csv",), index=False)
                except:
                    scraper_dataframe.loc[row_num, parameter] = 0
            else:
                logging.info(f"{parameter} not valid to scrape.")
            time.sleep(2)


if __name__ == "__main__":
    
    logging.basicConfig(
        filename="tennis_loggs.log",
        filemode="a",
        level=logging.INFO,
        format="%(asctime)s %(message)s",
    )
    main()
