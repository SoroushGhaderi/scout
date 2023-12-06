import argparse
import json

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def parse_args():
    """
    Args Parser for Tennis Web Scraper

    Return:
    - date: date for exmaple '2023-04-01'
    - statistics: flag means statistics information collected
    - match: flag means match information collected
    - point_by_point: flag means point by point information collected
    - votes: flag means votes information collected
    - odds: flag means odds information collected
    - tennis_power: flag means tennis power information collected
    """

    parser = argparse.ArgumentParser(description="scraper config")
    parser.add_argument(
        "-d",
        "--date",
        help="start date to scrape",
    )
    parser.add_argument(
        "-s",
        "--statistics",
        action="store_true",
        required=True,
        help="scrape statistics of match",
    )
    parser.add_argument(
        "-m",
        "--match",
        action="store_true",
        required=True,
        help="scrape information about players, tournament, time and scores",
    )
    parser.add_argument(
        "-p",
        "--point_by_point",
        action="store_true",
        required=True,
        help="scrape point by point during match",
    )
    parser.add_argument(
        "-v", "--votes", action="store_true", required=True, help="scrape people votes"
    )
    parser.add_argument(
        "-o",
        "--odds",
        action="store_true",
        required=True,
        help="scrape odds of important odds",
    )
    parser.add_argument(
        "-t",
        "--tennis_power",
        action="store_true",
        required=True,
        help="scrape tennis power",
    )

    args = parser.parse_args()
    return args


def build_stats_links(game_id: int) -> dict:
    """
    Build Links of request by Single Game ID

    Parameters:
    - game_id (int): game id of signle match

    Returns:
    - requests_structures (dict): url of requests like statistics, match, votes, point_by_point, odds and tennis power
    """

    requests_structures = {
        "statistics": f"https://api.sofascore.com/api/v1/event/{game_id}/statistics",
        "match": f"https://api.sofascore.com/api/v1/event/{game_id}",
        "votes": f"https://api.sofascore.com/api/v1/event/{game_id}/votes",
        "point_by_point": f"https://api.sofascore.com/api/v1/event/{game_id}/point-by-point",
        "odds": f"https://api.sofascore.com/api/v1/event/{game_id}/odds/1/all",
        "tennis_power": f"https://api.sofascore.com/api/v1/event/{game_id}/tennis-power",
    }

    return requests_structures


def build_date_possible_informations(
    list_of_game_ids: list[int], scrape_date: str
) -> pd.DataFrame:
    """
    Build DataFrame of All Possible Scrapes
    for each game id we create a cell for each parameter (statistics, match, votes, point_by_point, odds and tennis power)
    each cell value is 0, when scrape is successful value turns 1, if scrape is unsuccessful value will change to 'not_available'

    Parameters:
    - list_of_game_ids (list[int]): list of date game ids
    - scrape_date (str): scrape date for exmaple '2023-04-01'

    Returns:
    - possible_scrape_dataframe (pd.DataFrame): table of all possible scrapes per game_id per parameter
    """

    possible_scrape_dataframe = pd.DataFrame(
        list_of_game_ids, columns=["game_id"]
    ).assign(
        date=scrape_date,
        statistics="-",
        match="-",
        point_by_point="-",
        votes="-",
        odds="-",
        tennis_power="-",
    )

    return possible_scrape_dataframe


def json_to_parquet(json_input: json, write_path: str) -> None:
    """
    Convert a json to Parquet File and Store Data

    Parameters:
    - json_input (json): list json as input
    - parquet_output (pq): convert json data to parquet file
    - write_path (str): path to write parquet file
    """

    json_dataframe = pd.DataFrame(json_input)
    pyarrow_table = pa.Table.from_pandas(json_dataframe)
    pq.write_table(pyarrow_table, write_path)
