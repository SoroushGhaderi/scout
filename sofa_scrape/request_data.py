from constants import headers
import json

import requests


def get_daily_game_ids(games_date: str) -> list[int]:
    """
    Find All Game IDs for a Selected Date

    Parameters:
    - games_date (str): date for exmaple '2023-04-01'

    Returns:
    - date_tennis_single_games (list[int]): list of game ids that is single players matches
    """
    
    base_url = "https://api.sofascore.com/api/v1/sport/tennis/scheduled-events/"
    response_tennis_games = requests.get(base_url + games_date, headers=headers).json()
    # filter out games that are not doubles.
    date_tennis_single_games = [response_tennis_games["events"][game_id]["id"] for game_id in range(len(response_tennis_games["events"])) 
                           if "doubles" not in response_tennis_games["events"][game_id]["tournament"]["uniqueTournament"]["slug"]]
    
    return date_tennis_single_games


def get_tennis_responses(url: str) -> json:
    """
    Extract Json data from Links

    Parameters:
    - utl (str): url of the information

    Returns:
    - response_json (json): results of the request
    """
    
    response_json = requests.get(url, headers=headers).json()
    
    return response_json
