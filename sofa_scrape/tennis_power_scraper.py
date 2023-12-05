import json
from dataclasses import asdict
from tables_dataclasses import PowerInfo


def game_power_information(match_id: int, response_json: json, data_class_schema: PowerInfo) -> list[dict]:
    """
    fetch statistics information from sofascore.com/tennis and return them as a dictionary with the following keys:
    - match_id: match identifier
    - set_num: number of set
    - game_num: number of game
    - value: power of players at game
    - break_occurred: boolean indicating whether break occurred

    Parameters:
    - match_id (int): the match identifier for the statistic
    - response_json (json): response of game power match information request that provides from sofascore.com tennis matches
    - data_class_schema (OddsInfo): dataclass structure for statistics about match odds events
    """

    list_of_games_power = []
    for game_power in response_json["tennisPowerRankings"]:
        single_game_power = data_class_schema(
            match_id = match_id,
            set_num = game_power.get("set"),
            game_num = game_power.get("game"),
            value = game_power.get("value"),
            break_occurred = game_power.get("breakOccurred")
        )
        
        list_of_games_power.append(asdict(single_game_power))
        
    return list_of_games_power