import json
from dataclasses import asdict
from tables_dataclasses import GameInfo


def game_point_by_point_information(match_id: int, response_json: json, data_class_schema: GameInfo) -> list[dict]:
    """
    fetch statistics information from sofascore.com/tennis and return them as a dictionary with the following keys:
    - match_id: match identifier
    - set_id: set identifier
    - game_id: game identifier
    - point_id: point identifier
    - home_point: home point display 
    - away_point: away point display
    - point_description: game point description
    - home_point_type: home point type in game
    - away_point_type: away point type in game
    - home_score: home score after game played
    - away_score: away score after game played
    - serving: player that serves the game
    - scoring: who win the game
    

    Parameters:
    - match_id (int): the match identifier for the statistic
    - response_json (json): response of point by point match information request that provides from sofascore.com tennis matches
    - data_class_schema (GameInfo): dataclass structure for statistics about match events
    """
    
    list_of_game_info = []
    for set_num in range(len(response_json["pointByPoint"])):
        for game_num in range(len(response_json["pointByPoint"][set_num]["games"])):
            for point in range(len(response_json["pointByPoint"][set_num]["games"][game_num]["points"])):
                single_game_info = data_class_schema(
                    match_id = match_id,
                    set_id = response_json["pointByPoint"][set_num].get("set"),
                    game_id = response_json["pointByPoint"][set_num]["games"][game_num].get("game"),
                    point_id = point,
                    home_point = response_json["pointByPoint"][set_num]["games"][game_num]["points"][point].get("homePoint"),
                    away_point = response_json["pointByPoint"][set_num]["games"][game_num]["points"][point].get("awayPoint"),
                    point_description = response_json["pointByPoint"][set_num]["games"][game_num]["points"][point].get("pointDescription"),
                    home_point_type = response_json["pointByPoint"][set_num]["games"][game_num]["points"][point].get("homePointType"),
                    away_point_type = response_json["pointByPoint"][set_num]["games"][game_num]["points"][point].get("awayPointType"),
                    home_score = response_json["pointByPoint"][set_num]["games"][game_num]["score"].get("homeScore"),
                    away_score = response_json["pointByPoint"][set_num]["games"][game_num]["score"].get("awayScore"),
                    serving = response_json["pointByPoint"][set_num]["games"][game_num]["score"].get("serving"),
                    scoring = response_json["pointByPoint"][set_num]["games"][game_num]["score"].get("scoring")
                )
                list_of_game_info.append(asdict(single_game_info))
    
    return list_of_game_info
