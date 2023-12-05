import json
from dataclasses import asdict
from tables_dataclasses import MatchVotesInfo


def game_votes_information(match_id: int, response_json: json, data_class_schema: MatchVotesInfo) -> list[dict]:
    """
    fetch statistics information from sofascore.com/tennis and return them as a dictionary with the following keys:
    - match_id: match identifier
    - home_vote: total votes count of the home player
    - away_vote: total votes count of the away player

    Parameters:
    - match_id (int): the match identifier for the statistic
    - response_json (json): response of odds match information request that provides from sofascore.com tennis matches
    - data_class_schema (MatchVotesInfo): dataclass structure for statistics about match votes events
    """

    single_vote_info = data_class_schema(
        match_id = match_id,
        home_vote = response_json["vote"].get("vote1"),
        away_vote = response_json["vote"].get("vote2")
    )
    return [asdict(single_vote_info)]