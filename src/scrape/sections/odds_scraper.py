import json
from dataclasses import asdict, dataclass

import tables_dataclasses
from utils import json_to_parquet
from constants import file_path


def game_odds_information(
    match_id: int,
    response_json: json,
    data_class_schema: dataclass = tables_dataclasses.OddsInfo,
) -> list[dict]:
    """
    fetch statistics information from sofascore.com/tennis and return them as a dictionary with the following keys:
    - match_id: match identifier
    - market_id: market identifier
    - market_name: market name or bet option
    - is_live: if competition is live
    - suspended: if competition is suspended
    - initial_fractional_value: odd value before game starts
    - fractional_value: fractional value
    - choice_name: single option
    - choice_source_id: option identifier
    - winnig: is option won?
    - change: is option changed?


    Parameters:
    - match_id (int): the match identifier for the statistic
    - response_json (json): response of odds match information request that provides from sofascore.com tennis matches
    - data_class_schema (OddsInfo): dataclass structure for statistics about match odds events
    """

    all_odds = []
    for market in range(len(response_json["markets"])):
        for choice in range(len(response_json["markets"][market]["choices"])):
            single_odd_info = data_class_schema(
                match_id=match_id,
                market_id=response_json["markets"][market].get("marketId"),
                market_name=response_json["markets"][market]
                .get("marketName")
                .lower()
                .replace(" ", "_"),
                is_live=response_json["markets"][market].get("isLive"),
                suspended=response_json["markets"][market].get("suspended"),
                initial_fractional_value=response_json["markets"][market]["choices"][
                    choice
                ].get("initialFractionalValue"),
                fractional_value=response_json["markets"][market]["choices"][
                    choice
                ].get("fractionalValue"),
                choice_name=response_json["markets"][market]["choices"][choice].get(
                    "name"
                ),
                choice_source_id=response_json["markets"][market]["choices"][
                    choice
                ].get("sourceId"),
                winnig=response_json["markets"][market]["choices"][choice].get(
                    "winning"
                ),
                change=response_json["markets"][market]["choices"][choice].get(
                    "change"
                ),
            )
            all_odds.append(single_odd_info)

    return all_odds


def main(match_id: int, response_json: json):
    odds_stats = game_odds_information(match_id=match_id, response_json=response_json)
    if len(odds_stats) > 0:
        json_to_parquet(
            json_input=odds_stats,
            write_path=file_path["raw_odds"] + f"odds_{match_id}" + ".parquet",
        )
    else:
        pass
