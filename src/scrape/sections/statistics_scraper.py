import json
from dataclasses import asdict, dataclass

import tables_dataclasses
from utils import json_to_parquet
from constants import file_path


def statistic_information(
    match_id: int,
    response_json: json,
    data_class_schema: dataclass = tables_dataclasses.PeriodInfo,
) -> list[dict]:
    """
    fetch statistics information from sofascore.com/tennis and return them as a dictionary with the following keys:
    - match_id: match identifier number
    - period: period of game containing (1st, 2nd, 3rd, etc)
    - statistics_category_name: name of the statistic category containing (service, point, return, etc)
    - statistics_name: name of the statistic containing (ace, double_faults, first_serve, second_serve, first_serve_points,
                     second_serve_points, service_points_won, max_points_in_a_row)
    - home_stat: home statistics of that named statistics name for example for first_serve home stats is (53/81 (65%))
    - away_stat: away statistics of that named statistics name for example for second_serve home stats is (72/96 (75%))
    - compare_code: this parameter about of superiority of one player over other, for exmaple if compare code is 2 for aces
                  this means player 2 have better stats than player 1, if compare code is 3 this means both players are equal
    - statistic_type: this parameter indicates whether the statistic is positive or negative for example ace is positive and
                    double_faults is negative
    - value_type: its about home or away stats, if home_stat like first_serve contain of two numbers then this metric will be team
                otherwise is event
    - home_value: the actual value of the home team for that statistic_name
    - home_value: the actual value of the away team for that statistic_name
    - home_total: total value of home team when value_type=team, for example first_serve = 53/81 (65%), 53 is home value and 81 is home total
    - away_total: total value of away team when value_type=team, for example second_serve = 72/96 (75%), 53 is away value and 81 is away total

    Parameters:
    - match_id (int): the match identifier for the statistic
    - response_json (json): response of statistics information request that provides from sofascore.com tennis matches
    - data_class_schema (PeriodInfo): dataclass structure for statistics about periods
    """

    all_game_stats = []
    for period in range(len(response_json["statistics"])):
        for group in range(len(response_json["statistics"][period]["groups"])):
            for statistic_category in range(
                len(
                    response_json["statistics"][period]["groups"][group][
                        "statisticsItems"
                    ]
                )
            ):
                single_game_stats = data_class_schema(
                    match_id=match_id,
                    period=response_json["statistics"][period].get("period"),
                    statistic_category_name=response_json["statistics"][period][
                        "groups"
                    ][group]
                    .get("groupName")
                    .lower(),
                    statistic_name=response_json["statistics"][period]["groups"][group][
                        "statisticsItems"
                    ][statistic_category]
                    .get("name")
                    .lower()
                    .replace(" ", "_"),
                    home_stat=response_json["statistics"][period]["groups"][group][
                        "statisticsItems"
                    ][statistic_category].get("home"),
                    away_stat=response_json["statistics"][period]["groups"][group][
                        "statisticsItems"
                    ][statistic_category].get("away"),
                    compare_code=response_json["statistics"][period]["groups"][group][
                        "statisticsItems"
                    ][statistic_category].get("compareCode"),
                    statistic_type=response_json["statistics"][period]["groups"][group][
                        "statisticsItems"
                    ][statistic_category].get("statisticsType"),
                    value_type=response_json["statistics"][period]["groups"][group][
                        "statisticsItems"
                    ][statistic_category].get("valueType"),
                    home_value=response_json["statistics"][period]["groups"][group][
                        "statisticsItems"
                    ][statistic_category].get("homeValue"),
                    away_value=response_json["statistics"][period]["groups"][group][
                        "statisticsItems"
                    ][statistic_category].get("awayValue"),
                    home_total=response_json["statistics"][period]["groups"][group][
                        "statisticsItems"
                    ][statistic_category].get("homeTotal"),
                    away_total=response_json["statistics"][period]["groups"][group][
                        "statisticsItems"
                    ][statistic_category].get("awayTotal"),
                )
                all_game_stats.append(asdict(single_game_stats))

    return all_game_stats


def main(match_id: int, response_json: json):
    statistics_stat = statistic_information(
        match_id=match_id, response_json=response_json
    )
    if len(statistics_stat) > 0:
        json_to_parquet(
            json_input=statistics_stat,
            write_path=file_path["raw_statistics"]
            + f"statistics_{match_id}"
            + ".parquet",
        )
    else:
        pass
