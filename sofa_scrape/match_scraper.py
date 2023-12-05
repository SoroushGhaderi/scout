import json
from dataclasses import asdict
from tables_dataclasses import (MatchEventInfo,
                               MatchTournamentInfo,
                               MatchSeasonInfo,
                               MatchRoundInfo,
                               MatchVenueInfo,
                               MatchHomeTeamInfo,
                               MatchAwayTeamInfo,
                               MatchHomeScoreInfo,
                               MatchAwayScoreInfo,
                               MatchTimeInfo)
                               
                               
def match_event_information(match_id: int, response_json: json, data_class_schema: MatchEventInfo) -> list[dict]:
    """
    fetch statistics information from sofascore.com/tennis and return them as a dictionary with the following keys:
    - match_id: match identifier
    - first_to_serve: player that start the match
    - home_team_seed: home team seed number or other signs
    - away_team_seed: away team seed number or other signs
    - custom_id: hash code of tournament that will be used in game identification
    - winner_code: winner of match
    - default_period_count: total number of periods or sets
    - start_datetime: start datetime timestamp 
    - match_slug: name of players that will be used in game identification
    - final_result_only: final result of matches that not have details

    Parameters:
    - match_id (int): the match identifier for the statistic
    - response_json (json): response of match information request that provides from sofascore.com tennis matches
    - data_class_schema (MatchEventInfo): dataclass structure for statistics about match events
    """

    single_match_info = data_class_schema(
        match_id = match_id,
        first_to_serve = response_json["event"].get("firstToServe"),
        home_team_seed = response_json["event"].get("homeTeamSeed"),
        away_team_seed = response_json["event"].get("awayTeamSeed"),
        custom_id = response_json["event"].get("customId"),
        winner_code = response_json["event"].get("winnerCode"),
        default_period_count = response_json["event"].get("defaultPeriodCount"),
        start_datetime = response_json["event"].get("startTimestamp"),
        match_slug = response_json["event"].get("slug"),
        final_result_only = response_json["event"].get("finalResultOnly"),
    )
    return [asdict(single_match_info)]


def match_tournament_information(match_id: int, response_json: json, data_class_schema: MatchTournamentInfo) -> list[dict]:
    """
    fetch statistics information from sofascore.com/tennis and return them as a dictionary with the following keys:
    - match_id: match identifier
    - tournament_id: id of the tournament
    - tournament_name: name of the tournament
    - tournament_slug: slug of the tournament
    - tournament_category_name: ATP or WTA or etc
    - tournament_category_slug: ATP or WTA or etc slug
    - user_count: user that follows the tournament and matches
    - ground_type: type of ground for exmaple clay, grass, hard and ...
    - tennis_points: points for winner

    Parameters:
    - match_id (int): the match identifier for the statistic
    - response_json (json): response of match information request that provides from sofascore.com tennis matches
    - data_class_schema (MatchTournamentInfo): dataclass structure for statistics about tournament events
    """
    
    single_match_tournament_info = data_class_schema(
        match_id = match_id, 
        tournament_id = response_json["event"]["tournament"].get("id"),
        tournament_name = response_json["event"]["tournament"].get("name"),
        tournament_slug = response_json["event"]["tournament"].get("slug"),
        tournament_category_name = response_json["event"]["tournament"]["category"].get("name"),
        tournament_category_slug = response_json["event"]["tournament"]["category"].get("slug"),
        user_count = response_json["event"]["tournament"]["uniqueTournament"].get("userCount"),
        ground_type = response_json["event"]["tournament"]["uniqueTournament"].get("groundType"),
        tennis_points = response_json["event"]["tournament"]["uniqueTournament"].get("tennisPoints"),
        tournament_unique_id = response_json["event"]["tournament"]["uniqueTournament"].get("tournamentId"),
        has_event_player_statistics = response_json["event"]["tournament"]["uniqueTournament"].get("hasEventPlayerStatistics"),
        crowd_sourcing_enabled = response_json["event"]["tournament"]["uniqueTournament"].get("crowdsourcingEnabled"),
        has_performance_graph_feature = response_json["event"]["tournament"]["uniqueTournament"].get("hasPerformanceGraphFeature"),
        display_inverse_home_away_teams = response_json["event"]["tournament"]["uniqueTournament"].get("displayInverseHomeAwayTeams"),
        priority = response_json["event"]["tournament"].get("priority"),
        competition_type = response_json["event"]["tournament"].get("competitionType")    
    )
    return [asdict(single_match_tournament_info)]


def match_season_information(match_id: int, response_json: json, data_class_schema: MatchSeasonInfo) -> list[dict]:
    """
    fetch statistics information from sofascore.com/tennis and return them as a dictionary with the following keys:
    - match_id: match identifier
    - season_id: season number
    - name: name of the tournament with the tournament year
    - year: year of tournament
    
    Parameters:
    - match_id (int): the match identifier for the statistic
    - response_json (json): response of match information request that provides from sofascore.com tennis matches
    - data_class_schema (MatchSeasonInfo): dataclass structure for statistics about season events
    """
    
    single_match_season_info = data_class_schema(
        match_id = match_id,
        season_id = response_json["event"]["season"].get("id"),
        name = response_json["event"]["season"].get("name"),
        year = int(response_json["event"]["season"].get("year"))
    )
    return [single_match_season_info]


def match_round_information(match_id: int, response_json: json, data_class_schema: MatchRoundInfo) -> list[dict]:
    """
    fetch statistics information from sofascore.com/tennis and return them as a dictionary with the following keys:
    - match_id: match identifier
    - round_id: round number
    - name: name of the round
    - slug: slug of the round
    - cup_round_type: type fo round in cup
    
    Parameters:
    - match_id (int): the match identifier for the statistic
    - response_json (json): response of match information request that provides from sofascore.com tennis matches
    - data_class_schema (MatchRoundInfo): dataclass structure for statistics about round events
    """
    
    single_match_round_info = data_class_schema(
        match_id = match_id,    
        round_id = response_json["event"]["roundInfo"].get("round"),
        name = response_json["event"]["roundInfo"].get("name"),
        slug = response_json["event"]["roundInfo"].get("slug"),
        cup_round_type = response_json["event"]["roundInfo"].get("cupRoundType")
    )
    return [asdict(single_match_round_info)]


def match_venue_information(match_id: int, response_json: json, data_class_schema: MatchVenueInfo) -> list[dict]:
    """
    fetch statistics information from sofascore.com/tennis and return them as a dictionary with the following keys:
    - match_id: match identifier
    - city: city of venue information
    - stadium: name of the stadium
    - venue_id: id of venue 
    - country: venue country
    
    Parameters:
    - match_id (int): the match identifier for the statistic
    - response_json (json): response of match information request that provides from sofascore.com tennis matches
    - data_class_schema (MatchVenueInfo): dataclass structure for statistics about venue events
    """
    
    single_match_venue_info = data_class_schema(
        match_id = match_id,
        city = response_json["event"]["venue"]["city"].get("name"),
        stadium = response_json["event"]["venue"]["stadium"].get("name"),
        venue_id = response_json["event"]["venue"].get("id"),
        country = response_json["event"]["venue"]["country"].get("name")
    )
    return [asdict(single_match_venue_info)]


def match_home_team_information(match_id: int, response_json: json, data_class_schema: MatchHomeTeamInfo) -> list[dict]:
    """
    fetch statistics information from sofascore.com/tennis and return them as a dictionary with the following keys:
    - match_id: match identifier
    - name: name of home player
    - slug: slug of home player
    - gender: gender of home player
    - user_count: user that follows the home player
    - residence: location of home player
    - birthplace: birthplace of home player
    - height: height of home player
    - weight: weight of home player
    - plays: right handed or left handed
    - turned_pro: the year that player turned pro
    - current_prize: total prize during tournament
    - total_prize: total prize of tournament
    - player_id: player identifier
    - current_rank: rank of player at match
    - name_code: three name abbriviation
    - country: country of home player
    - full_name: name of home player
    
    Parameters:
    - match_id (int): the match identifier for the statistic
    - response_json (json): response of match information request that provides from sofascore.com tennis matches
    - data_class_schema (MatchHomeTeamInfo): dataclass structure for statistics about home team events
    """
    
    single_match_home_team_info = data_class_schema(
        match_id = match_id,
        name = response_json["event"]["homeTeam"].get("name"),
        slug = response_json["event"]["homeTeam"].get("slug"),
        gender = response_json["event"]["homeTeam"].get("gender"),
        user_count = response_json["event"]["homeTeam"].get("userCount"),
        residence = response_json["event"]["homeTeam"]["playerTeamInfo"].get("residence"),
        birthplace = response_json["event"]["homeTeam"]["playerTeamInfo"].get("birthplace"),
        height = response_json["event"]["homeTeam"]["playerTeamInfo"].get("height"),
        weight = response_json["event"]["homeTeam"]["playerTeamInfo"].get("weight"),
        plays = response_json["event"]["homeTeam"]["playerTeamInfo"].get("plays"),
        turned_pro = response_json["event"]["homeTeam"]["playerTeamInfo"].get("turnedPro"),
        current_prize = response_json["event"]["homeTeam"]["playerTeamInfo"].get("prizeCurrent"),
        total_prize = response_json["event"]["homeTeam"]["playerTeamInfo"].get("prizeTotal"),
        player_id = response_json["event"]["homeTeam"].get("id"),
        current_rank = response_json["event"]["homeTeam"].get("ranking"),
        name_code = response_json["event"]["homeTeam"].get("nameCode"),
        country = response_json["event"]["homeTeam"]["country"].get("name"),
        full_name = response_json["event"]["homeTeam"].get("fullName")
    )
    return [asdict(single_match_home_team_info)]


def match_away_team_information(match_id: int, response_json: json, data_class_schema: MatchAwayTeamInfo) -> list[dict]:
    """
    fetch statistics information from sofascore.com/tennis and return them as a dictionary with the following keys:
    - match_id: match identifier
    - name: name of away player
    - slug: slug of away player
    - gender: gender of away player
    - user_count: user that follows the away player
    - residence: location of away player
    - birthplace: birthplace of away player
    - height: height of away player
    - weight: weight of away player
    - plays: right handed or left handed
    - turned_pro: the year that player turned pro
    - current_prize: total prize during tournament
    - total_prize: total prize of tournament
    - player_id: player identifier
    - current_rank: rank of player at match
    - name_code: three name abbriviation
    - country: country of away player
    - full_name: name of away player
    
    Parameters:
    - match_id (int): the match identifier for the statistic
    - response_json (json): response of match information request that provides from sofascore.com tennis matches
    - data_class_schema (MatchAwayTeamInfo): dataclass structure for statistics about away team events
    """
    
    single_match_away_team_info = data_class_schema(
        match_id = match_id,
        name = response_json["event"]["awayTeam"].get("name"),
        slug = response_json["event"]["awayTeam"].get("slug"),
        gender = response_json["event"]["awayTeam"].get("gender"),
        user_count = response_json["event"]["awayTeam"].get("userCount"),
        residence = response_json["event"]["awayTeam"]["playerTeamInfo"].get("residence"),
        birthplace = response_json["event"]["awayTeam"]["playerTeamInfo"].get("birthplace"),
        height = response_json["event"]["awayTeam"]["playerTeamInfo"].get("height"),
        weight = response_json["event"]["awayTeam"]["playerTeamInfo"].get("weight"),
        plays = response_json["event"]["awayTeam"]["playerTeamInfo"].get("plays"),
        turned_pro = response_json["event"]["awayTeam"]["playerTeamInfo"].get("turnedPro"),
        current_prize = response_json["event"]["awayTeam"]["playerTeamInfo"].get("prizeCurrent"),
        total_prize = response_json["event"]["awayTeam"]["playerTeamInfo"].get("prizeTotal"),
        player_id = response_json["event"]["awayTeam"].get("id"),
        current_rank = response_json["event"]["awayTeam"].get("ranking"),
        name_code = response_json["event"]["awayTeam"].get("nameCode"),
        country = response_json["event"]["awayTeam"]["country"].get("name"),
        full_name = response_json["event"]["awayTeam"].get("fullName")
    )
    return [asdict(single_match_away_team_info)]


def match_home_team_score_information(match_id: int, response_json: json, data_class_schema: MatchHomeScoreInfo) -> list[dict]:
    """
    fetch statistics information from sofascore.com/tennis and return them as a dictionary with the following keys:
    - match_id: match identifier
    - current_score: score of home team 
    - display_score: score of home team that displayed
    - period_1: number of games in period 1
    - period_2: number of games in period 2
    - period_3: number of games in period 3
    - period_4: number of games in period 4
    - period_5: number of games in period 5
    - period_1_tie_break: number of points if period 1 has tie_break
    - period_2_tie_break: number of points if period 2 has tie_break
    - period_3_tie_break: number of points if period 3 has tie_break
    - period_4_tie_break: number of points if period 4 has tie_break
    - period_5_tie_break: number of points if period 5 has tie_break
    
    Parameters:
    - match_id (int): the match identifier for the statistic
    - response_json (json): response of match information request that provides from sofascore.com tennis matches
    - data_class_schema (MatchHomeScoreInfo): dataclass structure for statistics about home team score events
    """
    
    single_match_home_team_score_info = data_class_schema(
        match_id = match_id,
        current_score = response_json["event"]["homeScore"].get("current"),
        display_score = response_json["event"]["homeScore"].get("display"),
        period_1 = response_json["event"]["homeScore"].get("period1"),
        period_2 = response_json["event"]["homeScore"].get("period2"),
        period_3 = response_json["event"]["homeScore"].get("period3"),
        period_4 = response_json["event"]["homeScore"].get("period4"),
        period_5 = response_json["event"]["homeScore"].get("period5"),
        period_1_tie_break = response_json["event"]["homeScore"].get("period1TieBreak"),
        period_2_tie_break = response_json["event"]["homeScore"].get("period2TieBreak"),
        period_3_tie_break = response_json["event"]["homeScore"].get("period3TieBreak"),
        period_4_tie_break = response_json["event"]["homeScore"].get("period4TieBreak"),
        period_5_tie_break = response_json["event"]["homeScore"].get("period5TieBreak"),
        normal_time = response_json["event"]["homeScore"].get("normal_time")
    )
    return [asdict(single_match_home_team_score_info)]


def match_away_team_score_information(match_id: int, response_json: json, data_class_schema: MatchAwayScoreInfo) -> list[dict]:
    """
    fetch statistics information from sofascore.com/tennis and return them as a dictionary with the following keys:
    - match_id: match identifier
    - current_score: score of away team 
    - display_score: score of away team that displayed
    - period_1: number of games in period 1
    - period_2: number of games in period 2
    - period_3: number of games in period 3
    - period_4: number of games in period 4
    - period_5: number of games in period 5
    - period_1_tie_break: number of points if period 1 has tie_break
    - period_2_tie_break: number of points if period 2 has tie_break
    - period_3_tie_break: number of points if period 3 has tie_break
    - period_4_tie_break: number of points if period 4 has tie_break
    - period_5_tie_break: number of points if period 5 has tie_break
    
    Parameters:
    - match_id (int): the match identifier for the statistic
    - response_json (json): response of match information request that provides from sofascore.com tennis matches
    - data_class_schema (MatchAwayScoreInfo): dataclass structure for statistics about away team score events
    """
    
    single_match_away_team_score_info = data_class_schema(
        match_id = match_id,
        current_score = response_json["event"]["awayScore"].get("current"),
        display_score = response_json["event"]["awayScore"].get("display"),
        period_1 = response_json["event"]["awayScore"].get("period1"),
        period_2 = response_json["event"]["awayScore"].get("period2"),
        period_3 = response_json["event"]["awayScore"].get("period3"),
        period_4 = response_json["event"]["awayScore"].get("period4"),
        period_5 = response_json["event"]["awayScore"].get("period5"),
        period_1_tie_break = response_json["event"]["awayScore"].get("period1TieBreak"),
        period_2_tie_break = response_json["event"]["awayScore"].get("period2TieBreak"),
        period_3_tie_break = response_json["event"]["awayScore"].get("period3TieBreak"),
        period_4_tie_break = response_json["event"]["awayScore"].get("period4TieBreak"),
        period_5_tie_break = response_json["event"]["awayScore"].get("period5TieBreak"),
        normal_time = response_json["event"]["awayScore"].get("normal_time")
    )
    return [asdict(single_match_away_team_score_info)]


def match_time_information(match_id: int, response_json: json, data_class_schema: MatchTimeInfo) -> list[dict]:
    """
    fetch statistics information from sofascore.com/tennis and return them as a dictionary with the following keys:
    - match_id: match identifier
    - period_1: time of games in period 1
    - period_2: time of games in period 2
    - period_3: time of games in period 3
    - period_4: time of games in period 4
    - period_5: time of games in period 5
    - current_period_start_timestamp: time of the start of the period 1
    
    Parameters:
    - match_id (int): the match identifier for the statistic
    - response_json (json): response of match information request that provides from sofascore.com tennis matches
    - data_class_schema (MatchTimeInfo): dataclass structure for statistics about time events
    """
    
    single_match_time_info = data_class_schema(
        match_id = match_id,
        period_1 = response_json["event"]["time"].get("period1"),
        period_2 = response_json["event"]["time"].get("period2"),
        period_3 = response_json["event"]["time"].get("period3"),
        period_4 = response_json["event"]["time"].get("period4"),
        period_5 = response_json["event"]["time"].get("period5"),
        current_period_start_timestamp = response_json["event"]["time"].get("currentPeriodStartTimestamp")
    )
    return [asdict(single_match_time_info)]