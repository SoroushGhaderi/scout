from dataclasses import dataclass
from typing import Optional


@dataclass
class PeriodInfo:
    match_id: Optional[int]
    period: Optional[str]
    statistic_category_name: Optional[str]
    statistic_name: Optional[str]
    home_stat: Optional[str]
    away_stat: Optional[str]
    compare_code: Optional[int]
    statistic_type: Optional[str]
    value_type: Optional[str]
    home_value: Optional[int]
    away_value: Optional[int]
    home_total: Optional[int]
    away_total: Optional[int]


@dataclass
class MatchVotesInfo:
    match_id: Optional[int]
    home_vote: Optional[int]
    away_vote: Optional[int]


@dataclass
class MatchEventInfo:
    match_id: Optional[int]
    first_to_serve: Optional[int]
    home_team_seed: Optional[int]
    away_team_seed: Optional[int]
    custom_id: Optional[str]
    winner_code: Optional[int]
    default_period_count: Optional[int]
    start_datetime: Optional[int]
    match_slug: Optional[str]
    final_result_only: Optional[bool]


@dataclass
class MatchTournamentInfo:
    match_id: Optional[int]
    tournament_id: Optional[int]
    tournament_name: Optional[str]
    tournament_slug: Optional[str]
    tournament_unique_id: Optional[int]
    tournament_category_name: Optional[str]
    tournament_category_slug: Optional[str]
    user_count: Optional[str]
    ground_type: Optional[str]
    tennis_points: Optional[str]
    has_event_player_statistics: Optional[bool]
    crowd_sourcing_enabled: Optional[bool]
    has_performance_graph_feature: Optional[bool]
    display_inverse_home_away_teams: Optional[bool]
    priority: Optional[int]
    competition_type: Optional[int]


@dataclass
class MatchSeasonInfo:
    match_id: Optional[int]
    season_id: Optional[int]
    name: Optional[str]
    year: Optional[int]


@dataclass
class MatchRoundInfo:
    match_id: Optional[int]
    round_id: Optional[int]
    name: Optional[str]
    slug: Optional[str]
    cup_round_type: Optional[int]


@dataclass
class MatchVenueInfo:
    match_id: Optional[int]
    city: Optional[str]
    stadium: Optional[str]
    venue_id: Optional[int]
    country: Optional[str]


@dataclass
class MatchHomeTeamInfo:
    match_id: Optional[int]
    name: Optional[str]
    slug: Optional[str]
    gender: Optional[str]
    user_count: Optional[int]
    residence: Optional[str]
    birthplace: Optional[str]
    height: Optional[float]
    weight: Optional[int]
    plays: Optional[str]
    turned_pro: Optional[int]
    current_prize: Optional[int]
    total_prize: Optional[int]
    player_id: Optional[int]
    current_rank: Optional[int]
    name_code: Optional[str]
    country: Optional[str]
    full_name: Optional[str]


@dataclass
class MatchAwayTeamInfo:
    match_id: Optional[int]
    name: Optional[str]
    slug: Optional[str]
    gender: Optional[str]
    user_count: Optional[int]
    residence: Optional[str]
    birthplace: Optional[str]
    height: Optional[float]
    weight: Optional[int]
    plays: Optional[str]
    turned_pro: Optional[int]
    current_prize: Optional[int]
    total_prize: Optional[int]
    player_id: Optional[int]
    current_rank: Optional[int]
    name_code: Optional[str]
    country: Optional[str]
    full_name: Optional[str]


@dataclass
class MatchHomeScoreInfo:
    match_id: Optional[int]
    current_score: Optional[int]
    display_score: Optional[int]
    period_1: Optional[int]
    period_2: Optional[int]
    period_3: Optional[int]
    period_4: Optional[int]
    period_5: Optional[int]
    period_1_tie_break: Optional[int]
    period_2_tie_break: Optional[int]
    period_3_tie_break: Optional[int]
    period_4_tie_break: Optional[int]
    period_5_tie_break: Optional[int]
    normal_time: Optional[int]


@dataclass
class MatchAwayScoreInfo:
    match_id: Optional[int]
    current_score: Optional[int]
    display_score: Optional[int]
    period_1: Optional[int]
    period_2: Optional[int]
    period_3: Optional[int]
    period_4: Optional[int]
    period_5: Optional[int]
    period_1_tie_break: Optional[int]
    period_2_tie_break: Optional[int]
    period_3_tie_break: Optional[int]
    period_4_tie_break: Optional[int]
    period_5_tie_break: Optional[int]
    normal_time: Optional[int]


@dataclass
class MatchTimeInfo:
    match_id: Optional[int]
    period_1: Optional[int]
    period_2: Optional[int]
    period_3: Optional[int]
    period_4: Optional[int]
    period_5: Optional[int]
    current_period_start_timestamp: Optional[int]


@dataclass
class GameInfo:
    match_id: Optional[int]
    set_id: Optional[int]
    game_id: Optional[int]
    point_id: Optional[int]
    home_point: Optional[int]
    away_point: Optional[int]
    point_description: Optional[int]
    home_point_type: Optional[int]
    away_point_type: Optional[int]
    home_score: Optional[int]
    away_score: Optional[int]
    serving: Optional[int]
    scoring: Optional[int]


@dataclass
class OddsInfo:
    match_id: Optional[int]
    market_id: Optional[int]
    market_name: Optional[str]
    is_live: Optional[int]
    suspended: Optional[bool]
    initial_fractional_value: Optional[str]
    fractional_value: Optional[str]
    choice_name: Optional[str]
    choice_source_id: Optional[int]
    winnig: Optional[bool]
    change: Optional[str]


@dataclass
class PowerInfo:
    match_id: Optional[int]
    set_num: Optional[int]
    game_num: Optional[int]
    value: Optional[float]
    break_occurred: Optional[bool]
