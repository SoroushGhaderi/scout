"""Match-related Pydantic models."""

from datetime import datetime
from typing import Optional, Union
from pydantic import BaseModel, Field, ConfigDict


class MatchTimeline(BaseModel):
    """Represents the timeline and status of a football match."""

    model_config = ConfigDict(
        extra='ignore',
        validate_assignment=True,
        str_strip_whitespace=True,
    )

    match_id: Optional[int] = Field(None, description="Unique match identifier")
    match_time_utc: Optional[datetime] = Field(None, description="Scheduled match start time UTC")
    first_half_started: Optional[str] = Field(None, description="Actual first half start time")
    first_half_ended: Optional[str] = Field(None, description="First half end time")
    second_half_started: Optional[str] = Field(None, description="Second half start time")
    second_half_ended: Optional[str] = Field(None, description="Second half end time")
    first_extra_half_started: Optional[str] = Field(None, description="First extra time half start time")
    second_extra_half_started: Optional[str] = Field(None, description="Second extra time half start time")
    game_ended: Optional[str] = Field(None, description="Final match end time")
    game_finished: Optional[bool] = Field(False, description="Whether the match completed normally")
    game_started: Optional[bool] = Field(False, description="Whether the match started")
    game_cancelled: Optional[bool] = Field(False, description="Whether the match was cancelled")


class GeneralMatchStats(BaseModel):
    """Represents general statistics and information about a match."""

    model_config = ConfigDict(
        extra='ignore',
        validate_assignment=True,
        str_strip_whitespace=True,
    )

    match_id: Optional[int] = Field(None, description="Unique match identifier")
    match_round: Optional[str] = Field(None, description="Current round of the match")
    team_color_dark_mode_home: Optional[str] = Field(None, description="Home team color dark mode")
    team_color_dark_mode_away: Optional[str] = Field(None, description="Away team color dark mode")
    team_color_light_mode_home: Optional[str] = Field(None, description="Home team color light mode")
    team_color_light_mode_away: Optional[str] = Field(None, description="Away team color light mode")
    league_id: Optional[int] = Field(None, description="League ID")
    league_name: Optional[str] = Field(None, description="League name")
    league_round_name: Optional[str] = Field(None, description="Name of the league round")
    parent_league_id: Optional[int] = Field(None, description="Parent league ID")
    country_code: Optional[str] = Field(None, description="Country code of the league")
    parent_league_name: Optional[str] = Field(None, description="Parent league name")
    parent_league_season: Optional[str] = Field(None, description="Parent league season")
    parent_league_tournament_id: Optional[int] = Field(None, description="Parent league tournament ID")
    home_team_name: Optional[str] = Field(None, description="Home team name")
    home_team_id: Optional[int] = Field(None, description="Home team ID")
    away_team_name: Optional[str] = Field(None, description="Away team name")
    away_team_id: Optional[int] = Field(None, description="Away team ID")
    coverage_level: Optional[str] = Field(None, description="Match coverage level")
    match_time_utc: Optional[str] = Field(None, description="Scheduled match time UTC")
    match_time_utc_date: Optional[str] = Field(None, description="Match date UTC")
    match_started: bool = Field(False, description="Whether match has started")
    match_finished: bool = Field(False, description="Whether match has finished")


class InfoBox(BaseModel):
    """Represents general information details about the match, stadium, and referee."""

    model_config = ConfigDict(
        extra='ignore',
        validate_assignment=True,
        str_strip_whitespace=True,
    )

    match_id: Optional[int] = Field(None, description="Unique identifier for the match")
    match_date_utc: Optional[datetime] = Field(None, description="Match date and time UTC")
    match_date_verified: Optional[bool] = Field(None, description="Flag indicating if date is verified")
    tournament_id: Optional[int] = Field(None, description="Tournament ID")
    parent_league_id: Optional[int] = Field(None, description="Parent league ID")
    league_name: Optional[str] = Field(None, description="Name of the league")
    round_name: Optional[str] = Field(None, description="Name of the round")
    round_number: Optional[str] = Field(None, description="Round number")
    season: Optional[str] = Field(None, description="Current season")
    stadium_name: Optional[str] = Field(None, description="Stadium name")
    stadium_city: Optional[str] = Field(None, description="City where stadium is located")
    stadium_country: Optional[str] = Field(None, description="Country where stadium is located")
    stadium_lat: Optional[float] = Field(None, description="Latitude coordinate")
    stadium_long: Optional[float] = Field(None, description="Longitude coordinate")
    referee_name: Optional[str] = Field(None, description="Referee name")
    referee_country: Optional[str] = Field(None, description="Referee nationality")
    attendance: Optional[int] = Field(None, description="Number of attendees")


class MomentumDataPoint(BaseModel):
    """Represents a single data point in the match momentum chart."""

    model_config = ConfigDict(extra='ignore')

    match_id: int = Field(..., description="Unique match identifier")
    minute: Optional[float] = Field(..., description="Match minute timestamp")
    value: Optional[int] = Field(..., description="Momentum value (-100 to 100)")
    momentum_team: Optional[str] = Field(..., description="Which team has momentum")


class PeriodStats(BaseModel):
    """Represents aggregated statistics for a specific period of the match."""

    model_config = ConfigDict(
        extra='ignore',
        validate_assignment=True,
    )

    match_id: int = Field(..., description="Unique match identifier")
    period: str = Field(..., description="Match period")
    ball_possession_home: Optional[int] = Field(None, description="Ball possession % for home team")
    ball_possession_away: Optional[int] = Field(None, description="Ball possession % for away team")
    expected_goals_home: Optional[float] = Field(None, description="xG for home team")
    expected_goals_away: Optional[float] = Field(None, description="xG for away team")
    expected_goals_open_play_home: Optional[float] = Field(None, description="xG from open play for home")
    expected_goals_open_play_away: Optional[float] = Field(None, description="xG from open play for away")
    expected_goals_set_play_home: Optional[float] = Field(None, description="xG from set play for home")
    expected_goals_set_play_away: Optional[float] = Field(None, description="xG from set play for away")
    expected_goals_non_penalty_home: Optional[float] = Field(None, description="xG excluding penalties for home")
    expected_goals_non_penalty_away: Optional[float] = Field(None, description="xG excluding penalties for away")
    expected_goals_on_target_home: Optional[float] = Field(None, description="xGOT for home team")
    expected_goals_on_target_away: Optional[float] = Field(None, description="xGOT for away team")
    distance_covered_home: Optional[float] = Field(None, description="Distance covered by home team (meters)")
    distance_covered_away: Optional[float] = Field(None, description="Distance covered by away team (meters)")
    walking_distance_home: Optional[float] = Field(None, description="Walking distance by home team (meters)")
    walking_distance_away: Optional[float] = Field(None, description="Walking distance by away team (meters)")
    running_distance_home: Optional[float] = Field(None, description="Running distance by home team (meters)")
    running_distance_away: Optional[float] = Field(None, description="Running distance by away team (meters)")
    sprinting_distance_home: Optional[float] = Field(None, description="Sprinting distance by home team (meters)")
    sprinting_distance_away: Optional[float] = Field(None, description="Sprinting distance by away team (meters)")
    number_of_sprints_home: Optional[int] = Field(None, description="Number of sprints by home team")
    number_of_sprints_away: Optional[int] = Field(None, description="Number of sprints by away team")
    top_speed_home: Optional[float] = Field(None, description="Top speed by home team (km/h)")
    top_speed_away: Optional[float] = Field(None, description="Top speed by away team (km/h)")
    total_shots_home: Optional[int] = Field(None, description="Total shots by home team")
    total_shots_away: Optional[int] = Field(None, description="Total shots by away team")
    shots_on_target_home: Optional[int] = Field(None, description="Shots on target by home")
    shots_on_target_away: Optional[int] = Field(None, description="Shots on target by away")
    shots_off_target_home: Optional[int] = Field(None, description="Shots off target by home")
    shots_off_target_away: Optional[int] = Field(None, description="Shots off target by away")
    blocked_shots_home: Optional[int] = Field(None, description="Blocked shots by home")
    blocked_shots_away: Optional[int] = Field(None, description="Blocked shots by away")
    shots_woodwork_home: Optional[int] = Field(None, description="Shots hitting woodwork by home")
    shots_woodwork_away: Optional[int] = Field(None, description="Shots hitting woodwork by away")
    shots_sidebox_home: Optional[int] = Field(None, description="Shots from inside box by home")
    shots_sidebox_away: Optional[int] = Field(None, description="Shots from inside box by away")
    shots_outside_box_home: Optional[int] = Field(None, description="Shots from outside box by home")
    shots_outside_box_away: Optional[int] = Field(None, description="Shots from outside box by away")
    big_chances_home: Optional[int] = Field(None, description="Big chances by home")
    big_chances_away: Optional[int] = Field(None, description="Big chances by away")
    big_chances_missed_home: Optional[int] = Field(None, description="Big chances missed by home")
    big_chances_missed_away: Optional[int] = Field(None, description="Big chances missed by away")
    fouls_home: Optional[int] = Field(None, description="Fouls committed by home")
    fouls_away: Optional[int] = Field(None, description="Fouls committed by away")
    corners_home: Optional[int] = Field(None, description="Corners by home")
    corners_away: Optional[int] = Field(None, description="Corners by away")
    shots_home: Optional[int] = Field(None, description="Shots by home (alternative)")
    shots_away: Optional[int] = Field(None, description="Shots by away (alternative)")
    passes_home: Optional[int] = Field(None, description="Total passes by home")
    passes_away: Optional[int] = Field(None, description="Total passes by away")
    accurate_passes_home: Optional[str] = Field(None, description="Accurate passes for home (ratio or count)")
    accurate_passes_away: Optional[str] = Field(None, description="Accurate passes for away (ratio or count)")
    own_half_passes_home: Optional[int] = Field(None, description="Passes in own half by home")
    own_half_passes_away: Optional[int] = Field(None, description="Passes in own half by away")
    opposition_half_passes_home: Optional[int] = Field(None, description="Passes in opposition half by home")
    opposition_half_passes_away: Optional[int] = Field(None, description="Passes in opposition half by away")
    long_balls_accurate_home: Optional[str] = Field(None, description="Accurate long balls for home (ratio or count)")
    long_balls_accurate_away: Optional[str] = Field(None, description="Accurate long balls for away (ratio or count)")
    accurate_crosses_home: Optional[str] = Field(None, description="Accurate crosses for home (ratio or count)")
    accurate_crosses_away: Optional[str] = Field(None, description="Accurate crosses for away (ratio or count)")
    player_throws_home: Optional[int] = Field(None, description="Throw-ins by home")
    player_throws_away: Optional[int] = Field(None, description="Throw-ins by away")
    touches_opp_box_home: Optional[int] = Field(None, description="Touches in opp box by home")
    touches_opp_box_away: Optional[int] = Field(None, description="Touches in opp box by away")
    tackles_succeeded_home: Optional[str] = Field(None, description="Successful tackles for home (ratio or count)")
    tackles_succeeded_away: Optional[str] = Field(None, description="Successful tackles for away (ratio or count)")
    interceptions_home: Optional[int] = Field(None, description="Interceptions by home")
    interceptions_away: Optional[int] = Field(None, description="Interceptions by away")
    shot_blocks_home: Optional[int] = Field(None, description="Shot blocks by home")
    shot_blocks_away: Optional[int] = Field(None, description="Shot blocks by away")
    clearances_home: Optional[int] = Field(None, description="Clearances by home")
    clearances_away: Optional[int] = Field(None, description="Clearances by away")
    keeper_saves_home: Optional[int] = Field(None, description="Keeper saves by home")
    keeper_saves_away: Optional[int] = Field(None, description="Keeper saves by away")
    duels_won_home: Optional[int] = Field(None, description="Duels won by home")
    duels_won_away: Optional[int] = Field(None, description="Duels won by away")
    ground_duels_won_home: Optional[str] = Field(None, description="Ground duels won by home (ratio or count)")
    ground_duels_won_away: Optional[str] = Field(None, description="Ground duels won by away (ratio or count)")
    aerials_won_home: Optional[str] = Field(None, description="Aerials won by home (ratio or count)")
    aerials_won_away: Optional[str] = Field(None, description="Aerials won by away (ratio or count)")
    dribbles_succeeded_home: Optional[str] = Field(None, description="Successful dribbles by home (ratio or count)")
    dribbles_succeeded_away: Optional[str] = Field(None, description="Successful dribbles by away (ratio or count)")
    yellow_cards_home: Optional[int] = Field(None, description="Yellow cards for home")
    yellow_cards_away: Optional[int] = Field(None, description="Yellow cards for away")
    red_cards_home: Optional[int] = Field(None, description="Red cards for home")
    red_cards_away: Optional[int] = Field(None, description="Red cards for away")
    offsides_home: Optional[int] = Field(None, description="Offsides by home")
    offsides_away: Optional[int] = Field(None, description="Offsides by away")
    home_color: Optional[str] = Field(None, description="Home team color for display")
    away_color: Optional[str] = Field(None, description="Away team color for display")
