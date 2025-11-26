"""Venue and match information models."""

from typing import Optional
from pydantic import BaseModel, Field, ConfigDict


class MatchVenue(BaseModel):
    """
    Complete venue, stadium, and match information.
    Includes capacity, attendance, referee, and tournament details.
    """
    
    model_config = ConfigDict(extra='ignore', validate_assignment=True)
    
    match_id: int = Field(..., description="Unique match identifier")
    
    # Stadium Information
    stadium_name: Optional[str] = Field(None, description="Full name of the stadium")
    stadium_city: Optional[str] = Field(None, description="City where stadium is located")
    stadium_country: Optional[str] = Field(None, description="Country where stadium is located")
    stadium_latitude: Optional[float] = Field(None, description="GPS latitude coordinate")
    stadium_longitude: Optional[float] = Field(None, description="GPS longitude coordinate")
    stadium_capacity: Optional[int] = Field(None, description="Maximum stadium capacity")
    stadium_surface: Optional[str] = Field(None, description="Playing surface type (e.g., grass, artificial)")
    
    # Match Attendance & Officials
    attendance: Optional[int] = Field(None, description="Actual match attendance")
    referee_name: Optional[str] = Field(None, description="Name of the match referee")
    referee_country: Optional[str] = Field(None, description="Referee's country")
    referee_image_url: Optional[str] = Field(None, description="URL to referee's image")
    
    # Match Date & Time
    match_date_utc: Optional[str] = Field(None, description="Match date and time in UTC")
    match_date_verified: Optional[bool] = Field(None, description="Whether match date is confirmed")
    
    # Tournament Information
    tournament_id: Optional[int] = Field(None, description="Tournament/competition ID")
    tournament_name: Optional[str] = Field(None, description="Name of the tournament")
    tournament_round: Optional[str] = Field(None, description="Tournament round/matchday")
    tournament_parent_league_id: Optional[int] = Field(None, description="Parent league ID")
    tournament_link: Optional[str] = Field(None, description="Link to tournament page")


