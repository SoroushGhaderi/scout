"""Tab-specific parsers for different odds types"""

import logging
import json
from typing import List, Optional, Union
from datetime import datetime

from .models import (
    Odds1X2, OddsAsianHandicap, OddsOverUnder
)

logger = logging.getLogger(__name__)


class OddsParserFactory:
    """Factory to create appropriate parser based on tab name"""
    
    @staticmethod
    def get_parser(tab_name: str):
        """Get parser for specific tab type"""
        tab_lower = tab_name.lower().strip()
        
        # Map tab names to parsers (log only once)
        if any(x in tab_lower for x in ['1x2', '1 x 2', 'match result', 'full time result']):
            return Parse1X2()
        
        elif any(x in tab_lower for x in ['asian handicap', 'handicap', 'ah']):
            return ParseAsianHandicap()
        
        elif any(x in tab_lower for x in ['total goals', 'over/under', 'o/u', 'goals']):
            return ParseOverUnder('goals')
        
        elif any(x in tab_lower for x in ['total corners', 'corners']):
            return ParseOverUnder('corners')
        
        else:
            # Default to generic parser
            return ParseGeneric()


class BaseOddsParser:
    """Base class for odds parsers"""
    
    def parse_row(self, cells: List[str], match_id: str, match_url: str) -> Optional[object]:
        """Parse a single table row - to be overridden"""
        raise NotImplementedError
    
    def _extract_float(self, value: str) -> Optional[float]:
        """Extract float from string, handling various formats"""
        if not value:
            return None
        
        # Clean the string
        cleaned = value.strip().replace(',', '')
        
        # If there are multiple numbers (e.g., "0 1.97"), try to extract the last meaningful one
        if ' ' in cleaned:
            parts = cleaned.split()
            for part in reversed(parts):  # Check from right to left
                try:
                    val = float(part)
                    if val > 0.5:  # Prefer odds-like values
                        return val
                except:
                    continue
        
        # Try to parse as is
        try:
            return float(cleaned)
        except (ValueError, AttributeError):
            return None


class Parse1X2(BaseOddsParser):
    """Parser for 1X2 (Match Result) odds"""
    
    def parse_row(self, cells: List[str], match_id: str, match_url: str) -> Optional[Odds1X2]:
        """
        Parse 1X2 row
        Expected format: [Bookmaker, Home Odds, Draw Odds, Away Odds]
        But flexible to handle variations
        """
        if len(cells) < 3:
            logger.debug(f"Parse1X2: Need at least 3 cells, got {len(cells)}")
            return None
        
        # Try to find bookmaker and odds
        bookmaker = cells[0].strip() if cells[0] else "Unknown"
        
        # Extract all numeric values from remaining cells
        odds_values = []
        for cell in cells[1:]:
            val = self._extract_float(cell)
            if val:
                odds_values.append(val)
        
        # Need at least 2 odds (home and away minimum)
        if len(odds_values) < 2:
            logger.debug(f"Parse1X2: Not enough odds values found")
            return None
        
        # Assign odds based on what we have
        home_odds = odds_values[0] if len(odds_values) > 0 else None
        draw_odds = odds_values[1] if len(odds_values) > 1 else None
        away_odds = odds_values[2] if len(odds_values) > 2 else None
        
        # If we have exactly 2 values, it might be home/away (no draw)
        if len(odds_values) == 2:
            draw_odds = None
            away_odds = odds_values[1]
        
        return Odds1X2(
            match_id=match_id,
            match_url=match_url,
            bookmaker=bookmaker,
            home_odds=home_odds,
            draw_odds=draw_odds,
            away_odds=away_odds,
            scraped_at=datetime.now()
        )


class ParseAsianHandicap(BaseOddsParser):
    """Parser for Asian Handicap odds - 4 columns with combined handicap+odds"""
    
    def parse_row(self, cells: List[str], match_id: str, match_url: str) -> Optional[OddsAsianHandicap]:
        """
        Parse Asian Handicap row
        Format: [Time, Score, "Home_Handicap Home_Odds", "Away_Handicap Away_Odds"]
        Example: ["90+4'", "1-0", "0 5.10", "0 1.17"]
        
        Returns 6 metrics: time, score, home_handicap, home_odds, away_handicap, away_odds
        """
        if len(cells) < 4:
            return None
        
        # Extract time and score
        match_time = cells[0].strip()
        moment_result = cells[1].strip()
        
        if not match_time or not moment_result:
            return None
        
        # Parse home: "0 5.10" or "-0/0.5 6.49"
        home_parts = cells[2].strip().split()
        if len(home_parts) < 2:
            return None
        
        home_handicap = home_parts[0]  # "0" or "-0/0.5"
        home_odds = self._extract_float(home_parts[1])  # "5.10"
        
        # Parse away: "0 1.17" or "+0/0.5 1.09"
        away_parts = cells[3].strip().split()
        if len(away_parts) < 2:
            return None
        
        away_handicap = away_parts[0]  # "0" or "+0/0.5"
        away_odds = self._extract_float(away_parts[1])  # "1.17"
        
        # Validate all fields
        if not home_handicap or not away_handicap:
            return None
            
        if home_odds is None or away_odds is None:
            return None
        
        return OddsAsianHandicap(
            match_id=match_id,
            match_url=match_url,
            match_time=match_time,
            moment_result=moment_result,
            home_handicap=home_handicap,
            home_odds=home_odds,
            away_handicap=away_handicap,
            away_odds=away_odds,
            scraped_at=datetime.now()
        )


class ParseOverUnder(BaseOddsParser):
    """Parser for Over/Under (Total Goals/Corners) odds"""
    
    def __init__(self, market_type: str = 'goals'):
        self.market_type = market_type
    
    def parse_row(self, cells: List[str], match_id: str, match_url: str) -> Optional[OddsOverUnder]:
        """
        Parse Over/Under row
        Flexible parser that extracts total line and over/under odds
        """
        if len(cells) < 2:
            return None
        
        bookmaker = cells[0].strip() if cells[0] else "Unknown"
        
        # Try to find total line and odds
        total_line = None
        odds_values = []
        
        for cell in cells[1:]:
            # Try to extract numeric value
            val = self._extract_float(cell)
            if val:
                # If value is between 0-20, it's probably a total line (e.g., 2.5, 10.5)
                if total_line is None and 0 < val < 20:
                    total_line = val
                # If value is > 1, it's probably odds
                elif val >= 1.01:
                    odds_values.append(val)
        
        if total_line is None or len(odds_values) < 2:
            return None
        
        over_odds = odds_values[0]
        under_odds = odds_values[1]
        
        return OddsOverUnder(
            match_id=match_id,
            match_url=match_url,
            bookmaker=bookmaker,
            total_line=total_line,
            over_odds=over_odds,
            under_odds=under_odds,
            market_type=self.market_type,
            scraped_at=datetime.now()
        )


def parse_table_row(
    row_element,
    match_id: str,
    match_url: str,
    tab_name: str,
    row_idx: int,
    parser=None  # Pass parser to avoid recreating it for each row
) -> Optional[Union[Odds1X2, OddsAsianHandicap, OddsOverUnder]]:
    """
    Parse a single table row using appropriate parser
    
    Args:
        row_element: Selenium WebElement (tr)
        match_id: Match ID
        match_url: Match URL
        tab_name: Name of current tab
        row_idx: Row index
        parser: Pre-created parser (for efficiency)
    
    Returns:
        Odds object or None
    """
    try:
        from selenium.webdriver.common.by import By
        
        # Extract cells
        cells_elements = row_element.find_elements(By.TAG_NAME, "td")
        cells = [cell.text.strip() for cell in cells_elements]
        
        if not cells:
            return None
        
        logger.debug(f"Row {row_idx} [{tab_name}]: {cells}")
        
        # Get appropriate parser (create if not provided)
        if parser is None:
            parser = OddsParserFactory.get_parser(tab_name)
        
        # Parse the row
        if isinstance(parser, ParseGeneric):
            return parser.parse_row(cells, match_id, match_url, market_type=tab_name)
        else:
            return parser.parse_row(cells, match_id, match_url)
        
    except Exception as e:
        logger.debug(f"Error parsing row {row_idx}: {e}")
        return None

