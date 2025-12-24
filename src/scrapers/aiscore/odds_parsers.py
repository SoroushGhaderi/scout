"""Tab-specific parsers for different odds types."""

import json
from typing import List, Optional, Union
from datetime import datetime

from .models import (
    Odds1X2,
    OddsAsianHandicap,
    OddsOverUnder
)
from ...utils.logging_utils import get_logger

logger = get_logger()


class OddsParserFactory:
    """Factory to create appropriate parser based on tab name."""

    @staticmethod
    def get_parser(tab_name: str):
        """Get parser for specific tab type."""
        tab_lower = tab_name.lower().strip()

        if any(x in tab_lower for x in ['1x2', '1 x 2', 'match result', 'full time result']):
            return Parse1X2()

        elif any(x in tab_lower for x in ['asian handicap', 'handicap', 'ah']):
            return ParseAsianHandicap()

        elif any(x in tab_lower for x in ['total goals', 'over/under', 'o/u', 'goals']):
            return ParseOverUnder('goals')

        elif any(x in tab_lower for x in ['total corners', 'corners']):
            return ParseOverUnder('corners')

        else:
            return ParseOverUnder('goals')


class BaseOddsParser:
    """Base class for odds parsers."""

    def parse_row(self, cells: List[str], match_id: str, match_url: str) -> Optional[object]:
        """Parse a single table row - to be overridden."""
        raise NotImplementedError

    def _extract_float(self, value: str) -> Optional[float]:
        """Extract float from string, handling various formats including Asian handicap notation."""
        if value is None:
            return None

        if not isinstance(value, str):
            try:
                value_str = str(value)
                if value_str.lower() == 'none' or value_str == 'None':
                    return None
                value = value_str
            except (TypeError, ValueError, AttributeError):
                return None

        if value is None:
            return None

        if not value or (
            isinstance(value, str) and value.strip().lower() == 'none'
        ):
            return None

        try:
            if not isinstance(value, str):
                return None
            cleaned = value.strip().replace(',', '')
            if not cleaned or cleaned is None:
                return None
        except (AttributeError, TypeError, ValueError):
            return None

        if cleaned is None or not isinstance(cleaned, str):
            return None

        # Handle Asian handicap notation like "3.5/4" = 3.75, "2.5/3" = 2.75
        if '/' in cleaned:
            try:
                parts = cleaned.split('/')
                if len(parts) == 2:
                    val1 = float(parts[0])
                    val2 = float(parts[1])
                    # Average of the two values
                    return (val1 + val2) / 2.0
            except (ValueError, TypeError, AttributeError):
                pass

        if ' ' in cleaned:
            parts = cleaned.split()
            for part in reversed(parts):
                if part is None:
                    continue
                if not part or (
                    isinstance(part, str) and part.strip().lower() == 'none'
                ):
                    continue
                try:
                    if part is None:
                        continue
                    val = float(part)
                    if val > 0.5:
                        return val
                except (ValueError, TypeError, AttributeError):
                    continue

        try:
            if cleaned is None:
                return None

            if not isinstance(cleaned, str) or cleaned is None:
                return None

            if cleaned is None:
                return None
            return float(cleaned)
        except (ValueError, TypeError, AttributeError) as e:
            return None


class Parse1X2(BaseOddsParser):
    """Parser for 1X2 (Match Result) odds."""

    def parse_row(
        self, cells: List[str], match_id: str, match_url: str
    ) -> Optional[Odds1X2]:
        """Parse 1X2 row.

        Expected formats:
        - [Bookmaker, Home Odds, Draw Odds, Away Odds] - standard format
        - [Time, Score, Home Odds, Draw Odds, Away Odds] - with temporal data
        - [Time, Score, '', '', ''] - suspended odds (save with nulls)

        Accepts rows with empty odds if they have time+score info.
        """
        if len(cells) < 3:
            return None

        try:
            if cells[0] is None:
                bookmaker = "Unknown"
            else:
                bookmaker = str(cells[0]).strip() if cells[0] else "Unknown"
        except (AttributeError, TypeError):
            bookmaker = "Unknown"

        # Check if this row has time+score (indicates suspended odds worth saving)
        has_temporal_data = False
        if len(cells) >= 2:
            # Check if cells[0] looks like a time (e.g., "88'", "45+2'")
            # and cells[1] looks like a score (e.g., "2-1", "0-0")
            cell0 = str(cells[0]).strip()
            cell1 = str(cells[1]).strip()
            is_time = cell0 and "'" in cell0
            is_score = cell1 and '-' in cell1 and len(cell1) <= 5
            has_temporal_data = is_time and is_score

        odds_values = []
        for cell in cells[1:]:
            if cell is not None:
                val = self._extract_float(cell)
                if val is not None:
                    odds_values.append(val)

        # If we have temporal data (time+score), save even with no odds
        if has_temporal_data and len(odds_values) == 0:
            return Odds1X2(
                match_id=match_id,
                match_url=match_url,
                bookmaker=bookmaker,  # This will be the time (e.g., "88'")
                home_odds=None,
                draw_odds=None,
                away_odds=None,
                scraped_at=datetime.now()
            )

        # Otherwise require at least 2 odds values
        if len(odds_values) < 2:
            return None

        home_odds = odds_values[0] if len(odds_values) > 0 else None
        draw_odds = odds_values[1] if len(odds_values) > 1 else None
        away_odds = odds_values[2] if len(odds_values) > 2 else None

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
    """Parser for Asian Handicap odds - 4 columns with combined handicap+odds."""

    def parse_row(
        self, cells: List[str], match_id: str, match_url: str
    ) -> Optional[OddsAsianHandicap]:
        """Parse Asian Handicap row.

        Formats:
        - [Time, Score, "Home_Handicap Home_Odds", "Away_Handicap Away_Odds"]
        - [Time, Score, '', ''] - suspended odds (save with nulls)

        Example: ["90+4'", "1-0", "0 5.10", "0 1.17"]
        Suspended: ["88'", "2-1", "", ""]

        Accepts rows with time+score even if odds are empty.
        """
        if len(cells) < 4:
            return None

        try:
            match_time = (
                cells[0].strip() if cells[0] is not None else None
            )
            moment_result = (
                cells[1].strip() if cells[1] is not None else None
            )
        except (AttributeError, TypeError):
            return None

        # Must have time and score to be valid
        if not match_time or not moment_result:
            return None

        # Try to parse home handicap data
        home_handicap = None
        home_odds = None
        try:
            if cells[2] and str(cells[2]).strip():
                home_parts = str(cells[2]).strip().split()
                if len(home_parts) >= 2:
                    home_handicap = str(home_parts[0])
                    home_odds = self._extract_float(home_parts[1])
        except (AttributeError, TypeError, IndexError):
            pass

        # Try to parse away handicap data
        away_handicap = None
        away_odds = None
        try:
            if cells[3] and str(cells[3]).strip():
                away_parts = str(cells[3]).strip().split()
                if len(away_parts) >= 2:
                    away_handicap = str(away_parts[0])
                    away_odds = self._extract_float(away_parts[1])
        except (AttributeError, TypeError, IndexError):
            pass

        # Always save if we have time+score (even with null odds = suspended)
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
    """Parser for Over/Under (Total Goals/Corners) odds."""

    def __init__(self, market_type: str = 'goals'):
        self.market_type = market_type

    def parse_row(
        self, cells: List[str], match_id: str, match_url: str
    ) -> Optional[OddsOverUnder]:
        """Parse Over/Under row.

        Formats:
        - [Time, Score, Total, Over, Under] - with temporal data
        - [Time, Score, '', '', ''] - suspended odds (save with nulls)
        - [Bookmaker, Total, Over, Under] - standard format

        Accepts rows with time+score even if odds are empty.
        """
        if len(cells) < 2:
            logger.debug(f"[ParseOverUnder] Row rejected: < 2 cells ({len(cells)})")
            return None

        try:
            if cells[0] is None:
                bookmaker = "Unknown"
            else:
                bookmaker = str(cells[0]).strip() if cells[0] else "Unknown"
        except (AttributeError, TypeError):
            bookmaker = "Unknown"

        # Check if this row has time+score format (temporal data)
        match_time = None
        moment_result = None
        has_temporal_data = False
        
        if len(cells) >= 2:
            cell0 = str(cells[0]).strip()
            cell1 = str(cells[1]).strip()
            is_time = cell0 and "'" in cell0
            is_score = cell1 and '-' in cell1 and len(cell1) <= 5
            
            if is_time and is_score:
                has_temporal_data = True
                match_time = cell0
                moment_result = cell1
                bookmaker = cell0  # Use time as bookmaker for temporal rows

        # Try positional parsing first (most common format)
        
        # Format 1: [Time, Score, Total, Over, Under] - 5 columns with current score
        if len(cells) >= 5 and has_temporal_data:
            total_val = self._extract_float(cells[2])
            over_val = self._extract_float(cells[3])
            under_val = self._extract_float(cells[4])
            
            # If we have time+score but no odds, save as suspended
            if total_val is None and over_val is None and under_val is None:
                return OddsOverUnder(
                    match_id=match_id,
                    match_url=match_url,
                    bookmaker=bookmaker,
                    match_time=match_time,
                    moment_result=moment_result,
                    total_line=None,
                    over_odds=None,
                    under_odds=None,
                    market_type=self.market_type,
                    scraped_at=datetime.now()
                )
            
            # If we have valid odds data
            if (total_val is not None and 
                over_val is not None and 
                under_val is not None and
                0 < total_val <= 50 and
                over_val >= 1.01 and
                under_val >= 1.01):
                
                return OddsOverUnder(
                    match_id=match_id,
                    match_url=match_url,
                    bookmaker=bookmaker,
                    match_time=match_time,
                    moment_result=moment_result,
                    total_line=total_val,
                    over_odds=over_val,
                    under_odds=under_val,
                    market_type=self.market_type,
                    scraped_at=datetime.now()
                )
        
        # Format 2: [Time/Bookmaker, Total, Over, Under] - 4 columns standard format
        if len(cells) >= 4:
            total_val = self._extract_float(cells[1])
            over_val = self._extract_float(cells[2])
            under_val = self._extract_float(cells[3])
            
            # Check if this looks like the expected format
            if (total_val is not None and 
                over_val is not None and 
                under_val is not None and
                0 < total_val <= 50 and  # Reasonable total line range
                over_val >= 1.01 and  # Reasonable odds range
                under_val >= 1.01):
                
                return OddsOverUnder(
                    match_id=match_id,
                    match_url=match_url,
                    bookmaker=bookmaker,
                    match_time=match_time,
                    moment_result=moment_result,
                    total_line=total_val,
                    over_odds=over_val,
                    under_odds=under_val,
                    market_type=self.market_type,
                    scraped_at=datetime.now()
                )
        
        # Fallback to heuristic parsing for non-standard formats
        total_line = None
        odds_values = []

        for cell in cells[1:]:
            if cell is None:
                continue
            val = self._extract_float(cell)
            if val is not None:
                is_half_value = (
                    (val % 1 == 0.5) or
                    (val % 0.5 == 0 and val % 1 != 0)
                )
                if (
                    total_line is None and
                    0 < val <= 30 and
                    (val < 20 or is_half_value)
                ):
                    total_line = val
                elif val >= 1.01:
                    odds_values.append(val)

        if total_line is None or len(odds_values) < 2:
            logger.debug(
                f"[ParseOverUnder] Row rejected: bookmaker={bookmaker}, "
                f"total_line={total_line}, odds_values={odds_values}, "
                f"cells={cells[:6]}"
            )
            return None

        over_odds = odds_values[0]
        under_odds = odds_values[1]

        return OddsOverUnder(
            match_id=match_id,
            match_url=match_url,
            bookmaker=bookmaker,
            match_time=match_time,
            moment_result=moment_result,
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
    parser=None
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

        cells_elements = row_element.find_elements(By.TAG_NAME, "td")
        cells = []
        for cell in cells_elements:
            try:
                cell_text = cell.text if cell.text is not None else ""
                cells.append(cell_text.strip())
            except (AttributeError, TypeError):
                cells.append("")

        if not cells or all(not c for c in cells):
            return None

        if parser is None:
            parser = OddsParserFactory.get_parser(tab_name)

        return parser.parse_row(cells, match_id, match_url)

    except Exception as e:
        logger.debug(f"Error parsing row {row_idx}: {e}")
        return None
