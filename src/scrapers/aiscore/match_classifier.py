"""Match Classifier - Detect unimportant matches to skip.

Filters out low-value matches to save scraping time and resources.
"""

import re
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta

from ...utils.date_utils import DATE_FORMAT_COMPACT

logger = logging.getLogger(__name__)


class MatchClassifier:
    """Classifies matches as important or unimportant for scraping."""

    UNIMPORTANT_LEAGUES = [
        'friendly', 'friendlies', 'amical', 'test match',
        'youth', 'u21', 'u19', 'u18', 'u17', 'u16',
        'reserve', 'reserves', 'b team', 'ii team',
        'amateur', 'regional', 'local cup',
        'women', 'female', 'ladies',
        'esports', 'virtual', 'e-sports',
        'futsal', 'beach soccer',
        'club friendly', 'international friendly'
    ]

    IMPORTANT_LEAGUES = [
        'premier league', 'la liga', 'serie a', 'bundesliga', 'ligue 1',
        'champions league', 'europa league', 'conference league',
        'world cup', 'euro', 'copa america',
        'premier liga', 'eredivisie', 'primeira liga',
        'championship', 'segunda division'
    ]

    UNIMPORTANT_KEYWORDS = [
        'friendly', 'test', 'exhibition', 'training',
        'youth', 'reserve', 'u21', 'u19', 'u18',
        'amateur', 'regional', 'local',
        'women', 'female', 'ladies',
        'virtual', 'esports', 'futsal'
    ]

    def __init__(self, config=None):
        self.config = config
        self.stats = {
            'total_checked': 0,
            'important': 0,
            'unimportant': 0,
            'reasons': {}
        }

    def is_important_match(
        self,
        match_url: str = None,
        teams: Dict[str, str] = None,
        league: str = None,
        match_date: str = None,
        odds_count: int = None
    ) -> tuple[bool, str]:
        """
        Determine if a match is important enough to scrape.

        Args:
            match_url: Match URL
            teams: Dictionary with 'home' and 'away' team names
            league: League/competition name
            match_date: Match date (YYYYMMDD or date string)
            odds_count: Number of odds already available

        Returns:
            Tuple of (is_important: bool, reason: str)
        """
        self.stats['total_checked'] += 1

        if league:
            is_important, reason = self._check_league(league)
            if not is_important:
                self._record_skip(reason)
                return False, reason

        if teams:
            is_important, reason = self._check_teams(teams)
            if not is_important:
                self._record_skip(reason)
                return False, reason

        if match_date:
            is_important, reason = self._check_date(match_date)
            if not is_important:
                self._record_skip(reason)
                return False, reason

        if odds_count is not None:
            is_important, reason = self._check_odds_count(odds_count)
            if not is_important:
                self._record_skip(reason)
                return False, reason

        if match_url:
            is_important, reason = self._check_url(match_url)
            if not is_important:
                self._record_skip(reason)
                return False, reason

        self.stats['important'] += 1
        return True, "Important match"

    def _check_league(self, league: str) -> tuple[bool, str]:
        """Check if league is important"""
        league_lower = league.lower()

        for important in self.IMPORTANT_LEAGUES:
            if important in league_lower:
                return True, f"Important league: {league}"

        for unimportant in self.UNIMPORTANT_LEAGUES:
            if unimportant in league_lower:
                return False, f"Unimportant league: {league}"

        return True, "Unknown league (assumed important)"

    def _check_teams(self, teams: Dict[str, str]) -> tuple[bool, str]:
        """Check if team names indicate unimportant match"""
        home = teams.get('home', '').lower()
        away = teams.get('away', '').lower()

        for keyword in self.UNIMPORTANT_KEYWORDS:
            if keyword in home:
                return False, f"Unimportant team keyword in home: '{keyword}'"

        for keyword in self.UNIMPORTANT_KEYWORDS:
            if keyword in away:
                return False, f"Unimportant team keyword in away: '{keyword}'"

        pattern = r'\b(b|ii|iii|reserves?|u\d{1,2})\b'
        if re.search(pattern, home, re.IGNORECASE):
            return False, f"Reserve/Youth team detected: {teams['home']}"
        if re.search(pattern, away, re.IGNORECASE):
            return False, f"Reserve/Youth team detected: {teams['away']}"

        return True, "Teams look important"

    def _check_date(self, match_date: str) -> tuple[bool, str]:
        """Check if match is too old or too far future"""
        try:
            if len(match_date) == 8:
                date = datetime.strptime(match_date, DATE_FORMAT_COMPACT)
            else:
                date = datetime.fromisoformat(match_date)

            now = datetime.now()
            days_diff = (now - date).days

            if days_diff > 30:
                return False, f"Match too old: {days_diff} days ago"

            if days_diff < -7:
                return False, f"Match too far future: {abs(days_diff)} days"

            return True, f"Match date acceptable: {match_date}"

        except Exception as e:
            logger.debug(f"Could not parse date: {match_date}")
            return True, "Date check skipped (parse error)"

    def _check_odds_count(self, odds_count: int) -> tuple[bool, str]:
        """Check if there are enough odds to make it worthwhile"""

        if odds_count == 0:
            return False, "No odds available"

        if odds_count < 3:
            return False, f"Too few odds: {odds_count}"

        return True, f"Good odds count: {odds_count}"

    def _check_url(self, match_url: str) -> tuple[bool, str]:
        """Check URL for unimportant patterns"""
        url_lower = match_url.lower()

        for keyword in self.UNIMPORTANT_KEYWORDS:
            if keyword in url_lower:
                return False, f"Unimportant keyword in URL: '{keyword}'"

        return True, "URL looks good"

    def _record_skip(self, reason: str):
        """Record why a match was skipped"""
        self.stats['unimportant'] += 1

        if 'league' in reason.lower():
            category = 'league'
        elif 'team' in reason.lower() or 'reserve' in reason.lower() or 'youth' in reason.lower():
            category = 'team'
        elif 'date' in reason.lower() or 'old' in reason.lower():
            category = 'date'
        elif 'odds' in reason.lower():
            category = 'odds'
        else:
            category = 'other'

        self.stats['reasons'][category] = self.stats['reasons'].get(category, 0) + 1

    def get_stats(self) -> Dict:
        """Get classification statistics"""
        return {
            'total_checked': self.stats['total_checked'],
            'important': self.stats['important'],
            'unimportant': self.stats['unimportant'],
            'skip_rate': f"{(self.stats['unimportant'] / max(self.stats['total_checked'], 1)) * 100:.1f}%",
            'skip_reasons': self.stats['reasons']
        }

    def reset_stats(self):
        """Reset statistics"""
        self.stats = {
            'total_checked': 0,
            'important': 0,
            'unimportant': 0,
            'reasons': {}
        }


def should_scrape_match(
    match_url: str = None,
    teams: Dict[str, str] = None,
    league: str = None,
    match_date: str = None,
    odds_count: int = None
) -> bool:
    """
    Quick check if match should be scraped.

    Returns True if important, False if should skip.
    """
    classifier = MatchClassifier()
    is_important, reason = classifier.is_important_match(
        match_url=match_url,
        teams=teams,
        league=league,
        match_date=match_date,
        odds_count=odds_count
    )

    if not is_important:
        logger.info(f"⏭ Skipping match: {reason}")

    return is_important
