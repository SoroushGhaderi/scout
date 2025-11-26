"""Tests for data validation."""

import pytest
import pandas as pd
import numpy as np

from src.utils.validation import DataQualityChecker


class TestDataQualityChecker:
    """Tests for DataQualityChecker."""
    
    def test_check_general_stats_valid(self):
        """Test validation of valid general stats."""
        df = pd.DataFrame({
            'match_id': [123, 456],
            'home_team_id': [1, 2],
            'away_team_id': [3, 4],
            'league_name': ['Premier League', 'La Liga']
        })
        
        result = DataQualityChecker.check_general_stats(df)
        
        assert result['passed']
        assert len(result['issues']) == 0
        assert result['row_count'] == 2
    
    def test_check_general_stats_missing_fields(self):
        """Test validation with missing required fields."""
        df = pd.DataFrame({
            'match_id': [123],
            'home_team_id': [1]
            # missing away_team_id
        })
        
        result = DataQualityChecker.check_general_stats(df)
        
        assert not result['passed']
        assert any('Missing required fields' in issue for issue in result['issues'])
    
    def test_check_general_stats_null_values(self):
        """Test validation with null values in critical fields."""
        df = pd.DataFrame({
            'match_id': [123, None],
            'home_team_id': [1, 2],
            'away_team_id': [3, 4]
        })
        
        result = DataQualityChecker.check_general_stats(df)
        
        assert not result['passed']
        assert any('Null values' in issue for issue in result['issues'])
    
    def test_check_general_stats_empty(self):
        """Test validation with empty DataFrame."""
        df = pd.DataFrame()
        
        result = DataQualityChecker.check_general_stats(df)
        
        assert not result['passed']
        assert 'DataFrame is empty' in result['issues']
    
    def test_check_player_stats_valid(self):
        """Test validation of valid player stats."""
        df = pd.DataFrame({
            'player_id': [1, 2],
            'fotmob_rating': [7.5, 8.2],
            'expected_goals': [0.3, 0.5],
            'expected_assists': [0.1, 0.2],
            'minutes_played': [90, 75],
            'pass_accuracy': [85.5, 92.3]
        })
        
        result = DataQualityChecker.check_player_stats(df)
        
        assert result['passed']
        assert len(result['issues']) == 0
    
    def test_check_player_stats_invalid_rating(self):
        """Test validation with invalid rating values."""
        df = pd.DataFrame({
            'player_id': [1, 2],
            'fotmob_rating': [7.5, 11.0],  # 11.0 is invalid (max is 10)
        })
        
        result = DataQualityChecker.check_player_stats(df)
        
        assert not result['passed']
        assert any('invalid ratings' in issue.lower() for issue in result['issues'])
    
    def test_check_player_stats_negative_xg(self):
        """Test validation with negative xG values."""
        df = pd.DataFrame({
            'player_id': [1, 2],
            'expected_goals': [0.3, -0.1],  # Negative xG is invalid
        })
        
        result = DataQualityChecker.check_player_stats(df)
        
        assert not result['passed']
        assert any('Negative values' in issue for issue in result['issues'])
    
    def test_check_goal_events_valid(self):
        """Test validation of valid goal events."""
        df = pd.DataFrame({
            'event_id': [1, 2],
            'goal_time': [23, 67],
            'home_score': [1, 1],
            'away_score': [0, 1]
        })
        
        result = DataQualityChecker.check_goal_events(df)
        
        assert result['passed']
    
    def test_check_goal_events_invalid_time(self):
        """Test validation with invalid goal time."""
        df = pd.DataFrame({
            'event_id': [1],
            'goal_time': [200],  # Invalid (>150)
            'home_score': [1],
            'away_score': [0]
        })
        
        result = DataQualityChecker.check_goal_events(df)
        
        assert not result['passed']
        assert any('Invalid goal_time' in issue for issue in result['issues'])
    
    def test_check_shot_events_valid(self):
        """Test validation of valid shot events."""
        df = pd.DataFrame({
            'id': [1, 2],
            'expected_goals': [0.25, 0.75],
            'x': [50, 80],
            'y': [60, 70]
        })
        
        result = DataQualityChecker.check_shot_events(df)
        
        assert result['passed']
    
    def test_check_shot_events_invalid_xg(self):
        """Test validation with invalid xG values."""
        df = pd.DataFrame({
            'id': [1, 2],
            'expected_goals': [0.25, 1.5],  # 1.5 is invalid (max is 1.0)
        })
        
        result = DataQualityChecker.check_shot_events(df)
        
        assert not result['passed']
        assert any('Invalid xG' in issue for issue in result['issues'])
    
    def test_validate_all_dataframes(self):
        """Test validation of multiple dataframes."""
        dataframes = {
            'general_stats': pd.DataFrame({
                'match_id': [123],
                'home_team_id': [1],
                'away_team_id': [2]
            }),
            'player_stats': pd.DataFrame({
                'player_id': [1],
                'fotmob_rating': [7.5]
            })
        }
        
        results = DataQualityChecker.validate_all_dataframes(dataframes)
        
        assert 'general_stats' in results
        assert 'player_stats' in results
        assert results['general_stats']['passed']
        assert results['player_stats']['passed']

