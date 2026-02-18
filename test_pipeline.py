"""
Unit tests for EPL Data Pipeline
Run with: pytest test_pipeline.py -v
"""

import pytest
import pandas as pd
from datetime import datetime
import sys
import os

# Add parent directory to path to import modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from exporting_data import (
    validate_standings,
    validate_top_scorers,
    validate_top_assists
)

# =========================
# TEST DATA FIXTURES
# =========================

@pytest.fixture
def valid_standings_df():
    """Create valid standings dataframe for testing."""
    return pd.DataFrame({
        'rank': [1, 2, 3],
        'team': ['Liverpool', 'Arsenal', 'Man City'],
        'points': [84, 74, 71],
        'played': [38, 38, 38],
        'wins': [25, 20, 21],
        'draws': [9, 14, 8],
        'losses': [4, 4, 9],
        'goals_for': [86, 91, 96],
        'goals_against': [41, 56, 68],
        'goal_difference': [45, 35, 28]
    })

@pytest.fixture
def invalid_standings_df():
    """Create invalid standings dataframe for testing."""
    return pd.DataFrame({
        'rank': [1, 2],
        'team': ['Liverpool', 'Liverpool'],  # Duplicate team
        'points': [84, -5],  # Negative points
        'played': [38, 38],
        'wins': [25, 20],
        'draws': [9, 14],
        'losses': [4, 4],
        'goals_for': [86, 91],
        'goals_against': [41, 56],
        'goal_difference': [45, 35]
    })

@pytest.fixture
def valid_scorers_df():
    """Create valid top scorers dataframe for testing."""
    return pd.DataFrame({
        'player': ['Mohamed Salah', 'Erling Haaland', 'Alexander Isak'],
        'team': ['Liverpool', 'Man City', 'Newcastle'],
        'goals': [29, 22, 23],
        'appearances': [38, 32, 34]
    })

@pytest.fixture
def invalid_scorers_df():
    """Create invalid top scorers dataframe for testing."""
    return pd.DataFrame({
        'player': ['Mohamed Salah'],
        'team': ['Liverpool'],
        'goals': [-5],  # Negative goals
        'appearances': [0]  # Zero appearances
    })

@pytest.fixture
def valid_assists_df():
    """Create valid top assists dataframe for testing."""
    return pd.DataFrame({
        'player': ['Mohamed Salah', 'Alexander Isak', 'Bruno Fernandes'],
        'team': ['Liverpool', 'Newcastle', 'Man United'],
        'assists': [18, 11, 10],
        'appearances': [38, 35, 37]
    })

# =========================
# STANDINGS VALIDATION TESTS
# =========================

class TestStandingsValidation:
    """Test suite for standings data validation."""
    
    def test_valid_standings(self, valid_standings_df):
        """Test that valid standings pass validation."""
        assert validate_standings(valid_standings_df) == True
    
    def test_missing_columns(self):
        """Test that missing required columns fail validation."""
        df = pd.DataFrame({'rank': [1, 2], 'team': ['Liverpool', 'Arsenal']})
        assert validate_standings(df) == False
    
    def test_duplicate_teams(self, invalid_standings_df):
        """Test that duplicate teams fail validation."""
        assert validate_standings(invalid_standings_df) == False
    
    def test_negative_points(self):
        """Test that negative points fail validation."""
        df = pd.DataFrame({
            'rank': [1],
            'team': ['Liverpool'],
            'points': [-10],
            'played': [38],
            'wins': [0],
            'draws': [0],
            'losses': [38],
            'goals_for': [20],
            'goals_against': [80],
            'goal_difference': [-60]
        })
        assert validate_standings(df) == False
    
    def test_points_calculation(self):
        """Test that points must equal (wins * 3) + draws."""
        df = pd.DataFrame({
            'rank': [1],
            'team': ['Liverpool'],
            'points': [100],  # Incorrect calculation
            'played': [38],
            'wins': [25],
            'draws': [9],
            'losses': [4],
            'goals_for': [86],
            'goals_against': [41],
            'goal_difference': [45]
        })
        assert validate_standings(df) == False
    
    def test_games_played_calculation(self):
        """Test that played games must equal wins + draws + losses."""
        df = pd.DataFrame({
            'rank': [1],
            'team': ['Liverpool'],
            'points': [84],
            'played': [40],  # Incorrect total
            'wins': [25],
            'draws': [9],
            'losses': [4],
            'goals_for': [86],
            'goals_against': [41],
            'goal_difference': [45]
        })
        assert validate_standings(df) == False
    
    def test_empty_dataframe(self):
        """Test that empty dataframe fails validation."""
        df = pd.DataFrame()
        assert validate_standings(df) == False

# =========================
# TOP SCORERS VALIDATION TESTS
# =========================

class TestTopScorersValidation:
    """Test suite for top scorers data validation."""
    
    def test_valid_scorers(self, valid_scorers_df):
        """Test that valid scorers pass validation."""
        assert validate_top_scorers(valid_scorers_df) == True
    
    def test_missing_columns(self):
        """Test that missing required columns fail validation."""
        df = pd.DataFrame({'player': ['Salah'], 'goals': [29]})
        assert validate_top_scorers(df) == False
    
    def test_negative_goals(self, invalid_scorers_df):
        """Test that negative goals fail validation."""
        assert validate_top_scorers(invalid_scorers_df) == False
    
    def test_zero_appearances(self, invalid_scorers_df):
        """Test that zero/negative appearances fail validation."""
        assert validate_top_scorers(invalid_scorers_df) == False
    
    def test_empty_dataframe(self):
        """Test that empty dataframe fails validation."""
        df = pd.DataFrame()
        assert validate_top_scorers(df) == False
    
    def test_valid_with_duplicates(self):
        """Test that duplicate players generate warning but pass."""
        df = pd.DataFrame({
            'player': ['Salah', 'Salah'],
            'team': ['Liverpool', 'Liverpool'],
            'goals': [29, 29],
            'appearances': [38, 38]
        })
        # Should still pass but log warning
        assert validate_top_scorers(df) == True

# =========================
# TOP ASSISTS VALIDATION TESTS
# =========================

class TestTopAssistsValidation:
    """Test suite for top assists data validation."""
    
    def test_valid_assists(self, valid_assists_df):
        """Test that valid assists pass validation."""
        assert validate_top_assists(valid_assists_df) == True
    
    def test_missing_columns(self):
        """Test that missing required columns fail validation."""
        df = pd.DataFrame({'player': ['Salah'], 'assists': [18]})
        assert validate_top_assists(df) == False
    
    def test_negative_assists(self):
        """Test that negative assists fail validation."""
        df = pd.DataFrame({
            'player': ['Salah'],
            'team': ['Liverpool'],
            'assists': [-5],
            'appearances': [38]
        })
        assert validate_top_assists(df) == False
    
    def test_invalid_appearances(self):
        """Test that zero/negative appearances fail validation."""
        df = pd.DataFrame({
            'player': ['Salah'],
            'team': ['Liverpool'],
            'assists': [18],
            'appearances': [0]
        })
        assert validate_top_assists(df) == False
    
    def test_empty_dataframe(self):
        """Test that empty dataframe fails validation."""
        df = pd.DataFrame()
        assert validate_top_assists(df) == False

# =========================
# INTEGRATION TESTS
# =========================

class TestDataFrameStructure:
    """Test suite for dataframe structure and types."""
    
    def test_standings_column_types(self, valid_standings_df):
        """Test that standings have correct column types."""
        assert valid_standings_df['rank'].dtype in ['int64', 'int32']
        assert valid_standings_df['team'].dtype == 'object'
        assert valid_standings_df['points'].dtype in ['int64', 'int32']
    
    def test_scorers_column_types(self, valid_scorers_df):
        """Test that scorers have correct column types."""
        assert valid_scorers_df['player'].dtype == 'object'
        assert valid_scorers_df['team'].dtype == 'object'
        assert valid_scorers_df['goals'].dtype in ['int64', 'int32']
    
    def test_assists_column_types(self, valid_assists_df):
        """Test that assists have correct column types."""
        assert valid_assists_df['player'].dtype == 'object'
        assert valid_assists_df['assists'].dtype in ['int64', 'int32']

# =========================
# RUN TESTS
# =========================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])