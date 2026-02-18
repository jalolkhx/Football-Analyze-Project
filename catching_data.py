import requests
import pandas as pd
import os
import logging
from datetime import datetime
from time import sleep

# Setup logging
logger = logging.getLogger(__name__)

# =========================
# CONFIG
# =========================
API_KEY = os.getenv("API_FOOTBALL_KEY")
BASE_URL = "https://v3.football.api-sports.io"
LEAGUE_ID = 39        # Premier League
SEASON = datetime.now().year - 1 if datetime.now().month < 8 else datetime.now().year

HEADERS = {
    "x-apisports-key": API_KEY
}

# =========================
# VALIDATION
# =========================
def validate_api_key():
    """Validate API key exists."""
    if not API_KEY:
        raise ValueError("API_FOOTBALL_KEY environment variable not set")
    logger.info(f"API Key configured: {API_KEY[:10]}...")

# =========================
# API HELPER WITH RETRY LOGIC
# =========================
def api_get(endpoint, params=None, max_retries=3):
    """
    Make API request with retry logic and error handling.
    
    Args:
        endpoint: API endpoint
        params: Query parameters
        max_retries: Maximum number of retry attempts
        
    Returns:
        Response data or None if failed
    """
    url = f"{BASE_URL}{endpoint}"
    
    for attempt in range(max_retries):
        try:
            logger.info(f"API Request: {endpoint} (Attempt {attempt + 1}/{max_retries})")
            response = requests.get(url, headers=HEADERS, params=params, timeout=10)
            
            # Check for rate limiting
            if response.status_code == 429:
                logger.warning(f"Rate limit exceeded. Waiting 60 seconds...")
                sleep(60)
                continue
            
            # Check for other errors
            response.raise_for_status()
            
            data = response.json()
            
            # Validate response structure
            if "response" not in data:
                logger.error(f"Invalid API response structure: {data}")
                return None
            
            # Check for API errors
            if "errors" in data and data["errors"]:
                logger.error(f"API returned errors: {data['errors']}")
                return None
            
            logger.info(f"✓ API request successful: {len(data['response'])} results")
            return data["response"]
            
        except requests.exceptions.Timeout:
            logger.warning(f"Request timeout (attempt {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                sleep(2 ** attempt)  # Exponential backoff
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {str(e)}")
            if attempt < max_retries - 1:
                sleep(2 ** attempt)
            
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            return None
    
    logger.error(f"All retry attempts failed for {endpoint}")
    return None

# =========================
# 1. EPL STANDINGS
# =========================
def get_standings():
    """Fetch and parse EPL standings data."""
    try:
        logger.info(f"Fetching standings for League {LEAGUE_ID}, Season {SEASON}")
        
        data = api_get("/standings", {
            "league": LEAGUE_ID,
            "season": SEASON
        })
        
        if not data:
            logger.error("No standings data received from API")
            raise ValueError("Failed to fetch standings data")
        
        # Validate response structure
        if not data[0].get("league", {}).get("standings"):
            logger.error("Invalid standings data structure")
            raise ValueError("Invalid standings response")
        
        table = data[0]["league"]["standings"][0]
        
        if not table:
            logger.error("Empty standings table")
            raise ValueError("No teams in standings")
        
        rows = []
        for t in table:
            try:
                rows.append({
                    "rank": t["rank"],
                    "team": t["team"]["name"],
                    "points": t["points"],
                    "played": t["all"]["played"],
                    "wins": t["all"]["win"],
                    "draws": t["all"]["draw"],
                    "losses": t["all"]["lose"],
                    "goals_for": t["all"]["goals"]["for"],
                    "goals_against": t["all"]["goals"]["against"],
                    "goal_difference": t["goalsDiff"]
                })
            except KeyError as e:
                logger.error(f"Missing key in team data: {e}")
                continue
        
        if not rows:
            raise ValueError("No valid team data parsed")
        
        df = pd.DataFrame(rows)
        logger.info(f"✓ Parsed {len(df)} teams from standings")
        return df
        
    except Exception as e:
        logger.error(f"Error fetching standings: {str(e)}")
        raise

# =========================
# 2. TOP SCORERS
# =========================
def get_top_scorers():
    """Fetch and parse top scorers data."""
    try:
        logger.info(f"Fetching top scorers for League {LEAGUE_ID}, Season {SEASON}")
        
        data = api_get("/players/topscorers", {
            "league": LEAGUE_ID,
            "season": SEASON
        })
        
        if not data:
            logger.error("No top scorers data received from API")
            raise ValueError("Failed to fetch top scorers data")
        
        rows = []
        for p in data:
            try:
                stats = p["statistics"][0]
                
                # Skip players with null goals
                if stats["goals"]["total"] is None:
                    continue
                
                rows.append({
                    "player": p["player"]["name"],
                    "team": stats["team"]["name"],
                    "goals": stats["goals"]["total"],
                    "appearances": stats["games"]["appearences"] or 0
                })
            except (KeyError, IndexError) as e:
                logger.warning(f"Skipping player due to missing data: {e}")
                continue
        
        if not rows:
            raise ValueError("No valid top scorers data parsed")
        
        df = pd.DataFrame(rows)
        logger.info(f"✓ Parsed {len(df)} players from top scorers")
        return df
        
    except Exception as e:
        logger.error(f"Error fetching top scorers: {str(e)}")
        raise

# =========================
# 3. TOP ASSISTS
# =========================
def get_top_assists():
    """Fetch and parse top assists data."""
    try:
        logger.info(f"Fetching top assists for League {LEAGUE_ID}, Season {SEASON}")
        
        data = api_get("/players/topassists", {
            "league": LEAGUE_ID,
            "season": SEASON
        })
        
        if not data:
            logger.error("No top assists data received from API")
            raise ValueError("Failed to fetch top assists data")
        
        rows = []
        for p in data:
            try:
                stats = p["statistics"][0]
                
                # Skip players with null assists
                if stats["goals"]["assists"] is None:
                    continue
                
                rows.append({
                    "player": p["player"]["name"],
                    "team": stats["team"]["name"],
                    "assists": stats["goals"]["assists"],
                    "appearances": stats["games"]["appearences"] or 0
                })
            except (KeyError, IndexError) as e:
                logger.warning(f"Skipping player due to missing data: {e}")
                continue
        
        if not rows:
            raise ValueError("No valid top assists data parsed")
        
        df = pd.DataFrame(rows)
        logger.info(f"✓ Parsed {len(df)} players from top assists")
        return df
        
    except Exception as e:
        logger.error(f"Error fetching top assists: {str(e)}")
        raise

# Validate API key on module import
try:
    validate_api_key()
except Exception as e:
    logger.error(f"API configuration error: {str(e)}")