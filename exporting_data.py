import os
import pandas as pd
import urllib.parse
from datetime import datetime, timezone
import logging
from sqlalchemy import create_engine, event, text, types
from catching_data import (
    get_standings,
    get_top_scorers,
    get_top_assists
)

# =========================
# LOGGING SETUP
# =========================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# =========================
# SQL SERVER CONFIG (ENV)
# =========================
SQL_SERVER = os.getenv(r"SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE")
SQL_USERNAME = os.getenv("SQL_USERNAME")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_DRIVER = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")
SQL_SCHEMA = os.getenv("SQL_SCHEMA", "dbo")

# =========================
# VALIDATION FUNCTIONS
# =========================
def validate_standings(df: pd.DataFrame) -> bool:
    """Validate EPL standings data quality."""
    try:
        # Check required columns exist
        required_cols = ['rank', 'team', 'points', 'played', 'wins', 'draws', 'losses']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            return False
        
        # Check we have 20 teams (full EPL)
        if len(df) != 20:
            logger.warning(f"Expected 20 teams, got {len(df)}")
        
        # Check for duplicate teams
        if df['team'].duplicated().any():
            logger.error("Duplicate teams found in standings")
            return False
        
        # Validate points are non-negative
        if (df['points'] < 0).any():
            logger.error("Negative points found")
            return False
        
        # Validate math: points should equal (wins * 3) + draws
        calculated_points = (df['wins'] * 3) + df['draws']
        if not (df['points'] == calculated_points).all():
            logger.error("Points calculation mismatch")
            return False
        
        # Validate games played = wins + draws + losses
        total_games = df['wins'] + df['draws'] + df['losses']
        if not (df['played'] == total_games).all():
            logger.error("Games played calculation mismatch")
            return False
        
        logger.info(f"✓ Standings validation passed: {len(df)} teams")
        return True
        
    except Exception as e:
        logger.error(f"Validation error: {str(e)}")
        return False

def validate_top_scorers(df: pd.DataFrame) -> bool:
    """Validate top scorers data quality."""
    try:
        required_cols = ['player', 'team', 'goals', 'appearances']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            return False
        
        # Check for empty dataframe
        if len(df) == 0:
            logger.error("Top scorers data is empty")
            return False
        
        # Validate goals are non-negative
        if (df['goals'] < 0).any():
            logger.error("Negative goals found")
            return False
        
        # Validate appearances are positive
        if (df['appearances'] <= 0).any():
            logger.error("Invalid appearances found")
            return False
        
        # Check for duplicate players
        if df['player'].duplicated().any():
            logger.warning("Duplicate players found in top scorers")
        
        logger.info(f"✓ Top scorers validation passed: {len(df)} players")
        return True
        
    except Exception as e:
        logger.error(f"Validation error: {str(e)}")
        return False

def validate_top_assists(df: pd.DataFrame) -> bool:
    """Validate top assists data quality."""
    try:
        required_cols = ['player', 'team', 'assists', 'appearances']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            return False
        
        if len(df) == 0:
            logger.error("Top assists data is empty")
            return False
        
        if (df['assists'] < 0).any():
            logger.error("Negative assists found")
            return False
        
        if (df['appearances'] <= 0).any():
            logger.error("Invalid appearances found")
            return False
        
        if df['player'].duplicated().any():
            logger.warning("Duplicate players found in top assists")
        
        logger.info(f"✓ Top assists validation passed: {len(df)} players")
        return True
        
    except Exception as e:
        logger.error(f"Validation error: {str(e)}")
        return False

# =========================
# ENGINE SETUP
# =========================
def create_db_engine():
    """Create database engine with error handling."""
    try:
        conn_str = (
            f"DRIVER={{{SQL_DRIVER}}};"
            f"SERVER={SQL_SERVER};"
            f"DATABASE={SQL_DATABASE};"
            f"UID={SQL_USERNAME};"
            f"PWD={SQL_PASSWORD};"
            "TrustServerCertificate=yes;"
        )
        
        engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(conn_str)}"
        )
        
        @event.listens_for(engine, "before_cursor_execute")
        def enable_fast_executemany(conn, cursor, statement, parameters, context, executemany):
            if executemany:
                cursor.fast_executemany = True
        
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        logger.info("✓ Database connection established")
        return engine
        
    except Exception as e:
        logger.error(f"Failed to create database engine: {str(e)}")
        raise

# =========================
# EXPORT FUNCTION
# =========================
def overwrite(df: pd.DataFrame, table: str, engine):
    """Export dataframe to SQL Server with validation and error handling."""
    try:
        # Add timestamp
        if "exported_at" not in df.columns:
            df = df.copy()
            df["exported_at"] = datetime.now(timezone.utc)
        else:
            df = df.copy()
            try:
                if pd.api.types.is_datetime64tz_dtype(df["exported_at"]):
                    df["exported_at"] = df["exported_at"].dt.tz_convert("UTC").dt.tz_localize(None)
                else:
                    df["exported_at"] = pd.to_datetime(df["exported_at"])
            except Exception:
                df["exported_at"] = pd.to_datetime(df["exported_at"])
        
        # Drop existing table
        drop_sql = f"IF OBJECT_ID('{SQL_SCHEMA}.{table}', 'U') IS NOT NULL DROP TABLE {SQL_SCHEMA}.{table};"
        
        with engine.begin() as conn:
            conn.execute(text(drop_sql))
            
            # Write DataFrame to SQL
            df.to_sql(
                name=table,
                con=conn,
                schema=SQL_SCHEMA,
                if_exists="replace",
                index=False,
                method="multi",
                dtype={"exported_at": types.DateTime()}
            )
        
        logger.info(f"✓ Successfully wrote {len(df)} rows to {SQL_SCHEMA}.{table}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to write table {table}: {str(e)}")
        return False

# =========================
# MAIN PIPELINE
# =========================
def run_pipeline():
    """Main pipeline execution with comprehensive error handling."""
    logger.info("="*50)
    logger.info("Starting EPL Data Pipeline")
    logger.info("="*50)
    
    pipeline_success = True
    
    try:
        # Create database engine
        engine = create_db_engine()
        
        # 1. Process Standings
        logger.info("Fetching EPL standings...")
        try:
            standings_df = get_standings()
            if validate_standings(standings_df):
                if overwrite(standings_df, "epl_standings", engine):
                    logger.info("✓ Standings processed successfully")
                else:
                    logger.error("✗ Failed to write standings")
                    pipeline_success = False
            else:
                logger.error("✗ Standings validation failed")
                pipeline_success = False
        except Exception as e:
            logger.error(f"✗ Error processing standings: {str(e)}")
            pipeline_success = False
        
        # 2. Process Top Scorers
        logger.info("Fetching top scorers...")
        try:
            scorers_df = get_top_scorers()
            if validate_top_scorers(scorers_df):
                if overwrite(scorers_df, "epl_top_scorers", engine):
                    logger.info("✓ Top scorers processed successfully")
                else:
                    logger.error("✗ Failed to write top scorers")
                    pipeline_success = False
            else:
                logger.error("✗ Top scorers validation failed")
                pipeline_success = False
        except Exception as e:
            logger.error(f"✗ Error processing top scorers: {str(e)}")
            pipeline_success = False
        
        # 3. Process Top Assists
        logger.info("Fetching top assists...")
        try:
            assists_df = get_top_assists()
            if validate_top_assists(assists_df):
                if overwrite(assists_df, "epl_top_assists", engine):
                    logger.info("✓ Top assists processed successfully")
                else:
                    logger.error("✗ Failed to write top assists")
                    pipeline_success = False
            else:
                logger.error("✗ Top assists validation failed")
                pipeline_success = False
        except Exception as e:
            logger.error(f"✗ Error processing top assists: {str(e)}")
            pipeline_success = False
        
        # Summary
        logger.info("="*50)
        if pipeline_success:
            logger.info("✓ PIPELINE COMPLETED SUCCESSFULLY")
        else:
            logger.warning("⚠ PIPELINE COMPLETED WITH ERRORS")
        logger.info("="*50)
        
        return pipeline_success
        
    except Exception as e:
        logger.error(f"✗ PIPELINE FAILED: {str(e)}")
        logger.info("="*50)
        return False

# =========================
# ENTRY POINT
# =========================
if __name__ == "__main__":
    success = run_pipeline()
    exit(0 if success else 1)