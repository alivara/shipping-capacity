#!/usr/bin/env python3
"""
ETL Manager Script for Shipping Capacity Data

This script provides a CLI interface to manage data loading and clearing operations
independently from the application. It should be run as needed to refresh data.

Usage:
    # Load data from CSV
    python scripts/etl_manager.py load --csv-path data/sailing_level_raw.csv

    # Load with automatic table clearing first
    python scripts/etl_manager.py load --csv-path data/sailing_level_raw.csv --clear-first

    # Clear table data
    python scripts/etl_manager.py clear

    # Check table status
    python scripts/etl_manager.py status

Requirements:
    - Run from project root directory
    - Ensure database is accessible
    - Set environment variables (or use .env file)
"""


# Add project root to path
import asyncio
import logging
import logging.config
import sys
from pathlib import Path
from typing import Optional

import click
import psycopg2

sys.path.insert(0, str(Path(__file__).parent.parent))  # noqa: E402

from app.config import get_settings  # noqa: E402
from app.database.model import SailingTable  # noqa: E402
from app.database.utils import clear_table, load_csv_to_database  # noqa: E402
from app.logging_config import LOGGING_CONFIG  # noqa: E402

# Configure logging
logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger("ETL_MANAGER")


async def get_table_stats() -> dict:
    """Get statistics about the sailing table."""
    settings = get_settings()

    try:
        conn = psycopg2.connect(settings.DATABASE.POSTGRESQL_URL)
        with conn.cursor() as curs:
            # Get row count
            curs.execute(f"SELECT COUNT(*) FROM {SailingTable.__tablename__};")
            row_count = curs.fetchone()[0]

            # Get table size
            curs.execute(
                f"""
                SELECT pg_size_pretty(pg_total_relation_size('{SailingTable.__tablename__}'));
            """
            )
            table_size = curs.fetchone()[0]

            # Get date range if data exists
            if row_count > 0:
                curs.execute(
                    f"""
                    SELECT
                        MIN(origin_at_utc) as earliest_date,
                        MAX(origin_at_utc) as latest_date
                    FROM {SailingTable.__tablename__};
                """
                )
                earliest, latest = curs.fetchone()
            else:
                earliest, latest = None, None

        conn.close()

        return {
            "row_count": row_count,
            "table_size": table_size,
            "earliest_date": earliest,
            "latest_date": latest,
            "is_empty": row_count == 0,
        }
    except Exception as e:
        logger.error(f"Error getting table stats: {e}")
        raise


@click.group()
def cli():
    """ETL Manager for Shipping Capacity Data"""
    pass


@cli.command()
@click.option(
    "--csv-path",
    type=click.Path(exists=True),
    help="Path to CSV file to load",
)
@click.option(
    "--clear-first",
    is_flag=True,
    help="Clear table before loading new data",
)
@click.option(
    "--force",
    is_flag=True,
    help="Force load even if table already has data",
)
def load(csv_path: Optional[str], clear_first: bool, force: bool):
    """
    Load CSV data into the database.

    Examples:
        python scripts/etl_manager.py load --csv-path data/sailing_level_raw.csv
        python scripts/etl_manager.py load --clear-first
    """
    settings = get_settings()

    # Use default CSV path if not provided
    if not csv_path:
        csv_path = settings.DATABASE.CSV_FILE_PATH
        logger.info(f"Using default CSV path: {csv_path}")

    # Check if file exists
    if not Path(csv_path).exists():
        logger.error(f"CSV file not found: {csv_path}")
        sys.exit(1)

    async def _load():
        try:
            # Check if table already has data
            stats = await get_table_stats()

            if not stats["is_empty"] and not force and not clear_first:
                logger.warning(f"Table already contains {stats['row_count']:,} rows")
                logger.warning(
                    "Use --clear-first to clear before loading or --force to load anyway"
                )
                if not click.confirm("Continue with loading (may cause duplicates)?"):
                    logger.info("Load cancelled")
                    return

            # Clear table if requested
            if clear_first:
                logger.info("Clearing table before loading...")
                await clear_table()
                logger.info("Table cleared")

            # Load data
            logger.info(f"Loading data from {csv_path}...")
            await load_csv_to_database(csv_file_path=csv_path)

            # Show final stats
            final_stats = await get_table_stats()
            logger.info("=" * 60)
            logger.info("✓ ETL LOAD COMPLETED SUCCESSFULLY")
            logger.info(f"  Rows loaded: {final_stats['row_count']:,}")
            logger.info(f"  Table size: {final_stats['table_size']}")
            if final_stats["earliest_date"] and final_stats["latest_date"]:
                logger.info(
                    f"  Date range: {final_stats['earliest_date']} to {final_stats['latest_date']}"
                )
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"ETL load failed: {e}", exc_info=True)
            sys.exit(1)

    asyncio.run(_load())


@cli.command()
@click.option(
    "--force",
    is_flag=True,
    help="Skip confirmation prompt",
)
def clear(force: bool):
    """
    Clear all data from the sailing table.

    Examples:
        python scripts/etl_manager.py clear
        python scripts/etl_manager.py clear --force
    """

    async def _clear():
        try:
            # Get stats before clearing
            stats = await get_table_stats()

            if stats["is_empty"]:
                logger.info("Table is already empty")
                return

            # Confirm deletion
            if not force:
                logger.warning(
                    f"About to delete {stats['row_count']:,} rows from {SailingTable.__tablename__}"
                )
                if not click.confirm("Are you sure you want to continue?"):
                    logger.info("Clear cancelled")
                    return

            # Clear table
            logger.info("Clearing table...")
            await clear_table()
            logger.info("✓ Table cleared successfully")

        except Exception as e:
            logger.error(f"Clear operation failed: {e}", exc_info=True)
            sys.exit(1)

    asyncio.run(_clear())


@cli.command()
def status():
    """
    Show current status of the sailing table.

    Examples:
        python scripts/etl_manager.py status
    """

    async def _status():
        try:
            settings = get_settings()
            stats = await get_table_stats()

            logger.info("=" * 60)
            logger.info("DATABASE STATUS")
            logger.info("=" * 60)
            logger.info(f"Environment: {settings.BASE.ENV}")
            logger.info(f"Database: {settings.DATABASE.POSTGRESQL_DB}")
            logger.info(f"Table: {SailingTable.__tablename__}")
            logger.info(f"Status: {'EMPTY' if stats['is_empty'] else 'POPULATED'}")
            logger.info(f"Row count: {stats['row_count']:,}")
            logger.info(f"Table size: {stats['table_size']}")

            if not stats["is_empty"]:
                logger.info(f"Earliest date: {stats['earliest_date']}")
                logger.info(f"Latest date: {stats['latest_date']}")

            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"Status check failed: {e}", exc_info=True)
            sys.exit(1)

    asyncio.run(_status())


@cli.command()
@click.option(
    "--csv-path",
    type=click.Path(exists=True),
    help="Path to CSV file to load",
)
def refresh(csv_path: Optional[str]):
    """
    Refresh data: clear table and reload from CSV (convenience command).

    This is equivalent to: clear --force && load --csv-path <path>

    Examples:
        python scripts/etl_manager.py refresh
        python scripts/etl_manager.py refresh --csv-path data/new_data.csv
    """
    settings = get_settings()

    # Use default CSV path if not provided
    if not csv_path:
        csv_path = settings.DATABASE.CSV_FILE_PATH
        logger.info(f"Using default CSV path: {csv_path}")

    # Check if file exists
    if not Path(csv_path).exists():
        logger.error(f"CSV file not found: {csv_path}")
        sys.exit(1)

    async def _refresh():
        try:
            # Get current stats
            stats = await get_table_stats()

            if not stats["is_empty"]:
                logger.info(f"Current table has {stats['row_count']:,} rows")
                if not click.confirm("This will delete all existing data. Continue?"):
                    logger.info("Refresh cancelled")
                    return

            # Clear
            logger.info("Step 1/2: Clearing table...")
            await clear_table()
            logger.info("✓ Table cleared")

            # Load
            logger.info("Step 2/2: Loading new data...")
            await load_csv_to_database(csv_file_path=csv_path)

            # Show final stats
            final_stats = await get_table_stats()
            logger.info("=" * 60)
            logger.info("✓ REFRESH COMPLETED SUCCESSFULLY")
            logger.info(f"  Rows loaded: {final_stats['row_count']:,}")
            logger.info(f"  Table size: {final_stats['table_size']}")
            if final_stats["earliest_date"] and final_stats["latest_date"]:
                logger.info(
                    f"  Date range: {final_stats['earliest_date']} to {final_stats['latest_date']}"
                )
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"Refresh failed: {e}", exc_info=True)
            logger.error(
                "Table may be in an inconsistent state - recommend running 'clear' and 'load' manually"
            )
            sys.exit(1)

    asyncio.run(_refresh())


if __name__ == "__main__":
    cli()
