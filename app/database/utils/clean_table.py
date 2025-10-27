import logging

import psycopg2

from app.config import get_settings

from ..model import SailingTable

logger = logging.getLogger("DATABASE_UTILS")


async def clear_table():
    """
    Clears all data from the sailing table.
    """
    settings = get_settings()
    logger.info(f"Clearing all data from table {SailingTable.__tablename__}...")
    conn = psycopg2.connect(settings.DATABASE.POSTGRESQL_URL)
    with conn.cursor() as curs:
        curs.execute(f"DELETE FROM {SailingTable.__tablename__};")
    conn.commit()
    conn.close()
    logger.info("Table cleared.")
