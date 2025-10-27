import io
import logging

import pandas as pd
import psycopg2

from app.config import get_settings

from ..model import SailingTable

logger = logging.getLogger("ETL")


class ETLPipeline:
    """
    ETL Pipeline for loading CSV data into the database.
    """

    def __init__(self, table_name: str = SailingTable.__tablename__):
        self.table_name = table_name
        self.settings = get_settings()

    async def extract(self, csv_file_path: str) -> pd.DataFrame:
        """
        Extracts data from the CSV file.
        """
        logger.info(f"Extracting data from {csv_file_path}...")
        try:
            df = pd.read_csv(csv_file_path)
            logger.info(f"Extracted {len(df)} rows.")
            return df
        except FileNotFoundError:
            logger.error(f"CSV file not found: {csv_file_path}")
            raise
        except Exception as e:
            logger.error(f"Error during CSV extraction: {e}", exc_info=True)
            raise

    async def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the raw DataFrame
        """
        logger.info("Starting data transformation...")

        # Standardize column names
        df.columns = [col.lower() for col in df.columns]

        # # Transformations
        # df["offered_capacity_teu"] = (
        #     pd.to_numeric(df["offered_capacity_teu"], errors="coerce").fillna(0).astype(int)
        # )
        # df["origin_at_utc"] = pd.to_datetime(df["origin_at_utc"], utc=True, errors="coerce")

        logger.info("Data transformation complete.")
        return df

    async def load(self, df_transformed: pd.DataFrame):
        logger.info(f"Loading {len(df_transformed)} rows into {self.table_name}...")

        conn = psycopg2.connect(self.settings.DATABASE.POSTGRESQL_URL)
        with conn.cursor() as curs:
            # Convert DataFrame to CSV in memory
            csv_buffer = io.BytesIO()
            df_transformed.to_csv(csv_buffer, index=False, header=False)
            csv_buffer.seek(0)
            # Use asyncpg copy
            curs.copy_from(
                csv_buffer, self.table_name, sep=",", columns=df_transformed.columns.tolist()
            )
        conn.commit()
        conn.close()

        logger.info("Data loading complete.")


async def load_csv_to_database(csv_file_path: str):
    """
    Orchestrates CSV → Transform → Database load.
    """
    pipeline = ETLPipeline()
    df_extracted = await pipeline.extract(csv_file_path)
    df_transformed = await pipeline.transform(df_extracted)
    await pipeline.load(df_transformed)
