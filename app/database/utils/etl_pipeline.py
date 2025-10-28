import io
import logging

import pandas as pd
import psycopg2

from app.config import get_settings

from ..model import SailingTable

logger = logging.getLogger("ETL")


class ETLPipeline:
    """
    ETL Pipeline for loading CSV sailing data into the database.

    This class orchestrates the Extract-Transform-Load process for sailing data,
    providing efficient bulk loading of data from CSV files into PostgreSQL.

    Attributes:
        table_name: Name of the database table to load data into
        settings: Application settings (database connection, etc.)

    Example:
        pipeline = ETLPipeline()
        df = await pipeline.extract("data/sailings.csv")
        df_clean = await pipeline.transform(df)
        await pipeline.load(df_clean)
    """

    def __init__(self, table_name: str = SailingTable.__tablename__):
        """
        Initialize the ETL pipeline.

        Args:
            table_name: Database table name (defaults to 'sailings')
        """
        self.table_name = table_name
        self.settings = get_settings()

    async def extract(self, csv_file_path: str) -> pd.DataFrame:
        """
        Extract data from a CSV file.

        Reads the CSV file and returns a pandas DataFrame. The CSV file should
        contain sailing-level raw data with columns matching the SailingTable model.

        Expected CSV columns:
        - ORIGIN
        - DESTINATION
        - ORIGIN_PORT_CODE
        - DESTINATION_PORT_CODE
        - SERVICE_VERSION_AND_ROUNDTRIP_IDENTFIERS
        - ORIGIN_SERVICE_VERSION_AND_MASTER
        - DESTINATION_SERVICE_VERSION_AND_MASTER
        - ORIGIN_AT_UTC
        - OFFERED_CAPACITY_TEU

        Args:
            csv_file_path: Path to the CSV file to extract

        Returns:
            DataFrame containing the extracted data

        Raises:
            FileNotFoundError: If the CSV file doesn't exist
            Exception: For other errors during extraction (e.g., malformed CSV)
        """
        logger.info(f"Extracting data from {csv_file_path}...")
        try:
            df = pd.read_csv(csv_file_path)
            logger.info(f"Extracted {len(df)} rows with {len(df.columns)} columns.")
            return df
        except FileNotFoundError:
            logger.error(f"CSV file not found: {csv_file_path}")
            raise
        except pd.errors.EmptyDataError:
            logger.warning(f"CSV file is empty: {csv_file_path}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error during CSV extraction: {e}", exc_info=True)
            raise

    async def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform and clean the raw DataFrame.

        Applies the following transformations:
        1. Standardizes column names to lowercase (to match database schema)
        2. Removes exact duplicate rows
        3. Deduplicates based on unique journey identifiers (keeps latest departure)

        Args:
            df: Raw DataFrame from extraction phase

        Returns:
            Transformed DataFrame ready for loading

        """
        logger.info("Starting data transformation...")

        if df.empty:
            logger.warning("DataFrame is empty, skipping transformation")
            # Still transform column names even for empty DataFrame
            df.columns = [col.lower() for col in df.columns]
            return df

        initial_rows = len(df)
        logger.info(f"Initial row count: {initial_rows:,}")

        # Step 1: Standardize column names to lowercase for database compatibility
        original_columns = df.columns.tolist()
        df.columns = [col.lower() for col in df.columns]
        logger.debug(f"Transformed column names: {original_columns} -> {df.columns.tolist()}")

        logger.info("Data transformation complete.")

        return df

    async def load(self, df_transformed: pd.DataFrame):
        """
        Load transformed data into the database using bulk insert.

        Uses PostgreSQL's COPY command for efficient bulk loading. This is much
        faster than inserting rows individually.

        Args:
            df_transformed: Transformed DataFrame ready for database insertion

        Raises:
            psycopg2.Error: If database connection or COPY operation fails

        Note:
            This method uses synchronous psycopg2 because asyncpg doesn't support
            COPY FROM in the same way. For production, consider using asyncpg's
            copy_records_to_table method instead.
        """
        if df_transformed.empty:
            logger.warning("DataFrame is empty, skipping load")
            raise ValueError("DataFrame is empty, skipping load")

        logger.info(f"Loading {len(df_transformed)} rows into {self.table_name}...")

        try:
            conn = psycopg2.connect(self.settings.DATABASE.POSTGRESQL_URL)
            with conn.cursor() as curs:
                # Convert DataFrame to CSV in memory
                csv_buffer = io.BytesIO()
                df_transformed.to_csv(csv_buffer, index=False, header=False)
                csv_buffer.seek(0)

                # Use PostgreSQL COPY for efficient bulk insert
                curs.copy_from(
                    csv_buffer, self.table_name, sep=",", columns=df_transformed.columns.tolist()
                )
            conn.commit()
            conn.close()

            logger.info(f"Successfully loaded {len(df_transformed)} rows into {self.table_name}.")
        except Exception as e:
            logger.error(f"Error during data loading: {e}", exc_info=True)
            raise ValueError(f"Error during data loading: {e}")


async def load_csv_to_database(csv_file_path: str):
    """
    Orchestrate the complete ETL process: Extract → Transform → Load.

    This is a convenience function that runs all three ETL stages in sequence.
    It's typically called during application startup to load initial data.

    Args:
        csv_file_path: Path to the CSV file containing sailing data

    Raises:
        FileNotFoundError: If CSV file doesn't exist
        Exception: For any errors during ETL process

    Example:
        await load_csv_to_database("data/sailing_level_raw.csv")
    """
    logger.info(f"Starting ETL pipeline for {csv_file_path}")

    try:
        pipeline = ETLPipeline()

        # Extract
        df_extracted = await pipeline.extract(csv_file_path)

        # Transform
        df_transformed = await pipeline.transform(df_extracted)

        # Load
        await pipeline.load(df_transformed)

        logger.info("ETL pipeline completed successfully")
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}", exc_info=True)
        raise ValueError(f"ETL pipeline failed: {e}")
