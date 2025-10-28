import os
from datetime import datetime, timedelta, timezone
from typing import AsyncGenerator, Generator

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import Settings, get_settings
from app.database.base_class import BaseTable
from app.database.model import SailingTable
from app.main import app


def utc_datetime(*args) -> datetime:
    """Helper to create UTC-aware datetime for tests."""
    return datetime(*args, tzinfo=timezone.utc)


# Test database configuration
class TestSettings(Settings):
    """Override settings for testing"""

    def __init__(self):
        super().__init__()
        # Use environment variables for Docker compatibility
        # Defaults to localhost for local dev, but can use 'postgres' in Docker
        self.DATABASE.POSTGRESQL_USER = os.getenv("DATABASE_POSTGRESQL_USER", "admin")
        self.DATABASE.POSTGRESQL_PASSWORD = os.getenv("DATABASE_POSTGRESQL_PASSWORD", "admin")
        self.DATABASE.POSTGRESQL_HOST = os.getenv("DATABASE_POSTGRESQL_HOST", "localhost")
        self.DATABASE.POSTGRESQL_PORT = os.getenv("DATABASE_POSTGRESQL_PORT", "5432")
        self.DATABASE.POSTGRESQL_DB = os.getenv("DATABASE_POSTGRESQL_DB", "test_db")


@pytest.fixture(scope="session")
def test_settings() -> Settings:
    """Provide test settings"""
    return TestSettings()


@pytest.fixture(scope="session")
def test_database(test_settings: Settings):
    """
    Create test database at the start of test session and drop it at the end.
    This ensures a clean test database is created for each test run.

    Note: This is a synchronous fixture (not async) because database creation
    is an administrative task that doesn't need async operations.
    """
    from sqlalchemy import create_engine, text

    # Connect to postgres database to create/drop test database
    admin_url = (
        f"postgresql://{test_settings.DATABASE.POSTGRESQL_USER}:"
        f"{test_settings.DATABASE.POSTGRESQL_PASSWORD}@"
        f"{test_settings.DATABASE.POSTGRESQL_HOST}:"
        f"{test_settings.DATABASE.POSTGRESQL_PORT}/postgres"
    )
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")

    # Drop test database if it exists (cleanup from previous failed run)
    with admin_engine.connect() as conn:
        conn.execute(text(f"DROP DATABASE IF EXISTS {test_settings.DATABASE.POSTGRESQL_DB}"))
        conn.execute(text(f"CREATE DATABASE {test_settings.DATABASE.POSTGRESQL_DB}"))

    admin_engine.dispose()

    yield

    # Drop test database after all tests complete
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")
    with admin_engine.connect() as conn:
        # Terminate all connections to the test database
        conn.execute(
            text(
                f"""
            SELECT pg_terminate_backend(pg_stat_activity.pid)
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = '{test_settings.DATABASE.POSTGRESQL_DB}'
            AND pid <> pg_backend_pid()
        """
            )
        )
        conn.execute(text(f"DROP DATABASE IF EXISTS {test_settings.DATABASE.POSTGRESQL_DB}"))

    admin_engine.dispose()


@pytest_asyncio.fixture(scope="function")
async def test_engine(test_settings: Settings, test_database):
    """
    Create async engine for testing and set up database schema.

    Note: We install uuid-ossp extension and create tables directly
    instead of running Alembic migrations to avoid configuration conflicts.
    """
    engine = create_async_engine(
        test_settings.DATABASE.POSTGRESQL_URL_ASYNC,
        echo=False,
        pool_pre_ping=True,  # Verify connections before use
    )

    # Set up database: install extensions and create tables
    async with engine.begin() as conn:
        # Install uuid-ossp extension (required for uuid_generate_v4())
        await conn.execute(text('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";'))

        # Create alembic_version table manually (for migration tests)
        await conn.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS alembic_version (
                version_num VARCHAR(32) NOT NULL,
                CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num)
            );
        """
            )
        )

        # Insert current version (get latest migration version)
        await conn.execute(
            text(
                """
            INSERT INTO alembic_version (version_num)
            VALUES ('b6d2f47fc83c')
            ON CONFLICT (version_num) DO NOTHING;
        """
            )
        )

        # Create all tables from SQLAlchemy metadata
        await conn.run_sync(BaseTable.metadata.create_all)

    yield engine

    # Drop all tables and extensions after the test
    async with engine.begin() as conn:
        await conn.run_sync(BaseTable.metadata.drop_all)
        await conn.execute(text("DROP TABLE IF EXISTS alembic_version;"))
        await conn.execute(text('DROP EXTENSION IF EXISTS "uuid-ossp";'))

    await engine.dispose()


@pytest.fixture(scope="function")
def test_inspector(test_settings: Settings, test_engine):
    """
    Provide a synchronous inspector for schema validation tests.

    This creates a separate sync engine to avoid greenlet issues with
    the async engine. Depends on test_engine to ensure tables exist.
    """
    from sqlalchemy import create_engine, inspect

    # test_engine has already created the tables via Alembic
    # Now create a synchronous engine just for inspection
    sync_engine = create_engine(
        test_settings.DATABASE.POSTGRESQL_URL,
        pool_pre_ping=True,
    )

    inspector = inspect(sync_engine)

    yield inspector

    # Clean up sync engine
    sync_engine.dispose()


@pytest_asyncio.fixture(scope="function")
async def test_db_session(test_engine: AsyncEngine) -> AsyncGenerator[AsyncSession, None]:
    """
    Create a test database session with proper transaction rollback.

    This ensures test isolation - each test starts with a clean database state.
    Changes made during the test are rolled back after the test completes.
    """
    # Create a connection and begin a transaction
    async with test_engine.connect() as connection:
        async with connection.begin() as transaction:
            # Create session bound to this transaction
            async_session = async_sessionmaker(
                bind=connection,
                class_=AsyncSession,
                expire_on_commit=False,
                join_transaction_mode="create_savepoint",  # Use savepoints for nested transactions
            )

            async with async_session() as session:
                yield session

                # Rollback the transaction after test completes
                # This undoes all changes made during the test
                await transaction.rollback()


@pytest_asyncio.fixture(scope="function")
async def test_db_with_data(test_db_session: AsyncSession) -> AsyncSession:
    """
    Create test database with sample sailing data.

    This fixture creates realistic test data for the period 2024-01-01 to 2024-03-31
    with multiple unique vessel/service combinations per week.
    """
    # Create test data: 13 weeks from 2024-01-01 to 2024-03-31
    # Use UTC-aware datetime to avoid timezone conversion issues
    base_date = utc_datetime(2024, 1, 1, 0, 0, 0)

    test_sailings = []
    for week in range(13):  # 13 weeks for Q1
        week_date = base_date + timedelta(weeks=week)

        # Create 3-5 unique vessel/service combinations per week
        num_vessels = 3 + (week % 3)
        for vessel_idx in range(num_vessels):
            sailing = SailingTable(
                origin="china_main",
                destination="north_europe_main",
                origin_port_code="CNSHA",
                destination_port_code="NLRTM",
                service_version_and_roundtrip_identfiers=f"SERVICE_{week}_{vessel_idx}",
                origin_service_version_and_master=f"MASTER_{week}_{vessel_idx}_ORIGIN",
                destination_service_version_and_master=f"MASTER_{week}_{vessel_idx}_DEST",
                origin_at_utc=week_date + timedelta(days=vessel_idx),
                offered_capacity_teu=15000 + (vessel_idx * 5000) + (week * 1000),
            )

            test_sailings.append(sailing)

    # Add some sailings with different routes (should be filtered out)
    test_sailings.append(
        SailingTable(
            origin="china_main",
            destination="us_west_coast",
            origin_port_code="CNSHA",
            destination_port_code="USLAX",
            service_version_and_roundtrip_identfiers="SERVICE_OTHER_1",
            origin_service_version_and_master="MASTER_OTHER_1_ORIGIN",
            destination_service_version_and_master="MASTER_OTHER_1_DEST",
            origin_at_utc=base_date,
            offered_capacity_teu=20000,
        )
    )

    test_db_session.add_all(test_sailings)
    await test_db_session.commit()

    return test_db_session


@pytest_asyncio.fixture
async def async_client() -> AsyncGenerator[AsyncClient, None]:
    """Create async HTTP client for testing FastAPI app"""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client


@pytest.fixture
def sync_client() -> Generator[TestClient, None, None]:
    """Create synchronous test client for FastAPI app"""
    with TestClient(app) as client:
        yield client


@pytest.fixture(autouse=True)
def override_get_settings_fixture(test_settings):
    """
    Override the get_settings dependency for all tests.
    This ensures tests use test database configuration and disables
    the lifespan event (which would try to load CSV data on startup).
    """
    # Temporarily replace the app's lifespan to prevent CSV loading
    from contextlib import asynccontextmanager

    original_lifespan = app.router.lifespan_context

    @asynccontextmanager
    async def test_lifespan(app_instance):
        # Do nothing on startup/shutdown for tests
        yield

    app.router.lifespan_context = test_lifespan
    app.dependency_overrides[get_settings] = lambda: test_settings

    yield

    # Restore original lifespan and clear overrides
    app.router.lifespan_context = original_lifespan
    app.dependency_overrides.clear()


@pytest_asyncio.fixture
async def async_client_with_db(test_db_session: AsyncSession) -> AsyncGenerator[AsyncClient, None]:
    """
    Create async HTTP client that uses the same test database session.
    This ensures API tests can see data inserted by test fixtures.
    The session uses transaction rollback for test isolation.
    """
    from app.database.session import get_db_session

    # Override get_db_session to use the test session
    async def override_get_db_session():
        # Don't create a new session, use the one from the fixture
        # This ensures all queries happen in the same transaction
        yield test_db_session

    app.dependency_overrides[get_db_session] = override_get_db_session

    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            yield client
    finally:
        # Clear overrides after this client is done
        app.dependency_overrides.pop(get_db_session, None)
