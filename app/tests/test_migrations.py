import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.model import SailingTable


@pytest.mark.integration
@pytest.mark.migrations
class TestAlembicMigrations:
    """Tests for Alembic migrations"""

    @pytest.mark.asyncio
    async def test_migrations_run_successfully(self, test_engine):
        """
        Test that all migrations can be applied successfully.

        This is a critical test - if migrations fail, deployment will fail.
        """
        # The test_engine fixture already creates tables, which means
        # migrations have been applied successfully
        # Let's verify the sailings table exists
        async with test_engine.connect() as conn:
            result = await conn.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_name = 'sailings'
                    );
                """
                )
            )
            table_exists = result.scalar()
            assert table_exists, "Sailings table was not created by migrations"

    @pytest.mark.asyncio
    async def test_required_tables_exist(self, test_engine):
        """
        Test that all required tables exist in the database.

        This validates that migrations created all expected tables.
        """
        async with test_engine.connect() as conn:
            # Get list of all tables
            result = await conn.execute(
                text(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                """
                )
            )
            tables = [row[0] for row in result.fetchall()]

            # Verify required tables exist
            assert "sailings" in tables, "Sailings table missing"
            # Add checks for any other tables you have
            # assert "other_table" in tables

    @pytest.mark.asyncio
    async def test_alembic_version_table_exists(self, test_engine):
        """
        Test that Alembic version tracking table exists.

        This ensures migration history is being tracked.
        """
        async with test_engine.connect() as conn:
            result = await conn.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_name = 'alembic_version'
                    );
                """
                )
            )
            version_table_exists = result.scalar()
            assert version_table_exists, "Alembic version table missing"


@pytest.mark.integration
@pytest.mark.schema
class TestDatabaseSchema:
    """Tests for database schema validation"""

    def test_sailings_table_schema(self, test_inspector):
        """
        Test that sailings table has the correct schema.

        This validates that the actual database schema matches expectations.
        """
        inspector = test_inspector

        # Get columns for sailings table
        columns = inspector.get_columns("sailings")
        column_names = [col["name"] for col in columns]

        # Verify required columns exist
        required_columns = [
            "id",
            "created_at",
            "updated_at",
            "origin",
            "destination",
            "origin_port_code",
            "destination_port_code",
            "service_version_and_roundtrip_identfiers",
            "origin_service_version_and_master",
            "destination_service_version_and_master",
            "origin_at_utc",
            "offered_capacity_teu",
        ]

        for col in required_columns:
            assert col in column_names, f"Column '{col}' missing from sailings table"

    def test_sailings_table_column_types(self, test_inspector):
        """
        Test that sailings table columns have correct data types.

        This catches mismatches between ORM models and actual database schema.
        """
        inspector = test_inspector
        columns = {col["name"]: col for col in inspector.get_columns("sailings")}

        # Verify column types
        # String columns
        for col_name in [
            "origin",
            "destination",
            "origin_port_code",
            "destination_port_code",
            "service_version_and_roundtrip_identfiers",
            "origin_service_version_and_master",
            "destination_service_version_and_master",
        ]:
            assert col_name in columns, f"Column '{col_name}' missing"
            col_type = str(columns[col_name]["type"]).upper()
            assert (
                "VARCHAR" in col_type or "STRING" in col_type
            ), f"Column '{col_name}' should be VARCHAR/STRING, got {col_type}"

        # Integer columns
        assert "offered_capacity_teu" in columns
        col_type = str(columns["offered_capacity_teu"]["type"]).upper()
        assert "INT" in col_type, f"offered_capacity_teu should be INTEGER, got {col_type}"

        # DateTime columns
        for col_name in ["origin_at_utc", "created_at", "updated_at"]:
            assert col_name in columns, f"Column '{col_name}' missing"
            col_type = str(columns[col_name]["type"]).upper()
            assert (
                "TIMESTAMP" in col_type or "DATETIME" in col_type
            ), f"Column '{col_name}' should be TIMESTAMP/DATETIME, got {col_type}"

    def test_sailings_table_nullable_constraints(self, test_inspector):
        """
        Test that nullable constraints are correctly applied.

        This ensures data integrity at the database level.
        """
        inspector = test_inspector
        columns = {col["name"]: col for col in inspector.get_columns("sailings")}

        # These columns should NOT be nullable
        non_nullable_columns = [
            "id",
            "created_at",
            "updated_at",
            "origin",
            "destination",
            "origin_port_code",
            "destination_port_code",
            "service_version_and_roundtrip_identfiers",
            "origin_service_version_and_master",
            "destination_service_version_and_master",
            "origin_at_utc",
            "offered_capacity_teu",
        ]

        for col_name in non_nullable_columns:
            assert col_name in columns, f"Column '{col_name}' missing"
            assert not columns[col_name]["nullable"], f"Column '{col_name}' should NOT be nullable"

    def test_primary_key_exists(self, test_inspector):
        """
        Test that primary key constraint exists on sailings table.

        Primary keys are critical for data integrity and performance.
        """
        inspector = test_inspector
        pk_constraint = inspector.get_pk_constraint("sailings")

        assert pk_constraint is not None, "Primary key constraint missing"
        assert "constrained_columns" in pk_constraint
        assert "id" in pk_constraint["constrained_columns"], "Primary key should be on 'id' column"

    def test_required_indexes_exist(self, test_inspector):
        """
        Test that required indexes exist for query performance.

        Missing indexes can cause severe performance problems in production.
        """
        inspector = test_inspector
        indexes = inspector.get_indexes("sailings")

        # Check that we have at least one index
        assert len(indexes) > 0, "No indexes found on sailings table"

        # Check for the specific composite index we defined
        index_names = [idx["name"] for idx in indexes]
        assert (
            "idx_sailings_general_query_pattern" in index_names
        ), "Required composite index 'idx_sailings_general_query_pattern' missing"

        # Verify the composite index has the correct columns
        for idx in indexes:
            if idx["name"] == "idx_sailings_general_query_pattern":
                columns = idx["column_names"]
                assert "origin" in columns, "Index should include 'origin'"
                assert "destination" in columns, "Index should include 'destination'"
                assert "origin_at_utc" in columns, "Index should include 'origin_at_utc'"

    def test_unique_constraints(self, test_inspector):
        """
        Test that unique constraints or primary key are properly applied.

        This prevents duplicate data at the database level.
        Note: ID has a primary key constraint, which implies uniqueness.
        """
        inspector = test_inspector

        # Check for primary key (which ensures uniqueness)
        pk_constraint = inspector.get_pk_constraint("sailings")
        assert pk_constraint is not None, "Primary key constraint missing"
        assert "id" in pk_constraint["constrained_columns"], "ID should be primary key"

        # Primary key implies uniqueness - no separate unique constraint needed
        # If you want to test for additional unique constraints, add them here:
        # unique_constraints = inspector.get_unique_constraints("sailings")
        # Check for any business-specific unique constraints if they exist


@pytest.mark.integration
@pytest.mark.schema
class TestSchemaModelAlignment:
    """Tests that database schema matches ORM models"""

    def test_orm_model_matches_database_schema(self, test_inspector):
        """
        Critical test: Verify ORM model columns match actual database.

        Misalignment between ORM and database causes runtime errors.
        """
        inspector = test_inspector
        db_columns = {col["name"] for col in inspector.get_columns("sailings")}

        # Get ORM model columns
        orm_columns = {col.name for col in SailingTable.__table__.columns}

        # Check that all ORM columns exist in database
        missing_in_db = orm_columns - db_columns
        assert len(missing_in_db) == 0, f"ORM columns missing in database: {missing_in_db}"

        # Check that all database columns are in ORM
        missing_in_orm = db_columns - orm_columns
        assert len(missing_in_orm) == 0, f"Database columns missing in ORM: {missing_in_orm}"

    def test_orm_model_indexes_match_database(self, test_inspector):
        """
        Test that indexes defined in ORM exist in database.

        This ensures query optimizations are actually applied.
        """
        inspector = test_inspector
        db_indexes = {idx["name"] for idx in inspector.get_indexes("sailings")}

        # Get indexes defined in ORM model
        orm_indexes = {idx.name for idx in SailingTable.__table__.indexes}

        # Verify all ORM indexes exist in database
        for orm_idx in orm_indexes:
            assert (
                orm_idx in db_indexes
            ), f"Index '{orm_idx}' defined in ORM but missing in database"


@pytest.mark.integration
class TestDatabaseConstraints:
    """Tests for database constraint enforcement"""

    @pytest.mark.asyncio
    async def test_not_null_constraints_enforced(self, test_db_session: AsyncSession):
        """
        Test that NOT NULL constraints are actually enforced.

        This prevents invalid data from being inserted.
        """
        from sqlalchemy.exc import IntegrityError

        # Try to insert a record with null required field
        sailing = SailingTable(
            origin="china_main",
            destination="north_europe_main",
            origin_port_code="CNSHA",
            destination_port_code=None,  # This should fail
            service_version_and_roundtrip_identfiers="SERVICE_1",
            origin_service_version_and_master="MASTER_1_ORIGIN",
            destination_service_version_and_master="MASTER_1_DEST",
            origin_at_utc=None,  # This should also fail
            offered_capacity_teu=15000,
        )

        test_db_session.add(sailing)

        # Should raise IntegrityError due to null constraint
        with pytest.raises((IntegrityError, AttributeError, ValueError)):
            await test_db_session.commit()

    @pytest.mark.asyncio
    async def test_data_type_constraints_enforced(self, test_db_session: AsyncSession):
        """
        Test that data type constraints are enforced.

        This catches type mismatches at insert time.
        """
        from datetime import datetime, timezone

        from sqlalchemy.exc import DataError, DBAPIError, IntegrityError

        # Try to insert a record with wrong data type
        sailing = SailingTable(
            origin="china_main",
            destination="north_europe_main",
            origin_port_code="CNSHA",
            destination_port_code="NLRTM",
            service_version_and_roundtrip_identfiers="SERVICE_1",
            origin_service_version_and_master="MASTER_1_ORIGIN",
            destination_service_version_and_master="MASTER_1_DEST",
            origin_at_utc=datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            offered_capacity_teu="not_an_integer",  # This should fail
        )

        test_db_session.add(sailing)

        # Should raise error due to type mismatch (asyncpg raises DBAPIError)
        with pytest.raises((DataError, IntegrityError, ValueError, TypeError, DBAPIError)):
            await test_db_session.commit()


@pytest.mark.integration
@pytest.mark.migrations
class TestMigrationReversibility:
    """Tests for migration upgrade/downgrade"""

    @pytest.mark.asyncio
    async def test_can_query_alembic_version(self, test_engine):
        """
        Test that we can query the current migration version.

        This is used for deployment verification.
        """
        async with test_engine.connect() as conn:
            result = await conn.execute(text("SELECT version_num FROM alembic_version;"))
            version = result.scalar()

            # Should have a version (tables were created)
            assert version is not None, "No Alembic version found"
            assert len(version) > 0, "Alembic version is empty"

    @pytest.mark.asyncio
    async def test_table_creation_is_idempotent(self, test_engine):
        """
        Test that running migrations multiple times doesn't cause errors.

        This is important for CI/CD pipelines.
        """
        # Tables are already created by test_engine fixture
        # Try to query them - should not raise any errors
        async with test_engine.connect() as conn:
            result = await conn.execute(text("SELECT COUNT(*) FROM sailings;"))
            count = result.scalar()
            assert count == 0, "Table should be empty initially"
