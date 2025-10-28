import pandas as pd
import pytest

from app.database.utils.etl_pipeline import ETLPipeline


@pytest.mark.unit
@pytest.mark.etl
class TestETLPipelineExtract:
    """Tests for the Extract phase of ETL pipeline"""

    @pytest.mark.asyncio
    async def test_extract_valid_csv(self, tmp_path):
        """
        Test extraction of valid CSV file.

        Should successfully read CSV and return DataFrame.
        """
        # Create a temporary CSV file
        columns = [
            "ORIGIN",
            "DESTINATION",
            "ORIGIN_PORT_CODE",
            "DESTINATION_PORT_CODE",
            "SERVICE_VERSION_AND_ROUNDTRIP_IDENTFIERS",
            "ORIGIN_SERVICE_VERSION_AND_MASTER",
            "DESTINATION_SERVICE_VERSION_AND_MASTER",
            "ORIGIN_AT_UTC",
            "OFFERED_CAPACITY_TEU",
        ]
        csv_content = f"{','.join(columns)}\n"
        csv_content += (
            "china_main,north_europe_main,CNSHA,NLRTM,"
            "SERVICE_1,MASTER_1_ORIGIN,MASTER_1_DEST,"
            "2024-01-01 00:00:00,15000\n"
        )
        csv_content += (
            "china_main,north_europe_main,CNSHA,DEHAM,"
            "SERVICE_2,MASTER_2_ORIGIN,MASTER_2_DEST,"
            "2024-01-08 00:00:00,16000\n"
        )

        csv_file = tmp_path / "test_data.csv"
        csv_file.write_text(csv_content)

        pipeline = ETLPipeline()
        df = await pipeline.extract(str(csv_file))

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert list(df.columns) == [
            "ORIGIN",
            "DESTINATION",
            "ORIGIN_PORT_CODE",
            "DESTINATION_PORT_CODE",
            "SERVICE_VERSION_AND_ROUNDTRIP_IDENTFIERS",
            "ORIGIN_SERVICE_VERSION_AND_MASTER",
            "DESTINATION_SERVICE_VERSION_AND_MASTER",
            "ORIGIN_AT_UTC",
            "OFFERED_CAPACITY_TEU",
        ]

    @pytest.mark.asyncio
    async def test_extract_nonexistent_file(self):
        """
        Test extraction of non-existent CSV file.

        Should raise FileNotFoundError.
        """
        pipeline = ETLPipeline()

        with pytest.raises(FileNotFoundError):
            await pipeline.extract("/nonexistent/path/data.csv")

    @pytest.mark.asyncio
    async def test_extract_empty_csv(self, tmp_path):
        """
        Test extraction of empty CSV file.

        Should return empty DataFrame but not crash.
        """
        columns = [
            "ORIGIN",
            "DESTINATION",
            "ORIGIN_PORT_CODE",
            "DESTINATION_PORT_CODE",
            "SERVICE_VERSION_AND_ROUNDTRIP_IDENTFIERS",
            "ORIGIN_SERVICE_VERSION_AND_MASTER",
            "DESTINATION_SERVICE_VERSION_AND_MASTER",
            "ORIGIN_AT_UTC",
            "OFFERED_CAPACITY_TEU",
        ]
        csv_content = f"{','.join(columns)}\n"
        csv_content += "\n"

        csv_file = tmp_path / "empty_data.csv"
        csv_file.write_text(csv_content)

        pipeline = ETLPipeline()
        df = await pipeline.extract(str(csv_file))

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0


@pytest.mark.unit
@pytest.mark.etl
class TestETLPipelineTransform:
    """Tests for the Transform phase of ETL pipeline"""

    @pytest.mark.asyncio
    async def test_transform_column_names_to_lowercase(self):
        """
        Test that column names are transformed to lowercase.

        This is necessary for matching with database column names.
        """
        df = pd.DataFrame(
            {
                "ORIGIN": ["china_main"],
                "DESTINATION": ["north_europe_main"],
                "ORIGIN_PORT_CODE": ["CNSHA"],
                "DESTINATION_PORT_CODE": ["NLRTM"],
                "SERVICE_VERSION_AND_ROUNDTRIP_IDENTFIERS": ["SERVICE_1"],
                "ORIGIN_SERVICE_VERSION_AND_MASTER": ["MASTER_1_ORIGIN"],
                "DESTINATION_SERVICE_VERSION_AND_MASTER": ["MASTER_1_DEST"],
                "ORIGIN_AT_UTC": ["2024-01-01 00:00:00"],
                "OFFERED_CAPACITY_TEU": [15000],
            }
        )

        pipeline = ETLPipeline()
        df_transformed = await pipeline.transform(df)

        # All column names should be lowercase
        assert all(col.islower() or "_" in col for col in df_transformed.columns)
        assert "origin" in df_transformed.columns
        assert "destination" in df_transformed.columns
        assert "ORIGIN" not in df_transformed.columns

    @pytest.mark.asyncio
    async def test_transform_preserves_different_journeys(self):
        """
        Test that different journeys are preserved (not deduplicated).

        Rows with different unique identifier combinations should all be kept.
        """
        df = pd.DataFrame(
            {
                "ORIGIN": ["china_main", "china_main", "china_main"],
                "DESTINATION": ["north_europe_main", "north_europe_main", "north_europe_main"],
                "ORIGIN_PORT_CODE": ["CNSHA", "CNSHA", "CNSHA"],
                "DESTINATION_PORT_CODE": ["NLRTM", "DEHAM", "GBSOU"],
                "SERVICE_VERSION_AND_ROUNDTRIP_IDENTFIERS": ["SERVICE_1", "SERVICE_2", "SERVICE_3"],
                "ORIGIN_SERVICE_VERSION_AND_MASTER": ["MASTER_1", "MASTER_2", "MASTER_3"],
                "DESTINATION_SERVICE_VERSION_AND_MASTER": ["MASTER_1", "MASTER_2", "MASTER_3"],
                "ORIGIN_AT_UTC": ["2024-01-01", "2024-01-01", "2024-01-01"],
                "OFFERED_CAPACITY_TEU": [15000, 16000, 17000],
            }
        )

        pipeline = ETLPipeline()
        df_transformed = await pipeline.transform(df)

        # All three different services should be preserved
        assert len(df_transformed) == 3

    @pytest.mark.asyncio
    async def test_transform_preserves_data(self):
        """
        Test that transformation preserves the actual data values.

        Only column names should change, not data content.
        """
        df = pd.DataFrame(
            {
                "ORIGIN": ["china_main", "china_main"],
                "DESTINATION": ["north_europe_main", "north_europe_main"],
                "ORIGIN_PORT_CODE": ["CNSHA", "CNSHA"],
                "DESTINATION_PORT_CODE": ["NLRTM", "DEHAM"],
                "SERVICE_VERSION_AND_ROUNDTRIP_IDENTFIERS": ["SERVICE_1", "SERVICE_2"],
                "ORIGIN_SERVICE_VERSION_AND_MASTER": ["MASTER_1", "MASTER_2"],
                "DESTINATION_SERVICE_VERSION_AND_MASTER": ["MASTER_1", "MASTER_2"],
                "ORIGIN_AT_UTC": ["2024-01-01", "2024-01-08"],
                "OFFERED_CAPACITY_TEU": [15000, 16000],
            }
        )

        pipeline = ETLPipeline()
        df_transformed = await pipeline.transform(df)

        # Data should be preserved (but order may change due to sorting)
        assert len(df_transformed) == 2
        assert df_transformed["origin"].tolist() == ["china_main", "china_main"]
        # Sort by capacity to check values regardless of order
        assert sorted(df_transformed["offered_capacity_teu"].tolist()) == [15000, 16000]

    @pytest.mark.asyncio
    async def test_transform_empty_dataframe(self):
        """
        Test transformation of empty DataFrame.

        Should handle empty data gracefully.
        """
        df = pd.DataFrame(
            columns=[
                "ORIGIN",
                "DESTINATION",
                "ORIGIN_PORT_CODE",
                "DESTINATION_PORT_CODE",
                "SERVICE_VERSION_AND_ROUNDTRIP_IDENTFIERS",
                "ORIGIN_SERVICE_VERSION_AND_MASTER",
                "DESTINATION_SERVICE_VERSION_AND_MASTER",
                "ORIGIN_AT_UTC",
                "OFFERED_CAPACITY_TEU",
            ]
        )

        pipeline = ETLPipeline()
        df_transformed = await pipeline.transform(df)

        assert isinstance(df_transformed, pd.DataFrame)
        assert len(df_transformed) == 0
        # Columns should still be transformed
        assert all(col.islower() or "_" in col for col in df_transformed.columns)


@pytest.mark.unit
@pytest.mark.etl
class TestETLPipelineIntegration:
    """Integration tests for complete ETL pipeline"""

    @pytest.mark.asyncio
    async def test_extract_and_transform_integration(self, tmp_path):
        """
        Test complete extract and transform workflow.

        Verifies that data flows correctly from CSV to transformed DataFrame.
        """
        # Create a realistic CSV file
        columns = [
            "ORIGIN",
            "DESTINATION",
            "ORIGIN_PORT_CODE",
            "DESTINATION_PORT_CODE",
            "SERVICE_VERSION_AND_ROUNDTRIP_IDENTFIERS",
            "ORIGIN_SERVICE_VERSION_AND_MASTER",
            "DESTINATION_SERVICE_VERSION_AND_MASTER",
            "ORIGIN_AT_UTC",
            "OFFERED_CAPACITY_TEU",
        ]
        csv_content = f"{','.join(columns)}\n"
        csv_content += (
            "china_main,north_europe_main,CNSHA,NLRTM,SERVICE_1,MASTER_1_ORIGIN,MASTER_1_DEST,"
            "2024-01-01 00:00:00,15000\n"
            "china_main,north_europe_main,CNSHA,DEHAM,SERVICE_2,MASTER_2_ORIGIN,MASTER_2_DEST,"
            "2024-01-08 00:00:00,16000\n"
            "china_main,us_west_coast,CNSHA,USLAX,SERVICE_3,MASTER_3_ORIGIN,MASTER_3_DEST,"
            "2024-01-15 00:00:00,20000\n"
        )
        csv_file = tmp_path / "integration_test.csv"
        csv_file.write_text(csv_content)

        pipeline = ETLPipeline()

        # Extract
        df_extracted = await pipeline.extract(str(csv_file))
        assert len(df_extracted) == 3

        # Transform
        df_transformed = await pipeline.transform(df_extracted)
        assert len(df_transformed) == 3

        # Verify columns are lowercase
        assert "origin" in df_transformed.columns
        assert "ORIGIN" not in df_transformed.columns

        # Verify data integrity (order may change due to sorting)
        assert df_transformed["origin"].tolist() == [
            "china_main",
            "china_main",
            "china_main",
        ]
        # Sort to check values regardless of order
        assert sorted(df_transformed["offered_capacity_teu"].tolist()) == [15000, 16000, 20000]
