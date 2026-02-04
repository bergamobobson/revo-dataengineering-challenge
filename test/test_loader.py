"""
test/test_loader.py

Unit tests for DimensionsLoader and FactLoader (UPSERT version).
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch, call
from datetime import datetime, timezone

from src.loader import BaseLoader, DimensionsLoader, FactLoader
from config.source import DatabaseConfig

pytestmark = pytest.mark.loader


# ============== FIXTURES ==============

@pytest.fixture
def mock_config():
    return DatabaseConfig(
        USERNAME="test_user",
        PASSWORD="test_pass",
        DATABASE="test_db",
        SCHEMA="test_schema",
        HOST="localhost",
        PORT=5432,
    )


@pytest.fixture
def sample_dimensions():
    """Dimensions with correct keys matching TABLE_MAPPING."""
    now = pd.Timestamp.now(tz="UTC")
    return {
        "Population": pd.DataFrame({
            "key": ["P1", "P2"],
            "title": ["Pop 1", "Pop 2"],
            "description": ["Desc 1", "Desc 2"],
            "ingested_at": [now, now],
        }),
        "Periods": pd.DataFrame({
            "key": ["2023JJ00", "2024JJ00"],
            "title": ["Year 2023", "Year 2024"],
            "description": ["Full 2023", "Full 2024"],
            "ingested_at": [now, now],
        }),
    }


@pytest.fixture
def sample_facts():
    """Facts DataFrame matching FactLoader.FACT_COLUMNS."""
    now = pd.Timestamp.now(tz="UTC")
    return pd.DataFrame({
        "fact_id": ["abc123def456abc123def456abc12345", "def456abc123def456abc123def45678"],
        "travel_motive_key": ["TM1", "TM2"],
        "population_key": ["P1", "P2"],
        "travel_mode_key": ["M1", "M2"],
        "margin_key": ["MG1", "MG2"],
        "region_key": ["R1", "R2"],
        "period_key": ["2023JJ00", "2024JJ00"],
        "trips_daily": [10.5, 20.0],
        "distance_daily": [5.2, 10.1],
        "time_daily": [30.0, 45.0],
        "trips_yearly": [3800.0, 7300.0],
        "distance_yearly": [1900.0, 3700.0],
        "time_yearly": [11000.0, 16500.0],
        "ingested_at": [now, now],
    })


# ============== BASE LOADER TESTS ==============

class TestBaseLoader:

    @pytest.mark.happy_path
    def test_config_stored(self, mock_config):
        loader = BaseLoader(mock_config)
        assert loader.config == mock_config

    @pytest.mark.happy_path
    @patch("src.loader.create_engine")
    def test_engine_created_lazily_and_cached(self, mock_create_engine, mock_config):
        fake_engine = MagicMock()
        mock_create_engine.return_value = fake_engine

        loader = BaseLoader(mock_config)

        # Lazy: not created at init
        assert loader._engine is None

        # First access creates it
        e1 = loader.engine
        mock_create_engine.assert_called_once_with(mock_config.connection_string)
        assert e1 is fake_engine

        # Second access uses cache
        e2 = loader.engine
        assert e2 is fake_engine
        mock_create_engine.assert_called_once()

    @pytest.mark.happy_path
    def test_build_upsert_query_basic(self, mock_config):
        loader = BaseLoader(mock_config)
        
        query = loader._build_upsert_query(
            table_name="dim_test",
            columns=["key", "title", "description"],
            conflict_column="key",
            update_columns=["title", "description"]
        )
        
        assert "INSERT INTO test_schema.dim_test" in query
        assert "(key, title, description)" in query
        assert "VALUES (:key, :title, :description)" in query
        assert "ON CONFLICT (key) DO UPDATE SET" in query
        assert "title = EXCLUDED.title" in query
        assert "description = EXCLUDED.description" in query

    @pytest.mark.edge_case
    def test_build_upsert_query_no_schema(self, mock_config):
        # Config without schema
        config_no_schema = DatabaseConfig(
            USERNAME="test_user",
            PASSWORD="test_pass",
            DATABASE="test_db",
            SCHEMA="",  # Empty schema
            HOST="localhost",
            PORT=5432,
        )
        loader = BaseLoader(config_no_schema)
        
        query = loader._build_upsert_query(
            table_name="dim_test",
            columns=["key", "title"],
            conflict_column="key",
        )
        
        # Should not have schema prefix
        assert "INSERT INTO dim_test" in query
        assert ".dim_test" not in query


# ============== DIMENSIONS LOADER TESTS ==============

class TestDimensionsLoader:

    @pytest.mark.happy_path
    @patch("src.loader.create_engine")
    def test_load_executes_upsert_for_each_dimension(self, mock_create_engine, mock_config, sample_dimensions):
        # Setup mock engine and connection
        fake_conn = MagicMock()
        fake_engine = MagicMock()
        fake_engine.begin.return_value.__enter__ = MagicMock(return_value=fake_conn)
        fake_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        mock_create_engine.return_value = fake_engine

        loader = DimensionsLoader(mock_config)
        results = loader.load(sample_dimensions)

        # Verify counts returned
        assert results == {
            "dim_population": 2,
            "dim_periods": 2,
        }

        # Verify execute called for each dimension
        assert fake_conn.execute.call_count == 2

    @pytest.mark.happy_path
    @patch("src.loader.create_engine")
    def test_upsert_query_contains_correct_columns(self, mock_create_engine, mock_config, sample_dimensions):
        fake_conn = MagicMock()
        fake_engine = MagicMock()
        fake_engine.begin.return_value.__enter__ = MagicMock(return_value=fake_conn)
        fake_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        mock_create_engine.return_value = fake_engine

        loader = DimensionsLoader(mock_config)
        loader.load(sample_dimensions)

        # Get the SQL queries that were executed
        calls = fake_conn.execute.call_args_list
        
        for call_obj in calls:
            query_text = str(call_obj[0][0])  # First positional arg is the query
            
            # Check UPSERT structure
            assert "INSERT INTO" in query_text
            assert "ON CONFLICT (key) DO UPDATE SET" in query_text
            assert "title = EXCLUDED.title" in query_text
            assert "description = EXCLUDED.description" in query_text
            assert "ingested_at = EXCLUDED.ingested_at" in query_text

    @pytest.mark.edge_case
    @patch("src.loader.create_engine")
    def test_load_skips_unknown_dimension(self, mock_create_engine, mock_config):
        fake_engine = MagicMock()
        mock_create_engine.return_value = fake_engine

        loader = DimensionsLoader(mock_config)
        unknown = {"UnknownDim": pd.DataFrame({"key": ["X"]})}

        results = loader.load(unknown)

        assert results == {}
        # Engine should not be accessed if nothing to load
        fake_engine.begin.assert_not_called()

    @pytest.mark.happy_path
    def test_table_mapping_uses_original_cbs_names(self):
        """TABLE_MAPPING keys should match CBS metadata section names."""
        expected_keys = [
            "Population",
            "TravelMotives",
            "TravelModes",
            "Margins",
            "RegionCharacteristics",
            "Periods",
        ]
        for key in expected_keys:
            assert key in DimensionsLoader.TABLE_MAPPING

    @pytest.mark.happy_path
    def test_table_mapping_values_are_dim_tables(self):
        """TABLE_MAPPING values should be dim_* table names."""
        for table_name in DimensionsLoader.TABLE_MAPPING.values():
            assert table_name.startswith("dim_")


# ============== FACT LOADER TESTS ==============

class TestFactLoader:

    @pytest.mark.happy_path
    @patch("src.loader.create_engine")
    def test_load_executes_upsert(self, mock_create_engine, mock_config, sample_facts):
        fake_conn = MagicMock()
        fake_engine = MagicMock()
        fake_engine.begin.return_value.__enter__ = MagicMock(return_value=fake_conn)
        fake_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        mock_create_engine.return_value = fake_engine

        loader = FactLoader(mock_config)
        result = loader.load(sample_facts)

        assert result == 2
        fake_conn.execute.assert_called_once()

    @pytest.mark.happy_path
    @patch("src.loader.create_engine")
    def test_upsert_query_structure(self, mock_create_engine, mock_config, sample_facts):
        fake_conn = MagicMock()
        fake_engine = MagicMock()
        fake_engine.begin.return_value.__enter__ = MagicMock(return_value=fake_conn)
        fake_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        mock_create_engine.return_value = fake_engine

        loader = FactLoader(mock_config)
        loader.load(sample_facts)

        # Get executed query
        query_text = str(fake_conn.execute.call_args[0][0])

        # Check structure
        assert "INSERT INTO test_schema.fact_mobility" in query_text
        assert "ON CONFLICT (fact_id) DO UPDATE SET" in query_text
        
        # Should update metrics, not FK columns
        assert "trips_daily = EXCLUDED.trips_daily" in query_text
        assert "distance_yearly = EXCLUDED.distance_yearly" in query_text
        
        # FK columns should NOT be in UPDATE SET
        assert "travel_motive_key = EXCLUDED" not in query_text
        assert "population_key = EXCLUDED" not in query_text

    @pytest.mark.happy_path
    @patch("src.loader.create_engine")
    def test_load_passes_records_as_dict(self, mock_create_engine, mock_config, sample_facts):
        fake_conn = MagicMock()
        fake_engine = MagicMock()
        fake_engine.begin.return_value.__enter__ = MagicMock(return_value=fake_conn)
        fake_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        mock_create_engine.return_value = fake_engine

        loader = FactLoader(mock_config)
        loader.load(sample_facts)

        # Second argument should be list of dicts
        records = fake_conn.execute.call_args[0][1]
        
        assert isinstance(records, list)
        assert len(records) == 2
        assert isinstance(records[0], dict)
        assert "fact_id" in records[0]
        assert "trips_daily" in records[0]

    @pytest.mark.happy_path
    def test_fact_columns_complete(self):
        """FACT_COLUMNS should include all required columns."""
        required = [
            "fact_id",
            "travel_motive_key",
            "population_key",
            "travel_mode_key",
            "margin_key",
            "region_key",
            "period_key",
            "trips_daily",
            "distance_daily",
            "time_daily",
            "trips_yearly",
            "distance_yearly",
            "time_yearly",
            "ingested_at",
        ]
        for col in required:
            assert col in FactLoader.FACT_COLUMNS

    @pytest.mark.happy_path
    def test_metric_columns_are_subset_of_fact_columns(self):
        """METRIC_COLUMNS should only contain columns that exist in FACT_COLUMNS."""
        for col in FactLoader.METRIC_COLUMNS:
            assert col in FactLoader.FACT_COLUMNS

    @pytest.mark.happy_path
    def test_table_name(self):
        assert FactLoader.TABLE_NAME == "fact_mobility"