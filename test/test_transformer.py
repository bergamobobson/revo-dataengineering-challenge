"""
test/test_transformer.py

Unit tests for DimensionTransformer and FactTransformer.
"""

import pytest
import pandas as pd
import numpy as np
import hashlib

from src.transformer import DimensionTransformer, FactTransformer


pytestmark = pytest.mark.transformer


# ============== FIXTURES ==============

@pytest.fixture
def raw_dimension_df():
    return pd.DataFrame({
        "Key": ["P1", "P2"],
        "Title": ["  Pop 1  ", "Pop 2"],
        "Description": ["Desc 1", ""],
    })


@pytest.fixture
def raw_fact_df():
    return pd.DataFrame({
        "TravelMotives": ["TM1"],
        "Population": ["P1"],
        "TravelModes": ["M1"],
        "Margins": ["MG1"],
        "RegionCharacteristics": ["R1"],
        "Periods": ["2023"],
        "Trips_1": [10.5],
        "DistanceTravelled_2": [100.5],
        "TimeTravelled_3": [30.0],
        "Trips_4": [3800],
        "DistanceTravelled_5": [36500],
        "TimeTravelled_6": [10950.0],
    })


# ============== DIMENSION TESTS ==============

class TestDimensionTransformer:

    @pytest.mark.happy_path
    def test_transform_renames_columns(self, raw_dimension_df):
        result = DimensionTransformer(raw_dimension_df).transform()
        assert "key" in result.columns
        assert "title" in result.columns
        assert "description" in result.columns
        assert "Key" not in result.columns

    @pytest.mark.happy_path
    def test_transform_strips_whitespace(self, raw_dimension_df):
        result = DimensionTransformer(raw_dimension_df).transform()
        assert result.iloc[0]["title"] == "Pop 1"

    @pytest.mark.happy_path
    def test_transform_replaces_empty_with_none(self, raw_dimension_df):
        result = DimensionTransformer(raw_dimension_df).transform()
        assert result.iloc[1]["description"] is None

    @pytest.mark.happy_path
    def test_transform_adds_audit_column(self, raw_dimension_df):
        result = DimensionTransformer(raw_dimension_df).transform()
        assert "ingested_at" in result.columns
        assert result["ingested_at"].notna().all()

    @pytest.mark.edge_case
    def test_original_df_unchanged(self, raw_dimension_df):
        original = raw_dimension_df.copy()
        DimensionTransformer(raw_dimension_df).transform()
        pd.testing.assert_frame_equal(raw_dimension_df, original)

    @pytest.mark.edge_case
    def test_handles_nan_values_in_description(self):
        """Test that NaN values are handled (become null in DB)."""
        df = pd.DataFrame({
            "Key": ["P1", "P2"],
            "Title": ["Pop 1", "Pop 2"],
            "Description": ["Desc 1", np.nan],
        })
        result = DimensionTransformer(df).transform()
        # NaN or None both become NULL in PostgreSQL
        assert pd.isna(result.iloc[1]["description"])

    @pytest.mark.edge_case
    def test_handles_whitespace_in_key(self):
        """Test that whitespace is stripped from keys."""
        df = pd.DataFrame({
            "Key": ["  P1  ", "P2"],
            "Title": ["Pop 1", "Pop 2"],
            "Description": ["Desc 1", "Desc 2"],
        })
        result = DimensionTransformer(df).transform()
        assert result.iloc[0]["key"] == "P1"


# ============== FACT TESTS ==============

class TestFactTransformer:

    @pytest.mark.happy_path
    def test_transform_renames_columns(self, raw_fact_df):
        result = FactTransformer(raw_fact_df).transform()
        assert "travel_motive_key" in result.columns
        assert "trips_daily" in result.columns
        assert "TravelMotives" not in result.columns

    @pytest.mark.happy_path
    def test_fact_id_generated(self, raw_fact_df):
        result = FactTransformer(raw_fact_df).transform()
        assert "fact_id" in result.columns
        assert len(result["fact_id"].iloc[0]) == 32  # MD5 hex length

    @pytest.mark.happy_path
    def test_fact_id_matches_manual_hash(self, raw_fact_df):
        result = FactTransformer(raw_fact_df).transform()
        expected = hashlib.md5("TM1|P1|M1|MG1|R1|2023".encode()).hexdigest()
        assert result["fact_id"].iloc[0] == expected

    @pytest.mark.happy_path
    def test_transform_adds_audit_column(self, raw_fact_df):
        result = FactTransformer(raw_fact_df).transform()
        assert "ingested_at" in result.columns
        assert result["ingested_at"].notna().all()

    @pytest.mark.happy_path
    def test_column_order(self, raw_fact_df):
        """Test that columns are in the correct order for database."""
        result = FactTransformer(raw_fact_df).transform()
        expected_order = [
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
        assert list(result.columns) == expected_order

    @pytest.mark.edge_case
    def test_original_df_unchanged(self, raw_fact_df):
        original = raw_fact_df.copy()
        FactTransformer(raw_fact_df).transform()
        pd.testing.assert_frame_equal(raw_fact_df, original)

    @pytest.mark.edge_case
    def test_handles_dot_as_null(self):
        """Test that '.' is converted to null for PostgreSQL."""
        df = pd.DataFrame({
            "TravelMotives": ["TM1"],
            "Population": ["P1"],
            "TravelModes": ["M1"],
            "Margins": ["MG1"],
            "RegionCharacteristics": ["R1"],
            "Periods": ["2023"],
            "Trips_1": ["."],
            "DistanceTravelled_2": [100.0],
            "TimeTravelled_3": [30.0],
            "Trips_4": [3800],
            "DistanceTravelled_5": [36500.0],
            "TimeTravelled_6": [10950.0],
        })
        result = FactTransformer(df).transform()
        # NaN or None both become NULL in PostgreSQL
        assert pd.isna(result["trips_daily"].iloc[0])

    @pytest.mark.edge_case
    def test_handles_nan_as_null(self):
        """Test that NaN is handled (becomes NULL in PostgreSQL)."""
        df = pd.DataFrame({
            "TravelMotives": ["TM1"],
            "Population": ["P1"],
            "TravelModes": ["M1"],
            "Margins": ["MG1"],
            "RegionCharacteristics": ["R1"],
            "Periods": ["2023"],
            "Trips_1": [np.nan],
            "DistanceTravelled_2": [100.0],
            "TimeTravelled_3": [30.0],
            "Trips_4": [3800],
            "DistanceTravelled_5": [36500.0],
            "TimeTravelled_6": [10950.0],
        })
        result = FactTransformer(df).transform()
        assert pd.isna(result["trips_daily"].iloc[0])

    @pytest.mark.edge_case
    def test_different_keys_produce_different_hash(self):
        """Test that different dimension keys produce different fact_ids."""
        df1 = pd.DataFrame({
            "TravelMotives": ["TM1"], "Population": ["P1"], "TravelModes": ["M1"],
            "Margins": ["MG1"], "RegionCharacteristics": ["R1"], "Periods": ["2023"],
            "Trips_1": [1], "DistanceTravelled_2": [1], "TimeTravelled_3": [1],
            "Trips_4": [1], "DistanceTravelled_5": [1], "TimeTravelled_6": [1],
        })
        df2 = df1.copy()
        df2["Periods"] = ["2024"]

        result1 = FactTransformer(df1).transform()
        result2 = FactTransformer(df2).transform()

        assert result1["fact_id"].iloc[0] != result2["fact_id"].iloc[0]

    @pytest.mark.edge_case
    def test_strips_whitespace_from_fk_columns(self):
        """Test that whitespace is stripped from foreign key columns."""
        df = pd.DataFrame({
            "TravelMotives": ["  TM1  "],
            "Population": ["P1"],
            "TravelModes": ["M1"],
            "Margins": ["MG1"],
            "RegionCharacteristics": ["R1"],
            "Periods": ["2023"],
            "Trips_1": [10.0],
            "DistanceTravelled_2": [100.0],
            "TimeTravelled_3": [30.0],
            "Trips_4": [3800],
            "DistanceTravelled_5": [36500.0],
            "TimeTravelled_6": [10950.0],
        })
        result = FactTransformer(df).transform()
        assert result["travel_motive_key"].iloc[0] == "TM1"

    @pytest.mark.edge_case
    def test_metrics_converted_to_numeric(self):
        """Test that string metrics are converted to numeric."""
        df = pd.DataFrame({
            "TravelMotives": ["TM1"],
            "Population": ["P1"],
            "TravelModes": ["M1"],
            "Margins": ["MG1"],
            "RegionCharacteristics": ["R1"],
            "Periods": ["2023"],
            "Trips_1": ["10.5"],  # String
            "DistanceTravelled_2": [100.0],
            "TimeTravelled_3": [30.0],
            "Trips_4": [3800],
            "DistanceTravelled_5": [36500.0],
            "TimeTravelled_6": [10950.0],
        })
        result = FactTransformer(df).transform()
        assert result["trips_daily"].iloc[0] == 10.5
        assert isinstance(result["trips_daily"].iloc[0], (float, np.floating))