"""
test/test_validator.py

Unit tests for the DataValidator class.
"""

import pytest
import pandas as pd
import pandera as pa
from pandera.errors import SchemaError

from src.validator import DataValidator, dimension_schema, fact_schema


pytestmark = [
    pytest.mark.validator,
    pytest.mark.unit,
]


# ============== FIXTURES ==============

@pytest.fixture
def valid_dimensions():
    """Create valid dimension dataframes with CBS original names."""
    now = pd.Timestamp.now(tz="UTC")
    return {
        "Population": pd.DataFrame({
            "key": ["P1", "P2", "P3"],
            "title": ["Pop 1", "Pop 2", "Pop 3"],
            "description": ["Description 1", "Description 2", "Description 3"],
            "ingested_at": [now, now, now],
        }),
        "TravelMotives": pd.DataFrame({
            "key": ["TM1", "TM2"],
            "title": ["Work", "Leisure"],
            "description": ["Work travel", "Leisure travel"],
            "ingested_at": [now, now],
        }),
        "TravelModes": pd.DataFrame({
            "key": ["M1", "M2", "M3"],
            "title": ["Car", "Bike", "Train"],
            "description": ["By car", "By bike", "By train"],
            "ingested_at": [now, now, now],
        }),
        "Margins": pd.DataFrame({
            "key": ["MG1", "MG2"],
            "title": ["Margin 1", "Margin 2"],
            "description": ["Margin desc 1", "Margin desc 2"],
            "ingested_at": [now, now],
        }),
        "RegionCharacteristics": pd.DataFrame({
            "key": ["R1", "R2"],
            "title": ["North", "South"],
            "description": ["Northern region", "Southern region"],
            "ingested_at": [now, now],
        }),
        "Periods": pd.DataFrame({
            "key": ["2023JJ00", "2024JJ00"],
            "title": ["Year 2023", "Year 2024"],
            "description": ["Full year 2023", "Full year 2024"],
            "ingested_at": [now, now],
        }),
    }


@pytest.fixture
def valid_fact():
    """Create a valid fact dataframe with transformed column names."""
    now = pd.Timestamp.now(tz="UTC")
    return pd.DataFrame({
        "fact_id": ["abc123def456abc123def456abc12345", "def456abc123def456abc123def45678"],
        "travel_motive_key": ["TM1", "TM2"],
        "population_key": ["P1", "P2"],
        "travel_mode_key": ["M1", "M2"],
        "margin_key": ["MG1", "MG2"],
        "region_key": ["R1", "R2"],
        "period_key": ["2023JJ00", "2024JJ00"],
        "trips_daily": [10.0, 20.0],
        "distance_daily": [100.5, 200.5],
        "time_daily": [30.0, 60.0],
        "trips_yearly": [100.0, 200.0],
        "distance_yearly": [1000.5, 2000.5],
        "time_yearly": [300.0, 600.0],
        "ingested_at": [now, now],
    })


# ============== DIMENSION SCHEMA TESTS ==============

class TestDimensionSchema:
    """Tests for the dimension_schema validation."""
    
    @pytest.mark.happy_path
    def test_valid_dimension(self):
        """Test schema accepts valid dimension data."""
        now = pd.Timestamp.now(tz="UTC")
        df = pd.DataFrame({
            "key": ["A", "B", "C"],
            "title": ["Title A", "Title B", "Title C"],
            "description": ["Desc A", "Desc B", "Desc C"],
            "ingested_at": [now, now, now],
        })
        
        result = dimension_schema.validate(df)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
    
    @pytest.mark.error_handling
    def test_missing_key_column(self):
        """Test schema rejects missing 'key' column."""
        now = pd.Timestamp.now(tz="UTC")
        df = pd.DataFrame({
            "title": ["Title A"],
            "description": ["Desc A"],
            "ingested_at": [now],
        })
        
        with pytest.raises(SchemaError):
            dimension_schema.validate(df)
    
    @pytest.mark.error_handling
    def test_missing_title_column(self):
        """Test schema rejects missing 'title' column."""
        now = pd.Timestamp.now(tz="UTC")
        df = pd.DataFrame({
            "key": ["A"],
            "description": ["Desc A"],
            "ingested_at": [now],
        })
        
        with pytest.raises(SchemaError):
            dimension_schema.validate(df)
    
    @pytest.mark.error_handling
    def test_duplicate_keys(self):
        """Test schema rejects duplicate keys."""
        now = pd.Timestamp.now(tz="UTC")
        df = pd.DataFrame({
            "key": ["A", "A", "B"],
            "title": ["Title 1", "Title 2", "Title 3"],
            "description": ["Desc 1", "Desc 2", "Desc 3"],
            "ingested_at": [now, now, now],
        })
        
        with pytest.raises(SchemaError):
            dimension_schema.validate(df)
    
    @pytest.mark.error_handling
    def test_null_key(self):
        """Test schema rejects null key values."""
        now = pd.Timestamp.now(tz="UTC")
        df = pd.DataFrame({
            "key": ["A", None, "B"],
            "title": ["Title 1", "Title 2", "Title 3"],
            "description": ["Desc 1", "Desc 2", "Desc 3"],
            "ingested_at": [now, now, now],
        })
        
        with pytest.raises(SchemaError):
            dimension_schema.validate(df)
    
    @pytest.mark.error_handling
    def test_null_title(self):
        """Test schema rejects null title values."""
        now = pd.Timestamp.now(tz="UTC")
        df = pd.DataFrame({
            "key": ["A", "B"],
            "title": ["Title 1", None],
            "description": ["Desc 1", "Desc 2"],
            "ingested_at": [now, now],
        })
        
        with pytest.raises(SchemaError):
            dimension_schema.validate(df)
    
    @pytest.mark.edge_case
    def test_null_description_allowed(self):
        """Test schema allows null description values."""
        now = pd.Timestamp.now(tz="UTC")
        df = pd.DataFrame({
            "key": ["A", "B"],
            "title": ["Title 1", "Title 2"],
            "description": ["Desc 1", None],
            "ingested_at": [now, now],
        })
        
        result = dimension_schema.validate(df)
        
        assert len(result) == 2


# ============== DATAVALIDATOR DIMENSION TESTS ==============

class TestValidateDimensions:
    """Tests for DataValidator.validate_dimensions()."""
    
    @pytest.mark.happy_path
    def test_all_dimensions_valid(self, valid_dimensions):
        """Test validation passes for all valid dimensions."""
        validator = DataValidator(valid_dimensions, pd.DataFrame())
        result = validator.validate_dimensions()
        
        assert len(result) == 6
        assert all(name in result for name in valid_dimensions.keys())
    
    @pytest.mark.error_handling
    def test_one_invalid_dimension_fails_all(self, valid_dimensions):
        """Test validation fails if any dimension is invalid."""
        invalid = valid_dimensions.copy()
        invalid["Population"] = pd.DataFrame({"wrong_column": [1, 2, 3]})
        
        validator = DataValidator(invalid, pd.DataFrame())
        
        with pytest.raises(SchemaError):
            validator.validate_dimensions()
    
    @pytest.mark.edge_case
    def test_empty_dimension(self, valid_dimensions):
        """Test validation handles empty dimension."""
        now = pd.Timestamp.now(tz="UTC")
        valid_dimensions["Population"] = pd.DataFrame({
            "key": pd.Series([], dtype=str),
            "title": pd.Series([], dtype=str),
            "description": pd.Series([], dtype=str),
            "ingested_at": pd.Series([], dtype="datetime64[ns, UTC]"),
        })
        
        validator = DataValidator(valid_dimensions, pd.DataFrame())
        result = validator.validate_dimensions()
        
        assert len(result["Population"]) == 0


# ============== FACT SCHEMA TESTS ==============

class TestFactSchema:
    """Tests for the fact_schema validation."""
    
    @pytest.mark.happy_path
    def test_valid_fact(self, valid_dimensions, valid_fact):
        """Test schema accepts valid fact data."""
        schema = fact_schema(valid_dimensions)
        result = schema.validate(valid_fact)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
    
    @pytest.mark.error_handling
    @pytest.mark.parametrize("column,dim_key", [
        ("population_key", "Population"),
        ("travel_motive_key", "TravelMotives"),
        ("travel_mode_key", "TravelModes"),
        ("margin_key", "Margins"),
        ("region_key", "RegionCharacteristics"),
        ("period_key", "Periods"),
    ])
    def test_invalid_foreign_key(self, valid_dimensions, valid_fact, column, dim_key):
        """Test schema rejects invalid foreign keys."""
        invalid_fact = valid_fact.copy()
        invalid_fact.loc[0, column] = "INVALID_KEY"
        
        schema = fact_schema(valid_dimensions)
        
        with pytest.raises(SchemaError):
            schema.validate(invalid_fact)
    
    @pytest.mark.edge_case
    @pytest.mark.parametrize("column", [
        "trips_daily",
        "distance_daily",
        "time_daily",
        "trips_yearly",
        "distance_yearly",
        "time_yearly",
    ])
    def test_null_metrics_allowed(self, valid_dimensions, valid_fact, column):
        """Test schema allows null values in metric columns."""
        valid_fact.loc[0, column] = None
        
        schema = fact_schema(valid_dimensions)
        result = schema.validate(valid_fact)
        
        assert len(result) == 2


# ============== DATAVALIDATOR FACT TESTS ==============

class TestValidateFact:
    """Tests for DataValidator.validate_fact()."""
    
    @pytest.mark.happy_path
    def test_valid_fact(self, valid_dimensions, valid_fact):
        """Test validate_fact passes for valid data."""
        validator = DataValidator(valid_dimensions, valid_fact)
        result = validator.validate_fact()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
    
    @pytest.mark.error_handling
    def test_invalid_fk_raises_error(self, valid_dimensions, valid_fact):
        """Test validate_fact raises error on invalid FK."""
        invalid_fact = valid_fact.copy()
        invalid_fact.loc[0, "population_key"] = "NON_EXISTENT"
        
        validator = DataValidator(valid_dimensions, invalid_fact)
        
        with pytest.raises(SchemaError):
            validator.validate_fact()


# ============== VALIDATE_ALL TESTS ==============

class TestValidateAll:
    """Tests for DataValidator.validate_all()."""
    
    @pytest.mark.happy_path
    def test_validate_all_success(self, valid_dimensions, valid_fact):
        """Test validate_all passes when everything is valid."""
        validator = DataValidator(valid_dimensions, valid_fact)
        result = validator.validate_all()
        
        assert result is True
    
    @pytest.mark.error_handling
    def test_validate_all_fails_on_invalid_dimension(self, valid_dimensions, valid_fact):
        """Test validate_all fails if dimension is invalid."""
        invalid = valid_dimensions.copy()
        invalid["Margins"] = pd.DataFrame({"bad": [1]})
        
        validator = DataValidator(invalid, valid_fact)
        
        with pytest.raises(SchemaError):
            validator.validate_all()
    
    @pytest.mark.error_handling
    def test_validate_all_fails_on_invalid_fact(self, valid_dimensions, valid_fact):
        """Test validate_all fails if fact is invalid."""
        invalid_fact = valid_fact.copy()
        invalid_fact.loc[0, "period_key"] = "INVALID"
        
        validator = DataValidator(valid_dimensions, invalid_fact)
        
        with pytest.raises(SchemaError):
            validator.validate_all()