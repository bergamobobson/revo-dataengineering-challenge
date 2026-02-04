"""
test/test_datasource.py

Unit tests for the DataSource class.
"""

import pytest
import pandas as pd
from pathlib import Path
import tempfile
import textwrap

from src.datasource import DataSource
from config.source import SourceConfig

pytestmark = [
    pytest.mark.reader,
    pytest.mark.unit,
]
# ============== FIXTURES ==============

@pytest.fixture
def temp_csv():
    """
    Factory fixture to create temporary CSV files.
    
    Returns a function that creates a temp file with given content.
    """
    created_files = []
    
    def _create(content: str) -> Path:
        f = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        f.write(content)
        f.close()
        created_files.append(Path(f.name))
        return Path(f.name)
    
    yield _create
    
    # Cleanup after test
    for file in created_files:
        if file.exists():
            file.unlink()


@pytest.fixture
def sample_config(temp_csv):
    """
    Create a SourceConfig with temporary test files.
    
    Data content:
        id;name;value
        1;foo;100
        2;bar;200
        3;baz;300
    """
    data_content = textwrap.dedent("""\
        id;name;value
        1;foo;100
        2;bar;200
        3;baz;300
    """)
    
    metadata_content = textwrap.dedent("""\
        "Section1"
        key;title;desc
        a;A;desc_a
    """)
    
    data_path = temp_csv(data_content)
    metadata_path = temp_csv(metadata_content)
    
    return SourceConfig(
        DATA_PATH=data_path,
        METADATA_PATH=metadata_path,
        SEPARATOR=";"
    )


# ============== TESTS ==============

class TestDataSource:
    """Tests for DataSource class."""
    
    def test_read_csv(self, sample_config):
        """
        Test basic CSV reading.
        
        Given: A valid config with a CSV file
        When: read() is called
        Then: Returns a DataFrame with correct structure
        """
        ds = DataSource(sample_config)
        df = ds.read()
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert list(df.columns) == ["id", "name", "value"]
    
    def test_read_with_semicolon_separator(self, sample_config):
        """
        Test reading with semicolon separator (default).
        
        Given: A CSV with semicolon separator
        When: read() is called with SEPARATOR=";"
        Then: Data is correctly parsed
        """
        ds = DataSource(sample_config)
        df = ds.read()
        
        assert df.iloc[0]["name"] == "foo"
        assert df.iloc[1]["name"] == "bar"
    
    def test_read_with_comma_separator(self, temp_csv):
        """
        Test reading with comma separator.
        
        Given: A CSV with comma separator
        When: read() is called with SEPARATOR=","
        Then: Data is correctly parsed
        """
        content = textwrap.dedent("""\
            id,name,value
            1,foo,100
            2,bar,200
        """)
        data_path = temp_csv(content)
        
        config = SourceConfig(
            DATA_PATH=data_path,
            METADATA_PATH=data_path,
            SEPARATOR=","
        )
        
        ds = DataSource(config)
        df = ds.read()
        
        assert len(df) == 2
        assert df.iloc[0]["name"] == "foo"
        assert df.iloc[1]["value"] == 200
    
    def test_read_file_not_found(self):
        """
        Test error handling when file doesn't exist.
        
        Given: A config pointing to non-existent file
        When: read() is called
        Then: Raises FileNotFoundError
        """
        config = SourceConfig(
            DATA_PATH=Path("/nonexistent/file.csv"),
            METADATA_PATH=Path("/nonexistent/meta.csv"),
            SEPARATOR=";"
        )
        
        ds = DataSource(config)
        
        with pytest.raises(FileNotFoundError):
            ds.read()
    
    def test_read_values_integers(self, sample_config):
        """
        Test that integer values are correctly parsed.
        
        Given: A CSV with integer columns
        When: read() is called
        Then: Integer values are correctly typed
        """
        ds = DataSource(sample_config)
        df = ds.read()
        
        assert df.iloc[0]["id"] == 1
        assert df.iloc[1]["id"] == 2
        assert df.iloc[2]["value"] == 300
    
    def test_read_values_strings(self, sample_config):
        """
        Test that string values are correctly parsed.
        
        Given: A CSV with string columns
        When: read() is called
        Then: String values are preserved
        """
        ds = DataSource(sample_config)
        df = ds.read()
        
        assert df.iloc[0]["name"] == "foo"
        assert df.iloc[1]["name"] == "bar"
        assert df.iloc[2]["name"] == "baz"
    
    def test_read_empty_file(self, temp_csv):
        """
        Test reading an empty CSV (headers only).
        
        Given: A CSV with only headers
        When: read() is called
        Then: Returns empty DataFrame with correct columns
        """
        content = "id;name;value\n"
        data_path = temp_csv(content)
        
        config = SourceConfig(
            DATA_PATH=data_path,
            METADATA_PATH=data_path,
            SEPARATOR=";"
        )
        
        ds = DataSource(config)
        df = ds.read()
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
        assert list(df.columns) == ["id", "name", "value"]
    
    def test_read_with_special_characters(self, temp_csv):
        """
        Test reading CSV with special characters in values.
        
        Given: A CSV with quotes and special chars
        When: read() is called
        Then: Values are correctly parsed
        """
        content = textwrap.dedent("""\
            id;name;desc
            1;"foo bar";"description with; semicolon"
            2;"baz";"normal"
        """)
        data_path = temp_csv(content)
        
        config = SourceConfig(
            DATA_PATH=data_path,
            METADATA_PATH=data_path,
            SEPARATOR=";"
        )
        
        ds = DataSource(config)
        df = ds.read()
        
        assert df.iloc[0]["name"] == "foo bar"
        assert "semicolon" in df.iloc[0]["desc"]
    
    def test_read_returns_new_dataframe_each_time(self, sample_config):
        """
        Test that each read() returns a fresh DataFrame.
        
        Given: A DataSource instance
        When: read() is called multiple times
        Then: Each call returns independent DataFrames
        """
        ds = DataSource(sample_config)
        
        df1 = ds.read()
        df2 = ds.read()
        
        # Modify df1
        df1.loc[0, "name"] = "modified"
        
        # df2 should be unchanged
        assert df2.iloc[0]["name"] == "foo"