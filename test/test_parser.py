import pytest
import pandas as pd
from src.parser import MetadataParser
import textwrap
from pathlib import Path


pytestmark = [pytest.mark.parsing, pytest.mark.reader]


@pytest.fixture
def temp_csv(tmp_path):
    """Create a temporary CSV file."""
    def _create(content: str, filename: str = "temp.csv") -> Path:
        path = tmp_path / filename
        path.write_text(content)
        return path
    return _create


@pytest.fixture
def sample_metadata_content():
    """Sample metadata content with 3 sections."""
    return textwrap.dedent("""\
        "Header 1"
        key;title;desc
        1;"Header 1";d1
        2;t2;d2
        3;t3;d3

        "Header 2"
        key;title;desc
        4;t4;d4
        5;t5;d5
        6;t6;d6

        "Header 3"
        key;title;desc
        7;t7;d7
        8;t5;d8
        9;t6;d9
    """)


@pytest.mark.unit
class TestMetadataParser:
    
    @pytest.mark.happy_path
    def test_parse_returns_dict(self, temp_csv, sample_metadata_content):
        """Test parser returns a dictionary."""
        path = temp_csv(sample_metadata_content)
        
        parser = MetadataParser(filepath=path)
        result = parser.parse()
        
        assert isinstance(result, dict)
    
    @pytest.mark.happy_path
    def test_parse_finds_all_sections(self, temp_csv, sample_metadata_content):
        """Test parser finds all section headers."""
        path = temp_csv(sample_metadata_content)
        
        parser = MetadataParser(filepath=path)
        result = parser.parse()
        
        assert set(result.keys()) == {"Header 1", "Header 2", "Header 3"}
    
    @pytest.mark.happy_path
    def test_parse_returns_dataframes(self, temp_csv, sample_metadata_content):
        """Test each section is a DataFrame."""
        path = temp_csv(sample_metadata_content)
        
        parser = MetadataParser(filepath=path)
        result = parser.parse()
        
        for name, df in result.items():
            assert isinstance(df, pd.DataFrame), f"{name} is not a DataFrame"
    
    @pytest.mark.happy_path
    def test_dataframe_columns(self, temp_csv, sample_metadata_content):
        """Test DataFrame has correct columns."""
        path = temp_csv(sample_metadata_content)
        
        parser = MetadataParser(filepath=path)
        result = parser.parse()
        
        df1 = result["Header 1"]
        assert list(df1.columns) == ["key", "title", "desc"]
    
    @pytest.mark.happy_path
    def test_dataframe_row_count(self, temp_csv, sample_metadata_content):
        """Test DataFrame has correct number of rows."""
        path = temp_csv(sample_metadata_content)
        
        parser = MetadataParser(filepath=path)
        result = parser.parse()
        
        assert len(result["Header 1"]) == 3
        assert len(result["Header 2"]) == 3
        assert len(result["Header 3"]) == 3
    
    @pytest.mark.happy_path
    def test_dataframe_content(self, temp_csv, sample_metadata_content):
        """Test DataFrame values are correctly parsed."""
        path = temp_csv(sample_metadata_content)
        
        parser = MetadataParser(filepath=path)
        result = parser.parse()
        
        df1 = result["Header 1"]
        assert df1.iloc[0]["key"] == 1
        assert df1.iloc[0]["title"] == "Header 1"
        assert df1.iloc[0]["desc"] == "d1"
    
    @pytest.mark.edge_case
    def test_custom_separator(self, temp_csv):
        """Test parser with comma separator."""
        content = textwrap.dedent("""\
            "Section"
            key,title,desc
            1,foo,bar
        """)
        path = temp_csv(content)
        
        parser = MetadataParser(filepath=path, separator=",")
        result = parser.parse()
        
        assert result["Section"].iloc[0]["title"] == "foo"
    
    @pytest.mark.edge_case
    def test_single_section(self, temp_csv):
        """Test parser with only one section."""
        content = textwrap.dedent("""\
            "OnlySection"
            key;title;desc
            1;a;b
        """)
        path = temp_csv(content)
        
        parser = MetadataParser(filepath=path)
        result = parser.parse()
        
        assert set(result.keys()) == {"OnlySection"}
        assert len(result["OnlySection"]) == 1
    
    @pytest.mark.error_handling
    def test_file_not_found(self):
        """Test error when file doesn't exist."""
        parser = MetadataParser(filepath=Path("/nonexistent/file.csv"))
        
        with pytest.raises(FileNotFoundError):
            parser.parse()
    
    @pytest.mark.edge_case
    def test_empty_section(self, temp_csv):
        """Test parser handles section with no data rows."""
        content = textwrap.dedent("""\
            "EmptySection"
            key;title;desc
        """)
        path = temp_csv(content)
        
        parser = MetadataParser(filepath=path)
        result = parser.parse()
        
        assert "EmptySection" in result
        assert len(result["EmptySection"]) == 0