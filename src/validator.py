import pandera as pa
from pandera import Column, Check, DataFrameSchema
import pandas as pd
from typing import Dict


# Schema for dimensions (tous ont la même structure)
dimension_schema = DataFrameSchema(
    {
        "key": Column(str, unique=True, nullable=False),
        "title": Column(str, nullable=False),
        "description": Column(str, nullable=True),  
        "ingested_at": Column(nullable=False),
    },
    strict=False,  # Allow extra columns
)


# Schema for the fact table
def fact_schema(dimensions: Dict[str, pd.DataFrame]) -> DataFrameSchema:
    """Create fact schema with FK validation against dimensions."""
    
    return DataFrameSchema(
        {
            # Primary key
            "fact_id": Column(str, nullable=False),
            
            # Foreign keys
            "travel_motive_key": Column(str, Check.isin(dimensions["TravelMotives"]["key"])),
            "population_key": Column(str, Check.isin(dimensions["Population"]["key"])),
            "travel_mode_key": Column(str, Check.isin(dimensions["TravelModes"]["key"])),
            "margin_key": Column(str, Check.isin(dimensions["Margins"]["key"])),
            "region_key": Column(str, Check.isin(dimensions["RegionCharacteristics"]["key"])),
            "period_key": Column(str, Check.isin(dimensions["Periods"]["key"])),
            
            # Metrics (daily)
            "trips_daily": Column(float, nullable=True),
            "distance_daily": Column(float, nullable=True),
            "time_daily": Column(float, nullable=True),
            
            # Metrics (yearly)
            "trips_yearly": Column(float, nullable=True),
            "distance_yearly": Column(float, nullable=True),
            "time_yearly": Column(float, nullable=True),
            
            # Audit
            "ingested_at": Column(nullable=False),
        },
        strict=False,
    )


class DataValidator:
    """Validate dimension and fact dataframes."""
    
    def __init__(self, dimensions: Dict[str, pd.DataFrame], fact: pd.DataFrame):
        self.dimensions = dimensions
        self.fact = fact
    
    def validate_dimensions(self) -> Dict[str, pd.DataFrame]:
        """Validate all dimension tables."""
        validated = {}
        for name, df in self.dimensions.items():
            try:
                validated[name] = dimension_schema.validate(df)
                print(f"{name} valid")
            except pa.errors.SchemaError as e:
                print(f"{name} invalid: {e}")
                raise
        return validated
    
    def validate_fact(self) -> pd.DataFrame:
        """Validate fact table with FK checks."""
        schema = fact_schema(self.dimensions)
        try:
            validated = schema.validate(self.fact)
            print("fact_mobility valid")
            return validated
        except pa.errors.SchemaError as e:
            print(f"✗ fact_mobility invalid: {e}")
            raise
    
    def validate_all(self) -> bool:
        """Run all validations."""
        self.validate_dimensions()
        self.validate_fact()
        return True