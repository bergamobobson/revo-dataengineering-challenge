"""
odin_mob.src.transformer

This module contains transformer classes for dimensions and facts.
"""

import hashlib
import pandas as pd


class DimensionTransformer:
    """Transform dimension tables."""
    
    COLUMN_MAPPING = {
        "Key": "key",
        "Title": "title", 
        "Description": "description",
    }
    
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
    
    def rename_columns(self) -> "DimensionTransformer":
        """Standardize column names to lowercase."""
        self.df = self.df.rename(columns=self.COLUMN_MAPPING)
        return self
    
    def clean_strings(self) -> "DimensionTransformer":
        """Trim whitespace and normalize strings."""
        for col in ["key", "title", "description"]:
            if col in self.df.columns:
                # Convert to string first, then strip
                self.df[col] = self.df[col].astype(str).str.strip()
                # Replace 'nan' string with None
                self.df[col] = self.df[col].replace({"nan": None, "None": None})
        return self
    
    def handle_nulls(self) -> "DimensionTransformer":
        """Replace empty strings with None."""
        self.df = self.df.replace({"": None, " ": None})
        return self
    
    def add_audit_columns(self) -> "DimensionTransformer":
        """Add ingestion timestamp."""
        self.df["ingested_at"] = pd.Timestamp.now(tz="UTC")
        return self
    
    def transform(self) -> pd.DataFrame:
        """Run full pipeline."""
        return (
            self.rename_columns()
                .clean_strings()
                .handle_nulls()
                .add_audit_columns()
                .df
        )


class FactTransformer:
    """Transform fact table."""
    
    COLUMN_MAPPING = {
        "TravelMotives": "travel_motive_key",
        "Population": "population_key",
        "TravelModes": "travel_mode_key",
        "Margins": "margin_key",
        "RegionCharacteristics": "region_key",
        "Periods": "period_key",
        "Trips_1": "trips_daily",
        "DistanceTravelled_2": "distance_daily",
        "TimeTravelled_3": "time_daily",
        "Trips_4": "trips_yearly",
        "DistanceTravelled_5": "distance_yearly",
        "TimeTravelled_6": "time_yearly",
    }
    
    FK_COLUMNS = [
        "travel_motive_key",
        "population_key", 
        "travel_mode_key",
        "margin_key",
        "region_key",
        "period_key",
    ]
    
    METRIC_COLUMNS = [
        "trips_daily",
        "distance_daily",
        "time_daily",
        "trips_yearly",
        "distance_yearly",
        "time_yearly",
    ]

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
    
    def rename_columns(self) -> "FactTransformer":
        """Map source columns to target names."""
        self.df = self.df.rename(columns=self.COLUMN_MAPPING)
        return self
    
    def drop_unused_columns(self) -> "FactTransformer":
        """Remove columns not in data model."""
        keep = ["fact_id"] + self.FK_COLUMNS + self.METRIC_COLUMNS
        self.df = self.df[[c for c in keep if c in self.df.columns]]
        return self
    
    def compute_fact_id(self) -> "FactTransformer":
        """Generate MD5 hash from dimension keys."""
        self.df["fact_id"] = self.df.apply(self._hash_row, axis=1)
        return self
    
    def _hash_row(self, row: pd.Series) -> str:
        key_string = "|".join(str(row[col]) for col in self.FK_COLUMNS)
        return hashlib.md5(key_string.encode()).hexdigest()
    
    def cast_types(self) -> "FactTransformer":
        """Cast columns to correct types."""
        for col in self.FK_COLUMNS:
            self.df[col] = self.df[col].astype(str).str.strip()
        
        for col in self.METRIC_COLUMNS:
            self.df[col] = pd.to_numeric(self.df[col], errors="coerce")
        
        return self
    
    def handle_nulls(self) -> "FactTransformer":
        """Handle missing values in metrics - convert NaN to None for PostgreSQL."""
        # Replace "." string with NaN first
        self.df[self.METRIC_COLUMNS] = self.df[self.METRIC_COLUMNS].replace({".": float("nan")})
        
        # Convert NaN to None (which becomes NULL in PostgreSQL)
        self.df = self.df.where(pd.notnull(self.df), None)
        
        return self
    
    def add_audit_columns(self) -> "FactTransformer":
        """Add ingestion timestamp."""
        self.df["ingested_at"] = pd.Timestamp.now(tz="UTC")
        return self
    
    def reorder_columns(self) -> "FactTransformer":
        """Match database column order."""
        column_order = (
            ["fact_id"] 
            + self.FK_COLUMNS 
            + self.METRIC_COLUMNS 
            + ["ingested_at"]
        )
        self.df = self.df[[c for c in column_order if c in self.df.columns]]
        return self
    
    def transform(self) -> pd.DataFrame:
        """Run full transformation pipeline."""
        return (
            self.rename_columns()
                .cast_types()
                .handle_nulls()
                .compute_fact_id()
                .add_audit_columns()
                .reorder_columns()
                .df
        )