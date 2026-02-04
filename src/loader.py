"""
odin_mob.src.loader

This module contains loader classes for dimensions and facts.
Uses UPSERT (INSERT ... ON CONFLICT) for idempotency.
"""

from typing import Dict, List
import pandas as pd
from sqlalchemy import create_engine, Engine, text

from config.source import DatabaseConfig


class BaseLoader:
    """Base class for database loaders."""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self._engine: Engine = None
    
    @property
    def engine(self) -> Engine:
        if self._engine is None:
            self._engine = create_engine(self.config.connection_string)
        return self._engine
    
    def _build_upsert_query(
        self, 
        table_name: str, 
        columns: List[str], 
        conflict_column: str,
        update_columns: List[str] = None
    ) -> str:
        """Build PostgreSQL UPSERT query."""
        schema_prefix = f"{self.config.SCHEMA}." if self.config.SCHEMA else ""
        
        if update_columns is None:
            update_columns = [c for c in columns if c != conflict_column]
        
        cols_str = ", ".join(columns)
        placeholders = ", ".join(f":{c}" for c in columns)
        updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_columns)
        
        return f"""
            INSERT INTO {schema_prefix}{table_name} ({cols_str})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_column}) DO UPDATE SET {updates}
        """


class DimensionsLoader(BaseLoader):
    """Load dimension tables into PostgreSQL with UPSERT."""
    
    # Keys match parser output (original CBS names)
    TABLE_MAPPING = {
        "Population": "dim_population",
        "TravelMotives": "dim_travel_motives",
        "TravelModes": "dim_travel_modes",
        "Margins": "dim_margins",
        "RegionCharacteristics": "dim_regions",
        "Periods": "dim_periods",
    }
    
    DIMENSION_COLUMNS = ["key", "title", "description", "ingested_at"]
    
    def load(self, dimensions: Dict[str, pd.DataFrame]) -> Dict[str, int]:
        """Load all dimensions with UPSERT."""
        results = {}
        for source_name, df in dimensions.items():
            table_name = self.TABLE_MAPPING.get(source_name)
            if not table_name:
                continue
            rows = self._upsert_table(df, table_name)
            results[table_name] = rows
        return results
    
    def _upsert_table(self, df: pd.DataFrame, table_name: str) -> int:
        """UPSERT a single dimension table."""
        query = self._build_upsert_query(
            table_name=table_name,
            columns=self.DIMENSION_COLUMNS,
            conflict_column="key",
            update_columns=["title", "description", "ingested_at"]
        )
        
        records = df.to_dict(orient="records")
        
        with self.engine.begin() as conn:
            conn.execute(text(query), records)
        
        return len(records)


class FactLoader(BaseLoader):
    """Load fact table into PostgreSQL with UPSERT."""
    
    TABLE_NAME = "fact_mobility"
    
    FACT_COLUMNS = [
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
    
    METRIC_COLUMNS = [
        "trips_daily",
        "distance_daily",
        "time_daily",
        "trips_yearly",
        "distance_yearly",
        "time_yearly",
        "ingested_at",
    ]
    
    def load(self, df: pd.DataFrame) -> int:
        """UPSERT fact table."""
        query = self._build_upsert_query(
            table_name=self.TABLE_NAME,
            columns=self.FACT_COLUMNS,
            conflict_column="fact_id",
            update_columns=self.METRIC_COLUMNS
        )
        
        records = df.to_dict(orient="records")
        
        with self.engine.begin() as conn:
            conn.execute(text(query), records)
        
        return len(records)