"""
odin_mob.src.etl

ETL Pipeline class for Netherlands mobility data.
"""

import logging
from dataclasses import dataclass
from typing import Dict

import pandas as pd

from config.source import SourceConfig, DatabaseConfig
from src.parser import MetadataParser
from src.datasource import DataSource
from src.transformer import DimensionTransformer, FactTransformer
from src.validator import DataValidator
from src.loader import DimensionsLoader, FactLoader


@dataclass
class ETLResult:
    """Results of an ETL run."""
    dimensions_loaded: Dict[str, int]
    facts_loaded: int
    
    @property
    def total_dimensions(self) -> int:
        return sum(self.dimensions_loaded.values())


class ETLPipeline:
    """ETL Pipeline for Netherlands mobility data."""
    
    DIMENSION_NAMES = {
        "TravelMotives",
        "Population",
        "TravelModes",
        "Margins",
        "RegionCharacteristics",
        "Periods",
    }
    
    def __init__(
        self, 
        source_config: SourceConfig, 
        db_config: DatabaseConfig
    ):
        self.source_config = source_config
        self.db_config = db_config
        self.logger = logging.getLogger(__name__)
        
        # Data containers
        self.raw_dimensions: Dict[str, pd.DataFrame] = {}
        self.raw_facts: pd.DataFrame = None
        self.transformed_dimensions: Dict[str, pd.DataFrame] = {}
        self.transformed_facts: pd.DataFrame = None
    
    def extract(self) -> "ETLPipeline":
        """Extract data from sources."""
        self.logger.info("[EXTRACT] Parsing metadata for dimensions...")
        parser = MetadataParser(self.source_config.METADATA_PATH)
        self.raw_dimensions = parser.parse()
        self.logger.info(f"[EXTRACT] Found {len(self.raw_dimensions)} sections")
        
        self.logger.info("[EXTRACT] Reading main dataset...")
        datasource = DataSource(self.source_config)
        self.raw_facts = datasource.read()
        self.logger.info(f"[EXTRACT] Loaded {len(self.raw_facts)} fact rows")
        
        return self
    
    def transform(self) -> "ETLPipeline":
        """Transform extracted data."""
        self.logger.info("[TRANSFORM] Transforming dimensions...")
        for name, df in self.raw_dimensions.items():
            if name not in self.DIMENSION_NAMES:
                continue
            transformer = DimensionTransformer(df)
            self.transformed_dimensions[name] = transformer.transform()
            self.logger.info(f"  -> {name}: {len(df)} rows")
        
        self.logger.info("[TRANSFORM] Transforming facts...")
        transformer = FactTransformer(self.raw_facts)
        self.transformed_facts = transformer.transform()
        self.logger.info(f"  -> facts: {len(self.transformed_facts)} rows")
        
        return self
    
    def validate(self) -> "ETLPipeline":
        """Validate transformed data."""
        self.logger.info("[VALIDATE] Running validations...")
        validator = DataValidator(
            self.transformed_dimensions, 
            self.transformed_facts
        )
        validator.validate_all()
        self.logger.info("[VALIDATE] All validations passed")
        
        return self
    
    def load(self) -> ETLResult:
        """Load data into database."""
        self.logger.info("[LOAD] Loading dimensions...")
        dim_loader = DimensionsLoader(self.db_config)
        dim_results = dim_loader.load(self.transformed_dimensions)
        for table, count in dim_results.items():
            self.logger.info(f"  -> {table}: {count} rows")
        
        self.logger.info("[LOAD] Loading facts...")
        fact_loader = FactLoader(self.db_config)
        fact_count = fact_loader.load(self.transformed_facts)
        self.logger.info(f"  -> fact_mobility: {fact_count} rows")
        
        return ETLResult(
            dimensions_loaded=dim_results,
            facts_loaded=fact_count
        )
    
    def run(self) -> ETLResult:
        """Run the complete ETL pipeline."""
        self.logger.info("=" * 60)
        self.logger.info("ODIN Mobility ETL Pipeline - Starting")
        self.logger.info("=" * 60)
        
        result = (
            self.extract()
                .transform()
                .validate()
                .load()
        )
        
        self.logger.info("=" * 60)
        self.logger.info("ETL Pipeline - Completed")
        self.logger.info(f"  Dimensions: {result.total_dimensions} rows")
        self.logger.info(f"  Facts:      {result.facts_loaded} rows")
        self.logger.info("=" * 60)
        
        return result