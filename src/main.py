"""
odin_mob.src.main

Entry point for the ETL pipeline.
"""

import logging
import sys

from config.source import SourceConfig, DatabaseConfig
from src.etl import ETLPipeline


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


def main():
    """Entry point."""
    source_config = SourceConfig.from_env()
    db_config = DatabaseConfig.from_env()
    
    pipeline = ETLPipeline(source_config, db_config)
    pipeline.run()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        sys.exit(1)