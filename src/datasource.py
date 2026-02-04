from pathlib import Path
import pandas as pd

from config.source import SourceConfig


class DataSource:
    """Read data based on SourceConfig."""
    
    def __init__(self, config: SourceConfig):
        self.config = config
    
    def read(self) -> pd.DataFrame:
        return pd.read_csv(self.config.DATA_PATH, sep=self.config.SEPARATOR)