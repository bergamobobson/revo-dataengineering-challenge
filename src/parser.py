"""
odin_mob.src.parser

This module contains the MetadataParser class, which is responsible for
parsing a metadata file structured in named sections.
"""

import re
import io
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd


class MetadataParser:
    """
    Parser for metadata files organized into quoted sections.

    Attributes
    ----------
    dataframes : Dict[str, pd.DataFrame]
        Mapping between section names and their corresponding DataFrames.
    """

    def __init__(self, filepath: Path, separator: str = ";", encoding: str = "utf-8") -> None:
        """
        Initialize the MetadataParser.

        Parameters
        ----------
        filepath : Path
            Path to the metadata file.
        separator : str
            CSV separator (default: ";").
        encoding : str
            File encoding (default: "utf-8").
        """
        self._filepath = filepath
        self._separator = separator
        self._encoding = encoding
        self._content: Optional[str] = None
        self.dataframes: Dict[str, pd.DataFrame] = {}

    def _read_content(self) -> None:
        """Read the entire content of the file."""
        self._content = self._filepath.read_text(encoding=self._encoding)

    def _find_sections(self) -> List[str]:
        """Find all section headers in the metadata file."""
        return re.findall(
            r'^\s*\ufeff?"([^"]+)"\s*$',
            self._content,
            re.MULTILINE,
        )

    def parse(self) -> Dict[str, pd.DataFrame]:
        """Parse the metadata file and load each section into a DataFrame."""
        self._read_content()
        sections = self._find_sections()

        for i, marker in enumerate(sections):
            full_marker = f'"{marker}"'
            start = self._content.find(full_marker + "\n")

            if start == -1:
                continue

            end = (
                self._content.find(f'"{sections[i + 1]}"' + "\n")
                if i + 1 < len(sections)
                else len(self._content)
            )

            block_text = self._content[start:end].strip()

            try:
                self.dataframes[marker] = pd.read_csv(
                    io.StringIO(block_text),
                    sep=self._separator,
                    skiprows=1,
                    quotechar='"',
                )
            except Exception as e:
                print(f"Error while parsing section '{marker}': {e}")

        return self.dataframes