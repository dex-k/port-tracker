"""
Monthly data scraper for Newcastle Port statistics.

This module provides functionality to scrape monthly port data from the NSW Transport
Open Data portal. The data includes vessel visits, cargo imports/exports, and container
statistics for the Port of Newcastle.

Key Components:
- ExcelConfig: Configuration for Excel file structure
- TODExcelWorkbook: Adapter class that parses Excel files into clean DataFrames
- resolve_excel_url(): Discovers the current download URL
- download_excel(): Downloads Excel file from URL
- save_json(): Exports DataFrames to JSON format
- run_monthly_scraper(): Main orchestration function

Usage:
    from scraper.endpoints.monthly import run_monthly_scraper
    run_monthly_scraper("./output/")

Or via CLI:
    port-scraper monthly --output ./output/
"""

import pandas as pd
from bs4 import BeautifulSoup
from io import BytesIO
import numpy as np
import json
import os
import logging
from typing import Dict, Any

from ..http_client import spoof_get
from ..config import MONTHLY_DATA_PORTAL_URL, MONTHLY_DATA_EXCEL_URL

logger = logging.getLogger(__name__)


class ExcelConfig:
    """Configuration for Excel file structure and parsing rules.

    This class centralizes all assumptions about the Excel file format,
    making it easy to adapt if the source data structure changes.

    Attributes:
        HEADER_ROW (int): Row index containing section headers (0-indexed)
        DATA_START_ROW (int): Row index where actual data begins (0-indexed)
        MAIN_SHEET (str): Name of the main data sheet
        DATE_COLUMN (str): Name of the date column in the source data
        SHEET_NAMES (list): List of sheet names to load from Excel file

    Example:
        If the Excel structure changes, modify these constants:
        >>> config = ExcelConfig()
        >>> config.HEADER_ROW = 3  # Headers moved to row 4
    """

    HEADER_ROW = 2
    DATA_START_ROW = 3
    MAIN_SHEET = "Port of Newcastle"
    DATE_COLUMN = "Month"
    SHEET_NAMES = ["Readme", "Notes&Methods", "Port of Newcastle"]


def resolve_excel_url() -> str:
    """Resolve the current dynamic URL for the Excel file.

    The NSW Transport portal uses dynamic URLs that change periodically.
    This function discovers the current URL by parsing the portal page
    and following links to the actual download.

    Returns:
        str: The resolved download URL for the Excel file

    Raises:
        requests.RequestException: If network requests fail
        ValueError: If the expected page structure is not found

    Example:
        >>> url = resolve_excel_url()
        >>> print(url)
        https://opendata.transport.nsw.gov.au/.../port-of-newcastle.xlsx
    """
    logger.info("Resolving Excel file URL from NSW Transport website")

    try:
        response = spoof_get(MONTHLY_DATA_PORTAL_URL)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        # Search for the "opendata.transport.nsw.gov.au" link in the HTML content
        # 'tod' is "Transport Open Data"
        tod_link = soup.find(
            "a",
            href=lambda href: href and "opendata.transport.nsw.gov.au" in href,
        )
        if not tod_link:
            raise ValueError("Could not find Transport Open Data link")

        tod_url = tod_link["href"]

        response = spoof_get(tod_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        dynamic_link = soup.find("a", class_="resource-url-analytics")
        if not dynamic_link:
            raise ValueError("Could not find dynamic download link")

        dynamic_url = dynamic_link["href"]
        logger.debug(f"Resolved dynamic URL: {dynamic_url}")

        # Check if the dynamic URL differs from our static fallback
        if dynamic_url != MONTHLY_DATA_EXCEL_URL:
            logger.info(
                "NOTE: Dynamic URL differs from static fallback, using dynamic URL"
            )

        return str(dynamic_url)

    except Exception as e:
        logger.warning(f"Failed to resolve dynamic URL: {e}, using static fallback")
        return MONTHLY_DATA_EXCEL_URL


def download_excel(url: str) -> bytes:
    """Download Excel file from the given URL.

    Args:
        url: The URL to download the Excel file from

    Returns:
        bytes: The raw Excel file content

    Raises:
        requests.RequestException: If the download fails

    Example:
        >>> url = "https://example.com/data.xlsx"
        >>> excel_data = download_excel(url)
        >>> len(excel_data)
        56370
    """
    logger.info(f"Downloading Excel file from: {url}")
    response = spoof_get(url)
    response.raise_for_status()

    logger.info(f"Downloaded {len(response.content)} bytes")
    return response.content


class TODExcelWorkbook:
    """Transport Open Data Excel Workbook parser.

    This class acts as an adapter that converts the complex Excel file structure
    from NSW Transport into clean, normalized pandas DataFrames. It isolates
    the rest of the application from changes in the Excel format.

    The Excel file contains multiple data sections (imports, exports, containers,
    vessel visits) arranged in columns. This class automatically detects these
    sections and parses each into a separate DataFrame.

    Attributes:
        config (ExcelConfig): Configuration for Excel structure
        spreadsheet (dict): Raw Excel data loaded by pandas
        subsheets (dict): Parsed data sections as DataFrames

    Example:
        >>> with open("port-data.xlsx", "rb") as f:
        ...     excel_data = f.read()
        >>> workbook = TODExcelWorkbook(excel_data)
        >>> workbook.subsheets.keys()
        dict_keys(['Imports', 'Exports', 'Containers', 'Vessel Visits'])
        >>> imports_df = workbook.subsheets['Imports']
        >>> imports_df.columns
        Index(['Date', 'Coal (tonnes)', 'Grain (tonnes)', ...], dtype='object')
    """

    def __init__(self, bytes_data):
        logger.info("Parsing Excel workbook")
        self.config = ExcelConfig()

        try:
            # Load the Excel workbook
            self.spreadsheet = pd.read_excel(
                BytesIO(bytes_data), sheet_name=self.config.SHEET_NAMES, header=None
            )

            # Validate the Excel structure
            self._validate_excel_structure()

            # Parse all data sections
            self.subsheets = self._parse_all_sections()
            logger.info(f"Parsed {len(self.subsheets)} data sections")

        except Exception as e:
            logger.error(f"Failed to parse Excel workbook: {e}")
            raise ValueError(f"Invalid Excel file structure: {e}") from e

    def _validate_excel_structure(self) -> None:
        """Validate that the Excel file has the expected structure."""
        if self.config.MAIN_SHEET not in self.spreadsheet:
            raise ValueError(f"Missing required sheet: {self.config.MAIN_SHEET}")

        main_sheet = self.spreadsheet[self.config.MAIN_SHEET]

        # Check if the sheet has enough rows
        if main_sheet.shape[0] <= self.config.DATA_START_ROW:
            raise ValueError(
                f"Sheet has insufficient rows, expected at least {self.config.DATA_START_ROW + 1}"
            )

        # Check if header row exists and has data
        if main_sheet.shape[0] <= self.config.HEADER_ROW:
            raise ValueError(f"Missing header row at position {self.config.HEADER_ROW}")

        headers = main_sheet.iloc[self.config.HEADER_ROW].dropna()
        if len(headers) == 0:
            raise ValueError(
                f"No section headers found in row {self.config.HEADER_ROW}"
            )

        logger.debug(f"Excel structure validated: {len(headers)} sections found")

    def _parse_all_sections(self) -> Dict[str, pd.DataFrame]:
        """Parse all data sections from the Excel file."""
        try:
            section_ranges = self._find_section_ranges()
            sections = {}

            for name, col_range in section_ranges.items():
                logger.debug(
                    f"Parsing section: {name}, columns {col_range.start}-{col_range.stop}"
                )
                try:
                    sections[name] = self._extract_section_data(name, col_range)
                except Exception as e:
                    logger.warning(f"Failed to parse section '{name}': {e}")
                    # Continue with other sections rather than failing completely
                    continue

            if not sections:
                raise ValueError("No sections could be parsed successfully")

            return sections
        except Exception as e:
            logger.error(f"Failed to parse any data sections: {e}")
            raise

    def _find_section_ranges(self) -> Dict[str, range]:
        """Find column ranges for each data section in the Excel file."""
        main_sheet = self.spreadsheet[self.config.MAIN_SHEET]
        headers = main_sheet.iloc[self.config.HEADER_ROW].dropna()

        sections = {}
        header_positions = list(headers.index)

        for i, name in enumerate(headers.values):
            start_col = header_positions[i]
            end_col = (
                header_positions[i + 1]
                if i + 1 < len(header_positions)
                else main_sheet.shape[1]
            )
            sections[name] = range(start_col, end_col)

        return sections

    def _extract_section_data(self, name: str, col_range: range) -> pd.DataFrame:
        """Extract and clean data for a specific section."""
        main_sheet = self.spreadsheet[self.config.MAIN_SHEET]

        # Include the date column (column 0) plus the section columns
        cols = np.r_[0, col_range.start : col_range.stop]

        # Select rows starting from data start row, columns from our range
        res = main_sheet.iloc[self.config.DATA_START_ROW :, cols]

        # Clean and format the data
        res = self._clean_section_dataframe(res)

        return res

    def _clean_section_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and format a section dataframe."""
        # Make the first row the column headers
        df.columns = df.iloc[0].values
        df = df.drop(df.index[0])

        # Remove redundant Year column if present
        if "Year" in df.columns:
            df = df.drop(columns=["Year"])

        # Convert Month column to Date and format as datetime
        if self.config.DATE_COLUMN in df.columns:
            df.columns = [
                col if col != self.config.DATE_COLUMN else "Date" for col in df.columns
            ]
            df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d %H:%M:%S")

        # Sort by index and return
        return df.sort_index()


def save_json(sections: Dict[str, Any], output_path: str = "../data/monthly/") -> None:
    """Save data sections as JSON files - works with any dict of DataFrames.

    This function is data-agnostic and can work with DataFrames from any source,
    not just Excel files. Each section is saved as a separate JSON file with
    proper date formatting.

    Args:
        sections: Dictionary mapping section names to pandas DataFrames
        output_path: Directory path where JSON files will be saved

    Raises:
        OSError: If directory creation or file writing fails

    Example:
        >>> import pandas as pd
        >>> sections = {
        ...     "test_data": pd.DataFrame({"Date": ["2023-01-01"], "Value": [100]})
        ... }
        >>> save_json(sections, "./output/")
        # Creates: ./output/test_data.json
    """
    logger.info(f"Saving JSON files to {output_path}")

    # Create the directory if it doesn't exist
    if not os.path.exists(output_path):
        os.makedirs(output_path)
        logger.info(f"Created directory: {output_path}")

    for section_name, dataframe in sections.items():
        filename = section_name.replace(" ", "_").lower() + ".json"
        logger.info(f"Saving {filename}...")

        # Convert dataframe to JSON-serializable format
        records = dataframe.map(
            lambda x: x.isoformat() if hasattr(x, "isoformat") else x
        ).to_dict(orient="records")

        wrapped = {section_name: records}

        # Save pretty-printed JSON
        file_path = os.path.join(output_path, filename)
        with open(file_path, "w") as f:
            json.dump(wrapped, f, indent=4)

    logger.info("All JSON files saved successfully")


def run_monthly_scraper(output_path="../data/monthly/"):
    """Run the complete monthly data scraping process."""
    logger.info("Starting monthly data scraper")
    try:
        excel_url = resolve_excel_url()
        xlsx_data = download_excel(excel_url)
        workbook = TODExcelWorkbook(xlsx_data)
        save_json(workbook.subsheets, output_path)
        logger.info("Monthly data scraping completed successfully")
    except Exception as e:
        logger.error(f"Monthly data scraping failed: {e}")
        raise


def main():
    """Main entry point for standalone script usage."""
    run_monthly_scraper()


if __name__ == "__main__":
    main()
