"""
Monthly data scraper for Newcastle Port statistics.

Refactored from monthly_data.py to use shared utilities and better organization.
"""

import pandas as pd
from bs4 import BeautifulSoup
from io import BytesIO
import numpy as np
import json
import os
import logging

from ..http_client import spoof_get
from ..config import MONTHLY_DATA_PORTAL_URL, MONTHLY_DATA_EXCEL_URL

logger = logging.getLogger(__name__)


def get_url_dynamic():
    """Get the current dynamic URL for the Excel file."""
    logger.info("Fetching dynamic URL from NSW Transport website")
    response = spoof_get(MONTHLY_DATA_PORTAL_URL)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    # Search for the "opendata.transport.nsw.gov.au" link in the HTML content
    # 'tod' is "Transport Open Data"
    tod_url = soup.find(
        "a",
        href=lambda href: href and "opendata.transport.nsw.gov.au" in href,
    )["href"]

    response = spoof_get(tod_url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    dynamic_url = soup.find("a", class_="resource-url-analytics")["href"]
    logger.debug(f"Found dynamic URL: {dynamic_url}")

    return dynamic_url


def get_xlsx():
    """Download the Excel file with monthly data."""
    logger.info("Downloading Excel file with monthly data")
    dynamic_url = get_url_dynamic()

    # check to see if the status url has changed, out of curiousity.
    # unsure yet if it changes each month (or sooner)
    if dynamic_url != MONTHLY_DATA_EXCEL_URL:
        logger.info("NOTE: Saved static URL different to dynamic URL, using new URL")

    url = dynamic_url or MONTHLY_DATA_EXCEL_URL

    response = spoof_get(url)
    response.raise_for_status()

    logger.info(f"Downloaded {len(response.content)} bytes")
    return response.content


class TODExcelWorkbook:
    """Transport Open Data Excel Workbook parser."""

    SHEET_NAMES = ["Readme", "Notes&Methods", "Port of Newcastle"]

    def __init__(self, bytes_data):
        logger.info("Parsing Excel workbook")
        # Create the spreadsheet dataframe
        self.spreadsheet = pd.read_excel(
            BytesIO(bytes_data), sheet_name=self.SHEET_NAMES, header=None
        )

        self.subsheets = {
            name: self._build_subsheet(hdr["slice"])
            for name, hdr in self._get_headings().items()
        }
        logger.info(f"Parsed {len(self.subsheets)} data sections")

    def _get_headings(self):
        """Extract the data headings from the spreadsheet."""
        # Extract the data headings
        hdr = self.spreadsheet["Port of Newcastle"].iloc[2]
        hdr = hdr[hdr.notna()]
        idx = list(hdr.index)
        return {
            name: {"slice": slice_}
            for name, slice_ in zip(
                hdr.values,
                (slice(start, end) for start, end in zip(idx, idx[1:] + [None])),
            )
        }

    def _build_subsheet(self, slice_):
        """Build a clean dataframe from a slice of the main sheet."""
        df = self.spreadsheet["Port of Newcastle"]

        # Convert slice to concrete indices
        start = slice_.start or 0
        stop = slice_.stop or df.shape[1]  # If None, go to last column
        step = slice_.step or 1

        # Combine 0 and slice indices with np.r_
        cols = np.r_[0, start:stop:step]

        # Select rows and columns
        res = df.iloc[3:, cols]

        # Make the first row the column headers
        res.columns = res.iloc[0].values
        res.drop(res.index[0], inplace=True)

        # The "Year" column at the end is redundant
        if "Year" in res.columns:
            res.drop(columns=["Year"], inplace=True)

        # Rename the month column and set it as a datetime index
        if "Month" in res.columns:
            res.rename(columns={"Month": "Date"}, inplace=True)
            res["Date"] = pd.to_datetime(res["Date"], format="%Y-%m-%d %H:%M:%S")

        # Sort by index
        res.sort_index(inplace=True)

        return res


def save_json(xlsx, path="../data/monthly/"):
    """Save the parsed data as JSON files."""
    logger.info(f"Saving JSON files to {path}")

    # create the directory if it doesn't exist
    if not os.path.exists(path):
        os.makedirs(path)
        logger.info(f"Created directory: {path}")

    for ss in xlsx.subsheets:
        filename = ss.replace(" ", "_").lower() + ".json"
        logger.info(f"Saving {filename}...")

        wrapped = {
            ss: xlsx.subsheets[ss]
            .map(lambda x: x.isoformat() if hasattr(x, "isoformat") else x)
            .to_dict(orient="records")
        }

        # Save pretty-printed JSON
        with open(path + filename, "w") as f:
            json.dump(wrapped, f, indent=4)

    logger.info("All JSON files saved successfully")


def run_monthly_scraper(output_path="../data/monthly/"):
    """Run the complete monthly data scraping process."""
    logger.info("Starting monthly data scraper")
    try:
        xlsx_data = get_xlsx()
        workbook = TODExcelWorkbook(xlsx_data)
        save_json(workbook, output_path)
        logger.info("Monthly data scraping completed successfully")
    except Exception as e:
        logger.error(f"Monthly data scraping failed: {e}")
        raise


def main():
    """Main entry point for standalone script usage."""
    run_monthly_scraper()


if __name__ == "__main__":
    main()
