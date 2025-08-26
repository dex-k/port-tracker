"""
Daily vessel movements scraper for Newcastle Harbour.

Refactored from daily_movements.py to use shared utilities and better organization.
"""

from bs4 import BeautifulSoup
from datetime import datetime
import logging

from ..http_client import spoof_get
from ..config import DAILY_VESSEL_MOVEMENTS_URL

logger = logging.getLogger(__name__)


def gen_daily_movements():
    """Generator that yields daily vessel movements."""
    logger.info("Fetching daily vessel movements")
    response = spoof_get(DAILY_VESSEL_MOVEMENTS_URL)
    response.raise_for_status()  # Raise an error for bad responses

    soup = BeautifulSoup(response.text, "html.parser")
    movements = []

    table = soup.select_one(".view-vessel-movement .view-content table")
    thead = table and table.select_one("thead")
    tbody = table and table.select_one("tbody")
    if not table or not thead or not tbody:
        logger.warning("Table structure has changed or is missing.")
        return movements

    # Parse headings from the table
    headings = [th.get_text(strip=True) for th in thead.find_all("th")]
    logger.debug(f"Found table headings: {headings}")

    # Parse through each row in the table body
    movement_count = 0
    for row in tbody.select("tr"):
        columns = [td.get_text(strip=True) for td in row.find_all("td")]
        if columns:
            # Convert date and time to a datetime object
            date_str = f"{datetime.today().year} {columns[0]}"
            # No space between %b and %H
            columns[0] = datetime.strptime(date_str, "%Y %a %d %b%H:%M")
            movement = dict(zip(headings, columns))
            movement_count += 1

            logger.debug(f"Parsed movement: {movement}")
            yield movement

    logger.info(f"Found {movement_count} vessel movements")


def get_daily_movements():
    """Get all daily movements as a list."""
    return list(gen_daily_movements())


def run_daily_scraper():
    """Run the complete daily movements scraping process."""
    logger.info("Starting daily movements scraper")
    try:
        movements = get_daily_movements()
        logger.info(f"Successfully scraped {len(movements)} vessel movements")
        return movements
    except Exception as e:
        logger.error(f"Daily movements scraping failed: {e}")
        raise


def main():
    """Main entry point for standalone script usage."""
    movements = run_daily_scraper()
    for movement in movements:
        print(movement)


if __name__ == "__main__":
    main()
