"""
Port Tracker Scraper Package

A simple package for scraping Newcastle Port data from NSW Transport sources.
"""

import logging

__version__ = "0.1.0"

# Set up basic logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Create a logger for the scraper package
logger = logging.getLogger(__name__)
