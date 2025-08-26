"""
Shared HTTP client functionality for scrapers.
"""

import requests
from fake_useragent import UserAgent
import logging

logger = logging.getLogger(__name__)

# Get an up-to-date fake useragent
ua = UserAgent()


def spoof_get(url, **kwargs):
    """
    Performs a GET request to the given URL with a random User-Agent header.

    Args:
        url (str): The URL to make a GET request to
        **kwargs: Additional keyword arguments passed to requests.get

    Returns:
        requests.Response: The response object

    Any additional keyword arguments are passed to requests.get.
    """
    headers = kwargs.pop("headers", {})
    headers.setdefault("User-Agent", ua.random)

    logger.debug(f"Making request to: {url}")
    logger.debug(f"Using User-Agent: {headers['User-Agent']}")

    return requests.get(url, headers=headers, **kwargs)
