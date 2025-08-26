"""
Command-line interface for the port tracker scrapers.
"""

import argparse
import logging
import sys

from .endpoints.monthly import run_monthly_scraper
from .endpoints.daily import run_daily_scraper

logger = logging.getLogger(__name__)


def setup_logging(verbose=False):
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def run_monthly(args):
    """Run the monthly data scraper."""
    output_path = args.output or "../data/monthly/"
    logger.info(f"Running monthly scraper, output to: {output_path}")

    try:
        run_monthly_scraper(output_path)
        print(f"Monthly data saved to {output_path}")
    except Exception as e:
        print(f"Monthly scraper failed: {e}")
        return 1
    return 0


def run_daily(args):
    """Run the daily movements scraper."""
    logger.info("Running daily movements scraper")

    try:
        movements = run_daily_scraper()
        print(f"Found {len(movements)} vessel movements:")

        for i, movement in enumerate(movements, 1):
            print(f"  {i}. {movement}")

    except Exception as e:
        print(f"Daily scraper failed: {e}")
        return 1
    return 0


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Newcastle Port Data Scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  scraper monthly                    # Run monthly data scraper
  scraper daily                      # Run daily movements scraper  
  scraper monthly -o ./data/         # Custom output directory
  scraper --verbose daily            # Enable debug logging
        """,
    )

    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose (debug) logging"
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Monthly scraper command
    monthly_parser = subparsers.add_parser("monthly", help="Run monthly data scraper")
    monthly_parser.add_argument(
        "--output",
        "-o",
        help="Output directory for JSON files (default: ../data/monthly/)",
    )
    monthly_parser.set_defaults(func=run_monthly)

    # Daily scraper command
    daily_parser = subparsers.add_parser("daily", help="Run daily movements scraper")
    daily_parser.set_defaults(func=run_daily)

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.verbose)

    # If no command specified, show help
    if not args.command:
        parser.print_help()
        return 1

    # Run the selected command
    try:
        return args.func(args)
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        return 130
    except Exception as e:
        logger.exception("Unexpected error")
        print(f"Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
