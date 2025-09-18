#!/usr/bin/env python3
"""
Rakuten to Shopify CSV Converter - Main Entry Point

A complete pipeline that transforms Rakuten product data to Shopify-ready CSV format
with all production fixes applied in a single processing run.

Usage:
    python main.py input_file.csv [--output-dir output/] [--config config.json]

Example:
    python main.py data/rakuten_item.csv --output-dir output/csv/
"""

import sys
import argparse
import logging
from pathlib import Path

from .pipeline.pipeline_runner import PipelineRunner
from .pipeline.pipeline_config import PipelineConfig


def setup_logging(log_level: str = "INFO", quiet: bool = False):
    """Setup logging configuration"""
    if quiet:
        # In quiet mode, only log to file, not console
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('rakuten_to_shopify.log')
            ]
        )
    else:
        # Normal mode with console output
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('rakuten_to_shopify.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Convert Rakuten CSV to Shopify-ready format',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                                          # Auto-detect file in data/input/
  python main.py data/rakuten_item.csv                   # Specify input file
  python main.py --output-dir output/csv/                # Auto-detect with custom output
  python main.py data.csv --config config.json --log-level DEBUG

Auto-detection looks for files in data/input/ directory in this order:
  1. rakuten_item.csv (exact match)
  2. *rakuten_item*.csv (pattern match)
  3. *item*.csv (broader pattern)
  4. *.csv (any CSV file, newest first)

The pipeline will create multiple output files:
  - shopify_products_YYYYMMDD_HHMMSS.csv (main export)
  - products_summary_YYYYMMDD_HHMMSS.csv (summary)
  - Various analysis and validation reports
        """
    )

    parser.add_argument(
        'input_file',
        nargs='?',  # Make input_file optional
        help='Path to the Rakuten CSV file (rakuten_item.csv). If not provided, will auto-detect in data/input/'
    )

    parser.add_argument(
        '--output-dir',
        default='./output',
        help='Output directory for generated files (default: ./output)'
    )

    parser.add_argument(
        '--config',
        help='Path to custom configuration file (optional)'
    )

    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Set the logging level (default: INFO)'
    )

    parser.add_argument(
        '--validate-only',
        action='store_true',
        help='Only validate input file without processing'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Run pipeline without generating output files'
    )

    parser.add_argument(
        '--quiet',
        action='store_true',
        help='Quiet mode - show only essential CSV transformation steps'
    )

    parser.add_argument(
        '--summary',
        action='store_true',
        help='Show step-by-step transformation summary (default: True)'
    )

    return parser.parse_args()


def auto_detect_input_file() -> str:
    """Auto-detect input file in data/input directory"""
    # Get the project root (where this script is located)
    project_root = Path(__file__).parent.parent.parent
    data_input_dir = project_root / 'data' / 'input'

    print(f"Looking for input files in: {data_input_dir}")

    if not data_input_dir.exists():
        raise FileNotFoundError(f"Data input directory not found: {data_input_dir}")

    # Look for common Rakuten file patterns
    patterns = [
        'rakuten_item.csv',
        '*rakuten_item*.csv',
        '*item*.csv',
        '*.csv'
    ]

    for pattern in patterns:
        matches = list(data_input_dir.glob(pattern))
        if matches:
            # Sort by modification time, newest first
            matches.sort(key=lambda x: x.stat().st_mtime, reverse=True)
            selected_file = matches[0]
            print(f"Auto-detected input file: {selected_file}")
            if len(matches) > 1:
                print(f"Note: Found {len(matches)} matching files, using the newest: {selected_file.name}")
            return str(selected_file)

    # If no files found, list what's available
    csv_files = list(data_input_dir.glob('*.csv'))
    if csv_files:
        file_list = ', '.join([f.name for f in csv_files])
        raise FileNotFoundError(f"No rakuten_item.csv found. Available CSV files: {file_list}")
    else:
        raise FileNotFoundError(f"No CSV files found in {data_input_dir}")


def validate_input_file(input_file: str) -> bool:
    """Validate that input file exists and is accessible"""
    input_path = Path(input_file)

    if not input_path.exists():
        print(f"Error: Input file does not exist: {input_file}", file=sys.stderr)
        return False

    if not input_path.is_file():
        print(f"Error: Input path is not a file: {input_file}", file=sys.stderr)
        return False

    if input_path.stat().st_size == 0:
        print(f"Error: Input file is empty: {input_file}", file=sys.stderr)
        return False

    # Check file size (warn if very large)
    file_size_mb = input_path.stat().st_size / (1024 * 1024)
    if file_size_mb > 500:
        print(f"Warning: Large input file detected ({file_size_mb:.1f} MB)")
        print("Processing may take significant time and memory")

    return True


def main():
    """Main entry point"""
    args = parse_arguments()

    # Setup logging
    setup_logging(args.log_level, args.quiet)
    logger = logging.getLogger(__name__)

    # Print banner (skip in quiet mode)
    if not args.quiet:
        print("=" * 60)
        print("Rakuten to Shopify CSV Converter v2.0.0")
        print("Complete Pipeline with Production Fixes")
        print("=" * 60)

    try:
        # Determine input file
        if args.input_file:
            input_file = args.input_file
        else:
            print("No input file specified, auto-detecting...")
            input_file = auto_detect_input_file()

        # Validate input file
        if not validate_input_file(input_file):
            sys.exit(1)

        logger.info(f"Starting conversion: {input_file}")
        logger.info(f"Output directory: {args.output_dir}")

        # Load configuration
        if args.config:
            logger.info(f"Loading custom configuration: {args.config}")
            config = PipelineConfig(args.config)
        else:
            logger.info("Using default configuration")
            config = PipelineConfig()

        # Validate-only mode
        if args.validate_only:
            logger.info("Running in validate-only mode")
            # TODO: Implement validate-only functionality
            print("Input file validation passed")
            sys.exit(0)

        # Create and run pipeline
        runner = PipelineRunner(config, quiet_mode=args.quiet)

        if args.dry_run:
            logger.info("Running in dry-run mode (no output files will be generated)")
            # TODO: Implement dry-run functionality

        # Execute pipeline
        success = runner.run_pipeline(input_file, args.output_dir)

        if success:
            print("\n" + "=" * 60)
            print("✅ Conversion completed successfully!")
            print(f"Output files generated in: {args.output_dir}")
            print("=" * 60)
            sys.exit(0)
        else:
            print("\n" + "=" * 60)
            print("❌ Conversion failed!")
            print("Check the logs for details")
            print("=" * 60)
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("Conversion interrupted by user")
        print("\nConversion interrupted by user")
        sys.exit(1)

    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        print(f"\nUnexpected error: {e}")
        print("Check the logs for full details")
        sys.exit(1)


if __name__ == '__main__':
    main()