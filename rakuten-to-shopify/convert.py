#!/usr/bin/env python3
"""
Quick CLI script for Rakuten to Shopify conversion

This is a simplified entry point that wraps the main pipeline
for easier command-line usage.

Usage:
    python convert.py                    # Auto-detect file in data/input/
    python convert.py input_file.csv     # Specify input file
    python convert.py --quiet            # Clean output, CSV in → CSV out
    python convert.py --output-dir out/  # Auto-detect with custom output
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

# Import and run main
try:
    from rakuten_to_shopify.main import main
    if __name__ == '__main__':
        main()
except ImportError as e:
    print(f"Error importing pipeline modules: {e}")
    print("Make sure you're running from the project root directory")
    print("and that all dependencies are installed:")
    print("  uv sync  # or pip install -e .")
    sys.exit(1)
except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)