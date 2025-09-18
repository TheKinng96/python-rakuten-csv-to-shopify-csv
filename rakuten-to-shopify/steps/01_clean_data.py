#!/usr/bin/env python3
"""
Step 01: Clean Data

Quick runner for data cleaning step.

Usage:
    python steps/01_clean_data.py input.csv
    python steps/01_clean_data.py --from-step 00
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from step_runner import run_single_step, parse_arguments

def main():
    """Run step 01 specifically"""
    args = parse_arguments()

    output_dir = Path(args.output_dir or './step_output')
    input_file = args.input_file or (None if args.from_step else None)

    if not input_file and not args.from_step:
        print("Usage: python steps/01_clean_data.py input.csv")
        print("   or: python steps/01_clean_data.py --from-step 00")
        sys.exit(1)

    success = run_single_step('01', input_file, output_dir, args.quiet)
    sys.exit(0 if success else 1)

if __name__ == '__main__':
    main()