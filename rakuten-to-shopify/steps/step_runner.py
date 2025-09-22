#!/usr/bin/env python3
"""
Individual Step Runner

Run individual pipeline steps for debugging, testing, or partial processing.

Usage:
    python steps/step_runner.py 01 input.csv output/
    python steps/step_runner.py 03 --from-step 02 output/
    python steps/step_runner.py --list
"""

import sys
import argparse
import json
import logging
from pathlib import Path
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from rakuten_to_shopify.pipeline.pipeline_config import PipelineConfig
from rakuten_to_shopify.pipeline.steps import (
    step_00_load_and_validate,
    step_01_initial_cleaning,
    step_02_sku_processing,
    step_03_html_processing,
    step_04_image_processing,
    step_05_metafield_mapping,
    step_06_tax_classification,
    step_07_type_assignment,
    step_08_variant_grouping,
    step_09_attribute_processing,
    step_10_description_finalization,
    step_11_csv_formatting,
    step_12_header_completion,
    step_13_quality_validation,
    step_14_export_generation
)


STEPS = {
    '00': ('Load and Validate', step_00_load_and_validate),
    '01': ('Initial Cleaning', step_01_initial_cleaning),
    '02': ('SKU Processing', step_02_sku_processing),
    '03': ('HTML Processing', step_03_html_processing),
    '04': ('Image Processing', step_04_image_processing),
    '05': ('Metafield Mapping', step_05_metafield_mapping),
    '06': ('Tax Classification', step_06_tax_classification),
    '07': ('Type Assignment', step_07_type_assignment),
    '08': ('Variant Grouping', step_08_variant_grouping),
    '09': ('Attribute Processing', step_09_attribute_processing),
    '10': ('Description Finalization', step_10_description_finalization),
    '11': ('CSV Formatting', step_11_csv_formatting),
    '12': ('Header Completion', step_12_header_completion),
    '13': ('Quality Validation', step_13_quality_validation),
    '14': ('Export Generation', step_14_export_generation)
}


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Run individual pipeline steps',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python steps/step_runner.py 01 input.csv             # Run step 01 from CSV input
  python steps/step_runner.py 03 --from-step 02        # Run step 03 from step 02 output
  python steps/step_runner.py 05 --input data.pkl      # Run step 05 from pickle file
  python steps/step_runner.py --list                   # List all available steps
  python steps/step_runner.py --range 01-05 input.csv  # Run steps 01 through 05

Step Data Flow:
  00: CSV → validated data
  01: validated data → cleaned data
  02: cleaned data → SKU processed data
  03: SKU processed data → HTML processed data
  ...and so on
        """
    )

    parser.add_argument(
        'step_number',
        nargs='?',
        help='Step number to run (00-14)'
    )

    parser.add_argument(
        'input_file',
        nargs='?',
        help='Input CSV file (for step 00) or previous step output'
    )

    parser.add_argument(
        '--output-dir',
        default='./step_output',
        help='Output directory for step results (default: ./step_output)'
    )

    parser.add_argument(
        '--from-step',
        help='Load data from previous step output (e.g., --from-step 02)'
    )

    parser.add_argument(
        '--list',
        action='store_true',
        help='List all available steps'
    )

    parser.add_argument(
        '--range',
        help='Run a range of steps (e.g., --range 01-05)'
    )

    parser.add_argument(
        '--save-intermediate',
        action='store_true',
        default=True,
        help='Save intermediate results for chaining steps (default: True)'
    )

    parser.add_argument(
        '--quiet',
        action='store_true',
        help='Quiet mode with minimal output'
    )

    return parser.parse_args()


def list_steps():
    """List all available steps"""
    print("📋 Available Pipeline Steps:")
    print("=" * 50)
    for step_num, (step_name, _) in STEPS.items():
        print(f"  {step_num}: {step_name}")

    print("\n🔄 Data Flow:")
    print("  CSV → 00 → 01 → 02 → 03 → 04 → 05 → 06 → 07 → 08 → 09 → 10 → 11 → 12 → 13 → 14 → CSV")

    print("\n💡 Examples:")
    print("  python steps/step_runner.py 00               # Auto-detect input file")
    print("  python steps/step_runner.py 00 input.csv     # Start from specific CSV")
    print("  python steps/step_runner.py 03 --from-step 02  # Continue from step 02")
    print("  python steps/step_runner.py 05               # Auto-detect from output_04.csv")
    print("  python steps/step_runner.py --range 01-05    # Run multiple steps")

    print("\n📁 File Outputs:")
    print("  step_output/output_XX.csv    # CSV output for each step")
    print("  step_output/logs/step_XX_*.log  # Individual step logs")
    print("  step_output/step_XX_output.pkl  # Pickle data for chaining")


def auto_detect_input_file() -> str:
    """Auto-detect input file in data/input directory"""
    project_root = Path(__file__).parent.parent
    data_input_dir = project_root / 'data' / 'input'

    if not data_input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {data_input_dir}")

    # Priority order for file detection
    patterns = [
        'rakuten_item_backup.csv',
        'rakuten_item.csv',
        '*rakuten_item*.csv',
        '*item*.csv',
        '*.csv'
    ]

    for pattern in patterns:
        matches = list(data_input_dir.glob(pattern))
        if matches:
            # If multiple matches, use the newest file
            newest_file = max(matches, key=lambda f: f.stat().st_mtime)
            return str(newest_file)

    raise FileNotFoundError(f"No CSV files found in {data_input_dir}")


def auto_detect_previous_output(step_number: str, output_dir: Path) -> str:
    """Auto-detect previous step CSV output"""
    prev_step = f"{int(step_number) - 1:02d}"
    # Prefer pickle file for full data structure
    pkl_file = output_dir / f"step_{prev_step}_output.pkl"
    if pkl_file.exists():
        return str(pkl_file)

    # Fallback to CSV file
    csv_file = output_dir / f"output_{prev_step}.csv"
    if csv_file.exists():
        return str(csv_file)

    raise FileNotFoundError(f"Previous step output not found: {csv_file} or {pkl_file}")


def load_step_data(step_number: str, input_file: str, output_dir: Path) -> dict:
    """Load data for a specific step"""
    if step_number == '00':
        # Step 00 loads from CSV with auto-detection
        if not input_file:
            input_file = auto_detect_input_file()
            print(f"🔍 Auto-detected input file: {input_file}")

        return {
            'input_file': input_file,
            'output_dir': output_dir,
            'config': PipelineConfig()
        }
    else:
        # Other steps load from previous step output
        if not input_file:
            input_file = auto_detect_previous_output(step_number, output_dir)
            print(f"🔍 Auto-detected previous output: {input_file}")

        if input_file.endswith('.csv'):
            # Load CSV output from previous step
            import pandas as pd
            df = pd.read_csv(input_file, encoding='utf-8')
            return {
                'raw_df': df,
                'output_dir': output_dir,
                'config': PipelineConfig()
            }
        elif input_file.endswith('.pkl'):
            # Load pickle data
            import pickle
            with open(input_file, 'rb') as f:
                return pickle.load(f)
        else:
            raise ValueError(f"Unsupported file format: {input_file}")


def setup_step_logging(step_number: str, output_dir: Path) -> str:
    """Setup logging for individual step"""
    log_dir = output_dir / 'logs'
    log_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f"step_{step_number}_{timestamp}.log"

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

    return str(log_file)


def save_step_data(step_number: str, data: dict, output_dir: Path):
    """Save step output data for chaining"""
    import pickle
    import pandas as pd

    output_dir.mkdir(parents=True, exist_ok=True)

    # Save pickle data for chaining
    pkl_file = output_dir / f"step_{step_number}_output.pkl"
    with open(pkl_file, 'wb') as f:
        pickle.dump(data, f)

    # Save CSV output if dataframe exists
    df_key = None
    available_keys = [k for k in data.keys() if k.endswith('_df') and data[k] is not None]
    print(f"🔍 Available DataFrame keys: {available_keys}")

    for key in ['final_df', 'html_processed_df', 'image_processed_df', 'sku_processed_df', 'shopify_df', 'cleaned_df', 'raw_df']:
        if key in data and data[key] is not None:
            df_key = key
            print(f"📊 Selected DataFrame key: {df_key}")
            break

    if df_key:
        csv_file = output_dir / f"output_{step_number}.csv"
        # CRITICAL: Force save the DataFrame and ensure changes are persisted
        final_df = data[df_key]
        final_df.to_csv(csv_file, index=False, encoding='utf-8')
        print(f"📄 CSV output saved: {csv_file}")

        # Debug: Verify the DataFrame being saved has the changes
        if step_number in ['03', '04']:
            try:
                # Check the DataFrame directly before saving
                sample_data = final_df.head(5)
                df_content = str(sample_data.to_string())
                ec_up_count = df_content.count('EC-UP')
                shopify_count = df_content.count('cdn.shopify.com')
                print(f"📊 DataFrame verification (first 5 rows): EC-UP={ec_up_count}, Shopify={shopify_count}")

                # Also check the saved file
                test_df = pd.read_csv(csv_file, nrows=5)
                file_content = str(test_df.to_string())
                file_ec_up = file_content.count('EC-UP')
                file_shopify = file_content.count('cdn.shopify.com')
                print(f"📊 Saved file verification (first 5 rows): EC-UP={file_ec_up}, Shopify={file_shopify}")
            except Exception as e:
                print(f"Debug verification failed: {e}")

    print(f"💾 Step data saved: {pkl_file}")


def run_single_step(step_number: str, input_file: str, output_dir: Path, quiet: bool = False):
    """Run a single pipeline step"""
    if step_number not in STEPS:
        raise ValueError(f"Invalid step number: {step_number}")

    step_name, step_func = STEPS[step_number]

    # Setup logging for this step
    log_file = setup_step_logging(step_number, output_dir)

    if not quiet:
        print(f"🔄 Running Step {step_number}: {step_name}")
        print(f"📤 Output: {output_dir}")
        print(f"📝 Log: {log_file}")
        print("-" * 50)

    # Load input data
    data = load_step_data(step_number, input_file, output_dir)

    # Run the step
    try:
        logging.info(f"Starting Step {step_number}: {step_name}")
        result = step_func.execute(data)

        if result:
            print(f"🔄 Step returned keys: {list(result.keys())}")
            # Check if we're overriding processed data with unprocessed data
            if 'html_processed_df' in result and 'html_processed_df' in data:
                original_ec_up = str(data['html_processed_df'].head(2).to_string()).count('EC-UP')
                returned_ec_up = str(result['html_processed_df'].head(2).to_string()).count('EC-UP')
                print(f"🔍 Before update: original={original_ec_up}, returned={returned_ec_up}")
            data.update(result)

        # Save intermediate data for chaining
        save_step_data(step_number, data, output_dir)

        logging.info(f"Step {step_number} completed successfully")

        if not quiet:
            print(f"✅ Step {step_number} completed successfully")

            # Show key results
            if 'final_df' in data:
                print(f"📊 Final output: {len(data['final_df'])} rows")
            elif 'cleaned_df' in data:
                print(f"📊 Cleaned data: {len(data['cleaned_df'])} rows")
            elif 'sku_processed_df' in data:
                print(f"📊 SKU processed: {len(data['sku_processed_df'])} rows")
            elif 'raw_df' in data:
                print(f"📊 Raw data: {len(data['raw_df'])} rows")

        return True

    except Exception as e:
        logging.error(f"Step {step_number} failed: {e}")
        print(f"❌ Step {step_number} failed: {e}")
        return False


def run_step_range(start_step: str, end_step: str, input_file: str, output_dir: Path, quiet: bool = False):
    """Run a range of steps"""
    start_num = int(start_step)
    end_num = int(end_step)

    if not quiet:
        print(f"🔄 Running steps {start_step}-{end_step}")
        print("=" * 50)

    for step_num in range(start_num, end_num + 1):
        step_str = f"{step_num:02d}"

        # For first step in range, use provided input file
        step_input = input_file if step_num == start_num else None

        success = run_single_step(step_str, step_input, output_dir, quiet)
        if not success:
            print(f"❌ Range execution stopped at step {step_str}")
            return False

    if not quiet:
        print(f"✅ Successfully completed steps {start_step}-{end_step}")
    return True


def main():
    """Main entry point"""
    args = parse_arguments()

    if args.list:
        list_steps()
        return

    if not args.step_number and not args.range:
        print("Error: Must specify a step number or use --list")
        sys.exit(1)

    output_dir = Path(args.output_dir)

    try:
        if args.range:
            # Parse range
            if '-' not in args.range:
                raise ValueError("Range must be in format 'XX-YY' (e.g., '01-05')")

            start_step, end_step = args.range.split('-')
            run_step_range(start_step, end_step, args.input_file, output_dir, args.quiet)

        else:
            # Run single step
            input_file = args.input_file
            if args.from_step:
                # Override input to load from previous step
                input_file = None

            run_single_step(args.step_number, input_file, output_dir, args.quiet)

    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()