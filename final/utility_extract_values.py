"""
Utility Script: Extracts all unique values from specified columns
in the original Rakuten CSV file.

This is useful for analysis, such as seeing all possible tags or categories
that exist in the source data.
-------------------------------------------------------------------------
Usage: python utility_extract_values.py
"""
from pathlib import Path
import pandas as pd
import re

# ==============================================================================
# CONFIGURATION
# ==============================================================================

# --- Input File ---
# Path to the original Rakuten CSV data file.
DATA_DIR = Path("output")
RAKUTEN_INPUT_FILE = DATA_DIR / "4_final_sorted_products.csv"
RAKUTEN_ENCODING = "utf-8"

# --- Columns to Analyze ---
# Add or remove column names from this list to change which fields are analyzed.
COLUMNS_TO_ANALYZE = [
    "こだわり・認証 (product.metafields.custom.commitment)",
    "ご当地 (product.metafields.custom.area)",
    "商品カテゴリー (product.metafields.custom.attributes)",
]

# --- Output File ---
# The script will create this file in an 'output' subfolder.
OUT_DIR = Path("output")
UNIQUE_VALUES_OUTPUT_FILE = OUT_DIR / "unique_values_report.csv"


def main():
    """Main execution function."""
    print(f"--- Running: Unique Value Extraction Utility ---")
    OUT_DIR.mkdir(exist_ok=True)

    if not RAKUTEN_INPUT_FILE.is_file():
        print(f"  [ERROR] Input file not found: '{RAKUTEN_INPUT_FILE}'")
        print("          Please ensure the file exists in the 'data' folder.")
        return

    try:
        print(f"  - Reading source file: '{RAKUTEN_INPUT_FILE.name}'...")
        # Read only the necessary columns to save memory
        df = pd.read_csv(
            RAKUTEN_INPUT_FILE,
            encoding=RAKUTEN_ENCODING,
            usecols=COLUMNS_TO_ANALYZE,
            dtype=str,
            keep_default_na=False
        )
    except ValueError:
         print(f"  [ERROR] One or more specified columns not found in the CSV.")
         print(f"          Please check the names in the COLUMNS_TO_ANALYZE list.")
         return
    except Exception as e:
        print(f"  [ERROR] An unexpected error occurred while reading the file: {e}")
        return

    all_data_frames = []
    
    # This regex will split on commas, slashes, spaces, and full-width Japanese spaces.
    # It's robust for handling messy, multi-value fields.
    split_pattern = r'[,\s/　]+'

    for column_name in COLUMNS_TO_ANALYZE:
        print(f"  - Processing column: '{column_name}'...")
        
        # 1. Drop any truly empty rows for this column
        series = df[column_name].dropna()
        
        # 2. Split cells containing multiple values (e.g., "A, B / C") into a list
        series_split = series.str.split(split_pattern)
        
        # 3. Use 'explode' to create a new row for each item in the lists
        series_exploded = series_split.explode()
        
        # 4. Strip leading/trailing whitespace from each individual value
        series_stripped = series_exploded.str.strip()
        
        # 5. Get all unique, non-empty values and sort them
        unique_values = sorted(series_stripped[series_stripped != ''].unique())
        
        if unique_values:
            print(f"    > Found {len(unique_values)} unique values.")
            # Create a DataFrame for this column's results
            temp_df = pd.DataFrame({
                'Source Column': column_name,
                'Unique Value': unique_values
            })
            all_data_frames.append(temp_df)
        else:
            print(f"    > No unique values found in this column.")

    if not all_data_frames:
        print("\n  - No data was extracted from any of the specified columns.")
        return

    # Concatenate all results into a single DataFrame
    final_df = pd.concat(all_data_frames, ignore_index=True)

    # Save the final report to a CSV file
    try:
        final_df.to_csv(UNIQUE_VALUES_OUTPUT_FILE, index=False, encoding='utf-8')
        print(f"\n  - Successfully created report: '{UNIQUE_VALUES_OUTPUT_FILE.name}'")
        print(f"    -> {UNIQUE_VALUES_OUTPUT_FILE.resolve()}")
    except Exception as e:
        print(f"\n  [ERROR] Could not save the output file: {e}")


if __name__ == "__main__":
    main()
    print("--- Utility script finished ---\n")