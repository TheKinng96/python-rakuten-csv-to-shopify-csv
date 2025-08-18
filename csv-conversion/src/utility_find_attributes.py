# file: utility_find_attributes.py
"""
Utility Script: Finds products that have specific attribute values and reports
the corresponding attribute key (項目) and the product SKU.

This is useful for discovering how certain flags or labels are stored in the
Rakuten data.
---------------------------------------------------------------------------------
Usage: python utility_find_attributes.py
"""
from pathlib import Path
import pandas as pd

# ==============================================================================
# CONFIGURATION
# ==============================================================================

# --- Input File ---
DATA_DIR = Path("data")
RAKUTEN_INPUT_FILE = DATA_DIR / "rakuten_item.csv"
RAKUTEN_ENCODING = "cp932"

# --- Values to Search For ---
# The script will search for an exact match for each of these values.
# Add or remove values from this set as needed.
VALUES_TO_FIND = {
    "冷蔵配送",
    "同梱不可",
    "ラッピング不可",
    "日時指定不可",
    "予約注文",
    "数量限定",
    "販売終了間近",
    "新商品割引",
    "ワケありSALE",
    "タイムセール",
    "新入荷",
    "ケース販売"
}

# --- Output File ---
OUT_DIR = Path("output")
ATTRIBUTE_REPORT_FILE = OUT_DIR / "found_attributes_report.csv"


def main():
    """Main execution function."""
    print("--- Running: Find Specific Attributes Utility ---")
    OUT_DIR.mkdir(exist_ok=True)

    if not RAKUTEN_INPUT_FILE.is_file():
        print(f"  [ERROR] Input file not found: '{RAKUTEN_INPUT_FILE}'")
        return

    try:
        # We need to load all attribute columns and the SKU column.
        # It's often easiest to just load the whole file if memory allows.
        print(f"  - Reading source file: '{RAKUTEN_INPUT_FILE.name}'...")
        df = pd.read_csv(
            RAKUTEN_INPUT_FILE,
            encoding=RAKUTEN_ENCODING,
            dtype=str,
            keep_default_na=False
        )
        # Ensure the essential SKU column exists
        if "商品管理番号（商品URL）" not in df.columns:
            print("  [ERROR] The required column '商品管理番号（商品URL）' was not found.")
            return

    except Exception as e:
        print(f"  [ERROR] An unexpected error occurred while reading the file: {e}")
        return

    found_results = []
    
    print(f"  - Searching for {len(VALUES_TO_FIND)} specific values across 100 attribute columns...")
    
    # Iterate over each row in the DataFrame
    for index, row in df.iterrows():
        sku = row.get("商品管理番号（商品URL）", "UNKNOWN_SKU")
        
        # Iterate through all 100 possible attribute sets
        for i in range(1, 101):
            value_col = f"商品属性（値）{i}"
            key_col = f"商品属性（項目）{i}"
            
            # Check if these columns exist in the DataFrame to prevent errors
            if value_col in df.columns and key_col in df.columns:
                attribute_value = row[value_col].strip()
                
                # If the value in this cell is one of our targets...
                if attribute_value in VALUES_TO_FIND:
                    attribute_key = row[key_col].strip()
                    
                    # Record our findings
                    found_results.append({
                        'SKU': sku,
                        'Found Value': attribute_value,
                        'Attribute Key (項目)': attribute_key,
                        'Source Value Column': value_col,
                        'Source Key Column': key_col
                    })

    if not found_results:
        print("\n  - No matching attribute values were found in the file.")
        return

    # Convert the list of results into a DataFrame for easy saving
    results_df = pd.DataFrame(found_results)
    
    # For a cleaner report, you might want to see the unique keys found for each value
    unique_keys_report = results_df.groupby(['Found Value', 'Attribute Key (項目)']).size().reset_index(name='Count')
    print("\n--- Summary of Unique Keys Found ---")
    print(unique_keys_report.to_string(index=False))

    # Save the detailed, row-by-row report to a CSV file
    try:
        results_df.to_csv(ATTRIBUTE_REPORT_FILE, index=False, encoding='utf-8')
        print(f"\n  - Successfully created detailed report: '{ATTRIBUTE_REPORT_FILE.name}'")
        print(f"    -> {ATTRIBUTE_REPORT_FILE.resolve()}")
    except Exception as e:
        print(f"\n  [ERROR] Could not save the output file: {e}")


if __name__ == "__main__":
    main()
    print("--- Utility script finished ---\n")