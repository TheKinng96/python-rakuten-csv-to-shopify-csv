"""
Shopify CSV Handle Counter
--------------------------
Usage: python check_handles.py

This script reads a Shopify products CSV file and counts the
number of unique values in the 'Handle' column.
"""
from pathlib import Path
import pandas as pd

# --- Configuration ---
# Set the path to the CSV file you want to check.
# This points to the output file from your main conversion script.
OUTPUT_DIR = Path("output")
FILE_TO_CHECK = OUTPUT_DIR / "shopify_products.csv"
# ---------------------

def count_unique_handles_pandas(filepath: Path):
    """Counts unique handles in a CSV file using the pandas library."""
    if not filepath.is_file():
        print(f"❌ Error: The file '{filepath}' was not found.")
        return

    try:
        print(f"🔎 Reading file '{filepath}'...")
        # Read the CSV into a pandas DataFrame
        df = pd.read_csv(filepath, on_bad_lines='warn')

        # Check if the 'Handle' column exists
        if 'Handle' not in df.columns:
            print(f"❌ Error: The file '{filepath}' does not contain a 'Handle' column.")
            return

        # Use the nunique() method to get the count of unique non-null handles
        unique_handle_count = df['Handle'].nunique()

        print("\n--- Analysis Complete ---")
        print(f"✅ Found {unique_handle_count:,} unique product handles.")
        print("-------------------------")

    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")

# --- Run the script ---
if __name__ == "__main__":
    count_unique_handles_pandas(FILE_TO_CHECK)