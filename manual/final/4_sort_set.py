"""
Step 4: Sorts variants by 'Option1 Value' and assigns 'Variant Position'.
--------------------------------------------------------------------------
Usage: python 4_sort_set.py
"""
from pathlib import Path
import pandas as pd
import csv

# --- Configuration ---
OUT_DIR = Path("output")
SHOPIFY_INPUT_FILE = OUT_DIR / "final_shopify_products_ready_for_upload.csv"
FINAL_SORTED_OUTPUT_FILE = OUT_DIR / "4_final_sorted_products.csv"

# Define the standard Shopify header to ensure Variant Position is included.
SHOPIFY_HEADER = [
    "Handle","Title","Body (HTML)","Vendor","Product Category","Type","Tags",
    "Published","Option1 Name","Option1 Value","Option2 Name","Option2 Value",
    "Option3 Name","Option3 Value","Variant SKU","Variant Grams",
    "Variant Inventory Tracker","Variant Inventory Qty","Variant Inventory Policy",
    "Variant Fulfillment Service","Variant Price","Variant Compare At Price",
    "Variant Requires Shipping","Variant Taxable","Variant Barcode","Image Src",
    "Image Position","Image Alt Text","Gift Card","SEO Title","SEO Description",
    "Google Shopping / Google Product Category","Google Shopping / Gender",
    "Google Shopping / Age Group","Google Shopping / MPN",
    "Google Shopping / Adler System Custom Product","Variant Image",
    "Variant Weight Unit","Variant Tax Code","Cost per item","Included / United States",
    "Price / United States","Compare At Price / United States","Included / International",
    "Price / International","Compare At Price / International","Status",
    "Variant Position"
]


def main():
    """Main execution function."""
    print(f"--- Running: Step 4: Sort Variants and Assign Position ---")

    if not SHOPIFY_INPUT_FILE.is_file():
        print(f"  [ERROR] Input file not found: '{SHOPIFY_INPUT_FILE}'")
        print("          Please run steps 1-3 first.")
        return

    try:
        print(f"  - Reading input file: '{SHOPIFY_INPUT_FILE.name}'...")
        df = pd.read_csv(SHOPIFY_INPUT_FILE, dtype=str, keep_default_na=False)
        original_columns = df.columns.tolist() # Keep track of original + custom columns

        # --- PREPARE DATA ---
        print("  - Preparing data for sorting by propagating handles...")
        df['GroupHandle'] = df['Handle'].where(df['Handle'] != '').ffill()
        df['SortValue'] = pd.to_numeric(df['Option1 Value'], errors='coerce')
        df['IsVariantRow'] = (df['Variant SKU'] != '').astype(int)

        # --- SORTING ---
        print("  - Sorting rows within each product group...")
        df_sorted = df.sort_values(
            by=['GroupHandle', 'IsVariantRow', 'SortValue'],
            ascending=[True, False, True],
            na_position='last'
        )

        # --- ASSIGN VARIANT POSITION ---
        print("  - Assigning 'Variant Position' to sorted variants...")
        if 'Variant Position' not in df_sorted.columns:
            df_sorted['Variant Position'] = ''
        
        variant_positions = df_sorted[df_sorted['IsVariantRow'] == 1].groupby('GroupHandle').cumcount() + 1
        df_sorted.loc[variant_positions.index, 'Variant Position'] = variant_positions.astype(str)

        # ==============================================================================
        # --- CORRECTED FINAL CLEANUP AND SAVE ---
        # ==============================================================================
        
        # We need to define the final order of columns, prioritizing the standard Shopify
        # ones and then appending any other custom columns from the original file.
        
        # Start with a clean list of columns from the original file
        final_columns = original_columns
        
        # Ensure 'Variant Position' is in the list if it wasn't there originally
        if 'Variant Position' not in final_columns:
            # A good place to insert it is after 'Status'
            try:
                status_index = final_columns.index('Status')
                final_columns.insert(status_index + 1, 'Variant Position')
            except ValueError:
                # If 'Status' isn't found, just append it to the end
                final_columns.append('Variant Position')

        # Now, select only the desired columns from the sorted DataFrame.
        # This implicitly "drops" the temporary columns ('GroupHandle', 'SortValue', 'IsVariantRow')
        # because they are not in the `final_columns` list.
        df_to_save = df_sorted[final_columns]
        
        print(f"  - Saving sorted data to '{FINAL_SORTED_OUTPUT_FILE.name}'...")
        df_to_save.to_csv(FINAL_SORTED_OUTPUT_FILE, index=False, quoting=csv.QUOTE_ALL)
        
        print("\n  - Sorting and positioning complete. The file to upload to Shopify is:")
        print(f"    -> {FINAL_SORTED_OUTPUT_FILE.resolve()}")

    except Exception as e:
        print(f"  [ERROR] An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
    print("--- Step 4 Finished ---\n")