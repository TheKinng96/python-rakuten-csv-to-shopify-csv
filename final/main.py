"""Rakuten → Shopify CSV one‑shot converter
------------------------------------------------
Author : your‑name‑here
Usage  : python convert_rakuten_to_shopify.py

This script now correctly handles Rakuten's two-row product format and
complex variant structures by:
1.  Grouping all rows by SKU to merge master and attribute rows into complete
    records. This is the primary pre-processing step.
2.  Refactoring the final conversion loop to correctly aggregate data across
    all variants belonging to a single product handle.
3.  Ensuring all variants get their correct price, SKU, images, and that
    metafields are correctly aggregated with their units.
4.  Leaving the 'Type' column empty as required.
"""

from __future__ import annotations
import csv
import json
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Config & Setup
# ---------------------------------------------------------------------------
DATA_DIR = Path("data")
OUT_DIR = Path("output")
OUT_FILE = OUT_DIR / "shopify_products.csv"
LOG_FILE = OUT_DIR / "rejected_rows.csv"
RAKUTEN_ENCODING = "cp932"

CATALOG_ID_RAKUTEN_KEY = "カタログID"
CATALOG_ID_SHOPIFY_COLUMN = "カタログID (rakuten)"

SPECIAL_TAGS: dict[str, str] = {
    "販売形態（並行輸入品）": "label__並行輸入品",
    "販売形態（訳あり）"   : "label__訳あり",
}
FREE_TAG_KEYS = {"食品配送状態", "セット種別"}

print("[1/5] Loading static resources…")
try:
    with open(DATA_DIR / "mapping_meta.json", encoding="utf-8") as fp:
        META_MAP: dict[str, str] = json.load(fp)
except FileNotFoundError:
    print("  - Warning: data/mapping_meta.json not found. No metafields will be mapped.")
    META_MAP = {}

collection_map: dict[str, str] = {}
try:
    coll_df = pd.read_csv(DATA_DIR / "rakuten_collection.csv", dtype=str, keep_default_na=False, encoding=RAKUTEN_ENCODING)
    for sku, path in zip(coll_df["商品管理番号（商品URL）"], coll_df["表示先カテゴリ"]):
        if "\\" in path: path = ",".join(path.split("\\")[1:])
        else: path = ""
        collection_map[sku] = path
except FileNotFoundError:
    print("  - Warning: data/rakuten_collection.csv not found. Categories will be blank.")

# ---------------------------------------------------------------------------
# Header & Helpers
# ---------------------------------------------------------------------------
STANDARD_HEADER = [
    "Handle","Title","Body (HTML)","Vendor","Type","Tags","Published",
    "Option1 Name","Option1 Value","Option2 Name","Option2 Value",
    "Option3 Name","Option3 Value","Variant SKU","Variant Grams",
    "Variant Inventory Tracker","Variant Inventory Qty","Variant Inventory Policy",
    "Variant Fulfillment Service","Variant Price","Variant Compare At Price",
    "Variant Requires Shipping","Variant Taxable","Variant Barcode","Image Src",
    "Image Position","Image Alt Text","Gift Card","SEO Title","SEO Description",
    "Google Shopping / Google Product Category","Google Shopping / Gender",
    "Google Shopping / Age Group","Google Shopping / MPN",
    "Google Shopping / Custom Product","Variant Image","Variant Weight Unit",
    "Variant Tax Code","Cost per item","Included / United States",
    "Price / United States","Compare At Price / United States","Included / International",
    "Price / International","Compare At Price / International","Status"
]
META_HEADER_KEYS = {
    "商品カテゴリー (product.metafields.custom.attributes)",
    "その他 (product.metafields.custom.etc)",
}
META_HEADER = sorted(list(set(META_MAP.values()) | META_HEADER_KEYS))
HEADER = STANDARD_HEADER + [CATALOG_ID_SHOPIFY_COLUMN] + META_HEADER

def derive_handle(sku: str) -> str:
    if not isinstance(sku, str) or not sku.endswith('s') or '-' not in sku: return sku
    parts = sku.rsplit('-', 1)
    if len(parts) == 2:
        base, suffix = parts
        if suffix[:-1].isdigit(): return base
    return sku

def append(meta: dict[str, str], key: str, value: str, sep: str = ";") -> None:
    if not value: return
    current_value = meta.get(key, "")
    meta[key] = f"{current_value}{sep if current_value else ''}{value}"

# ---------------------------------------------------------------------------
# Pre-processing Stage with Group-and-Merge Logic
# ---------------------------------------------------------------------------
print("[2/5] Pre-processing & merging Rakuten item data…")
OUT_DIR.mkdir(exist_ok=True)
rejected_rows_log = []

try:
    raw_df = pd.read_csv(
        DATA_DIR / "rakuten_item.csv", dtype=str, keep_default_na=False, encoding=RAKUTEN_ENCODING
    )
    original_columns = raw_df.columns.tolist()
    print(f"  → Loaded {len(raw_df):,} raw rows.")

    REQUIRED_COLUMNS = {'商品管理番号（商品URL）', '商品名', 'カタログID'}
    if not REQUIRED_COLUMNS.issubset(raw_df.columns):
        missing = REQUIRED_COLUMNS - set(raw_df.columns)
        raise KeyError(f"Input CSV is missing essential columns: {', '.join(missing)}")
    
    raw_df['SKU'] = raw_df['商品管理番号（商品URL）'].str.strip()

    grouped = raw_df.groupby('SKU')
    merged_data = []
    
    for sku, group in grouped:
        if not sku:
            log_entry = group.copy()
            log_entry['Reason'] = 'Blank SKU'
            rejected_rows_log.append(log_entry)
            continue

        master_rows = group[group['商品名'].str.strip() != ''].copy()
        attribute_rows = group[group['商品名'].str.strip() == ''].copy()

        if master_rows.empty:
            log_entry = group.copy()
            log_entry['Reason'] = 'Orphaned attribute rows (no master row found for this SKU)'
            rejected_rows_log.append(log_entry)
            continue
        
        merged_row = master_rows.iloc[0].copy()
        for _, attr_row in attribute_rows.iterrows():
            for col, val in attr_row.items():
                if pd.notna(val) and str(val).strip() != '':
                    merged_row[col] = val
        merged_data.append(merged_row)

    item_df = pd.DataFrame(merged_data)
    print(f"  → Merged into {len(item_df):,} complete product records.")
    
    ss_sku_mask = item_df['SKU'].str.endswith('-ss', na=False)
    if ss_sku_mask.any():
        rejected_df = item_df[ss_sku_mask].copy()
        rejected_df['Reason'] = 'SKU ends with -ss'
        rejected_rows_log.append(rejected_df)
        item_df = item_df[~ss_sku_mask]
    
    print(f"  → {len(item_df):,} rows after filtering -ss SKUs.")

    item_df[CATALOG_ID_RAKUTEN_KEY] = item_df.get(CATALOG_ID_RAKUTEN_KEY, pd.Series(dtype=str)).str.strip()
    is_blank_id = item_df[CATALOG_ID_RAKUTEN_KEY].fillna('').str.strip() == ''
    item_df.loc[is_blank_id, CATALOG_ID_RAKUTEN_KEY] = item_df['SKU']

    processed_df = item_df.drop_duplicates(subset=['SKU', CATALOG_ID_RAKUTEN_KEY]).copy()
    print(f"  → {len(processed_df):,} rows after final deduplication.")
    
    processed_df['Handle'] = processed_df['SKU'].apply(derive_handle)
    print(f"  → Pre-processing complete. Found {len(processed_df['Handle'].unique()):,} unique products.")

except (FileNotFoundError, KeyError) as e:
    print(f"\nFATAL ERROR: {e}")
    exit(1)
except Exception as e:
    print(f"\nAn unexpected error occurred during pre-processing: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

if rejected_rows_log:
    all_rejected_df = pd.concat(rejected_rows_log, ignore_index=True)
    print(f"[3/5] Logging {len(all_rejected_df)} rejected rows to {LOG_FILE}")
    with open(LOG_FILE, "w", newline="", encoding="utf-8") as f:
        log_header = original_columns + ['Reason']
        all_rejected_df.to_csv(f, columns=log_header, index=False, header=True)

# ---------------------------------------------------------------------------
# Main Conversion Loop (Refactored)
# ---------------------------------------------------------------------------
print("[4/5] Generating Shopify CSV from processed data…")
with open(OUT_FILE, "w", newline="", encoding="utf-8") as fout:
    writer = csv.DictWriter(fout, fieldnames=HEADER, extrasaction='ignore')
    writer.writeheader()

    for handle, product_group in processed_df.groupby('Handle'):
        
        # --- Stage 1: Aggregate ALL data for the handle first ---
        product_meta = {h: "" for h in META_HEADER}
        product_tags: set[str] = set()
        product_images: dict[str, str] = {} # {src: alt}
        variants_data: list[dict] = []
        
        # Determine the definitive "main row" for Title and Body
        main_product_row = product_group[product_group['SKU'] == handle].iloc[0] if not product_group[product_group['SKU'] == handle].empty else product_group.iloc[0]

        # Process each variant row within the handle group
        for _, r in product_group.iterrows():
            sku = r['SKU']
            
            # --- Collect data for THIS specific variant ---
            variant_images = {} # {src: alt} for this variant
            for n in range(1, 21):
                src = r.get(f"商品画像パス{n}", "").strip()
                if src:
                    alt = r.get(f"商品画像名（ALT）{n}", "").strip()
                    variant_images[src] = alt
                    product_images[src] = alt # Also add to the main product image pool

            # --- Process and aggregate attributes and tags ---
            for i in range(1, 101):
                k = r.get(f"商品属性（項目）{i}", "").strip()
                v = r.get(f"商品属性（値）{i}", "").strip()
                if not k or not v: continue
                
                if k in SPECIAL_TAGS: product_tags.add(SPECIAL_TAGS[k]); continue
                if k in FREE_TAG_KEYS: product_tags.add(v); continue

                dest = META_MAP.get(k)
                if dest:
                    value_to_append = v
                    # Correctly append unit for size metafield
                    size_meta_key = META_MAP.get("容量・サイズ") # Get the target column name from map
                    if size_meta_key and dest == size_meta_key:
                        unit = r.get(f"商品属性（単位）{i}", "").strip()
                        value_to_append += unit
                    append(product_meta, dest, value_to_append)
                else:
                    append(product_meta, "その他 (product.metafields.custom.etc)", f"{k}:{v}")
            
            # --- Store this variant's complete data ---
            variants_data.append({
                "Variant SKU": sku,
                "Option1 Value": sku.replace(handle, '').lstrip('-') or handle, # Better option value
                "Variant Price": r.get("販売価格", "").strip(),
                "Variant Compare At Price": r.get("表示価格", "").strip(),
                "Variant Inventory Qty": r.get("在庫数", "0").strip(),
                CATALOG_ID_SHOPIFY_COLUMN: r.get(CATALOG_ID_RAKUTEN_KEY, ''),
                "variant_image_src": list(variant_images.keys())[0] if variant_images else None
            })

        # --- Stage 2: Assemble and write the Shopify rows ---
        
        # Sort variants to ensure the main one is first
        variants_data.sort(key=lambda v: v['Variant SKU'] != handle)
        product_image_list = list(product_images.items())

        # Write one row for each variant
        for i, v_data in enumerate(variants_data):
            row = {h: "" for h in HEADER} # Reset row
            row.update({
                "Handle": handle,
                "Option1 Name": "Style" if len(variants_data) > 1 else "", # Only add option name if multiple variants
                "Variant SKU": v_data["Variant SKU"],
                "Option1 Value": v_data["Option1 Value"] if len(variants_data) > 1 else "Default Title",
                "Variant Price": v_data["Variant Price"],
                "Variant Compare At Price": v_data["Variant Compare At Price"],
                "Variant Inventory Qty": v_data["Variant Inventory Qty"],
                "Variant Inventory Tracker": "shopify",
                "Variant Inventory Policy": "deny",
                "Variant Fulfillment Service": "manual",
                "Variant Requires Shipping": "TRUE",
                "Variant Taxable": "TRUE",
                "Variant Weight Unit": "g",
                CATALOG_ID_SHOPIFY_COLUMN: v_data[CATALOG_ID_SHOPIFY_COLUMN],
                "Variant Image": v_data["variant_image_src"],
            })

            # If this is the first row for the handle, add all the product-level data
            if i == 0:
                row["Title"] = main_product_row.get("商品名", "")
                row["Body (HTML)"] = main_product_row.get("PC用商品説明文", "")
                row["Vendor"] = main_product_row.get("ブランド名", "tsutsu-uraura")
                row["Published"] = "TRUE"
                row["Status"] = "active"
                row["Tags"] = ",".join(sorted(list(product_tags)))
                # Add aggregated metafields
                row.update(product_meta)
                # Set Type to be empty as requested
                row["Type"] = ""
                # Add main product image
                if product_image_list:
                    row["Image Src"] = product_image_list[0][0]
                    row["Image Position"] = 1
                    row["Image Alt Text"] = product_image_list[0][1]
            
            writer.writerow(row)
        
        # Write extra image rows for the product
        for pos, (src, alt) in enumerate(product_image_list[1:], start=2):
             writer.writerow({
                "Handle": handle,
                "Image Src": src,
                "Image Position": pos,
                "Image Alt Text": alt
            })

print("[5/5] Done →", OUT_FILE)