"""Rakuten → Shopify CSV one‑shot converter
------------------------------------------------
Author : your‑name‑here
Usage  : python convert_rakuten_to_shopify.py

This script produces a Shopify-compliant CSV by:
1.  Correctly aggregating ALL category paths for a product and its variants
    into a single, unique, comma-separated metafield.
2.  Using a Set to deduplicate other metafield values.
3.  Placing the 'Status' field ONLY on the main product row.
4.  Correctly merging Rakuten's two-row product format.
"""

from __future__ import annotations
import csv
import json
import re
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

IMAGE_DOMAIN = "https://tshop.r10s.jp/tsutsu-uraura"

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

# --- MODIFICATION: Store multiple category paths per SKU ---
collection_map: dict[str, list[str]] = {}
try:
    coll_df = pd.read_csv(DATA_DIR / "rakuten_collection.csv", dtype=str, keep_default_na=False, encoding=RAKUTEN_ENCODING)
    for sku, path in zip(coll_df["商品管理番号（商品URL）"], coll_df["表示先カテゴリ"]):
        if sku.strip() and path.strip():
            collection_map.setdefault(sku.strip(), []).append(path)
except FileNotFoundError:
    print("  - Warning: data/rakuten_collection.csv not found. Categories will be blank.")
# --- END MODIFICATION ---

# ---------------------------------------------------------------------------
# Header & Helpers
# ---------------------------------------------------------------------------
STANDARD_HEADER = [
    "Handle","Title","Body (HTML)","Vendor", "Product Category","Type","Tags","Published",
    "Option1 Name","Option1 Value","Option1 Linked to","Option2 Name","Option2 Value","Option2 Linked To","Option3 Name","Option3 Value","Option3 Linked To","Variant SKU","Variant Grams",
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
def get_set_count(sku: str) -> str:
    match = re.search(r'-(\d+)s$', sku); return match.group(1) if match else "1"
def to_absolute_url(src: str) -> str:
    if not src or src.startswith(('http://', 'https://')): return src
    return f"{IMAGE_DOMAIN}/{src.lstrip('/')}"

# ---------------------------------------------------------------------------
# Pre-processing Stage
# ---------------------------------------------------------------------------
print("[2/5] Pre-processing & merging Rakuten item data…")
OUT_DIR.mkdir(exist_ok=True); rejected_rows_log = []
try:
    raw_df = pd.read_csv(DATA_DIR / "rakuten_item.csv", dtype=str, keep_default_na=False, encoding=RAKUTEN_ENCODING)
    original_columns = raw_df.columns.tolist(); print(f"  → Loaded {len(raw_df):,} raw rows.")
    REQUIRED_COLUMNS = {'商品管理番号（商品URL）', '商品名', 'カタログID'}
    if not REQUIRED_COLUMNS.issubset(raw_df.columns): raise KeyError(f"Input CSV is missing essential columns: {', '.join(REQUIRED_COLUMNS - set(raw_df.columns))}")
    raw_df['SKU'] = raw_df['商品管理番号（商品URL）'].str.strip(); grouped = raw_df.groupby('SKU'); merged_data = []
    for sku, group in grouped:
        if not sku: log_entry = group.copy(); log_entry['Reason'] = 'Blank SKU'; rejected_rows_log.append(log_entry); continue
        master_rows = group[group['商品名'].str.strip() != ''].copy(); attribute_rows = group[group['商品名'].str.strip() == ''].copy()
        if master_rows.empty: log_entry = group.copy(); log_entry['Reason'] = 'Orphaned attribute rows'; rejected_rows_log.append(log_entry); continue
        merged_row = master_rows.iloc[0].copy()
        for _, attr_row in attribute_rows.iterrows():
            for col, val in attr_row.items():
                if pd.notna(val) and str(val).strip() != '': merged_row[col] = val
        merged_data.append(merged_row)
    item_df = pd.DataFrame(merged_data); print(f"  → Merged into {len(item_df):,} complete product records.")
    ss_sku_mask = item_df['SKU'].str.endswith('-ss', na=False)
    if ss_sku_mask.any(): rejected_df = item_df[ss_sku_mask].copy(); rejected_df['Reason'] = 'SKU ends with -ss'; rejected_rows_log.append(rejected_df); item_df = item_df[~ss_sku_mask]
    print(f"  → {len(item_df):,} rows after filtering -ss SKUs.")
    item_df[CATALOG_ID_RAKUTEN_KEY] = item_df.get(CATALOG_ID_RAKUTEN_KEY, pd.Series(dtype=str)).str.strip()
    is_blank_id = item_df[CATALOG_ID_RAKUTEN_KEY].fillna('').str.strip() == ''; item_df.loc[is_blank_id, CATALOG_ID_RAKUTEN_KEY] = item_df['SKU']
    processed_df = item_df.drop_duplicates(subset=['SKU', CATALOG_ID_RAKUTEN_KEY]).copy(); print(f"  → {len(processed_df):,} rows after final deduplication.")
    processed_df['Handle'] = processed_df['SKU'].apply(derive_handle); print(f"  → Pre-processing complete. Found {len(processed_df['Handle'].unique()):,} unique products.")
except (FileNotFoundError, KeyError) as e: print(f"\nFATAL ERROR: {e}"); exit(1)
except Exception as e: print(f"\nAn unexpected error occurred during pre-processing: {e}"); import traceback; traceback.print_exc(); exit(1)
if rejected_rows_log:
    all_rejected_df = pd.concat(rejected_rows_log, ignore_index=True); print(f"[3/5] Logging {len(all_rejected_df)} rejected rows to {LOG_FILE}")
    with open(LOG_FILE, "w", newline="", encoding="utf-8") as f: log_header = original_columns + ['Reason']; all_rejected_df.to_csv(f, columns=log_header, index=False, header=True)

# ---------------------------------------------------------------------------
# Main Conversion Loop
# ---------------------------------------------------------------------------
print("[4/5] Generating Shopify CSV from processed data…")
with open(OUT_FILE, "w", newline="", encoding="utf-8") as fout:
    writer = csv.DictWriter(fout, fieldnames=HEADER, extrasaction='ignore'); writer.writeheader()
    for handle, product_group in processed_df.groupby('Handle'):
        product_meta_sets: dict[str, set[str]] = {}; product_tags: set[str] = set(); product_images_seen = set(); product_image_list = []; variants_data: list[dict] = []
        main_product_row = product_group[product_group['SKU'] == handle].iloc[0] if not product_group[product_group['SKU'] == handle].empty else product_group.iloc[0]
        for _, r in product_group.iterrows():
            sku = r['SKU']; variant_image_src = None; weight_unit, volume_unit = None, None
            for n in range(1, 21):
                src = to_absolute_url(r.get(f"商品画像タイプ{n}", "") + "/" + r.get(f"商品画像パス{n}", "").strip())
                if src:
                    if not variant_image_src: variant_image_src = src
                    if src not in product_images_seen: alt = r.get(f"商品画像名（ALT）{n}", "").strip(); product_image_list.append((src, alt)); product_images_seen.add(src)
            for i in range(1, 101):
                k = r.get(f"商品属性（項目）{i}", "").strip(); v = r.get(f"商品属性（値）{i}", "").strip();
                if not k or not v: continue
                unit = r.get(f"商品属性（単位）{i}", "").strip()
                if k == '総重量' and v and unit: weight_unit = unit
                elif k == '総容量' and v and unit: volume_unit = unit
                if k in SPECIAL_TAGS: product_tags.add(SPECIAL_TAGS[k]); continue
                if k in FREE_TAG_KEYS: product_tags.add(v); continue
                dest = META_MAP.get(k)
                if dest:
                    value_to_append = v
                    if dest == "容量・サイズ(product.metafields.custom.size)" and unit: value_to_append += unit
                    product_meta_sets.setdefault(dest, set()).add(value_to_append)
                else: product_meta_sets.setdefault("その他 (product.metafields.custom.etc)", set()).add(f"{k}:{v}")
            variants_data.append({
                "Variant SKU": sku, "Option1 Value": get_set_count(sku),
                "Variant Price": r.get("通常購入販売価格", "").strip(), "Variant Compare At Price": r.get("表示価格", "").strip(),
                "Variant Inventory Qty": r.get("在庫数", "0").strip(),
                CATALOG_ID_SHOPIFY_COLUMN: r.get(CATALOG_ID_RAKUTEN_KEY, ''),
                "variant_image_src": variant_image_src, "variant_weight_unit": weight_unit or volume_unit or ""
            })
        variants_data.sort(key=lambda v: v['Variant SKU'] != handle); rows_to_write = []
        product_meta = {key: ";".join(sorted(list(val_set))) for key, val_set in product_meta_sets.items()}
        
        # --- MODIFICATION: Aggregate all category paths for the handle and its variants ---
        all_variant_skus = [v_data["Variant SKU"] for v_data in variants_data]
        all_paths = []
        for sku in all_variant_skus:
            all_paths.extend(collection_map.get(sku, []))
        
        unique_components = set()
        for path in all_paths:
            if "\\" in path:
                components = [p.strip() for p in path.split('\\')[1:]]
                unique_components.update(components)
        
        if unique_components:
            product_meta["商品カテゴリー (product.metafields.custom.attributes)"] = ",".join(sorted(list(unique_components)))
        # --- END MODIFICATION ---

        if variants_data:
            first_variant = variants_data[0]
            main_row = {"Handle": handle, "Title": main_product_row.get("商品名", ""), "Body (HTML)": main_product_row.get("PC用商品説明文", ""), "Vendor": main_product_row.get("ブランド名", "tsutsu-uraura"), "Product Category": "", "Type": "", "Published": "true", "Tags": ",".join(sorted(list(product_tags))), "Status": "active", "Option1 Name": "Set", "Option1 Value": first_variant["Option1 Value"], "Option1 Linked to": "", "Variant SKU": first_variant["Variant SKU"], "Variant Price": first_variant["Variant Price"], "Variant Compare At Price": first_variant["Variant Compare At Price"], "Variant Inventory Qty": first_variant["Variant Inventory Qty"], "Variant Inventory Tracker": "shopify", "Variant Inventory Policy": "deny", "Variant Fulfillment Service": "manual", "Variant Requires Shipping": "true", "Variant Taxable": "true", "Variant Weight Unit": first_variant["variant_weight_unit"], CATALOG_ID_SHOPIFY_COLUMN: first_variant[CATALOG_ID_SHOPIFY_COLUMN], "Variant Image": ""}
            main_row.update(product_meta)
            if product_image_list: main_row["Image Src"] = product_image_list[0][0]; main_row["Image Position"] = 1; main_row["Image Alt Text"] = product_image_list[0][1]
            main_row["Gift Card"] = "false"
            rows_to_write.append(main_row)
        for v_data in variants_data[1:]:
            variant_row = {"Handle": handle, "Option1 Name": "Set", "Option1 Value": v_data["Option1 Value"], "Option1 Linked to": "", "Variant SKU": v_data["Variant SKU"], "Variant Price": v_data["Variant Price"], "Variant Compare At Price": v_data["Variant Compare At Price"], "Variant Inventory Qty": v_data["Variant Inventory Qty"], "Variant Inventory Tracker": "shopify", "Variant Inventory Policy": "deny", "Variant Fulfillment Service": "manual", "Variant Requires Shipping": "true", "Variant Taxable": "true", "Variant Weight Unit": v_data["variant_weight_unit"], CATALOG_ID_SHOPIFY_COLUMN: v_data[CATALOG_ID_SHOPIFY_COLUMN], "Variant Image": v_data["variant_image_src"], "Gift Card": "false"}
            rows_to_write.append(variant_row)
        for pos, (src, alt) in enumerate(product_image_list[1:], start=2):
             image_row = {"Handle": handle, "Image Src": src, "Image Position": pos, "Image Alt Text": alt, "Gift Card": "false"}
             rows_to_write.append(image_row)
        writer.writerows(rows_to_write)
print("[5/5] Done →", OUT_FILE)