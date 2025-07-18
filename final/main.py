"""Rakuten → Shopify CSV one‑shot converter
------------------------------------------------
Author : your‑name‑here
Usage  : python convert_rakuten_to_shopify.py
"""

from __future__ import annotations
import json
import re
from pathlib import Path
import pandas as pd
import csv

from bs4 import BeautifulSoup, Tag, Comment

# --- Configuration ---
DATA_DIR = Path("data")
OUT_DIR = Path("output")
OUT_FILE = OUT_DIR / "shopify_products.csv"
LOG_FILE = OUT_DIR / "rejected_rows.csv"
RAKUTEN_ENCODING = "cp932"
HTML_COMPARISON_LOG = OUT_DIR / "body_html_comparison.csv"
ORIGINAL_HTML_OUT_FILE = OUT_DIR / "shopify_products_original_html.csv"

IMAGE_DOMAIN = "https://tshop.r10s.jp/tsutsu-uraura"
CATALOG_ID_RAKUTEN_KEY = "カタログID"
CATALOG_ID_SHOPIFY_COLUMN = "カタログID (rakuten)"
SPECIAL_TAGS: dict[str, str] = {
    "販売形態（並行輸入品）": "label__並行輸入品",
    "販売形態（訳あり）"   : "label__訳あり",
}
FREE_TAG_KEYS = {"食品配送状態", "セット種別"}
SPECIAL_QUOTED_EMPTY_FIELDS = {'Type', 'Tags', 'Variant Barcode'}

CATEGORY_EXCLUSION_LIST = {
    "食品", "飲料・ドリンク", "調味料", "お酒・ワイン", "ヘルス・ビューティー",
    "サプリメント・ダイエット・健康", "ホーム・キッチン", "ペットフード・ペット用品"
}

GOJUON_CHARS = {
    'あ', 'い', 'う', 'え', 'お', 'か', 'き', 'く', 'け', 'こ', 'さ', 'し', 'す', 'せ', 'そ',
    'た', 'ち', 'つ', 'て', 'と', 'な', 'に', 'ぬ', 'ね', 'の', 'は', 'ひ', 'ふ', 'へ', 'ほ',
    'ま', 'み', 'む', 'め', 'も', 'や', 'ゆ', 'よ', 'ら', 'り', 'る', 'れ', 'ろ', 'わ', 'を', 'ん',
    'ア', 'イ', 'ウ', 'エ', 'オ', 'カ', 'キ', 'ク', 'ケ', 'コ', 'サ', 'シ', 'ス', 'セ', 'ソ',
    'タ', 'チ', 'ツ', 'テ', 'ト', 'ナ', 'ニ', 'ヌ', 'ネ', 'ノ', 'ハ', 'ヒ', 'フ', 'ヘ', 'ホ',
    'マ', 'ミ', 'ム', 'メ', 'モ', 'ヤ', 'ユ', 'ヨ', 'ラ', 'リ', 'ル', 'レ', 'ロ', 'ワ', 'ヲ', 'ン'
}

# --- MODIFICATION: Added a third regex for the new pattern ---
gojuon_regex_chars = "".join(GOJUON_CHARS)
# Pattern 1: Matches a Gojuon character, optional space(s), then '行' (e.g., "ア 行")
GOJUON_ROW_PATTERN = re.compile(f'^[{gojuon_regex_chars}]\\s*行$')
# Pattern 2: Matches a Gojuon character, then '(その#)' (e.g., "ア（その２）")
GOJUON_SONO_PATTERN = re.compile(f'^[{gojuon_regex_chars}]（その[０-９0-9]+）$')


print("[1/5] Loading static resources…")
# (File loading sections are unchanged)
try:
    with open(DATA_DIR / "mapping_meta.json", encoding="utf-8") as fp:
        META_MAP: dict[str, str] = json.load(fp)
except FileNotFoundError:
    print("  - Warning: data/mapping_meta.json not found. No metafields will be mapped.")
    META_MAP = {}
collection_map: dict[str, list[str]] = {}
try:
    coll_df = pd.read_csv(DATA_DIR / "rakuten_collection.csv", dtype=str, keep_default_na=False, encoding=RAKUTEN_ENCODING)
    for sku, path in zip(coll_df["商品管理番号（商品URL）"], coll_df["表示先カテゴリ"]):
        if sku.strip() and path.strip():
            collection_map.setdefault(sku.strip(), []).append(path)
except FileNotFoundError:
    print("  - Warning: data/rakuten_collection.csv not found. Categories will be blank.")

# ---------------------------------------------------------------------------
# Header & Helpers (Unchanged)
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
META_HEADER_KEYS = { "商品カテゴリー (product.metafields.custom.attributes)", "その他 (product.metafields.custom.etc)" }
all_meta_values = set(META_MAP.values()) | META_HEADER_KEYS
META_HEADER = sorted(list(all_meta_values - set(STANDARD_HEADER)))
HEADER = STANDARD_HEADER + [CATALOG_ID_SHOPIFY_COLUMN] + META_HEADER
def derive_handle(sku: str) -> str:
    if not isinstance(sku, str) or not sku.endswith('s') or '-' not in sku: return sku
    parts = sku.rsplit('-', 1)
    if len(parts) == 2 and parts[1][:-1].isdigit(): return parts[0]
    return sku
def get_set_count(sku: str) -> str:
    match = re.search(r'-(\d+)s$', sku); return match.group(1) if match else "1"
def to_absolute_url(src: str) -> str:
    if not src or src.startswith(('http://', 'https://')): return src
    return f"{IMAGE_DOMAIN}/{src.lstrip('/')}"
def format_csv_value(value, header_name):
    if header_name in SPECIAL_QUOTED_EMPTY_FIELDS and (value is None or str(value).strip() == ''): return '""'
    if value is None or str(value).strip() == '': return ''
    s_value = str(value)
    if '"' in s_value or ',' in s_value or '\n' in s_value: return f'"{s_value.replace("\"", "\"\"")}"'
    return s_value
def clean_body_html(html_content: str) -> str: # Unchanged
    if not html_content or not html_content.strip(): return ""
    try:
        soup = BeautifulSoup(html_content, 'lxml')
        for pattern in [re.compile(r"この商品のお買い得なセットはこちらから"), re.compile(r"その他の商品はこちらから")]:
            for tag in soup.find_all('a', string=pattern): tag.decompose()
        for pattern in [re.compile(r"よく一緒に購入されている商品はこちら"), re.compile(r"類似商品はこちら"), re.compile(r"再入荷しました")]:
            element = soup.find(string=pattern)
            if element and (parent_block := element.find_parent(['div', 'table', 'section', 'p', 'tr'])): parent_block.decompose()
        for link in list(soup.find_all('a', href=re.compile(r"my\.bookmark\.rakuten\.co\.jp"))):
            if container := link.find_parent('div'):
                for br in list(container.find_previous_siblings()):
                    if (isinstance(br, Tag) and br.name != 'br') or (not isinstance(br, Tag) and str(br).strip()): break
                    if isinstance(br, Tag) and br.name == 'br': br.decompose()
                container.decompose()
            else: link.decompose()
        href_pattern = re.compile(r"(item|search)\.rakuten\.co\.jp")
        for link in list(soup.find_all('a', href=href_pattern)):
            if link.find_parent('table'):
                next_sib = link.find_next_sibling()
                if next_sib and isinstance(next_sib, Tag) and next_sib.name == 'img': next_sib.decompose()
                for br in list(link.find_previous_siblings()):
                    if (isinstance(br, Tag) and br.name != 'br') or (not isinstance(br, Tag) and str(br).strip()): break
                    if isinstance(br, Tag) and br.name == 'br': br.decompose()
                link.decompose()
            elif parent_container := link.find_parent(['div', 'li', 'p']): parent_container.decompose()
            else: link.decompose()
        for img in list(soup.find_all('img', src="https://image.rakuten.co.jp/tsutsu-uraura/cabinet/souryou_0301/2200okinawar3.jpg")):
            for prev_sibling in list(img.find_previous_siblings()):
                if (isinstance(prev_sibling, Tag) and prev_sibling.name == 'br') or (not isinstance(prev_sibling, Tag) and not str(prev_sibling).strip()): prev_sibling.decompose()
                else: break
            for next_sibling in list(img.find_next_siblings()):
                if (isinstance(next_sibling, Tag) and next_sibling.name == 'br') or (not isinstance(next_sibling, Tag) and not str(next_sibling).strip()): next_sibling.decompose()
                else: break
            img.decompose()
        while True:
            removed_something = False
            for container in list(soup.find_all(['table', 'center'])):
                if not container.get_text(strip=True) and container.find('img'):
                    container.decompose(); removed_something = True
            if not removed_something: break
        if start_comment := soup.find(string=lambda text: isinstance(text, Comment) and 'EC-UP_Favorite_1_START' in text):
            for node in list(start_comment.find_all_next()): node.decompose()
            start_comment.decompose()
        return str(soup)
    except Exception as e:
        print(f"  - Warning: Could not parse/clean HTML. Error: {e}"); return html_content

# ---------------------------------------------------------------------------
# Pre-processing Stage (Unchanged)
# ---------------------------------------------------------------------------
print("[2/5] Pre-processing & merging Rakuten item data…")
# ... This entire section is unchanged and can be considered correct ...
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
    with open(LOG_FILE, "w", newline="", encoding="utf-8") as f: all_rejected_df.to_csv(f, columns=original_columns + ['Reason'], index=False, header=True)
# ---------------------------------------------------------------------------
# Main Conversion Loop
# ---------------------------------------------------------------------------
print("[4/5] Generating Shopify CSV from processed data…")
print(f"  - Cleaned version will be saved to '{OUT_FILE}'")
print(f"  - Uncleaned version will be saved to '{ORIGINAL_HTML_OUT_FILE}'")
print(f"  - HTML comparison log will be saved to '{HTML_COMPARISON_LOG}'")

with open(OUT_FILE, "w", newline="", encoding="utf-8") as fout, \
     open(ORIGINAL_HTML_OUT_FILE, "w", newline="", encoding="utf-8") as fout_orig, \
     open(HTML_COMPARISON_LOG, "w", newline="", encoding="utf-8") as flog:

    fout.write(",".join(HEADER) + "\n"); fout_orig.write(",".join(HEADER) + "\n")
    log_writer = csv.DictWriter(flog, fieldnames=['Handle', 'Original Body (HTML)', 'Cleaned Body (HTML)']); log_writer.writeheader()

    for handle, product_group in processed_df.groupby('Handle'):
        # --- This initial data gathering section is unchanged ---
        product_meta_sets: dict[str, set[str]] = {}; product_tags: set[str] = set(); product_images_seen = set(); product_image_list = []; variants_data: list[dict] = []
        main_product_row = product_group[product_group['SKU'] == handle].iloc[0] if not product_group[product_group['SKU'] == handle].empty else product_group.iloc[0]
        for _, r in product_group.iterrows():
            sku = r['SKU']; variant_image_src = None; weight_value, weight_unit_str, volume_value, volume_unit_str = None, None, None, None
            for n in range(1, 21):
                src = to_absolute_url((r.get(f"商品画像タイプ{n}", "") + r.get(f"商品画像パス{n}", "").strip()).lower())
                if src and src not in product_images_seen:
                    if not variant_image_src: variant_image_src = src
                    alt = r.get(f"商品画像名（ALT）{n}", "").strip(); product_image_list.append((src, alt)); product_images_seen.add(src)
            for i in range(1, 101):
                k = r.get(f"商品属性（項目）{i}", "").strip(); v_raw = r.get(f"商品属性（値）{i}", "").strip()
                if not k or not v_raw or v_raw == '-': continue
                if k == '総重量': weight_value, weight_unit_str = v_raw, r.get(f"商品属性（単位）{i}", "").strip(); continue
                if k == '総容量': volume_value, volume_unit_str = v_raw, r.get(f"商品属性（単位）{i}", "").strip(); continue
                values_to_process = [item.strip() for item in v_raw.split('|') if item.strip()]
                for v in values_to_process:
                    if k in SPECIAL_TAGS: product_tags.add(SPECIAL_TAGS[k]); continue
                    if k in FREE_TAG_KEYS: product_tags.add(v); continue
                    dest = META_MAP.get(k)
                    if dest:
                        value_to_append = v
                        if dest == "容量・サイズ(product.metafields.custom.size)" and (unit := r.get(f"商品属性（単位）{i}", "").strip()): value_to_append += unit
                        product_meta_sets.setdefault(dest, set()).add(value_to_append)
                    else: product_meta_sets.setdefault("その他 (product.metafields.custom.etc)", set()).add(f"{k}:{v}")
            variant_grams = ''; variant_weight_unit = ''
            try:
                if weight_value and weight_unit_str:
                    variant_weight_unit = weight_unit_str.lower()
                    if 'kg' in variant_weight_unit: variant_grams = str(int(float(weight_value) * 1000))
                    else: variant_grams = str(int(float(weight_value)))
                elif volume_value and volume_unit_str:
                    variant_weight_unit = volume_unit_str.lower()
                    if 'l' in variant_weight_unit: variant_grams = str(int(float(volume_value) * 1000))
                    else: variant_grams = str(int(float(volume_value)))
            except (ValueError, TypeError): variant_grams = ''
            variants_data.append({"Variant SKU": sku, "Option1 Value": get_set_count(sku), "Variant Price": r.get("通常購入販売価格", ""), "Variant Compare At Price": r.get("表示価格", ""), "Variant Inventory Qty": r.get("在庫数", "0"), "CATALOG_ID_SHOPIFY_COLUMN": r.get(CATALOG_ID_RAKUTEN_KEY, ''), "variant_image_src": variant_image_src, "variant_grams": variant_grams, "variant_weight_unit": variant_weight_unit})
        
        # --- This data processing and cleaning section is where the changes are ---
        variants_data.sort(key=lambda v: v['Variant SKU'] != handle)
        rows_to_write = []
        product_meta = {key: "\n".join(sorted(list(val_set))) for key, val_set in product_meta_sets.items()}
        all_variant_skus = [v_data["Variant SKU"] for v_data in variants_data]
        all_paths = [path for sku in all_variant_skus for path in collection_map.get(sku, [])]
        unique_components = {comp.strip() for path in all_paths if "\\" in path for comp in path.split('\\')[1:]}
        excluded_types = {comp for comp in unique_components if comp in CATEGORY_EXCLUSION_LIST}
        filtered_components = {comp for comp in unique_components if comp not in CATEGORY_EXCLUSION_LIST}
        product_type_string = sorted(list(excluded_types))[0] if excluded_types else ""
        if filtered_components:
            product_meta["商品カテゴリー (product.metafields.custom.attributes)"] = "\n".join(sorted(list(filtered_components)))

        area_key = 'ご当地 (product.metafields.custom.area)'
        if area_key in product_meta:
            lines = product_meta[area_key].split('\n')
            valid_lines = [line for line in lines if not line.strip().isdigit()]
            if valid_lines: product_meta[area_key] = "\n".join(valid_lines)
            else: del product_meta[area_key]

        attr_key = '商品カテゴリー (product.metafields.custom.attributes)'
        if attr_key in product_meta:
            lines = product_meta[attr_key].split('\n')
            valid_lines = []
            for line in lines:
                clean_line = line.strip()
                if not clean_line: continue
                # Condition 1: Check for single Gojuon character
                if clean_line in GOJUON_CHARS: continue
                # Condition 2: Check for "row" pattern (e.g., "ア 行")
                if GOJUON_ROW_PATTERN.match(clean_line): continue
                # MODIFICATION: Condition 3: Check for "part #" pattern (e.g., "ア（その２）")
                if GOJUON_SONO_PATTERN.match(clean_line): continue
                valid_lines.append(line)
            if valid_lines: product_meta[attr_key] = "\n".join(valid_lines)
            else: del product_meta[attr_key]

        # --- The rest of the script writes the row and is unchanged ---
        if variants_data:
            first_variant = variants_data[0]
            raw_html_body = main_product_row.get("PC用商品説明文", "")
            cleaned_html_body = clean_body_html(raw_html_body)
            if raw_html_body != cleaned_html_body:
                log_writer.writerow({'Handle': handle, 'Original Body (HTML)': raw_html_body, 'Cleaned Body (HTML)': cleaned_html_body})
            main_row = {"Handle": handle, "Title": main_product_row.get("商品名", ""), "Body (HTML)": cleaned_html_body, "Vendor": main_product_row.get("ブランド名", "tsutsu-uraura"), "Type": product_type_string, "Tags": ",".join(sorted(list(product_tags))), "Published": "true", "Status": "active", "Option1 Name": "セット", "Option1 Value": first_variant["Option1 Value"], "Variant SKU": first_variant["Variant SKU"], "Variant Grams": first_variant["variant_grams"], "Variant Barcode": "", "Variant Price": first_variant["Variant Price"], "Variant Compare At Price": first_variant["Variant Compare At Price"], "Variant Inventory Qty": first_variant["Variant Inventory Qty"], "Variant Inventory Tracker": "shopify", "Variant Inventory Policy": "deny", "Variant Fulfillment Service": "manual", "Variant Requires Shipping": "true", "Variant Taxable": "true", "Variant Weight Unit": first_variant["variant_weight_unit"], "Variant Image": first_variant["variant_image_src"], "Gift Card": "false", CATALOG_ID_SHOPIFY_COLUMN: first_variant["CATALOG_ID_SHOPIFY_COLUMN"]}
            main_row.update(product_meta)
            if product_image_list:
                main_row["Image Src"] = product_image_list[0][0]; main_row["Image Position"] = 1; main_row["Image Alt Text"] = product_image_list[0][1]
            rows_to_write.append(main_row)
            for v_data in variants_data[1:]:
                rows_to_write.append({"Handle": handle, "Type": "", "Tags": "", "Option1 Name": "セット", "Option1 Value": v_data["Option1 Value"], "Variant SKU": v_data["Variant SKU"], "Variant Grams": v_data["variant_grams"], "Variant Barcode": "", "Variant Price": v_data["Variant Price"], "Variant Compare At Price": v_data["Variant Compare At Price"], "Variant Inventory Qty": v_data["Variant Inventory Qty"], "Variant Inventory Tracker": "shopify", "Variant Inventory Policy": "deny", "Variant Fulfillment Service": "manual", "Variant Requires Shipping": "true", "Variant Taxable": "true", "Variant Weight Unit": v_data["variant_weight_unit"], CATALOG_ID_SHOPIFY_COLUMN: v_data["CATALOG_ID_SHOPIFY_COLUMN"], "Variant Image": v_data["variant_image_src"], "Gift Card": "false"})
            for pos, (src, alt) in enumerate(product_image_list[1:], start=2):
                 rows_to_write.append({"Handle": handle, "Image Src": src, "Image Position": pos, "Image Alt Text": alt, "Gift Card": "false"})
            for row_dict in rows_to_write:
                values_in_order = [row_dict.get(h) for h in HEADER]
                formatted_values = [format_csv_value(val, h) for val, h in zip(values_in_order, HEADER)]
                fout.write(",".join(formatted_values) + "\n")
            if rows_to_write:
                rows_to_write[0]['Body (HTML)'] = raw_html_body
            for row_dict in rows_to_write:
                values_in_order = [row_dict.get(h) for h in HEADER]
                formatted_values = [format_csv_value(val, h) for val, h in zip(values_in_order, HEADER)]
                fout_orig.write(",".join(formatted_values) + "\n")

print("[5/5] Done. All files created successfully.")