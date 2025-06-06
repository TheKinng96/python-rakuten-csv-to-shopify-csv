"""Rakuten → Shopify CSV one‑shot converter
------------------------------------------------
Author : your‑name‑here
Usage  : python convert_rakuten_to_shopify.py

Expectations
============
* data/rakuten_item.csv            … Rakuten item export (Shift‑JIS or UTF‑8)
* data/rakuten_collection.csv      … Category export
* data/mapping_meta.json           … JSON mapping table (attribute‑key → Shopify column)

Produces
========
* output/shopify_products.csv      … Ready for Shopify import
"""

from __future__ import annotations
import csv
import json
import re
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DATA_DIR   = Path("data")
OUT_DIR    = Path("output")
OUT_FILE   = OUT_DIR / "shopify_products.csv"
CHUNK_SIZE = 10_000            # number of lines to process at once
RAKUTEN_ENCODING = "cp932"     # change to "cp932" if your file is Shift‑JIS

# Map 特殊キー → 強制タグ
SPECIAL_TAGS: Dict[str, str] = {
    "販売形態（並行輸入品）": "label__並行輸入品",
    "販売形態（訳あり）"   : "label__訳あり",
}

# キーがこれなら値を Tags に直接入れる（値そのまま）
FREE_TAG_KEYS = {"食品配送状態", "セット種別"}

# サイズ系キー（容量・サイズ metafield に寄せる）
SIZE_KEYS = {
    "総容量", "総重量", "単品重量", "総個数", "単品容量", "総入数", "単品（個装）個数",
    "総本数", "単品（個装）本数", "人数（分量）", "個数", "本数", "総枚数", "単品（個装）枚数",
    "本体横幅", "本体高さ", "本体奥行/マチ", "水筒・ボトル容量",
}

# ---------------------------------------------------------------------------
# Load static resources
# ---------------------------------------------------------------------------
print("[1/4] loading mapping table …")
with open(DATA_DIR / "mapping_meta.json", encoding="utf-8") as fp:
    META_MAP: Dict[str, str] = json.load(fp)

print("[2/4] building collection map …")
collection_map: Dict[str, str] = {}
coll_df = pd.read_csv(DATA_DIR / "rakuten_collection.csv", dtype=str, keep_default_na=False, encoding=RAKUTEN_ENCODING)
for sku, path in zip(coll_df["商品管理番号（商品URL）"], coll_df["表示先カテゴリ"]):
    # drop top level, join rest with commas
    if "\\" in path:
        path = ",".join(path.split("\\")[1:])
    else:
        path = ""
    collection_map[sku] = path

# ---------------------------------------------------------------------------
# Shopify header (standard + metafields)
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
    "Google Shopping / Age Group","Google Shopping / MPN","active",
    "Google Shopping / Custom Product","Variant Image","Variant Weight Unit",
    "Variant Tax Code","Cost per item","Included / United States",
    "Price / United States","Compare At Price / United States","Included / International",
    "Price / International","Compare At Price / International","Status"
]

# extract destination columns from mapping file & constant extras (no duplicates)
META_HEADER = sorted({col for col in META_MAP.values()} | {
    "商品カテゴリー (product.metafields.custom.attributes)",
    "その他 (product.metafields.custom.etc)",
})

HEADER = STANDARD_HEADER + META_HEADER

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
re_suffix = re.compile(r"-[^-]+$")


def strip_suffix(sku: str) -> str:
    """Return handle by removing last -suffix."""
    return re_suffix.sub("", sku)


def append(meta: Dict[str, str], key: str, value: str, sep: str = ";") -> None:
    if not value:
        return
    meta[key] = f"{meta[key]}{sep if meta[key] else ''}{value}"


# ---------------------------------------------------------------------------
# Main conversion loop
# ---------------------------------------------------------------------------
print("[3/4] processing rakuten_item.csv …")
OUT_DIR.mkdir(exist_ok=True)
with open(OUT_FILE, "w", newline="", encoding="utf-8") as fout:
    writer = csv.DictWriter(fout, fieldnames=HEADER)
    writer.writeheader()

    item_iter = pd.read_csv(
        DATA_DIR / "rakuten_item.csv",
        chunksize=CHUNK_SIZE,
        dtype=str,
        keep_default_na=False,
        encoding=RAKUTEN_ENCODING
    )

    for chunk_no, chunk in enumerate(item_iter, 1):
        print(f"  • chunk {chunk_no} → rows {len(chunk):,}")

        # containers reset per chunk
        per_handle_rows: List[Dict[str, str]] = []  # rows to write for this chunk

        grp_images: Dict[str, List[Tuple[str, str]]] = {}
        grp_meta:   Dict[str, Dict[str, str]] = {}
        grp_variants: Dict[str, List[Dict[str, str]]] = {}

        for _, r in chunk.iterrows():
            sku = r["商品管理番号（商品URL）"].strip()
            if sku.endswith("-ss"):
                continue

            handle = strip_suffix(sku)
            meta = grp_meta.setdefault(handle, {h: "" for h in META_HEADER})
            tags: set[str] = meta.setdefault("__tags", set())  # store tags inside meta

            # ---------------- category ----------------
            if not meta["商品カテゴリー (product.metafields.custom.attributes)"]:
                meta["商品カテゴリー (product.metafields.custom.attributes)"] = collection_map.get(sku, "")

            # ---------------- images ------------------
            img_list = grp_images.setdefault(handle, [])
            for n in range(1, 21):
                src = r.get(f"商品画像パス{n}", "").strip()
                if src:
                    alt = r.get(f"商品画像名（ALT）{n}", "").strip()
                    img_list.append((src, alt))

            # ---------------- attributes --------------
            for i in range(1, 101):
                k = r.get(f"商品属性（項目）{i}", "").strip()
                v = r.get(f"商品属性（値）{i}", "").strip()
                if not k or not v:
                    continue

                # special tags
                if k in SPECIAL_TAGS:
                    tags.add(SPECIAL_TAGS[k])
                    continue
                if k in FREE_TAG_KEYS:
                    tags.add(v)
                    continue

                # mapping table
                dest = META_MAP.get(k)
                if dest:
                    append(meta, dest, v)
                    continue

                # サイズ系
                if k in SIZE_KEYS:
                    append(meta, "容量・サイズ (product.metafields.custom.size)", v)
                    continue

                # fallback → etc
                append(meta, "その他 (product.metafields.custom.etc)", f"{k}:{v}")

            # ---------------- variants ----------------
            variant = {
                "Variant SKU": sku,
                "Option1 Name": "バリエーション",  # adjust if needed
                "Option1 Value": r.get("バリエーション項目選択肢1", ""),
                "Variant Price": r.get("通常購入販売価格", ""),
                "Variant Compare At Price": r.get("表示価格", ""),
                "Variant Inventory Qty": r.get("在庫数", ""),
                "Variant Inventory Tracker": "deny",
                "Variant Inventory Policy": "deny",
                "Variant Fulfillment Service": "manual",
                "Variant Requires Shipping": "TRUE",
                "Variant Taxable": "TRUE",
                "Variant Grams": "",
            }
            grp_variants.setdefault(handle, []).append(variant)

            # ---------------- main row meta -----------
            if sku == handle:  # main variant
                meta.update({
                    "Handle": handle,
                    "Title": r["商品名"],
                    "Body (HTML)": r.get("共通説明文（大）", ""),
                    "Vendor": "tsutsu-uraura",
                    "Published": "TRUE",
                    "Status": "active",
                })
                if collection_map.get(sku):
                    meta["Type"] = collection_map[sku].split(",")[0]

        # -------------- emit rows per handle -------------
        for handle, meta in grp_meta.items():
            img_list = grp_images.get(handle, [])
            if not img_list:
                img_list = [("", "")]

            # finalize tags
            meta["Tags"] = ",".join(sorted(meta.pop("__tags")))

            # main row
            row_main = {c: meta.get(c, "") for c in HEADER}
            row_main.update({
                "Handle": handle,
                "Image Src": img_list[0][0],
                "Image Alt Text": img_list[0][1],
                "Image Position": 1 if img_list[0][0] else "",
                "Variant SKU": handle,
                "Variant Requires Shipping": "TRUE",
                "Variant Taxable": "TRUE",
                "Variant Inventory Tracker": "deny",
                "Variant Inventory Policy": "deny",
                "Variant Fulfillment Service": "manual",
                "Variant Image": img_list[0][0],
                "Variant Weight Unit": "g",
            })
            per_handle_rows.append(row_main)

            # variant rows (except main)
            for v in grp_variants.get(handle, []):
                if v["Variant SKU"] == handle:
                    continue
                row = {c: "" for c in HEADER}
                row["Handle"] = handle
                row.update(v)
                per_handle_rows.append(row)

            # extra image rows
            for pos, (src, alt) in enumerate(img_list[1:], start=2):
                if not src:
                    continue
                row = {c: "" for c in HEADER}
                row.update({
                    "Handle": handle,
                    "Image Src": src,
                    "Image Position": pos,
                    "Image Alt Text": alt,
                    "Status": "active",
                })
                per_handle_rows.append(row)

        # write chunk rows
        with open(OUT_FILE, "a", newline="", encoding="utf-8") as fout:
            csv.DictWriter(fout, fieldnames=HEADER).writerows(per_handle_rows)

print("[4/4] Done →", OUT_FILE)
