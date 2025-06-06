# Rakuten → Shopify CSV One‑Shot Converter

Goal: Convert large Rakuten CSV exports (≈ 500 MB) into a single Shopify‑ready product CSV with images, variants, tags, and mapped metafields.

## 1. Project layout
```
project-root/
├─ data/
│   ├─ rakuten_item.csv            # Original Rakuten item data
│   ├─ rakuten_collection.csv      # Category / collection data
│   └─ mapping_meta.json           # Attribute‑to‑Shopify column map
│
├─ output/
│   └─ shopify_products.csv        # Final Shopify upload file
│
└─ scripts/
    └─ convert_rakuten_to_shopify.py
```

## 2. Quick start
```
# 1 ) create and activate a venv
python -m venv venv
source venv/bin/activate    # Windows: venv\\Scripts\\activate

# 2 ) install pandas only (no heavy deps!)
pip install "pandas==2.*"

# 3 ) drop your Rakuten CSVs in ./data and run
python scripts/convert_rakuten_to_shopify.py
```

*Result* → output/shopify_products.csv.  Import it in Shopify Admin → Products → Import → Preview only to verify before committing.

## 3. Config & mapping

| File | Purpose |
| --- | --- |
| data/mapping_meta.json | JSON map of 商品属性（項目） → full Shopify column header.  Tweak freely. |
| SPECIAL_TAGS (inside script) | Adds label__並行輸入品 / label__訳あり to Tags when keys exist.  Extend as needed. |
| CHUNK constant | Lines per chunk when streaming the 500 MB CSV (default 10_000). |

```
{
  "ブランド名": "ブランド・メーカー (product.metafields.custom.brand)",
  "原産国／製造国": "ご当地 (product.metafields.custom.area)",
  "自然派志向": "こだわり・認証 (product.metafields.custom.commitment)"
  // …
}
```

## 4. Core algorithm (per chunk)

| Step | Action |
| --- | --- |
| 0 | Write Shopify header once. |
| 1 | Read rakuten_item.csv chunk (pd.read_csv(chunksize=…)). |
| 2 | For each row → skip -ss, derive Handle (remove last -…). |
| 3 | Merge collection data → 商品カテゴリー metafield. |
| 4 | Extract images (商品画像パス1–20). |
| 5 | Walk 100 × (項目/値) → route to:• Shopify metafield column• Tags (special tag logic)• その他 (product.metafields.custom.etc) as キー:値. |
| 6 | Accumulate per-handle: images list, variant list, metafield dict, tag-set. |
| 7 | After chunk processed → emit rows:   a. Main row (first image, all metafields, tags).   b. Variant rows (skinny).   c. Extra image rows (Image Src, Position, Alt, Status). |
| 8 | csv.DictWriter(..., append=True) to stream‑write. |

Performance: ~2‑5 min on a 4‑core laptop (SSD).  Memory stays ≲ 300 MB due to chunked streaming.

## 5. Key business rules

### Handles & variants
- Main SKU = no suffix (e.g. abshiri-ap330)
- Variants = suffixes (-3s,-6s,-t …)
- Skip all SKUs ending -ss.

### Tags
- If 販売形態（並行輸入品） exists → add label__並行輸入品 to Tags.
- If 販売形態（訳あり） exists → add label__訳あり to Tags.
- 食品配送状態 & セット種別 → their values go to Tags (no prefix).

### Images
- First image → main/variant row (Image Position = 1).
- Additional images → one minimal row each (Position = 2…N).

### 商品カテゴリー metafield
- Built from collection path: split by \ → drop first level → join rest with commas.

### その他 (custom.etc)
- Any attribute key not in mapping_meta.json → store as キー:値.
- Multiple entries concatenated with `;`.

### 6. Encoding tips
- Shift‑JIS input?  add encoding="cp932" to read_csv or convert first:

`iconv -f CP932 -t UTF8 data/rakuten_item.csv > data/rakuten_item_utf8.csv`

- Output is UTF‑8 so Shopify imports cleanly.

### 7. Troubleshooting table

| Issue | Cause | Fix |
| --- | --- | --- |
| UnicodeDecodeError | Wrong encoding | Use encoding="cp932" or pre‑convert. |
| Images missing in Shopify | URL not public or no https | Ensure full HTTPS path. |
| Metafields empty | Header mismatch | Column headers must keep Japanese label + (key). |
| Tags truncated | >255 chars | Reduce tag list or abbreviate. |

### 8. Extending
- Add a new metafield → only edit mapping_meta.json; script auto‑picks it up.
- New special tag → add to SPECIAL_TAGS dict.
- Need more speed → raise CHUNK or parallelise with multiprocessing.Pool.

🥳 Enjoy your clean Shopify import!