# Rakuten to Shopify Conversion Requirements
Version 2.0 - Complete Production Requirements

## Overview
This document describes the complete requirements for converting Rakuten product data to Shopify-ready CSV format, incorporating all lessons learned from production deployments and post-import fixes.

## Evolution History
1. **Phase 1**: `manual/final` - Initial CSV conversion with basic processing
2. **Phase 2**: `old` folder - Post-import API fixes for discovered issues
3. **Phase 3**: Tax compliance - Japanese tax rate requirements
4. **Phase 4**: **This solution** - All fixes integrated into single conversion

## Input Files Required

### Primary Files
- `rakuten_item.csv` (Shift-JIS encoding) - Main product data (~400MB)
- `rakuten_collection.csv` (Shift-JIS encoding) - Category/collection data (~10MB)
- `mapping_meta.json` (UTF-8) - Metafield attribute mapping configuration

### Tax Compliance Files
- `商品マスタ_YYYYMMDD.csv` (Shift-JIS) - Product master with tax rates
- `セット商品マスタ_YYYYMMDD.csv` (Shift-JIS) - Set product tax rates
- `with-tax.csv` (UTF-8) - Consolidated tax mapping file

## Core Processing Requirements

### 1. Data Ingestion & Filtering
**Input Handling:**
- ✅ Read CSV with Shift-JIS (cp932) encoding detection
- ✅ Handle multiline fields and embedded HTML correctly
- ✅ Process files up to 500MB+ with chunked reading

**Filtering Rules:**
- ✅ Remove SKUs ending with `-ss` (sample/screenshot products)
- ✅ Filter out excluded categories (CATEGORY_EXCLUSION_LIST)
- ✅ Group by 商品管理番号（商品URL）for variant handling
- ✅ Skip empty or invalid product rows

### 2. Handle & Variant Generation
**Business Rules:**
```python
# SKU → Handle conversion
# Main product: xl-ekjd-8f7a
# Variant examples: xl-ekjd-8f7a-3s, xl-ekjd-8f7a-6s, xl-ekjd-8f7a-t
# Handle (all variants): xl-ekjd-8f7a
```

**Processing Logic:**
- Strip suffix to derive handle: `商品管理番号.replace(/-[a-z0-9]+$/, '')`
- Merge variant names for main product title
- Sort variants numerically by Option1 Value
- Assign sequential Variant Position (1, 2, 3...)

### 3. HTML Content Processing
**Critical Fixes Applied:**

#### A. Table Responsiveness (Mobile Fix)
```html
<!-- Input: Basic table -->
<table width="100%">
  <tr><td>商品詳細</td></tr>
</table>

<!-- Output: Mobile-responsive -->
<div style="overflow-x: auto; max-width: 100%;">
  <table style="width: 100%; max-width: 100%; table-layout: fixed; font-weight: normal;">
    <tr><td style="word-wrap: break-word;">商品詳細</td></tr>
  </table>
</div>
```

#### B. CSS Scoping (Theme Conflict Prevention)
```css
/* Input: Global CSS */
table { width: 100%; }
img { max-width: 600px; }

/* Output: Scoped CSS */
.shopify-product-description table { width: 100%; }
.shopify-product-description img { max-width: 100%; }
```

#### C. Marketing Content Removal
```html
<!-- Remove all Rakuten EC-UP blocks -->
<!--EC-UP_Rakuichi_123_START-->
<div>Rakuten promotional content</div>
<!--EC-UP_Rakuichi_123_END-->
<!-- ↑ This entire block gets removed -->
```

#### D. Font Weight Normalization
```html
<!-- Add to all table elements -->
<table style="font-weight: normal;">
```

### 4. Image Processing

#### A. Description Image URL Fixes
**Gold Pattern Fix (Cabinet URLs are correct):**
```python
def fix_image_url(url):
    # CORRECT Cabinet: https://image.rakuten.co.jp/tsutsu-uraura/cabinet/item.jpg ✅
    # WRONG Gold: https://image.rakuten.co.jp/tsutsu-uraura/gold/item.jpg ❌
    # FIXED Gold: https://image.rakuten.co.jp/gold/tsutsu-uraura/item.jpg ✅

    if 'tsutsu-uraura/gold/' in url:
        return url.replace('tsutsu-uraura/gold/', 'gold/tsutsu-uraura/')
    return url  # Cabinet URLs unchanged
```

#### B. CDN URL Generation
```python
# Generate Shopify CDN URLs for description images
original_url = "https://image.rakuten.co.jp/gold/tsutsu-uraura/detail.jpg"
filename = f"{handle}_detail.jpg"  # xl-ekjd-8f7a_detail.jpg
cdn_url = f"https://cdn.shopify.com/s/files/1/0637/6059/7127/files/{filename}"
```

### 5. Tax Classification System
**Japanese Tax Rates:**

#### 8% Reduced Rate Keywords
```python
food_keywords = [
    # Food and beverages
    '食品', '食材', '米', '肉', '魚', '野菜', '果物', 'パン', 'お菓子', 'スイーツ',
    '茶', 'コーヒー', '飲料', 'ジュース', '水', 'ミネラルウォーター',
    'カレー', 'ラーメン', 'うどん', 'そば', '味噌', '醤油', '調味料',
    # Health foods
    '酵素', 'サプリ', 'プロテイン', 'ビタミン', '健康食品', '栄養', '青汁'
]
```

#### 10% Standard Rate Keywords
```python
general_keywords = [
    # Electronics
    '電子', '電気', '家電', 'パソコン', 'PC', 'スマホ', 'カメラ', 'テレビ',
    # Household items
    '家具', '雑貨', 'インテリア', 'キッチン用品', '食器', '掃除', '洗剤',
    # Beauty & cosmetics
    '化粧品', 'コスメ', '美容', 'スキンケア', '香水',
    # Alcoholic beverages (10% tax in Japan)
    '酒', 'ワイン', 'ビール', '日本酒', '焼酎', 'ウイスキー', 'アルコール'
]
```

**Classification Logic:**
1. Check 商品マスタ for exact handle match
2. Check セット商品マスタ for set product codes
3. Apply keyword analysis on title + 商品カテゴリー + vendor
4. Count keyword matches for each rate
5. Default to 10% if uncertain

### 6. Metafield Mapping
**Standard Shopify Metafields:**
```json
{
  "custom.tax": "8%" | "10%",
  "custom.brand": "From ブランド名",
  "custom.size": "From 容量・サイズ",
  "custom.ingredients": "From 原材料",
  "custom.storage": "From 保存方法",
  "custom.allergy": "From アレルギー物質",
  "custom.area": "From 産地（都道府県）",
  "custom.attributes": "From 商品カテゴリー"
}
```

### 7. Product Type Assignment
**Business Rules:**
```python
type_mapping = {
    # Intelligent mapping
    '飲料＆ドリンク|ドリンク': '飲料・ドリンク',
    'ワイン|日本酒|焼酎|ウイスキー|ビール': 'お酒・ワイン',
    # Empty types → intelligent assignment based on keywords
}
```

### 8. Tag Generation
**Special Tags:**
```python
special_tags = {
    '販売形態（並行輸入品）': '並行輸入品',
    '販売形態（訳あり）': '訳あり',
    '食品配送状態': '冷凍' if '冷凍' in value else None
}
```

## Output CSV Structure

### Required Shopify Columns
```csv
Handle,Title,Body (HTML),Vendor,Product Category,Type,Tags,Published,
Option1 Name,Option1 Value,Option2 Name,Option2 Value,Option3 Name,Option3 Value,
Variant SKU,Variant Grams,Variant Inventory Tracker,Variant Inventory Qty,
Variant Inventory Policy,Variant Fulfillment Service,Variant Price,
Variant Compare At Price,Variant Requires Shipping,Variant Taxable,
Variant Barcode,Image Src,Image Position,Image Alt Text,Gift Card,
SEO Title,SEO Description,Google Shopping / Google Product Category,
Status,Variant Position
```

### Custom Metafield Columns
```csv
消費税率 (product.metafields.custom.tax),
ブランド・メーカー (product.metafields.custom.brand),
容量・サイズ(product.metafields.custom.size),
原材料名 (product.metafields.custom.ingredients),
保存方法 (product.metafields.custom.storage),
アレルギー物質 (product.metafields.custom.allergy),
ご当地 (product.metafields.custom.area),
商品カテゴリー (product.metafields.custom.attributes)
```

## Quality Validation Requirements

### Pre-Import Validation Checklist
- [ ] All products have valid handles (no empty/duplicate)
- [ ] HTML tables wrapped with responsive divs (`overflow-x: auto`)
- [ ] All CSS scoped with `.shopify-product-description`
- [ ] EC-UP marketing content completely removed
- [ ] Font-weight: normal applied to all tables
- [ ] Gold image URLs fixed (cabinet URLs unchanged)
- [ ] Description images reference Shopify CDN URLs
- [ ] Tax rates assigned (8% or 10%, no empty)
- [ ] Variant positions sequential (1, 2, 3...)
- [ ] Product types assigned (no empty for main products)
- [ ] Special characters properly escaped
- [ ] Encoding is UTF-8 for output

### Post-Conversion Validation
- [ ] Generated CSV imports successfully to Shopify
- [ ] Tables display correctly on mobile devices
- [ ] No CSS conflicts with active theme
- [ ] Tax metafields populated correctly
- [ ] Variants grouped and sorted properly
- [ ] Search functionality works with all products

## Implementation Notes

### Encoding Handling
```python
# Input: Always Shift-JIS/cp932 for Rakuten files
df = pd.read_csv(input_file, encoding='cp932', low_memory=False)

# Output: Always UTF-8 for Shopify
df.to_csv(output_file, encoding='utf-8', index=False)
```

### Memory Management
```python
# Process large files in chunks
chunk_size = 10000
for chunk in pd.read_csv(input_file, chunksize=chunk_size):
    process_chunk(chunk)
```

### Error Recovery
```python
# Graceful degradation for problematic rows
try:
    processed_html = process_html_complete(row['PC用商品説明文'])
except Exception:
    processed_html = fallback_html_processing(row['PC用商品説明文'])
```

## Performance Requirements
- **Input**: 500MB+ Rakuten CSV files
- **Processing Time**: < 30 minutes for full conversion
- **Memory Usage**: < 4GB RAM
- **Output**: Production-ready Shopify CSV
- **Success Rate**: > 99% of valid products converted

## Notes
- This converter incorporates ALL fixes discovered during production deployment
- No post-import API fixes should be required
- Description images require separate download/upload process
- Tax compliance is mandatory for Japanese market
- All HTML fixes prevent mobile layout issues
- CSS scoping prevents theme conflicts