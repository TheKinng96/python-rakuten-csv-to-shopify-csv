# Rakuten to Shopify planning
Handle => 商品管理番号（商品URL）
Title => 商品名
Body (HTML) => PC用商品説明文 | スマートフォン用商品説明文 | PC用販売説明文
Vendor => にっぽん津々浦々 (brand name)
Product Category => x
Type => ジャンルID
Published => true
Option1 Name => バリエーション項目名定義
Option1 Value => バリエーション項目キー定義
Option1 Linked To => x
Option2 Name => x
Option2 Value => x
Option2 Linked To => x
Option3 Name => x
Option3 Value => x
Option3 Linked To => x
Variant SKU => SKU管理番号
Variant Grams => get from 商品属性, there will have 単品重量
Variant Inventory Qty => 在庫数
Variant Inventory Tracker => shopify
Variant Inventory Policy => deny
Variant Fulfillment Service => manual
Variant Price => 販売価格
Variant Compare At Price => 表示価格
Variant Requires Shipping => true
Variant Taxable => true
Variant Barcode => empty

| if a product has more than 1 image, eg 商品画像タイプ2, rakuten is possible to have up to 20, distributed using 商品画像タイプ{number}

Image Src => https://tshop.r10s.jp/tsutsu-uraura/商品画像タイプ１/商品画像パス１
Image Position => 1 (following the {number})
Image Alt Text => 商品画像名(ALT) 1 (following the {number})
Gift Card => false
SEO Title => Title
SEO Description => キャッチコピー
avg_rating (product.metafields.demo.avg_rating) => 
Closure type (product.metafields.shopify.closure-type) => 
Color (product.metafields.shopify.color-pattern) => 
Footwear material (product.metafields.shopify.footwear-material) => 
Target gender (product.metafields.shopify.target-gender) => 
Snowboard binding mount (product.metafields.test_data.binding_mount) => 
Snowboard length (product.metafields.test_data.snowboard_length) => 
Variant Image => 
Variant Weight Unit => g
Variant Tax Code => x
Cost per item => empty
Status => 
Collection => get the longest one with \, eg 調味料\食用油＆オイル\えごま油, and pick えごま油
Tags => get all keywords on 表示先カテゴリ eg 調味料\食用油＆オイル\えごま油, then remove \ and set all to tags following format Tag 1, Tag 2, Tag 3

search for SKU管理番号 then group products, rules
- 商品名共通部分み残して統合
- SKU商品名をキーに統合
- 共通ワードカット

metafields process
- add all to store maybe with matrixify?

## Shopify Variant Rules

| Shopify Field | Rakuten Field | Rules/Notes |
|---------------|---------------|-------------|
| Option1 Name | バリエーション項目名定義 | Use the first variation option name |
| Option1 Value | バリエーション項目キー定義 | Use the corresponding value for the first option |
| Option2 Name | - | Leave empty (not used in example) |
| Option2 Value | - | Leave empty (not used in example) |
| Option3 Name | - | Leave empty (not used in example) |
| Option3 Value | - | Leave empty (not used in example) |
| SKU | SKU管理番号 | Use the SKU management number |
| Grams | 商品属性（単品重量） | Extract from metafields, convert to grams |
| Inventory Qty | 在庫数 | Use the inventory quantity |
| Inventory Tracker | - | Set to "shopify" |
| Inventory Policy | - | Set to "deny" |
| Fulfillment Service | - | Set to "manual" |
| Price | 販売価格 | Use the selling price |
| Compare At Price | 表示価格 | Use the display price |
| Requires Shipping | - | Set to "true" |
| Taxable | - | Set to "true" |
| Barcode | - | Leave empty |
| Weight Unit | - | Set to "g" |
| Tax Code | - | Leave empty |
| Cost per item | - | Leave empty |
| Status | - | Set to "active" |
| Image Src | 商品画像タイプ{number}/商品画像パス{number} | Format: https://tshop.r10s.jp/tsutsu-uraura/{path} |
| Image Position | - | Set to position number (1, 2, 3, etc.) |
| Image Alt Text | 商品画像名(ALT) {number} | Use the ALT text with corresponding number |

### Variant Grouping Rules
- Group products by SKU管理番号
- Keep common parts of product names when merging
- Use SKU product name as key for merging
- Remove common words when merging variants

### Metafields Processing
- Add all metafields to store (possibly with matrixify)
- Include units in metafield values where applicable

# New Planning
1. Create Smart Collections with admin GraphQL
2. Rakuten -> Shopify CSV through Shopify products import
  - limitation: 1 product can only assigned to 1 collection
  - use tags for smart collection rule matching