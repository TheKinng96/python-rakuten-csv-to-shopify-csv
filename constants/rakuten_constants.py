"""
Constants for Rakuten CSV headers.
This module contains all the column names used in the Rakuten CSV format.
"""

from typing import Final

# Product Identification
PRODUCT_MANAGEMENT_NUMBER: Final[str] = "商品管理番号（商品URL）"
PRODUCT_NUMBER: Final[str] = "商品番号"
PRODUCT_NAME: Final[str] = "商品名"
CATALOG_ID: Final[str] = "カタログID"
CATALOG_ID_REASON: Final[str] = "カタログIDなしの理由"
SET_PRODUCT_CATALOG_ID: Final[str] = "セット商品用カタログID"

# Warehouse and Display Settings
WAREHOUSE_SPECIFICATION: Final[str] = "倉庫指定"
SEARCH_DISPLAY: Final[str] = "サーチ表示"
INVENTORY_DISPLAY: Final[str] = "在庫表示"
SKU_WAREHOUSE_SPECIFICATION: Final[str] = "SKU倉庫指定"

# Tax Information
CONSUMPTION_TAX: Final[str] = "消費税"
CONSUMPTION_TAX_RATE: Final[str] = "消費税率"

# Sales Period
SALES_PERIOD_START: Final[str] = "販売期間指定（開始日時）"
SALES_PERIOD_END: Final[str] = "販売期間指定（終了日時）"

# Points
POINTS_MULTIPLIER: Final[str] = "ポイント変倍率"
POINTS_PERIOD_START: Final[str] = "ポイント変倍率適用期間（開始日時）"
POINTS_PERIOD_END: Final[str] = "ポイント変倍率適用期間（終了日時）"

# Order Settings
ORDER_BUTTON: Final[str] = "注文ボタン"
RESERVATION_RELEASE_DATE: Final[str] = "予約商品発売日"
INQUIRY_BUTTON: Final[str] = "商品問い合わせボタン"
DARK_MARKET_PASSWORD: Final[str] = "闇市パスワード"
CASH_ON_DELIVERY: Final[str] = "代引料"

# Category and Tags
GENRE_ID: Final[str] = "ジャンルID"
NON_PRODUCT_ATTRIBUTE_TAG_ID: Final[str] = "非製品属性タグID"

# Product Descriptions
CATCH_COPY: Final[str] = "キャッチコピー"
PC_DESCRIPTION: Final[str] = "PC用商品説明文"
SMARTPHONE_DESCRIPTION: Final[str] = "スマートフォン用商品説明文"
PC_SALES_DESCRIPTION: Final[str] = "PC用販売説明文"

# Images
VIDEO: Final[str] = "動画"
WHITE_BACKGROUND_TYPE: Final[str] = "白背景画像タイプ"
WHITE_BACKGROUND_PATH: Final[str] = "白背景画像パス"

# Layout Settings
PRODUCT_INFO_LAYOUT: Final[str] = "商品情報レイアウト"
HEADER_FOOTER_LEFTNAV: Final[str] = "ヘッダー・フッター・レフトナビ"
DISPLAY_ITEM_ORDER: Final[str] = "表示項目の並び順"
COMMON_DESCRIPTION_SMALL: Final[str] = "共通説明文（小）"
FEATURED_PRODUCT: Final[str] = "目玉商品"
COMMON_DESCRIPTION_LARGE: Final[str] = "共通説明文（大）"
REVIEW_TEXT_DISPLAY: Final[str] = "レビュー本文表示"
MANUFACTURER_INFO_DISPLAY: Final[str] = "メーカー提供情報表示"

# Variants and Options
VARIATION_KEY_DEFINITION: Final[str] = "バリエーション項目キー定義"
VARIATION_NAME_DEFINITION: Final[str] = "バリエーション項目名定義"
VARIATION_OPTION_TYPE: Final[str] = "選択肢タイプ"
PRODUCT_OPTION_NAME: Final[str] = "商品オプション項目名"
PRODUCT_OPTION_REQUIRED: Final[str] = "商品オプション選択必須"

# SKU Information
SKU_MANAGEMENT_NUMBER: Final[str] = "SKU管理番号"
SKU_SYSTEM_NUMBER: Final[str] = "システム連携用SKU番号"
SKU_IMAGE_TYPE: Final[str] = "SKU画像タイプ"
SKU_IMAGE_PATH: Final[str] = "SKU画像パス"
SKU_IMAGE_ALT: Final[str] = "SKU画像名（ALT）"

# Pricing
SALE_PRICE: Final[str] = "販売価格"
DISPLAY_PRICE: Final[str] = "表示価格"
DUAL_PRICE_TEXT_NUMBER: Final[str] = "二重価格文言管理番号"

# Inventory
ORDER_RECEIPT_COUNT: Final[str] = "注文受付数"
RESTOCK_NOTIFICATION: Final[str] = "再入荷お知らせボタン"
NOSHI_SUPPORT: Final[str] = "のし対応"
INVENTORY_QUANTITY: Final[str] = "在庫数"
INVENTORY_RETURN_FLAG: Final[str] = "在庫戻しフラグ"
OUT_OF_STOCK_ORDER: Final[str] = "在庫切れ時の注文受付"

# Lead Time
IN_STOCK_LEAD_TIME_NUMBER: Final[str] = "在庫あり時納期管理番号"
OUT_OF_STOCK_LEAD_TIME_NUMBER: Final[str] = "在庫切れ時納期管理番号"
IN_STOCK_SHIPPING_LEAD_TIME: Final[str] = "在庫あり時出荷リードタイム"
OUT_OF_STOCK_SHIPPING_LEAD_TIME: Final[str] = "在庫切れ時出荷リードタイム"
SHIPPING_LEAD_TIME: Final[str] = "配送リードタイム"

# Shipping
SHIPPING_METHOD_SET_NUMBER: Final[str] = "配送方法セット管理番号"
SHIPPING_FEE: Final[str] = "送料"
SHIPPING_CATEGORY_1: Final[str] = "送料区分1"
SHIPPING_CATEGORY_2: Final[str] = "送料区分2"
INDIVIDUAL_SHIPPING: Final[str] = "個別送料"
REGIONAL_INDIVIDUAL_SHIPPING_NUMBER: Final[str] = "地域別個別送料管理番号"
SINGLE_ITEM_SHIPPING_SETTING: Final[str] = "単品配送設定使用"

# Product Attributes
# Note: These are generated for attributes 1-100
def get_attribute_constants():
    """Generate constants for product attributes 1-100."""
    attributes = {}
    for i in range(1, 101):
        attributes[f'PRODUCT_ATTRIBUTE_ITEM_{i}'] = f'商品属性（項目）{i}'
        attributes[f'PRODUCT_ATTRIBUTE_VALUE_{i}'] = f'商品属性（値）{i}'
        attributes[f'PRODUCT_ATTRIBUTE_UNIT_{i}'] = f'商品属性（単位）{i}'
    return attributes

# Free Input Fields
# Note: These are generated for free input fields 1-5
def get_free_input_constants():
    """Generate constants for free input fields 1-5."""
    free_inputs = {}
    for i in range(1, 6):
        free_inputs[f'FREE_INPUT_ITEM_{i}'] = f'自由入力行（項目）{i}'
        free_inputs[f'FREE_INPUT_VALUE_{i}'] = f'自由入力行（値）{i}'
    return free_inputs

# Image Constants
# Note: These are generated for images 1-20
def get_image_constants():
    """Generate constants for product images 1-20."""
    images = {}
    for i in range(1, 21):
        images[f'IMAGE_TYPE_{i}'] = f'商品画像タイプ{i}'
        images[f'IMAGE_PATH_{i}'] = f'商品画像パス{i}'
        images[f'IMAGE_ALT_{i}'] = f'商品画像名（ALT）{i}'
    return images

# Variation Constants
# Note: These are generated for variations 1-6
def get_variation_constants():
    """Generate constants for variations 1-6."""
    variations = {}
    for i in range(1, 7):
        variations[f'VARIATION_{i}_OPTION'] = f'バリエーション{i}選択肢定義'
    return variations

# Product Option Constants
# Note: These are generated for product options 1-100
def get_product_option_constants():
    """Generate constants for product options 1-100."""
    options = {}
    for i in range(1, 101):
        options[f'PRODUCT_OPTION_{i}'] = f'商品オプション選択肢{i}'
    return options

# Variation Item Constants
# Note: These are generated for variation items 1-6
def get_variation_item_constants():
    """Generate constants for variation items 1-6."""
    items = {}
    for i in range(1, 7):
        items[f'VARIATION_ITEM_KEY_{i}'] = f'バリエーション項目キー{i}'
        items[f'VARIATION_ITEM_OPTION_{i}'] = f'バリエーション項目選択肢{i}'
    return items

# Update the module's namespace with generated constants
globals().update(get_attribute_constants())
globals().update(get_free_input_constants())
globals().update(get_image_constants())
globals().update(get_variation_constants())
globals().update(get_product_option_constants())
globals().update(get_variation_item_constants()) 