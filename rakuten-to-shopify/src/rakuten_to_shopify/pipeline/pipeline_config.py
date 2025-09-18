"""
Pipeline Configuration

Centralized configuration for all pipeline steps based on analysis
of manual/final, API fixes, and production requirements.
"""

import re
from pathlib import Path


class PipelineConfig:
    """Configuration for the complete Rakuten to Shopify pipeline"""

    def __init__(self, config_file=None):
        # === File Paths ===
        self.rakuten_encoding = "cp932"  # Shift-JIS
        self.output_encoding = "utf-8"

        # === Shopify Configuration ===
        self.image_domain = "https://tshop.r10s.jp/tsutsu-uraura"
        self.cdn_base_url = "https://cdn.shopify.com/s/files/1/0637/6059/7127/files/"
        self.scope_class = "shopify-product-description"

        # === Processing Settings ===
        self.chunk_size = 10000
        self.max_images_per_product = 20
        self.max_attributes_per_product = 100

        # === SKU and Handle Processing ===
        self.ss_sku_suffix = "-ss"
        self.catalog_id_rakuten_key = "カタログID"
        self.catalog_id_shopify_column = "カタログID (rakuten)"

        # === Special Tags Mapping ===
        self.special_tags = {
            "販売形態（並行輸入品）": "label__並行輸入品",
            "販売形態（訳あり）": "label__訳あり",
        }

        self.free_tag_keys = {"食品配送状態", "セット種別"}

        # === Category Exclusion List ===
        self.category_exclusion_list = {
            "食品", "飲料・ドリンク", "調味料", "お酒・ワイン", "ヘルス・ビューティー",
            "サプリメント・ダイエット・健康", "ホーム・キッチン", "ペットフード・ペット用品"
        }

        # === Gojuon Character Filtering ===
        self.gojuon_chars = {
            'あ', 'い', 'う', 'え', 'お', 'か', 'き', 'く', 'け', 'こ', 'さ', 'し', 'す', 'せ', 'そ',
            'た', 'ち', 'つ', 'て', 'と', 'な', 'に', 'ぬ', 'ね', 'の', 'は', 'ひ', 'ふ', 'へ', 'ほ',
            'ま', 'み', 'む', 'め', 'も', 'や', 'ゆ', 'よ', 'ら', 'り', 'る', 'れ', 'ろ', 'わ', 'を', 'ん',
            'ア', 'イ', 'ウ', 'エ', 'オ', 'カ', 'キ', 'ク', 'ケ', 'コ', 'サ', 'シ', 'ス', 'セ', 'ソ',
            'タ', 'チ', 'ツ', 'テ', 'ト', 'ナ', 'ニ', 'ヌ', 'ネ', 'ノ', 'ハ', 'ヒ', 'フ', 'ヘ', 'ホ',
            'マ', 'ミ', 'ム', 'メ', 'モ', 'ヤ', 'ユ', 'ヨ', 'ラ', 'リ', 'ル', 'レ', 'ロ', 'ワ', 'ヲ', 'ン'
        }

        # Build regex patterns for Gojuon filtering
        gojuon_regex_chars = "".join(self.gojuon_chars)
        self.gojuon_row_pattern = re.compile(f'^[{gojuon_regex_chars}]\\s*行$')
        self.gojuon_sono_pattern = re.compile(f'^[{gojuon_regex_chars}]（その[０-９0-9]+）$')

        # === Tax Classification ===
        # 8% reduced rate keywords (food, beverages, essential items)
        self.tax_8_keywords = [
            # Food and beverages
            '食品', '食材', '米', '肉', '魚', '野菜', '果物', 'パン', 'お菓子', 'スイーツ', '和菓子',
            '茶', 'コーヒー', '飲料', 'ジュース', '水', 'ミネラルウォーター',
            'カレー', 'ラーメン', 'うどん', 'そば', '味噌', '醤油', '調味料', 'ソース', 'ドレッシング',
            'オイル', '油', '塩', '砂糖', '蜂蜜', 'はちみつ', '酢', 'みりん', '料理', '弁当', '惣菜',
            # Specific food items
            'カルピス', 'バター', 'チーズ', 'ヨーグルト', '牛乳', 'ミルク', '卵', 'たまご',
            '酵素', 'サプリ', 'プロテイン', 'ビタミン', '健康食品', '栄養', '青汁',
            'にんにく', 'しょうが', '生姜', '七味', 'わさび', '柚子', 'ゆず', 'かぼす',
        ]

        # 10% standard rate keywords (general merchandise, electronics, alcohol)
        self.tax_10_keywords = [
            # Electronics and appliances
            '電子', '電気', '家電', 'パソコン', 'PC', 'スマホ', 'スマートフォン', 'カメラ', 'テレビ',
            # Household items
            '家具', '雑貨', 'インテリア', 'キッチン用品', '食器', 'グラス', 'カップ', 'プレート',
            '掃除', 'クリーナー', '洗剤', 'シャンプー', 'ソープ', '石鹸', 'タオル', 'シーツ',
            # Beauty and cosmetics
            '化粧品', 'コスメ', '美容', 'スキンケア', '香水', 'パフューム', 'クリーム', 'ローション',
            # Clothing and accessories
            '服', '衣類', 'ファッション', 'バッグ', '靴', 'シューズ', 'アクセサリー', '時計',
            # Alcoholic beverages (standard 10% tax in Japan)
            '酒', 'ワイン', 'ビール', '日本酒', '焼酎', 'ウイスキー', 'ブランデー', 'リキュール',
            'チューハイ', 'カクテル', 'サワー', 'ハイボール', 'シャンパン', 'スパークリング',
            'アルコール', 'エタノール', '酒造', '醸造', '蔵元',
        ]

        self.default_tax_rate = "10%"
        self.reduced_tax_rate = "8%"

        # === Product Type Mapping ===
        self.type_mapping = {
            '飲料＆ドリンク': '飲料・ドリンク',
            'ドリンク': '飲料・ドリンク',
            'ワイン': 'お酒・ワイン',
            '日本酒': 'お酒・ワイン',
            '焼酎': 'お酒・ワイン',
            'ウイスキー': 'お酒・ワイン',
            'ビール': 'お酒・ワイン',
        }

        # === CSV Special Formatting ===
        self.special_quoted_empty_fields = {'Type', 'Tags', 'Variant Barcode'}

        # === Standard Shopify CSV Header (86 columns total) ===
        self.standard_header = [
            "Handle", "Title", "Body (HTML)", "Vendor", "Product Category", "Type", "Tags", "Published",
            "Option1 Name", "Option1 Value", "Option1 Linked To", "Option2 Name", "Option2 Value",
            "Option2 Linked To", "Option3 Name", "Option3 Value", "Option3 Linked To", "Variant SKU",
            "Variant Grams", "Variant Inventory Tracker", "Variant Inventory Qty", "Variant Inventory Policy",
            "Variant Fulfillment Service", "Variant Price", "Variant Compare At Price", "Variant Requires Shipping",
            "Variant Taxable", "Variant Barcode", "Image Src", "Image Position", "Image Alt Text", "Gift Card",
            "SEO Title", "SEO Description"
        ]

        # === Custom Metafield Columns ===
        self.custom_metafields = [
            "アレルギー物質 (product.metafields.custom.allergy)",
            "[絞込み]ご当地 (product.metafields.custom.area)",
            "[絞込み]商品カテゴリー (product.metafields.custom.attributes)",
            "消費期限 (product.metafields.custom.best-before)",
            "[絞込み]ブランド・メーカー (product.metafields.custom.brand)",
            "[絞込み]こだわり・認証 (product.metafields.custom.commitment)",
            "[絞込み]成分・特性 (product.metafields.custom.component)",
            "食品の状態 (product.metafields.custom.condition)",
            "[絞込み]迷ったら (product.metafields.custom.doubt)",
            "その他 (product.metafields.custom.etc)",
            "[絞込み]季節イベント (product.metafields.custom.event)",
            "賞味期限 (product.metafields.custom.expiration-area)",
            "[絞込み]味・香り・フレーバー (product.metafields.custom.flavor)",
            "原材料名 (product.metafields.custom.ingredients)",
            "使用場所 (product.metafields.custom.location)",
            "名称 (product.metafields.custom.name)",
            "栄養成分表示 (product.metafields.custom.ngredient_list)",
            "[絞込み]お酒の分類 (product.metafields.custom.osake)",
            "[絞込み]ペットフード・用品分類 (product.metafields.custom.petfood)",
            "肌の悩み (product.metafields.custom.problem)",
            "[絞込み]容量・サイズ (product.metafields.custom.search_size)",
            "シリーズ名 (product.metafields.custom.series)",
            "肌質 (product.metafields.custom.skin)",
            "保存方法 (product.metafields.custom.storage)",
            "対象害虫 (product.metafields.custom.target)",
            "消費税率 (product.metafields.custom.tax)",
            "内容量 (product.metafields.custom.weight)",
            "[絞込み]ギフト (product.metafields.custom._gift)"
        ]

        # === Shopify Metafield Columns ===
        self.shopify_metafields = [
            "商品評価数 (product.metafields.reviews.rating_count)",
            "年齢層 (product.metafields.shopify.age-group)",
            "アレルゲン情報 (product.metafields.shopify.allergen-information)",
            "色 (product.metafields.shopify.color-pattern)",
            "構成成分 (product.metafields.shopify.constitutive-ingredients)",
            "国 (product.metafields.shopify.country)",
            "食事の好み (product.metafields.shopify.dietary-preferences)",
            "飲料用素材 (product.metafields.shopify.drinkware-material)",
            "辛味レベル (product.metafields.shopify.heat-level)",
            "素材 (product.metafields.shopify.material)",
            "製品形態 (product.metafields.shopify.product-form)",
            "スキンケア効果 (product.metafields.shopify.skin-care-effect)",
            "肌質に適している (product.metafields.shopify.suitable-for-skin-type)",
            "ワインの甘さ (product.metafields.shopify.wine-sweetness)",
            "ワインの種類 (product.metafields.shopify.wine-variety)",
            "付属商品 (product.metafields.shopify--discovery--product_recommendation.complementary_products)",
            "関連商品 (product.metafields.shopify--discovery--product_recommendation.related_products)",
            "関連商品の設定 (product.metafields.shopify--discovery--product_recommendation.related_products_display)",
            "販売促進する商品を検索する (product.metafields.shopify--discovery--product_search_boost.queries)"
        ]

        # === Additional Columns ===
        self.additional_columns = [
            "Variant Image", "Variant Weight Unit", "Variant Tax Code", "Cost per item", "Status"
        ]

        # Complete header (86 columns)
        self.complete_header = (
            self.standard_header +
            self.custom_metafields +
            self.shopify_metafields +
            self.additional_columns
        )

        # === HTML Cleaning Patterns ===
        self.ec_up_pattern = re.compile(
            r'<!--EC-UP_[^_]+_\d+_START-->.*?<!--EC-UP_[^_]+_\d+_END-->',
            re.IGNORECASE | re.DOTALL
        )

        self.marketing_patterns = [
            re.compile(r"この商品のお買い得なセットはこちらから"),
            re.compile(r"その他の商品はこちらから"),
            re.compile(r"よく一緒に購入されている商品はこちら"),
            re.compile(r"類似商品はこちら"),
            re.compile(r"再入荷しました")
        ]

        self.link_patterns = [
            re.compile(r"my\.bookmark\.rakuten\.co\.jp"),
            re.compile(r"(item|search)\.rakuten\.co\.jp")
        ]

    def derive_handle(self, sku: str) -> str:
        """Derive Shopify handle from Rakuten SKU"""
        if not isinstance(sku, str) or not sku:
            return sku

        # Remove variant suffixes like -3s, -6s, -t, -12s
        if sku.endswith('s') and '-' in sku:
            parts = sku.rsplit('-', 1)
            if len(parts) == 2 and parts[1][:-1].isdigit():
                return parts[0]

        return sku

    def get_set_count(self, sku: str) -> str:
        """Extract set count from variant SKU"""
        match = re.search(r'-(\d+)s$', sku)
        return match.group(1) if match else "1"

    def fix_gold_url(self, url: str) -> str:
        """Fix gold image URL pattern"""
        if not url or 'tsutsu-uraura/gold/' not in url:
            return url
        return url.replace('tsutsu-uraura/gold/', 'gold/tsutsu-uraura/')

    def to_absolute_url(self, src: str) -> str:
        """Convert relative URL to absolute"""
        if not src or src.startswith(('http://', 'https://')):
            return src
        return f"{self.image_domain}/{src.lstrip('/')}"

    def format_csv_value(self, value, header_name):
        """Format value for CSV output with special handling"""
        if header_name in self.special_quoted_empty_fields and (value is None or str(value).strip() == ''):
            return '""'
        if value is None or str(value).strip() == '':
            return ''

        s_value = str(value)
        if '"' in s_value or ',' in s_value or '\n' in s_value:
            escaped_value = s_value.replace('"', '""')
            return f'"{escaped_value}"'
        return s_value