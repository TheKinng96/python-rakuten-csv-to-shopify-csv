#!/usr/bin/env python3
"""
Apply tax data to Shopify product export CSVs

This script reads tax information from Japanese master files and applies it to
Shopify product export CSVs by matching Handle = 商品コード.

Usage:
    python 13_apply_tax_to_exports.py                    # Process all export files
    python 13_apply_tax_to_exports.py --dry-run          # Preview changes without modifying files
    python 13_apply_tax_to_exports.py --backup           # Create backup files before modification
    python 13_apply_tax_to_exports.py --file products_export_1.csv  # Process single file

Requirements:
    - セット商品マスタ_20250912.csv (SHIFT-JIS encoding)
    - 商品マスタ_20250912.csv (SHIFT-JIS encoding)
    - products_export_*.csv files (UTF-8 encoding)
"""

import csv
import json
import shutil
import argparse
from pathlib import Path
from datetime import datetime
from collections import defaultdict

class TaxDataApplicator:
    def __init__(self, data_dir='/Users/gen/corekara/rakuten-shopify/api-operations/data', backup=False, dry_run=False):
        self.data_dir = Path(data_dir)
        self.backup = backup
        self.dry_run = dry_run
        self.tax_data = {}
        self.set_code_mapping = {}  # Maps セット商品コード to tax rate
        self.results = {
            'files_processed': 0,
            'products_updated': 0,
            'products_matched': 0,
            'products_unmatched': 0,
            'errors': []
        }
        self.unmatched_products = []
        self.matched_products = []  # Track matched products for JSON export
        
        # Japanese tax classification keywords
        # 8% tax (reduced rate) - food, beverages, essential items
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
            # Cooking and food-related
            'レシピ', '食べ物', '食事', '朝食', '昼食', '夕食', '夜食', '間食', 'おやつ',
            # Common food suffixes/prefixes
            '味', '風味', 'フレーバー', '産', '県産', '国産', '有機', 'オーガニック'
        ]
        
        # 10% tax (standard rate) - general merchandise, electronics, etc.
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
            # Tools and equipment
            '工具', '道具', '機械', '器具', '用品', '用具', 'ツール', '箒', 'ほうき', 'ブラシ',
            # Stationery and books
            '文具', '文房具', 'ペン', 'ノート', '本', '書籍', '雑誌', 'カレンダー',
            # Toys and games
             'おもちゃ', 'ゲーム', 'パズル', 'フィギュア',
            # Sports and outdoor
            'スポーツ', '運動', 'アウトドア', 'キャンプ', '釣り',
            # Alcoholic beverages (standard 10% tax in Japan)
            '酒', 'ワイン', 'ビール', '日本酒', '焼酎', 'ウイスキー', 'ブランデー', 'リキュール', 
            'チューハイ', 'カクテル', 'サワー', 'ハイボール', 'シャンパン', 'スパークリング', 
            'ロゼ', '白ワイン', '赤ワイン', '甘酒', 'どぶろく', '梅酒', 'アルコール', 'エタノール',
            '酒造', '醸造', '蔵元', 'brewery', 'winery', 'distillery', 'sake', 'wine', 'beer', 'whisky',
            # General merchandise indicators
            'セット', 'キット', 'グッズ', 'アイテム', '商品', '製品', 'ブランド'
        ]
        
    def load_tax_data(self):
        """Load tax data from Japanese master files"""
        print("📊 Loading tax data from Japanese master files...")
        
        # Load セット商品マスタ
        set_products_file = self.data_dir / 'with-tax.csv'
        if set_products_file.exists():
            with open(set_products_file, 'r', encoding='shift-jis') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    product_code = row.get('商品コード', '').strip()
                    set_code = row.get('セット商品コード', '').strip()
                    tax_rate = row.get('消費税率（%）', '').strip()
                    
                    if tax_rate:
                        tax_formatted = f"{tax_rate}%"
                        
                        # Map both 商品コード and セット商品コード to tax rate (case-insensitive)
                        if product_code:
                            self.tax_data[product_code.lower()] = tax_formatted
                        if set_code:
                            self.set_code_mapping[set_code.lower()] = tax_formatted
            
            total_mappings = len(self.tax_data) + len(self.set_code_mapping)
            print(f"  ✅ Loaded from セット商品マスタ:")
            print(f"    商品コード mappings: {len(self.tax_data)}")
            print(f"    セット商品コード mappings: {len(self.set_code_mapping)}")
            print(f"    Total mappings: {total_mappings}")
        
        # Load 商品マスタ (will override duplicates from set products)
        products_file = self.data_dir / '商品マスタ_20250912.csv'
        if products_file.exists():
            initial_count = len(self.tax_data)
            with open(products_file, 'r', encoding='shift-jis') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    code = row['商品コード']
                    tax_rate = row['消費税率（%）']
                    if code and tax_rate:
                        # Format as percentage string for Shopify (case-insensitive)
                        self.tax_data[code.lower()] = f"{tax_rate}%"
            new_count = len(self.tax_data)
            print(f"  ✅ Loaded {new_count - initial_count} additional products from 商品マスタ")
        
        # Combine all tax data for statistics
        all_tax_data = {}
        all_tax_data.update(self.tax_data)
        all_tax_data.update(self.set_code_mapping)
        
        print(f"📋 Total unique tax mappings available: {len(all_tax_data)}")
        
        # Show tax rate distribution
        tax_distribution = defaultdict(int)
        for tax_rate in all_tax_data.values():
            tax_distribution[tax_rate] += 1
        
        print("📊 Tax rate distribution:")
        for rate, count in sorted(tax_distribution.items()):
            print(f"  {rate}: {count} mappings")
    
    def get_tax_rate_for_handle(self, handle):
        """Get tax rate for handle, checking both 商品コード and セット商品コード mappings (case-insensitive)"""
        handle_lower = handle.lower()
        
        # First check 商品コード mapping
        if handle_lower in self.tax_data:
            return self.tax_data[handle_lower]
        
        # Then check セット商品コード mapping
        if handle_lower in self.set_code_mapping:
            return self.set_code_mapping[handle_lower]
        
        # Not found in either mapping
        return None
    
    def suggest_tax_rate(self, title, vendor="", product_type=""):
        """Suggest tax rate based on product title and other attributes"""
        if not title:
            return "10%"  # Default to standard rate
        
        # Convert to lowercase for matching
        title_lower = title.lower()
        vendor_lower = vendor.lower() if vendor else ""
        type_lower = product_type.lower() if product_type else ""
        
        # Combine all text for analysis
        combined_text = f"{title_lower} {vendor_lower} {type_lower}"
        
        # Count matches for each tax rate
        tax_8_matches = sum(1 for keyword in self.tax_8_keywords if keyword in combined_text)
        tax_10_matches = sum(1 for keyword in self.tax_10_keywords if keyword in combined_text)
        
        # Determine tax rate based on matches
        if tax_8_matches > tax_10_matches:
            return "8%"
        elif tax_10_matches > tax_8_matches:
            return "10%"
        else:
            # No clear match or tie - use heuristics
            
            # Food-related indicators (favor 8%)
            food_indicators = ['味', 'フレーバー', '産', '県産', '国産', 'ml', 'g', 'kg', '袋', '個入り']
            if any(indicator in combined_text for indicator in food_indicators):
                return "8%"
            
            # Product/merchandise indicators (favor 10%)
            product_indicators = ['用品', '用具', 'セット', 'キット', 'cm', 'サイズ', '色']
            if any(indicator in combined_text for indicator in product_indicators):
                return "10%"
            
            # Default to standard rate
            return "10%"
    
    def backup_file(self, file_path):
        """Create backup of original file"""
        if not self.backup:
            return
            
        backup_path = file_path.with_suffix(f'.backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
        shutil.copy2(file_path, backup_path)
        print(f"  💾 Backup created: {backup_path.name}")
    
    def process_export_file(self, file_path):
        """Process a single export CSV file"""
        print(f"\n🔄 Processing {file_path.name}...")
        
        if not file_path.exists():
            error_msg = f"File not found: {file_path}"
            print(f"  ❌ {error_msg}")
            self.results['errors'].append(error_msg)
            return
        
        # Create backup if requested
        if self.backup:
            self.backup_file(file_path)
        
        # Read the file
        updated_rows = []
        file_stats = {
            'total': 0,
            'matched': 0,
            'unmatched': 0,
            'updated': 0,
            'skipped_variants': 0
        }
        
        # Track processed handles to identify main product vs variants
        processed_handles = set()
        
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            
            # Verify tax column exists
            tax_column = '消費税率 (product.metafields.custom.tax)'
            if tax_column not in fieldnames:
                error_msg = f"Tax column not found in {file_path.name}: {tax_column}"
                print(f"  ❌ {error_msg}")
                self.results['errors'].append(error_msg)
                return
            
            for row in reader:
                file_stats['total'] += 1
                handle = row['Handle']
                current_tax = row[tax_column].strip()
                
                # Check if this is the main product row (first occurrence of handle)
                is_main_product = handle not in processed_handles
                
                if is_main_product:
                    processed_handles.add(handle)
                    
                    # Check if we have tax data for this product (both mappings)
                    new_tax = self.get_tax_rate_for_handle(handle)
                    if new_tax:
                        file_stats['matched'] += 1
                        
                        # Track matched product for JSON export
                        title = row.get('Title', '').strip()
                        self.matched_products.append({
                            'handle': handle,
                            'tax_rate': new_tax,
                            'title': title,
                            'source_file': file_path.name
                        })
                        
                        # Update only if different
                        if current_tax != new_tax:
                            row[tax_column] = new_tax
                            file_stats['updated'] += 1
                        
                    else:
                        file_stats['unmatched'] += 1
                        # Track unmatched product details with intelligent tax suggestion
                        # Only for main product rows with actual titles
                        title = row.get('Title', '').strip()
                        vendor = row.get('Vendor', '').strip()
                        product_type = row.get('Type', '').strip()
                        
                        # Only add to unmatched list if it has meaningful content
                        if title or vendor or product_type:
                            suggested_tax = self.suggest_tax_rate(title, vendor, product_type)
                            
                            self.unmatched_products.append({
                                'handle': handle,
                                'title': title,
                                'vendor': vendor,
                                'type': product_type,
                                'source_file': file_path.name,
                                'current_tax': current_tax,
                                'suggested_tax_rate': suggested_tax
                            })
                else:
                    # This is a variant row - skip tax processing but copy any tax from main product
                    file_stats['skipped_variants'] += 1
                    # Variants inherit tax from main product if we have it
                    if not current_tax:
                        new_tax = self.get_tax_rate_for_handle(handle)
                        if new_tax:
                            row[tax_column] = new_tax
                
                updated_rows.append(row)
        
        # Write updated file (unless dry run)
        if not self.dry_run and file_stats['updated'] > 0:
            with open(file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(updated_rows)
            print(f"  ✅ File updated successfully")
        elif self.dry_run:
            print(f"  🔍 DRY RUN: Would update {file_stats['updated']} products")
        else:
            print(f"  ℹ️  No updates needed")
        
        # Print file statistics
        main_products = file_stats['matched'] + file_stats['unmatched']
        print(f"  📊 Statistics:")
        print(f"    Total rows: {file_stats['total']}")
        print(f"    Main products: {main_products}")
        print(f"    Variant rows (skipped): {file_stats['skipped_variants']}")
        print(f"    Matched with tax data: {file_stats['matched']} ({file_stats['matched']/main_products*100:.1f}% of main products)")
        print(f"    Products updated: {file_stats['updated']}")
        print(f"    Unmatched main products: {file_stats['unmatched']}")
        
        # Update overall results
        self.results['files_processed'] += 1
        self.results['products_updated'] += file_stats['updated']
        self.results['products_matched'] += file_stats['matched']
        self.results['products_unmatched'] += file_stats['unmatched']
    
    def process_all_exports(self, single_file=None):
        """Process all export files or a single specified file"""
        if single_file:
            # Process single file
            file_path = self.data_dir / single_file
            self.process_export_file(file_path)
        else:
            # Process all export files
            export_files = sorted(self.data_dir.glob('products_export_*.csv'))
            if not export_files:
                print("❌ No export files found matching pattern: products_export_*.csv")
                return
            
            print(f"📁 Found {len(export_files)} export files to process")
            for file_path in export_files:
                self.process_export_file(file_path)
    
    def generate_unmatched_csv_report(self):
        """Generate CSV report of unmatched products"""
        if not self.unmatched_products:
            print("ℹ️  No unmatched products to report")
            return
        
        # Create reports directory if it doesn't exist
        reports_dir = Path('api-operations/reports')
        reports_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate CSV report
        csv_file = reports_dir / 'unmatched_products_report.csv'
        
        fieldnames = ['handle', 'title', 'vendor', 'type', 'source_file', 'current_tax', 'suggested_tax_rate']
        
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.unmatched_products)
        
        print(f"📄 Unmatched products CSV report saved: {csv_file}")
        print(f"   Total unmatched products: {len(self.unmatched_products)}")
        
        # Show breakdown by source file
        file_counts = defaultdict(int)
        for product in self.unmatched_products:
            file_counts[product['source_file']] += 1
        
        print("📊 Unmatched products by source file:")
        for file_name, count in sorted(file_counts.items()):
            print(f"  {file_name}: {count} products")
        
        # Show tax suggestion distribution
        suggestion_counts = defaultdict(int)
        for product in self.unmatched_products:
            suggestion_counts[product['suggested_tax_rate']] += 1
        
        print("📊 Tax rate suggestions for unmatched products:")
        for rate, count in sorted(suggestion_counts.items()):
            print(f"  {rate}: {count} products")
    
    def generate_tax_update_json(self):
        """Generate JSON file for GraphQL script with handle→tax_rate mappings"""
        if not self.matched_products and not self.unmatched_products:
            print("ℹ️  No products to export to JSON")
            return
        
        # Create reports directory if it doesn't exist
        reports_dir = Path('api-operations/shared')
        reports_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare data for JSON export
        all_products = []
        
        # Add matched products (from master files)
        for product in self.matched_products:
            all_products.append({
                'handle': product['handle'],
                'tax_rate': product['tax_rate'],
                'title': product['title'],
                'source': 'master_file'
            })
        
        # Add unmatched products with intelligent suggestions
        for product in self.unmatched_products:
            all_products.append({
                'handle': product['handle'],
                'tax_rate': product['suggested_tax_rate'],
                'title': product['title'],
                'source': 'intelligent_suggestion'
            })
        
        json_data = {
            'timestamp': datetime.now().isoformat(),
            'description': 'Tax rate mappings for Shopify GraphQL updates (matched + suggested)',
            'total_products': len(all_products),
            'matched_count': len(self.matched_products),
            'suggested_count': len(self.unmatched_products),
            'data': all_products
        }
        
        # Save JSON file
        json_file = reports_dir / 'tax_data_to_update.json'
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
        
        print(f"📄 Tax update JSON saved: {json_file}")
        print(f"   Total products for Shopify update: {len(all_products)}")
        print(f"   From master files: {len(self.matched_products)}")
        print(f"   From intelligent suggestions: {len(self.unmatched_products)}")
        
        # Show tax rate distribution for all updates
        update_distribution = defaultdict(int)
        source_distribution = defaultdict(int)
        
        for product in all_products:
            update_distribution[product['tax_rate']] += 1
            source_distribution[product['source']] += 1
        
        print("📊 Tax rates to be updated in Shopify:")
        for rate, count in sorted(update_distribution.items()):
            print(f"  {rate}: {count} products")
        
        print("📊 Sources breakdown:")
        for source, count in sorted(source_distribution.items()):
            print(f"  {source}: {count} products")

    def generate_report(self):
        """Generate summary report"""
        print(f"\n📋 TAX APPLICATION SUMMARY")
        print(f"{'='*50}")
        print(f"Files processed: {self.results['files_processed']}")
        print(f"Products updated: {self.results['products_updated']}")
        print(f"Products matched: {self.results['products_matched']}")
        print(f"Products unmatched: {self.results['products_unmatched']}")
        
        if self.results['products_matched'] + self.results['products_unmatched'] > 0:
            total = self.results['products_matched'] + self.results['products_unmatched']
            match_rate = self.results['products_matched'] / total * 100
            print(f"Match rate: {match_rate:.1f}%")
        
        if self.results['errors']:
            print(f"\n❌ Errors encountered:")
            for error in self.results['errors']:
                print(f"  - {error}")
        
        # Save detailed report
        report_file = Path('api-operations/reports/tax_application_report.json')
        report_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Calculate tax distribution first
        tax_distribution = defaultdict(int)
        for tax_rate in self.tax_data.values():
            tax_distribution[tax_rate] += 1
        
        report_data = {
            'timestamp': datetime.now().isoformat(),
            'settings': {
                'dry_run': self.dry_run,
                'backup': self.backup
            },
            'results': self.results,
            'tax_data_stats': {
                'total_available': len(self.tax_data),
                'distribution': dict(tax_distribution)
            }
        }
        
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False)
        
        print(f"\n📄 Detailed report saved: {report_file}")
        
        # Generate CSV report for unmatched products
        self.generate_unmatched_csv_report()
        
        # Generate JSON file for GraphQL updates
        self.generate_tax_update_json()

def main():
    parser = argparse.ArgumentParser(description='Apply tax data to Shopify export CSVs')
    parser.add_argument('--dry-run', action='store_true', 
                       help='Preview changes without modifying files')
    parser.add_argument('--backup', action='store_true',
                       help='Create backup files before modification')
    parser.add_argument('--file', type=str,
                       help='Process single file instead of all exports')
    parser.add_argument('--data-dir', type=str, default='api-operations/data',
                       help='Directory containing data files (relative to current working directory)')
    
    args = parser.parse_args()
    
    print("🏷️  TAX DATA APPLICATOR")
    print("="*50)
    
    if args.dry_run:
        print("🔍 DRY RUN MODE: No files will be modified")
    if args.backup:
        print("💾 BACKUP MODE: Original files will be backed up")
    
    try:
        applicator = TaxDataApplicator(
            data_dir=args.data_dir,
            backup=args.backup,
            dry_run=args.dry_run
        )
        
        # Load tax data from master files
        applicator.load_tax_data()
        
        if len(applicator.tax_data) == 0:
            print("❌ No tax data loaded. Please check master files exist and are readable.")
            return
        
        # Process export files
        applicator.process_all_exports(single_file=args.file)
        
        # Generate summary report
        applicator.generate_report()
        
        print("\n✅ Tax application completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise

if __name__ == '__main__':
    main()