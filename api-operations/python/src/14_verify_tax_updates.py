#!/usr/bin/env python3
"""
Verify tax metafield updates in Shopify

This script verifies that tax metafields have been correctly updated in Shopify
by comparing the actual values with expected values from the master files.

Usage:
    python 14_verify_tax_updates.py                           # Verify all products
    python 14_verify_tax_updates.py --sample 100              # Verify random sample
    python 14_verify_tax_updates.py --handle product-handle   # Verify single product
    python 14_verify_tax_updates.py --failed-only             # Only verify previously failed updates

Requirements:
    - GraphQL access to Shopify store
    - Tax data from Japanese master files
    - Update results from previous runs
"""

import csv
import json
import requests
import random
import argparse
from pathlib import Path
from datetime import datetime
from collections import defaultdict

class TaxMetafieldVerifier:
    def __init__(self, config_path='../node/src/config.js', sample_size=None):
        self.sample_size = sample_size
        self.shopify_config = self.load_shopify_config(config_path)
        self.tax_data = {}
        self.verification_results = {
            'verified_correct': [],
            'verified_incorrect': [],
            'not_found': [],
            'no_tax_metafield': [],
            'connection_errors': []
        }
        
        self.stats = {
            'total_checked': 0,
            'correct': 0,
            'incorrect': 0,
            'not_found': 0,
            'no_metafield': 0,
            'errors': 0
        }
    
    def load_shopify_config(self, config_path):
        """Load Shopify configuration from Node.js config file"""
        print("🔧 Loading Shopify configuration...")
        
        # For now, we'll use environment variables or hardcoded test values
        # In production, you'd parse the actual config.js file
        config = {
            'shop_url': 'your-test-store.myshopify.com',
            'access_token': 'your-access-token',
            'api_version': '2025-07'
        }
        
        # Check if config file exists and try to extract values
        if Path(config_path).exists():
            print(f"  📂 Config file found: {config_path}")
            print("  ⚠️  Please ensure your Shopify credentials are properly configured")
        else:
            print(f"  ⚠️  Config file not found: {config_path}")
            print("  ℹ️  Using environment variables or default values")
        
        return config
    
    def load_tax_data(self):
        """Load expected tax data from master files"""
        print("📊 Loading expected tax data...")
        
        data_dir = Path('../data')
        
        # Load セット商品マスタ
        set_products_file = data_dir / 'セット商品マスタ_20250912.csv'
        if set_products_file.exists():
            with open(set_products_file, 'r', encoding='shift-jis') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    code = row['商品コード']
                    tax_rate = row['消費税率（%）']
                    if code and tax_rate:
                        # Format as percentage string for Shopify
                        self.tax_data[code] = f"{tax_rate}%"
            print(f"  ✅ Loaded {len(self.tax_data)} products from セット商品マスタ")
        
        # Load 商品マスタ (will override duplicates)
        products_file = data_dir / '商品マスタ_20250912.csv'
        if products_file.exists():
            initial_count = len(self.tax_data)
            with open(products_file, 'r', encoding='shift-jis') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    code = row['商品コード']
                    tax_rate = row['消費税率（%）']
                    if code and tax_rate:
                        self.tax_data[code] = f"{tax_rate}%"
            new_count = len(self.tax_data)
            print(f"  ✅ Loaded {new_count - initial_count} additional products from 商品マスタ")
        
        print(f"📋 Total expected tax data: {len(self.tax_data)} products")
        
        # Show distribution
        distribution = defaultdict(int)
        for tax_rate in self.tax_data.values():
            distribution[tax_rate] += 1
        
        print("📊 Expected tax rate distribution:")
        for rate, count in sorted(distribution.items()):
            print(f"  {rate}: {count} products")
    
    def query_shopify_product(self, handle):
        """Query a single product from Shopify GraphQL API"""
        query = '''
        query getProduct($handle: String!) {
          productByHandle(handle: $handle) {
            id
            handle
            title
            metafields(namespace: "custom", first: 50) {
              edges {
                node {
                  key
                  value
                  namespace
                }
              }
            }
          }
        }
        '''
        
        variables = {'handle': handle}
        
        headers = {
            'Content-Type': 'application/json',
            'X-Shopify-Access-Token': self.shopify_config['access_token']
        }
        
        url = f"https://{self.shopify_config['shop_url']}/admin/api/{self.shopify_config['api_version']}/graphql.json"
        
        try:
            response = requests.post(
                url,
                json={'query': query, 'variables': variables},
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                if 'errors' in data:
                    print(f"  ❌ GraphQL errors: {data['errors']}")
                    return None
                return data.get('data', {}).get('productByHandle')
            else:
                print(f"  ❌ HTTP error {response.status_code}: {response.text}")
                return None
                
        except Exception as e:
            print(f"  ❌ Connection error: {e}")
            return None
    
    def get_tax_metafield_value(self, product):
        """Extract tax metafield value from product data"""
        if not product or not product.get('metafields'):
            return None
        
        metafields = product['metafields']['edges']
        for edge in metafields:
            metafield = edge['node']
            if (metafield['namespace'] == 'custom' and 
                metafield['key'] == 'tax'):
                return metafield['value']
        
        return None
    
    def verify_product(self, handle):
        """Verify tax metafield for a single product"""
        print(f"🔍 Verifying {handle}...")
        
        # Get expected tax value
        expected_tax = self.tax_data.get(handle)
        if not expected_tax:
            print(f"  ⚠️  No expected tax data for {handle}")
            self.verification_results['not_found'].append({
                'handle': handle,
                'reason': 'No expected tax data'
            })
            self.stats['not_found'] += 1
            return
        
        # Query Shopify for actual value
        product = self.query_shopify_product(handle)
        
        if not product:
            print(f"  ❌ Product not found in Shopify: {handle}")
            self.verification_results['not_found'].append({
                'handle': handle,
                'reason': 'Product not found in Shopify'
            })
            self.stats['not_found'] += 1
            return
        
        actual_tax = self.get_tax_metafield_value(product)
        
        if actual_tax is None:
            print(f"  ❌ No tax metafield found for {handle}")
            self.verification_results['no_tax_metafield'].append({
                'handle': handle,
                'expected': expected_tax,
                'product_title': product.get('title', 'Unknown')
            })
            self.stats['no_metafield'] += 1
            return
        
        # Compare values
        if actual_tax == expected_tax:
            print(f"  ✅ Correct: {actual_tax}")
            self.verification_results['verified_correct'].append({
                'handle': handle,
                'tax_value': actual_tax,
                'product_title': product.get('title', 'Unknown')
            })
            self.stats['correct'] += 1
        else:
            print(f"  ❌ Incorrect: expected '{expected_tax}', got '{actual_tax}'")
            self.verification_results['verified_incorrect'].append({
                'handle': handle,
                'expected': expected_tax,
                'actual': actual_tax,
                'product_title': product.get('title', 'Unknown')
            })
            self.stats['incorrect'] += 1
        
        self.stats['total_checked'] += 1
    
    def verify_sample(self, sample_size):
        """Verify a random sample of products"""
        all_handles = list(self.tax_data.keys())
        
        if sample_size >= len(all_handles):
            print(f"📊 Sample size ({sample_size}) >= total products ({len(all_handles)}), verifying all")
            sample_handles = all_handles
        else:
            sample_handles = random.sample(all_handles, sample_size)
            print(f"📊 Verifying random sample of {sample_size} products from {len(all_handles)} total")
        
        for i, handle in enumerate(sample_handles, 1):
            print(f"\n[{i}/{len(sample_handles)}]", end=" ")
            self.verify_product(handle)
            
            # Add delay to avoid rate limiting
            if i % 10 == 0:
                print("⏳ Pausing to avoid rate limits...")
                import time
                time.sleep(2)
    
    def verify_single_product(self, handle):
        """Verify a single specific product"""
        print(f"🧪 Verifying single product: {handle}")
        self.verify_product(handle)
    
    def verify_failed_updates(self):
        """Verify products that previously failed to update"""
        print("🔄 Verifying previously failed updates...")
        
        # Load previous update results
        results_file = Path('../reports/14_tax_metafield_update_results.json')
        
        if not results_file.exists():
            print(f"❌ No previous update results found: {results_file}")
            return
        
        with open(results_file, 'r', encoding='utf-8') as f:
            update_results = json.load(f)
        
        failed_products = update_results.get('results', {}).get('failed', [])
        not_found_products = update_results.get('results', {}).get('notFound', [])
        
        if not failed_products and not not_found_products:
            print("✅ No previously failed updates to verify")
            return
        
        print(f"📋 Found {len(failed_products)} failed and {len(not_found_products)} not found from previous run")
        
        all_failed = []
        all_failed.extend([item['handle'] for item in failed_products if 'handle' in item])
        all_failed.extend([item['handle'] for item in not_found_products if 'handle' in item])
        
        for i, handle in enumerate(all_failed, 1):
            print(f"\n[{i}/{len(all_failed)}]", end=" ")
            self.verify_product(handle)
    
    def generate_report(self):
        """Generate verification summary report"""
        print(f"\n📋 TAX METAFIELD VERIFICATION SUMMARY")
        print(f"{'='*50}")
        print(f"Total checked: {self.stats['total_checked']}")
        print(f"Correct: {self.stats['correct']}")
        print(f"Incorrect: {self.stats['incorrect']}")
        print(f"Not found: {self.stats['not_found']}")
        print(f"No metafield: {self.stats['no_metafield']}")
        print(f"Connection errors: {self.stats['errors']}")
        
        if self.stats['total_checked'] > 0:
            accuracy = (self.stats['correct'] / self.stats['total_checked'] * 100)
            print(f"Accuracy: {accuracy:.1f}%")
        
        # Save detailed report
        report_file = Path('../reports/tax_metafield_verification_report.json')
        report_file.parent.mkdir(exist_ok=True)
        
        report_data = {
            'timestamp': datetime.now().isoformat(),
            'stats': self.stats,
            'results': self.verification_results,
            'sample_size': self.sample_size
        }
        
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False)
        
        print(f"\n📄 Detailed verification report saved: {report_file}")
        
        # Show examples of issues
        if self.verification_results['verified_incorrect']:
            print(f"\n❌ Products with incorrect tax values:")
            for item in self.verification_results['verified_incorrect'][:5]:
                print(f"  {item['handle']}: expected '{item['expected']}', got '{item['actual']}'")
        
        if self.verification_results['no_tax_metafield']:
            print(f"\n⚠️  Products missing tax metafield:")
            for item in self.verification_results['no_tax_metafield'][:5]:
                print(f"  {item['handle']}: expected '{item['expected']}'")

def main():
    parser = argparse.ArgumentParser(description='Verify tax metafield updates in Shopify')
    parser.add_argument('--sample', type=int, 
                       help='Verify random sample of N products')
    parser.add_argument('--handle', type=str,
                       help='Verify single product by handle')
    parser.add_argument('--failed-only', action='store_true',
                       help='Only verify previously failed updates')
    
    args = parser.parse_args()
    
    print("🔍 TAX METAFIELD VERIFIER")
    print("="*50)
    
    try:
        verifier = TaxMetafieldVerifier(sample_size=args.sample)
        verifier.load_tax_data()
        
        if len(verifier.tax_data) == 0:
            print("❌ No tax data loaded. Please check master files exist and are readable.")
            return
        
        if args.handle:
            verifier.verify_single_product(args.handle)
        elif args.failed_only:
            verifier.verify_failed_updates()
        elif args.sample:
            verifier.verify_sample(args.sample)
        else:
            # Default: verify all products (be careful with rate limits!)
            print("⚠️  Verifying ALL products. This may take a long time and hit rate limits.")
            response = input("Continue? (y/N): ")
            if response.lower() == 'y':
                verifier.verify_sample(len(verifier.tax_data))
            else:
                print("Cancelled. Use --sample N to verify a smaller subset.")
                return
        
        verifier.generate_report()
        
        print("\n✅ Verification completed!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise

if __name__ == '__main__':
    main()