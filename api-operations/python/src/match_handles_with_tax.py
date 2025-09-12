#!/usr/bin/env python3
"""
Match Shopify product handles with Rakuten tax information.

This script:
1. Collects all Shopify product handles from the data directory
2. Loads Rakuten product data with tax information
3. Attempts to match handles with tax data using multiple strategies
4. Creates JSON files with matched and unmatched products
"""

import pandas as pd
import json
import os
import re
from typing import Dict, List, Tuple, Optional
from pathlib import Path
import difflib

class HandleTaxMatcher:
    def __init__(self, shopify_data_dir: str, rakuten_csv_path: str, output_dir: str):
        """
        Initialize the matcher.
        
        Args:
            shopify_data_dir: Path to Shopify export CSV files
            rakuten_csv_path: Path to Rakuten CSV file
            output_dir: Directory to save output JSON files
        """
        self.shopify_data_dir = Path(shopify_data_dir)
        self.rakuten_csv_path = Path(rakuten_csv_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Data containers
        self.shopify_handles = []
        self.rakuten_data = {}
        self.matched_products = []
        self.unmatched_products = []
    
    def load_shopify_handles(self) -> List[str]:
        """Load all unique handles from Shopify export files."""
        print("Loading Shopify product handles...")
        handles = set()
        
        # Find all CSV files in the data directory
        csv_files = list(self.shopify_data_dir.glob("products_export_*.csv"))
        
        for csv_file in csv_files:
            print(f"  Processing {csv_file.name}...")
            try:
                # Read CSV file
                df = pd.read_csv(csv_file, usecols=['Handle'])
                file_handles = df['Handle'].dropna().unique()
                handles.update(file_handles)
                print(f"    Found {len(file_handles)} handles")
            except Exception as e:
                print(f"    Error reading {csv_file}: {e}")
        
        self.shopify_handles = list(handles)
        print(f"Total unique handles: {len(self.shopify_handles)}")
        return self.shopify_handles
    
    def load_rakuten_data(self) -> Dict:
        """Load Rakuten product data with tax information."""
        print("Loading Rakuten product data...")
        
        try:
            # Read Rakuten CSV with proper encoding
            df = pd.read_csv(self.rakuten_csv_path, encoding='shift_jis')
            print(f"  Loaded {len(df)} Rakuten products")
            
            # Extract relevant columns
            # Assuming columns: 商品管理番号（商品URL）, 商品番号, 商品名, 消費税, 消費税率
            columns = df.columns.tolist()
            print(f"  Columns: {columns[:10]}")  # First 10 columns
            
            # Create mapping from various identifiers to tax info
            rakuten_data = {}
            
            for _, row in df.iterrows():
                # Get identifiers
                mgmt_number = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
                product_number = str(row.iloc[1]) if pd.notna(row.iloc[1]) else ""
                product_name = str(row.iloc[2]) if pd.notna(row.iloc[2]) else ""
                
                # Get tax info (columns 5 and 6, 0-indexed)
                tax_flag = row.iloc[5] if pd.notna(row.iloc[5]) else None
                tax_rate = row.iloc[6] if pd.notna(row.iloc[6]) else None
                
                # Create product data
                product_data = {
                    "management_number": mgmt_number,
                    "product_number": product_number,
                    "product_name": product_name,
                    "tax_flag": tax_flag,
                    "tax_rate": tax_rate,
                    "has_tax_info": pd.notna(tax_flag) or pd.notna(tax_rate)
                }
                
                # Store with multiple keys for different matching strategies
                if mgmt_number:
                    rakuten_data[mgmt_number] = product_data
                if product_number:
                    rakuten_data[product_number] = product_data
                
                # Also create a normalized name for fuzzy matching
                if product_name:
                    normalized_name = self.normalize_name(product_name)
                    rakuten_data[f"name:{normalized_name}"] = product_data
            
            self.rakuten_data = rakuten_data
            print(f"  Created {len(rakuten_data)} data entries")
            
            # Count products with tax info
            products_with_tax = sum(1 for p in rakuten_data.values() if p.get('has_tax_info'))
            print(f"  Products with tax info: {products_with_tax}")
            
            return rakuten_data
            
        except Exception as e:
            print(f"Error loading Rakuten data: {e}")
            return {}
    
    def normalize_name(self, name: str) -> str:
        """Normalize product name for fuzzy matching."""
        if not name:
            return ""
        
        # Convert to lowercase, remove special characters
        normalized = re.sub(r'[^\w\s]', '', name.lower())
        normalized = re.sub(r'\s+', '-', normalized.strip())
        return normalized
    
    def match_handles(self):
        """Match Shopify handles with Rakuten tax data."""
        print("Matching handles with tax data...")
        
        matched_count = 0
        unmatched_count = 0
        
        for handle in self.shopify_handles:
            match_result = self.find_match(handle)
            
            if match_result:
                matched_count += 1
                self.matched_products.append({
                    "handle": handle,
                    "match_strategy": match_result["strategy"],
                    "matched_key": match_result["key"],
                    "rakuten_data": match_result["data"]
                })
            else:
                unmatched_count += 1
                self.unmatched_products.append({
                    "handle": handle,
                    "match_attempts": self.get_match_attempts(handle)
                })
        
        print(f"Matching complete:")
        print(f"  Matched: {matched_count}")
        print(f"  Unmatched: {unmatched_count}")
    
    def find_match(self, handle: str) -> Optional[Dict]:
        """Try to find a match for a handle using multiple strategies."""
        
        # Strategy 1: Direct match with management number
        if handle in self.rakuten_data:
            return {
                "strategy": "direct_management_number",
                "key": handle,
                "data": self.rakuten_data[handle]
            }
        
        # Strategy 2: Check if handle looks like a product number
        # Remove common prefixes/suffixes and try matching
        clean_handle = re.sub(r'^[^a-zA-Z0-9]*|[^a-zA-Z0-9]*$', '', handle)
        if clean_handle in self.rakuten_data:
            return {
                "strategy": "cleaned_handle",
                "key": clean_handle,
                "data": self.rakuten_data[clean_handle]
            }
        
        # Strategy 3: Fuzzy matching with normalized names
        normalized_handle = self.normalize_name(handle)
        name_key = f"name:{normalized_handle}"
        if name_key in self.rakuten_data:
            return {
                "strategy": "normalized_name",
                "key": name_key,
                "data": self.rakuten_data[name_key]
            }
        
        # Strategy 4: Partial matching
        # Look for handles that contain or are contained in Rakuten keys
        for key, data in self.rakuten_data.items():
            if key.startswith("name:"):
                continue
                
            # Check if handle is part of key or vice versa
            if (len(handle) > 3 and handle in key) or (len(key) > 3 and key in handle):
                return {
                    "strategy": "partial_match",
                    "key": key,
                    "data": data
                }
        
        # Strategy 5: Fuzzy string matching
        best_match = self.fuzzy_match(handle)
        if best_match:
            return {
                "strategy": "fuzzy_match",
                "key": best_match["key"],
                "data": best_match["data"]
            }
        
        return None
    
    def fuzzy_match(self, handle: str, threshold: float = 0.8) -> Optional[Dict]:
        """Try fuzzy string matching."""
        best_ratio = 0
        best_match = None
        
        # Only try fuzzy matching on shorter keys to avoid performance issues
        for key, data in list(self.rakuten_data.items())[:1000]:  # Limit to first 1000 for performance
            if key.startswith("name:"):
                continue
                
            ratio = difflib.SequenceMatcher(None, handle.lower(), key.lower()).ratio()
            if ratio > best_ratio and ratio >= threshold:
                best_ratio = ratio
                best_match = {"key": key, "data": data, "ratio": ratio}
        
        return best_match
    
    def get_match_attempts(self, handle: str) -> List[str]:
        """Get list of attempted matching strategies for debugging."""
        attempts = [
            f"direct: {handle}",
            f"cleaned: {re.sub(r'^[^a-zA-Z0-9]*|[^a-zA-Z0-9]*$', '', handle)}",
            f"normalized: {self.normalize_name(handle)}"
        ]
        return attempts
    
    def save_results(self):
        """Save matched and unmatched products to JSON files."""
        print("Saving results...")
        
        # Save matched products
        matched_file = self.output_dir / "products_with_tax_info.json"
        with open(matched_file, 'w', encoding='utf-8') as f:
            json.dump(self.matched_products, f, ensure_ascii=False, indent=2)
        print(f"  Saved {len(self.matched_products)} matched products to {matched_file}")
        
        # Save unmatched products
        unmatched_file = self.output_dir / "products_without_tax_info.json"
        with open(unmatched_file, 'w', encoding='utf-8') as f:
            json.dump(self.unmatched_products, f, ensure_ascii=False, indent=2)
        print(f"  Saved {len(self.unmatched_products)} unmatched products to {unmatched_file}")
        
        # Save summary statistics
        summary = {
            "total_shopify_handles": len(self.shopify_handles),
            "total_rakuten_products": len(self.rakuten_data),
            "matched_products": len(self.matched_products),
            "unmatched_products": len(self.unmatched_products),
            "match_strategies": self.get_match_strategy_stats(),
            "tax_rate_distribution": self.get_tax_rate_stats()
        }
        
        summary_file = self.output_dir / "matching_summary.json"
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        print(f"  Saved summary to {summary_file}")
    
    def get_match_strategy_stats(self) -> Dict:
        """Get statistics on matching strategies used."""
        strategies = {}
        for product in self.matched_products:
            strategy = product["match_strategy"]
            strategies[strategy] = strategies.get(strategy, 0) + 1
        return strategies
    
    def get_tax_rate_stats(self) -> Dict:
        """Get statistics on tax rates found."""
        tax_rates = {}
        for product in self.matched_products:
            tax_rate = product["rakuten_data"].get("tax_rate")
            if tax_rate is not None:
                tax_rates[str(tax_rate)] = tax_rates.get(str(tax_rate), 0) + 1
        return tax_rates
    
    def run(self):
        """Run the complete matching process."""
        print("Starting handle-tax matching process...")
        print("=" * 50)
        
        # Load data
        self.load_shopify_handles()
        self.load_rakuten_data()
        
        # Match handles
        self.match_handles()
        
        # Save results
        self.save_results()
        
        print("=" * 50)
        print("Process complete!")
        
        # Print final summary
        print(f"\nSummary:")
        print(f"  Total Shopify handles: {len(self.shopify_handles)}")
        print(f"  Total Rakuten products: {len(self.rakuten_data)}")
        print(f"  Successfully matched: {len(self.matched_products)}")
        print(f"  Unmatched: {len(self.unmatched_products)}")
        
        if self.matched_products:
            print(f"\nMatching strategies used:")
            for strategy, count in self.get_match_strategy_stats().items():
                print(f"    {strategy}: {count}")
        
        tax_stats = self.get_tax_rate_stats()
        if tax_stats:
            print(f"\nTax rate distribution:")
            for rate, count in sorted(tax_stats.items()):
                print(f"    {rate}: {count} products")


def main():
    """Main function to run the matcher."""
    # Set up paths
    script_dir = Path(__file__).parent
    shopify_data_dir = script_dir.parent.parent / "data"
    rakuten_csv_path = script_dir.parent.parent.parent / "csv-conversion" / "data" / "rakuten_item.csv"
    output_dir = script_dir.parent / "output"
    
    print(f"Shopify data directory: {shopify_data_dir}")
    print(f"Rakuten CSV path: {rakuten_csv_path}")
    print(f"Output directory: {output_dir}")
    
    # Verify paths exist
    if not shopify_data_dir.exists():
        print(f"Error: Shopify data directory not found: {shopify_data_dir}")
        return
    
    if not rakuten_csv_path.exists():
        print(f"Error: Rakuten CSV file not found: {rakuten_csv_path}")
        return
    
    # Run matcher
    matcher = HandleTaxMatcher(
        str(shopify_data_dir),
        str(rakuten_csv_path),
        str(output_dir)
    )
    
    matcher.run()


if __name__ == "__main__":
    main()