#!/usr/bin/env python3
"""
Script to analyze and discover all Rakuten EC-UP content patterns for GraphQL cleanup

This script:
1. Scans all CSV files to discover ALL EC-UP patterns (not just known ones)
2. Analyzes associated content and styling for each pattern
3. Generates JSON data for Node.js GraphQL operations
4. Outputs: shared/rakuten_content_to_clean.json
"""
import re
import sys
from pathlib import Path
from typing import List, Dict, Any, Set

import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm

# Add utils to path
sys.path.append(str(Path(__file__).parent))
from utils.json_output import (
    save_json_report,
    create_rakuten_cleanup_record,
    log_processing_summary,
    validate_json_structure
)


class ECUpPatternDiscovery:
    """Discovers and analyzes all EC-UP patterns in content"""
    
    def __init__(self):
        # Dynamic pattern discovery - will find ALL EC-UP patterns
        self.ec_up_pattern = re.compile(
            r'<!--EC-UP_([^_]+)_(\d+)_START-->(.*?)<!--EC-UP_\1_\2_END-->',
            re.IGNORECASE | re.DOTALL
        )
        
        # Known patterns for categorization
        self.known_patterns = {
            'Rakuichi': 'rakuichi',
            'Favorite': 'favorite', 
            'Similar': 'similar',
            'SameTime': 'sametime',
            'Resale': 'resale'
        }
    
    def discover_patterns_in_html(self, html_content: str) -> List[Dict[str, Any]]:
        """
        Discover all EC-UP patterns in HTML content
        Returns list of pattern discoveries
        """
        if not html_content or pd.isna(html_content):
            return []
        
        patterns_found = []
        matches = self.ec_up_pattern.finditer(str(html_content))
        
        for match in matches:
            pattern_name = match.group(1)
            pattern_number = match.group(2)  
            content_block = match.group(3)
            full_pattern = match.group(0)
            
            # Analyze the content block
            analysis = self._analyze_content_block(content_block)
            
            pattern_record = {
                'patternName': f"EC-UP_{pattern_name}_{pattern_number}",
                'patternType': self.known_patterns.get(pattern_name, 'unknown'),
                'fullMatch': full_pattern,
                'contentBlock': content_block,
                'contentLength': len(content_block),
                'contentPreview': self._create_content_preview(content_block),
                'hasImages': analysis['has_images'],
                'hasLinks': analysis['has_links'],
                'hasStyling': analysis['has_styling'],
                'stylingInfo': analysis['styling_info']
            }
            
            patterns_found.append(pattern_record)
        
        return patterns_found
    
    def _analyze_content_block(self, content_block: str) -> Dict[str, Any]:
        """Analyze a single EC-UP content block"""
        analysis = {
            'has_styling': False,
            'has_images': False,
            'has_links': False,
            'styling_info': []
        }
        
        try:
            soup = BeautifulSoup(content_block, 'lxml')
            
            # Check for images
            images = soup.find_all('img')
            analysis['has_images'] = len(images) > 0
            
            # Check for links
            links = soup.find_all('a')
            analysis['has_links'] = len(links) > 0
            
            # Check for styling
            style_info = []
            
            # CSS classes with ecup
            for element in soup.find_all(class_=True):
                classes = element.get('class', [])
                ecup_classes = [cls for cls in classes if 'ecup' in cls.lower()]
                if ecup_classes:
                    style_info.extend(ecup_classes)
            
            # Inline styles
            for element in soup.find_all(style=True):
                style_content = element.get('style')[:50]
                style_info.append(f"inline:{style_content}...")
            
            # Style tags
            style_tags = soup.find_all('style')
            for style_tag in style_tags:
                style_content = style_tag.get_text()[:100]
                style_info.append(f"style_tag:{style_content}...")
            
            analysis['has_styling'] = len(style_info) > 0
            analysis['styling_info'] = style_info[:5]  # Limit to first 5
            
        except Exception as e:
            # If parsing fails, just mark as unknown
            analysis['styling_info'] = [f"parse_error: {str(e)[:50]}"]
        
        return analysis
    
    def _create_content_preview(self, content_block: str, max_length: int = 200) -> str:
        """Create a preview of the content block"""
        try:
            soup = BeautifulSoup(content_block, 'lxml')
            text_content = soup.get_text(strip=True)
            
            if len(text_content) <= max_length:
                return text_content
            
            return text_content[:max_length] + "..."
            
        except Exception:
            # Fallback to raw content preview
            clean_content = re.sub(r'<[^>]+>', ' ', content_block)
            clean_content = ' '.join(clean_content.split())
            
            if len(clean_content) <= max_length:
                return clean_content
            
            return clean_content[:max_length] + "..."


def get_csv_files() -> List[Path]:
    """Get all CSV files from data directory"""
    data_dir = Path(__file__).parent.parent / "data"
    return list(data_dir.glob("products_export_*.csv"))


def analyze_csv_files_for_rakuten_content() -> List[Dict[str, Any]]:
    """
    Analyze CSV files to discover all EC-UP patterns
    Returns list of records for JSON output
    """
    print("🔍 Discovering EC-UP patterns in CSV files...")
    
    discoverer = ECUpPatternDiscovery()
    rakuten_records = []
    csv_files = get_csv_files()
    total_rows = 0
    total_patterns = 0
    
    for csv_file in csv_files:
        print(f"   📄 Processing {csv_file.name}...")
        
        try:
            # Read CSV in chunks
            chunk_size = 1000
            chunk_iter = pd.read_csv(
                csv_file,
                chunksize=chunk_size,
                encoding='utf-8',
                low_memory=False
            )
            
            file_records = {}  # Group by handle
            
            for chunk in chunk_iter:
                total_rows += len(chunk)
                
                for _, row in chunk.iterrows():
                    handle = row.get('Handle', '')
                    body_html = row.get('Body (HTML)', '')
                    
                    if pd.isna(body_html) or not body_html or pd.isna(handle) or not handle:
                        continue
                    
                    # Discover EC-UP patterns in this product
                    patterns_found = discoverer.discover_patterns_in_html(str(body_html))
                    
                    if patterns_found:
                        total_patterns += len(patterns_found)
                        
                        if handle not in file_records:
                            file_records[handle] = {
                                'productHandle': handle,
                                'productId': None,  # Will be filled by Node.js
                                'currentHtml': str(body_html),
                                'htmlLength': len(str(body_html)),
                                'patternsFound': patterns_found,
                                'totalPatterns': len(patterns_found),
                                'estimatedCleanupSize': sum(p['contentLength'] for p in patterns_found)
                            }
                        else:
                            # Merge patterns if multiple rows for same product
                            file_records[handle]['patternsFound'].extend(patterns_found)
                            file_records[handle]['totalPatterns'] = len(file_records[handle]['patternsFound'])
                            file_records[handle]['estimatedCleanupSize'] = sum(
                                p['contentLength'] for p in file_records[handle]['patternsFound']
                            )
            
            # Add file records to main list
            rakuten_records.extend(file_records.values())
            print(f"      ✅ Found {len(file_records)} products with EC-UP content")
                        
        except Exception as e:
            print(f"      ❌ Error processing {csv_file.name}: {e}")
            continue
    
    print(f"\n📊 Discovery Summary:")
    print(f"   📄 CSV rows processed: {total_rows:,}")
    print(f"   🎯 Products with EC-UP content: {len(rakuten_records)}")
    print(f"   🏷️  Total EC-UP patterns found: {total_patterns}")
    
    # Show pattern type distribution
    pattern_types = {}
    for record in rakuten_records:
        for pattern in record['patternsFound']:
            pattern_type = pattern['patternType']
            pattern_types[pattern_type] = pattern_types.get(pattern_type, 0) + 1
    
    print(f"   📈 Pattern distribution:")
    for pattern_type, count in sorted(pattern_types.items()):
        print(f"      - {pattern_type}: {count}")
    
    return rakuten_records


def main():
    """Main execution function"""
    print("=" * 70)
    print("🔍 RAKUTEN EC-UP CONTENT ANALYSIS (JSON OUTPUT)")
    print("=" * 70)
    
    try:
        # Phase 1: Analyze CSV files
        rakuten_records = analyze_csv_files_for_rakuten_content()
        
        if not rakuten_records:
            print("\n✅ No EC-UP patterns found!")
            # Save empty JSON file for consistency
            save_json_report([], "rakuten_content_to_clean.json", "No EC-UP patterns found in CSV data")
            return 0
        
        # Phase 2: Validate and save JSON
        print(f"\n💾 Saving JSON data for GraphQL processing...")
        
        required_fields = ['productHandle', 'currentHtml', 'patternsFound', 'totalPatterns']
        if not validate_json_structure(rakuten_records, required_fields):
            print("❌ JSON validation failed")
            return 1
        
        json_path = save_json_report(
            rakuten_records,
            "rakuten_content_to_clean.json",
            f"Products with EC-UP content for cleanup via GraphQL ({len(rakuten_records)} products)"
        )
        
        print(f"\n🎉 Analysis completed successfully!")
        print(f"   📄 JSON data saved to: {json_path}")
        print(f"   🚀 Ready for GraphQL processing")
        print(f"\n💡 Next step: cd node && npm run clean-rakuten")
        
        return 0
        
    except KeyboardInterrupt:
        print("\n⏹️  Analysis interrupted by user")
        return 1
    except Exception as e:
        print(f"\n❌ Analysis failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())