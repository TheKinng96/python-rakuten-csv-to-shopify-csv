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
import json
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
    
    def clean_ec_up_content(self, html_content: str) -> Dict[str, Any]:
        """
        Remove all EC-UP patterns from HTML content
        Returns cleaning results and cleaned HTML
        """
        if not html_content or pd.isna(html_content):
            return {
                'cleanedHtml': '',
                'patternsRemoved': [],
                'bytesRemoved': 0,
                'changesMade': False,
                'originalLength': 0,
                'cleanedLength': 0
            }
        
        original_html = str(html_content)
        cleaned_html = original_html
        patterns_removed = []
        bytes_removed = 0
        
        # Remove all EC-UP patterns
        def replace_pattern(match):
            nonlocal bytes_removed, patterns_removed
            
            pattern_name = match.group(1)
            pattern_number = match.group(2)
            content_block = match.group(3)
            full_match = match.group(0)
            
            pattern_id = f"EC-UP_{pattern_name}_{pattern_number}"
            
            patterns_removed.append({
                'patternId': pattern_id,
                'patternName': pattern_name,
                'patternNumber': pattern_number,
                'patternType': self.known_patterns.get(pattern_name, 'unknown'),
                'contentLength': len(content_block),
                'fullMatchLength': len(full_match),
                'contentPreview': content_block[:100] + ('...' if len(content_block) > 100 else '')
            })
            
            bytes_removed += len(full_match)
            return ''  # Remove the entire EC-UP block
        
        cleaned_html = self.ec_up_pattern.sub(replace_pattern, cleaned_html)
        
        # Clean up any remaining EC-UP artifacts
        artifacts = [
            r'<!--EC-UP_[^>]*-->',  # Orphaned EC-UP comments
            r'\s*<!\[endif\]-->',   # IE conditional comments often used with EC-UP
            r'<!--\[if[^>]*>\s*<!\[endif\]-->'  # Empty IE conditionals
        ]
        
        for artifact_pattern in artifacts:
            matches = re.findall(artifact_pattern, cleaned_html, re.IGNORECASE)
            for match in matches:
                bytes_removed += len(match)
            cleaned_html = re.sub(artifact_pattern, '', cleaned_html, flags=re.IGNORECASE)
        
        # Clean up excessive whitespace left behind
        cleaned_html = re.sub(r'\n\s*\n\s*\n', '\n\n', cleaned_html)  # Multiple empty lines to double
        cleaned_html = cleaned_html.strip()  # Trim start/end whitespace
        cleaned_html = re.sub(r'\s+(<\/)', r'\1', cleaned_html)  # Remove space before closing tags
        
        return {
            'cleanedHtml': cleaned_html,
            'patternsRemoved': patterns_removed,
            'bytesRemoved': bytes_removed,
            'changesMade': len(patterns_removed) > 0,
            'originalLength': len(original_html),
            'cleanedLength': len(cleaned_html),
            'compressionRatio': round((1 - len(cleaned_html) / len(original_html)) * 100, 1) if len(original_html) > 0 else 0
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


def analyze_csv_files_for_rakuten_content() -> tuple[List[Dict[str, Any]], Set[str]]:
    """
    Analyze CSV files to discover all EC-UP patterns and clean HTML content
    Returns tuple of (records_list, affected_handles_set)
    """
    print("🔍 Discovering and cleaning EC-UP patterns in CSV files...")
    
    discoverer = ECUpPatternDiscovery()
    rakuten_records = []
    affected_handles = set()
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
                    
                    # Clean EC-UP content from HTML
                    cleaning_result = discoverer.clean_ec_up_content(str(body_html))
                    
                    if cleaning_result['changesMade']:
                        total_patterns += len(cleaning_result['patternsRemoved'])
                        affected_handles.add(handle)
                        
                        if handle not in file_records:
                            file_records[handle] = {
                                'productHandle': handle,
                                'productId': None,  # Will be filled by Node.js
                                'originalHtml': str(body_html),
                                'cleanedHtml': cleaning_result['cleanedHtml'],
                                'originalLength': cleaning_result['originalLength'],
                                'cleanedLength': cleaning_result['cleanedLength'],
                                'bytesRemoved': cleaning_result['bytesRemoved'],
                                'compressionRatio': cleaning_result['compressionRatio'],
                                'patternsRemoved': cleaning_result['patternsRemoved'],
                                'totalPatterns': len(cleaning_result['patternsRemoved']),
                                'estimatedCleanupSize': sum(p['contentLength'] for p in cleaning_result['patternsRemoved'])
                            }
                        else:
                            # Merge patterns if multiple rows for same product
                            existing_patterns = file_records[handle]['patternsRemoved']
                            new_patterns = cleaning_result['patternsRemoved']
                            
                            # Avoid duplicates by checking pattern IDs
                            existing_ids = {p['patternId'] for p in existing_patterns}
                            for pattern in new_patterns:
                                if pattern['patternId'] not in existing_ids:
                                    existing_patterns.append(pattern)
                            
                            file_records[handle]['totalPatterns'] = len(existing_patterns)
                            file_records[handle]['estimatedCleanupSize'] = sum(
                                p['contentLength'] for p in existing_patterns
                            )
                            
                            # Update cleaned HTML to the latest version
                            file_records[handle]['cleanedHtml'] = cleaning_result['cleanedHtml']
                            file_records[handle]['bytesRemoved'] = cleaning_result['bytesRemoved']
            
            # Add file records to main list
            rakuten_records.extend(file_records.values())
            print(f"      ✅ Found {len(file_records)} products with EC-UP content")
                        
        except Exception as e:
            print(f"      ❌ Error processing {csv_file.name}: {e}")
            continue
    
    print(f"\n📊 Discovery Summary:")
    print(f"   📄 CSV rows processed: {total_rows:,}")
    print(f"   🎯 Products with EC-UP content: {len(rakuten_records)}")
    print(f"   🎯 Affected handles: {len(affected_handles)}")
    print(f"   🏷️  Total EC-UP patterns found: {total_patterns}")
    
    # Show pattern type distribution
    pattern_types = {}
    for record in rakuten_records:
        for pattern in record['patternsRemoved']:
            pattern_type = pattern['patternType']
            pattern_types[pattern_type] = pattern_types.get(pattern_type, 0) + 1
    
    print(f"   📈 Pattern distribution:")
    for pattern_type, count in sorted(pattern_types.items()):
        print(f"      - {pattern_type}: {count}")
    
    return rakuten_records, affected_handles


def save_handles_json(affected_handles: Set[str]) -> Path:
    """Save simple array of affected handles to JSON file"""
    shared_dir = Path(__file__).parent.parent / "shared"
    shared_dir.mkdir(exist_ok=True)
    
    handles_path = shared_dir / "affected_handles.json"
    handles_list = sorted(list(affected_handles))
    
    with open(handles_path, 'w', encoding='utf-8') as f:
        json.dump(handles_list, f, indent=2)
    
    print(f"📄 Affected handles saved to: {handles_path}")
    return handles_path


def main():
    """Main execution function"""
    print("=" * 70)
    print("🔍 EC-UP CONTENT ANALYSIS & CLEANING (JSON OUTPUT)")
    print("=" * 70)
    
    try:
        # Phase 1: Analyze CSV files
        rakuten_records, affected_handles = analyze_csv_files_for_rakuten_content()
        
        if not rakuten_records:
            print("\n✅ No EC-UP patterns found!")
            # Save empty JSON files for consistency
            save_json_report([], "rakuten_content_to_clean.json", "No EC-UP patterns found in CSV data")
            save_handles_json(set())
            return 0
        
        # Phase 2: Validate and save detailed JSON
        print(f"\n💾 Saving JSON data for GraphQL processing...")
        
        required_fields = ['productHandle', 'cleanedHtml', 'patternsRemoved', 'totalPatterns']
        if not validate_json_structure(rakuten_records, required_fields):
            print("❌ JSON validation failed")
            return 1
        
        json_path = save_json_report(
            rakuten_records,
            "rakuten_content_to_clean.json",
            f"Products with cleaned EC-UP content for GraphQL update ({len(rakuten_records)} products)"
        )
        
        # Phase 3: Save simple handles array
        print(f"\n💾 Saving affected handles array...")
        handles_path = save_handles_json(affected_handles)
        
        # Phase 3: Generate summary list with matched keys
        print(f"\n📋 Generating summary list...")
        
        summary_list = []
        all_matched_keys = set()
        
        for record in rakuten_records:
            matched_keys = []
            for pattern in record['patternsRemoved']:
                matched_keys.append(pattern['patternId'])
                all_matched_keys.add(pattern['patternId'])
            
            summary_list.append({
                'handle': record['productHandle'],
                'title': 'Title not available in CSV analysis',  # Title would need to be extracted from CSV
                'total_patterns': record['totalPatterns'],
                'cleanup_size': record['estimatedCleanupSize'],
                'bytes_removed': record['bytesRemoved'],
                'compression_ratio': record['compressionRatio'],
                'matched_keys': matched_keys,
                'pattern_types': list(set(p['patternType'] for p in record['patternsRemoved']))
            })
        
        # Sort by cleanup size (largest first)
        summary_list.sort(key=lambda x: -x['cleanup_size'])
        
        # Print summary list to console  
        print(f"\n📝 Summary List - Products with Rakuten EC-UP Content:")
        print(f"{'Handle':<30} {'Patterns':<10} {'Size (B)':<10} {'Pattern Types':<30} {'Keys':<40}")
        print("-" * 125)
        
        for item in summary_list[:20]:  # Show first 20
            types_str = ', '.join(item['pattern_types'][:3])
            if len(item['pattern_types']) > 3:
                types_str += f" (+{len(item['pattern_types'])-3})"
            
            keys_str = ', '.join(item['matched_keys'][:2])  # Show first 2 keys
            if len(item['matched_keys']) > 2:
                keys_str += f" (+{len(item['matched_keys'])-2} more)"
            
            print(f"{item['handle']:<30} {item['total_patterns']:<10} {item['cleanup_size']:<10} "
                  f"{types_str:<30} {keys_str:<40}")
        
        if len(summary_list) > 20:
            print(f"... and {len(summary_list)-20} more products")
        
        # Show all unique matched keys found
        print(f"\n📋 All Matched EC-UP Keys Found ({len(all_matched_keys)} unique):")
        sorted_keys = sorted(all_matched_keys)
        for i, key in enumerate(sorted_keys):
            if i % 3 == 0:
                print()  # New line every 3 keys
            print(f"  {key:<25}", end="")
        
        print(f"\n\n🎉 Analysis completed successfully!")
        print(f"   📄 Detailed JSON data saved to: {json_path}")
        print(f"   📄 Affected handles array saved to: {handles_path}")
        print(f"   🎯 {len(affected_handles)} handles affected")
        print(f"   🚀 Ready for GraphQL processing")
        print(f"\n💡 Next steps:")
        print(f"   1. Review affected handles in: affected_handles.json")
        print(f"   2. Run Node.js script: cd node && npm run clean-rakuten")
        print(f"   3. Or use the detailed JSON for custom processing")
        
        return 0
        
    except KeyboardInterrupt:
        print("\n⏹️  Analysis interrupted by user")
        return 1
    except Exception as e:
        print(f"\n❌ Analysis failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())