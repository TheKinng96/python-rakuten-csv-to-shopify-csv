#!/usr/bin/env python3
"""
Optimized CSS Style Scoper for Product Descriptions
- Faster processing with simplified parsing
- Batch processing capabilities
- Memory efficient
"""
import re
import sys
import json
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import multiprocessing as mp
from functools import partial

import pandas as pd
from bs4 import BeautifulSoup


class FastCSSScoper:
    """Optimized CSS scoper with performance improvements"""
    
    def __init__(self):
        self.scope_class = 'shopify-product-description'
        self.scope_selector = f'.{self.scope_class}'
        
        # Pre-compile regex patterns for performance
        self.style_tag_pattern = re.compile(r'<style[^>]*>(.*?)</style>', re.DOTALL | re.IGNORECASE)
        self.media_query_pattern = re.compile(r'(@media[^{]+{)(.*?)(}(?:\s*}))', re.DOTALL)
        self.css_rule_pattern = re.compile(r'([^{]+){([^}]+)}', re.DOTALL)
        
        # Simple selector patterns for quick matching
        self.element_selectors = re.compile(r'\b(img|hr|table|tr|td|th|div|p|span|h[1-6])\b')
        self.wildcard_pattern = re.compile(r'(\*(?:\[[^\]]+\])?)')
        self.html_body_pattern = re.compile(r'\b(html|body)\b')
    
    def process_html_fast(self, html_content: str, handle: str) -> Optional[Dict[str, Any]]:
        """
        Fast processing of HTML content
        """
        if not html_content or pd.isna(html_content):
            return None
        
        html_str = str(html_content)
        
        # Quick check for style tags
        if '<style' not in html_str.lower():
            return None
        
        try:
            # Extract and process style tags using regex (faster than BeautifulSoup for this)
            modified_html = html_str
            changes_made = []
            
            # Process all style tags
            def replace_style(match):
                style_content = match.group(1)
                scoped_css = self._scope_css_fast(style_content)
                changes_made.append('style_scoped')
                return f'<style>{scoped_css}</style>'
            
            modified_html = self.style_tag_pattern.sub(replace_style, modified_html)
            
            # Add wrapper if not present (simple check)
            if self.scope_class not in modified_html:
                # Use simple string concatenation instead of BeautifulSoup for wrapping
                modified_html = f'<div class="{self.scope_class}" data-product-handle="{handle}">{modified_html}</div>'
                changes_made.append('wrapper_added')
            
            if changes_made:
                return {
                    'productHandle': handle,
                    'originalHtml': html_str,
                    'cleanedHtml': modified_html,
                    'changesCount': len(changes_made),
                    'bytesChanged': len(modified_html) - len(html_str)
                }
            
        except Exception:
            # Silently skip problematic content
            pass
        
        return None
    
    def _scope_css_fast(self, css_content: str) -> str:
        """
        Fast CSS scoping using regex replacements
        """
        # Remove comments first
        css = re.sub(r'/\*.*?\*/', '', css_content, flags=re.DOTALL)
        
        # Process media queries
        def process_media_query(match):
            media_start = match.group(1)
            media_content = match.group(2)
            media_end = match.group(3)
            
            # Scope content inside media query
            scoped_content = self._scope_css_rules(media_content)
            return f'{media_start}{scoped_content}{media_end}'
        
        css = self.media_query_pattern.sub(process_media_query, css)
        
        # Process regular CSS rules (outside media queries)
        # Split into media and non-media sections
        media_sections = []
        for match in self.media_query_pattern.finditer(css):
            media_sections.append((match.start(), match.end()))
        
        if not media_sections:
            # No media queries, process entire CSS
            css = self._scope_css_rules(css)
        else:
            # Process non-media sections
            result = []
            last_end = 0
            for start, end in media_sections:
                if last_end < start:
                    # Process section before media query
                    result.append(self._scope_css_rules(css[last_end:start]))
                # Keep media query as-is (already processed)
                result.append(css[start:end])
                last_end = end
            # Process remaining section
            if last_end < len(css):
                result.append(self._scope_css_rules(css[last_end:]))
            css = ''.join(result)
        
        return css
    
    def _scope_css_rules(self, css_content: str) -> str:
        """
        Scope CSS rules using simple regex replacements
        """
        def scope_rule(match):
            selector = match.group(1).strip()
            properties = match.group(2)
            
            # Skip if already scoped
            if self.scope_class in selector:
                return match.group(0)
            
            # Remove html/body selectors
            if self.html_body_pattern.search(selector):
                selector = self.html_body_pattern.sub('', selector).strip()
                if not selector:
                    return ''  # Skip empty selector
            
            # Handle comma-separated selectors
            if ',' in selector:
                selectors = [s.strip() for s in selector.split(',')]
                scoped_selectors = []
                for sel in selectors:
                    scoped_sel = self._scope_single_fast(sel)
                    if scoped_sel:
                        scoped_selectors.append(scoped_sel)
                if scoped_selectors:
                    return f"{', '.join(scoped_selectors)} {{{properties}}}"
                return ''
            else:
                scoped = self._scope_single_fast(selector)
                if scoped:
                    return f"{scoped} {{{properties}}}"
                return ''
        
        return self.css_rule_pattern.sub(scope_rule, css_content)
    
    def _scope_single_fast(self, selector: str) -> str:
        """
        Fast single selector scoping
        """
        if not selector or not selector.strip():
            return ''
        
        selector = selector.strip()
        
        # Already scoped
        if self.scope_class in selector:
            return selector
        
        # Element selectors (img, hr, table, etc.)
        if self.element_selectors.match(selector):
            return f'{self.scope_selector} {selector}'
        
        # Wildcard selectors
        if selector.startswith('*'):
            return f'{self.scope_selector} {selector}'
        
        # Class selectors
        if selector.startswith('.'):
            return f'{self.scope_selector} {selector}'
        
        # ID selectors
        if selector.startswith('#'):
            return f'{self.scope_selector} {selector}'
        
        # Attribute selectors
        if selector.startswith('['):
            return f'{self.scope_selector} {selector}'
        
        # Complex selectors - check first part
        first_part = re.split(r'[\s>+~]', selector)[0]
        if self.element_selectors.match(first_part):
            return f'{self.scope_selector} {selector}'
        
        # Default: return as-is (likely already specific enough)
        return selector


def process_batch(batch_data: List[tuple], scoper: FastCSSScoper) -> List[Dict]:
    """
    Process a batch of products
    """
    results = []
    for handle, body_html in batch_data:
        result = scoper.process_html_fast(body_html, handle)
        if result:
            results.append(result)
    return results


def process_csv_parallel(csv_file: Path, max_rows: Optional[int] = None) -> List[Dict]:
    """
    Process CSV file using parallel processing
    """
    print(f"   📄 Processing {csv_file.name}...")
    
    # Read CSV
    if max_rows:
        df = pd.read_csv(csv_file, encoding='utf-8', low_memory=False, nrows=max_rows)
    else:
        df = pd.read_csv(csv_file, encoding='utf-8', low_memory=False)
    
    print(f"      Loaded {len(df)} rows")
    
    # Filter rows with style tags
    products_with_styles = []
    for _, row in df.iterrows():
        handle = row.get('Handle', '')
        body_html = row.get('Body (HTML)', '')
        
        if pd.notna(body_html) and pd.notna(handle):
            html_str = str(body_html)
            if '<style' in html_str.lower():
                products_with_styles.append((handle, html_str))
    
    print(f"      Found {len(products_with_styles)} products with style tags")
    
    if not products_with_styles:
        return []
    
    # Process in batches using multiprocessing
    scoper = FastCSSScoper()
    batch_size = 50
    batches = [products_with_styles[i:i+batch_size] for i in range(0, len(products_with_styles), batch_size)]
    
    all_results = []
    
    # Use multiprocessing for faster processing
    num_workers = min(mp.cpu_count(), 4)  # Limit to 4 workers
    
    print(f"      Processing with {num_workers} workers...")
    
    with mp.Pool(num_workers) as pool:
        process_func = partial(process_batch, scoper=scoper)
        batch_results = pool.map(process_func, batches)
        
        for results in batch_results:
            all_results.extend(results)
    
    print(f"      ✅ Processed {len(all_results)} products")
    return all_results


def main():
    """Main execution function"""
    print("=" * 70)
    print("🚀 FAST CSS STYLE SCOPING FOR PRODUCT DESCRIPTIONS")
    print("=" * 70)
    
    try:
        # Get CSV files
        data_dir = Path(__file__).parent.parent.parent / "data"
        csv_files = sorted(data_dir.glob("products_export_*.csv"))
        
        if not csv_files:
            print("❌ No CSV files found!")
            return 1
        
        print(f"Found {len(csv_files)} CSV files")
        
        all_records = []
        
        # Process each CSV file
        for csv_file in csv_files:  # Process all files
            records = process_csv_parallel(csv_file)
            all_records.extend(records)
        
        if not all_records:
            print("\n✅ No products need CSS scoping!")
            return 0
        
        # Save results
        print(f"\n💾 Saving JSON data...")
        
        json_data = {
            'timestamp': datetime.now().isoformat(),
            'summary': {
                'total_products': len(all_records),
                'scope_class': 'shopify-product-description'
            },
            'data': all_records
        }
        
        json_path = Path(__file__).parent.parent / "shared" / "css_scoped_descriptions.json"
        json_path.parent.mkdir(exist_ok=True)
        
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
        
        print(f"   ✅ Saved to: {json_path}")
        
        # Print summary
        print(f"\n📊 Summary:")
        print(f"   Total products processed: {len(all_records)}")
        print(f"   Average bytes changed: {sum(r['bytesChanged'] for r in all_records) / len(all_records):.0f}")
        
        # Show sample products
        print(f"\n📋 Sample products:")
        for record in all_records[:5]:
            print(f"   - {record['productHandle']}: {record['changesCount']} changes")
        
        if len(all_records) > 5:
            print(f"   ... and {len(all_records) - 5} more")
        
        print(f"\n🎉 CSS scoping completed successfully!")
        print(f"\n💡 Next step: Run the Node.js updater with the generated JSON")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    # For multiprocessing on macOS
    mp.set_start_method('fork', force=True)
    sys.exit(main())