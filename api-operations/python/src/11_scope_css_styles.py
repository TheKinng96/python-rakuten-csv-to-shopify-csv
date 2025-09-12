#!/usr/bin/env python3
"""
Script to scope CSS styles in product descriptions to prevent theme conflicts

This script:
1. Reads product CSV files with HTML descriptions
2. Identifies and parses CSS within <style> tags
3. Adds proper scoping to all CSS selectors
4. Wraps content in a scoped container div
5. Generates JSON for Node.js GraphQL updates
"""
import re
import sys
import json
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup, Tag

# Add utils to path
sys.path.append(str(Path(__file__).parent))
from utils.json_output import save_json_report


class CSSStyleScoper:
    """Scopes CSS styles to prevent theme conflicts"""
    
    def __init__(self):
        # Unique class for scoping all product description styles
        self.scope_class = 'shopify-product-description'
        self.scope_selector = f'.{self.scope_class}'
        
        # Patterns that need special handling
        self.problematic_selectors = {
            # Global element selectors
            r'\bimg\b': f'{self.scope_selector} img',
            r'\bhr\b': f'{self.scope_selector} hr',
            r'\btable\b': f'{self.scope_selector} table',
            r'\btr\b': f'{self.scope_selector} tr',
            r'\btd\b': f'{self.scope_selector} td',
            r'\bth\b': f'{self.scope_selector} th',
            r'\bdiv\b': f'{self.scope_selector} div',
            r'\bp\b': f'{self.scope_selector} p',
            r'\bspan\b': f'{self.scope_selector} span',
            r'\bh[1-6]\b': f'{self.scope_selector} h\\g<0>',
            
            # Wildcard selectors
            r'^\*(?!\[)': f'{self.scope_selector} *',  # * but not *[
            r'^\*\[': f'{self.scope_selector} *[',  # *[attribute]
            
            # Body and html (should never affect these)
            r'\bhtml\b': '',  # Remove html selectors
            r'\bbody\b': '',  # Remove body selectors
        }
        
        # Selectors that should be preserved without modification
        self.preserve_patterns = [
            r'^@media',  # Media queries (handled separately)
            r'^@keyframes',  # Animations
            r'^:root',  # CSS variables (will be scoped differently)
        ]
        
        # Classes that already have proper scoping
        self.already_scoped_classes = [
            'mobile-responsive-',
            'tab__container',
        ]
    
    def process_html(self, html_content: str, handle: str) -> Dict[str, Any]:
        """
        Process HTML content to add CSS scoping
        Returns processed HTML and metadata
        """
        if not html_content or pd.isna(html_content):
            return {
                'processed': False,
                'reason': 'empty_content'
            }
        
        html_str = str(html_content)
        original_length = len(html_str)
        
        try:
            soup = BeautifulSoup(html_str, 'html.parser')
            
            # Find all style tags
            style_tags = soup.find_all('style')
            
            if not style_tags:
                # Check if there are inline styles that need scoping
                has_inline_styles = any(elem.get('style') for elem in soup.find_all())
                if not has_inline_styles:
                    return {
                        'processed': False,
                        'reason': 'no_styles_found'
                    }
            
            changes_made = []
            
            # Process each style tag
            for style_tag in style_tags:
                css_content = style_tag.string or ''
                if css_content:
                    scoped_css, style_changes = self._scope_css_content(css_content)
                    if style_changes:
                        style_tag.string = scoped_css
                        changes_made.extend(style_changes)
            
            # Check if content already has wrapper
            has_wrapper = self._has_scope_wrapper(soup)
            
            if not has_wrapper:
                # Wrap all content in scoped container
                wrapper_div = soup.new_tag('div')
                wrapper_div['class'] = [self.scope_class]
                wrapper_div['data-product-handle'] = handle
                
                # Move all original content into wrapper
                original_contents = list(soup.children)
                for content in original_contents:
                    wrapper_div.append(content.extract())
                
                soup.append(wrapper_div)
                changes_made.append({
                    'type': 'wrapper_added',
                    'description': f'Added {self.scope_class} wrapper'
                })
            
            # Process inline styles on elements
            inline_changes = self._process_inline_styles(soup)
            changes_made.extend(inline_changes)
            
            modified_html = str(soup)
            
            return {
                'processed': True,
                'original_html': html_str,
                'modified_html': modified_html,
                'changes_made': changes_made,
                'bytes_changed': len(modified_html) - original_length,
                'style_tags_processed': len(style_tags),
                'has_wrapper': True
            }
            
        except Exception as e:
            return {
                'processed': False,
                'reason': 'processing_error',
                'error': str(e)
            }
    
    def _scope_css_content(self, css_content: str) -> Tuple[str, List[Dict]]:
        """
        Scope CSS content by prefixing selectors
        Returns scoped CSS and list of changes made
        """
        changes = []
        
        # Split CSS into rules (handle media queries separately)
        rules = self._parse_css_rules(css_content)
        scoped_rules = []
        
        for rule in rules:
            if rule['type'] == 'media_query':
                # Process media query content
                scoped_content, media_changes = self._scope_media_query(rule['content'], rule['condition'])
                scoped_rules.append(f"@media {rule['condition']} {{\n{scoped_content}\n}}")
                changes.extend(media_changes)
            
            elif rule['type'] == 'regular':
                # Process regular CSS rule
                selector = rule['selector']
                properties = rule['properties']
                
                # Skip if already scoped
                if self._is_already_scoped(selector):
                    scoped_rules.append(f"{selector} {{\n{properties}\n}}")
                    continue
                
                # Apply scoping
                scoped_selector = self._scope_selector(selector)
                
                if scoped_selector and scoped_selector != selector:
                    scoped_rules.append(f"{scoped_selector} {{\n{properties}\n}}")
                    changes.append({
                        'type': 'selector_scoped',
                        'original': selector,
                        'scoped': scoped_selector
                    })
                elif scoped_selector:  # Selector unchanged but valid
                    scoped_rules.append(f"{selector} {{\n{properties}\n}}")
            
            elif rule['type'] == 'keyframes':
                # Preserve keyframes as-is
                scoped_rules.append(rule['content'])
        
        scoped_css = '\n'.join(scoped_rules)
        return scoped_css, changes
    
    def _parse_css_rules(self, css_content: str) -> List[Dict]:
        """
        Parse CSS content into individual rules
        """
        rules = []
        
        # Handle media queries
        media_pattern = r'@media\s*([^{]+)\s*{([^{}]*(?:{[^{}]*}[^{}]*)*)}'
        media_matches = re.finditer(media_pattern, css_content, re.DOTALL)
        
        media_positions = []
        for match in media_matches:
            media_positions.append((match.start(), match.end()))
            rules.append({
                'type': 'media_query',
                'condition': match.group(1).strip(),
                'content': match.group(2).strip(),
                'position': (match.start(), match.end())
            })
        
        # Handle keyframes
        keyframes_pattern = r'@keyframes\s+[\w-]+\s*{[^{}]*(?:{[^{}]*}[^{}]*)*}'
        keyframes_matches = re.finditer(keyframes_pattern, css_content, re.DOTALL)
        
        keyframes_positions = []
        for match in keyframes_matches:
            keyframes_positions.append((match.start(), match.end()))
            rules.append({
                'type': 'keyframes',
                'content': match.group(0),
                'position': (match.start(), match.end())
            })
        
        # Extract regular rules (not inside media queries or keyframes)
        regular_pattern = r'([^{}@]+)\s*{\s*([^{}]+)\s*}'
        
        # Create a version of CSS with media queries and keyframes removed
        temp_css = css_content
        for start, end in sorted(media_positions + keyframes_positions, reverse=True):
            temp_css = temp_css[:start] + ' ' * (end - start) + temp_css[end:]
        
        regular_matches = re.finditer(regular_pattern, temp_css)
        
        for match in regular_matches:
            selector = match.group(1).strip()
            properties = match.group(2).strip()
            
            # Clean up selector - remove comments
            selector = re.sub(r'/\*.*?\*/', '', selector, flags=re.DOTALL).strip()
            
            if selector and properties:
                rules.append({
                    'type': 'regular',
                    'selector': selector,
                    'properties': properties,
                    'position': (match.start(), match.end())
                })
        
        # Sort rules by position to maintain order
        rules.sort(key=lambda x: x['position'][0])
        
        return rules
    
    def _scope_media_query(self, content: str, condition: str) -> Tuple[str, List[Dict]]:
        """
        Scope CSS rules inside a media query
        """
        changes = []
        
        # Parse rules inside media query
        rules = []
        rule_pattern = r'([^{}]+)\s*{\s*([^{}]+)\s*}'
        
        for match in re.finditer(rule_pattern, content):
            selector = match.group(1).strip()
            properties = match.group(2).strip()
            
            if selector and properties:
                # Apply scoping to selector
                scoped_selector = self._scope_selector(selector)
                
                if scoped_selector and scoped_selector != selector:
                    rules.append(f"  {scoped_selector} {{\n    {properties}\n  }}")
                    changes.append({
                        'type': 'media_query_selector_scoped',
                        'condition': condition,
                        'original': selector,
                        'scoped': scoped_selector
                    })
                elif scoped_selector:
                    rules.append(f"  {selector} {{\n    {properties}\n  }}")
        
        return '\n'.join(rules), changes
    
    def _scope_selector(self, selector: str) -> str:
        """
        Add scope to a CSS selector
        """
        # Clean up selector first
        selector = selector.strip()
        
        # Remove comments from selector (they can interfere with parsing)
        selector = re.sub(r'/\*.*?\*/', '', selector, flags=re.DOTALL).strip()
        
        # Handle comma-separated selectors
        if ',' in selector:
            selectors = [s.strip() for s in selector.split(',')]
            scoped_selectors = [self._scope_single_selector(s) for s in selectors if s.strip()]
            return ', '.join(filter(None, scoped_selectors))
        
        return self._scope_single_selector(selector)
    
    def _scope_single_selector(self, selector: str) -> str:
        """
        Scope a single selector
        """
        selector = selector.strip()
        
        # Skip empty selectors
        if not selector:
            return ''
        
        # Remove html and body selectors entirely
        if selector in ['html', 'body']:
            return ''
        
        # Check if already scoped
        if self._is_already_scoped(selector):
            return selector
        
        # Handle special cases
        
        # 1. Direct element selectors (img, hr, table, etc.)
        if re.match(r'^(img|hr|table|tr|td|th|div|p|span|h[1-6])(\s|$|:|\.|\[)', selector):
            return f'{self.scope_selector} {selector}'
        
        # 2. Wildcard selectors (including *[attribute])
        if selector.startswith('*'):
            return f'{self.scope_selector} {selector}'
        
        # 3. Class selectors
        if selector.startswith('.'):
            # Always scope class selectors that might be too generic
            if 'mobile-responsive' in selector:
                # These are already contextual but still need parent scope
                return f'{self.scope_selector} {selector}'
            return f'{self.scope_selector} {selector}'
        
        # 4. ID selectors (scope them too)
        if selector.startswith('#'):
            return f'{self.scope_selector} {selector}'
        
        # 5. Attribute selectors
        if selector.startswith('['):
            return f'{self.scope_selector} {selector}'
        
        # 6. Pseudo-elements and pseudo-classes on elements
        element_with_pseudo = re.match(r'^(img|hr|table|tr|td|th|div|p|span|h[1-6])(:[:\w-]+)', selector)
        if element_with_pseudo:
            return f'{self.scope_selector} {selector}'
        
        # 7. Complex selectors with descendant/child combinators
        if ' ' in selector or '>' in selector or '+' in selector or '~' in selector:
            # Check if first part needs scoping
            parts = re.split(r'(\s+|>|~|\+)', selector, 1)
            if parts[0] and self._needs_scoping(parts[0]):
                return f'{self.scope_selector} {selector}'
        
        # Default: add scope if selector appears to target elements
        if self._needs_scoping(selector):
            return f'{self.scope_selector} {selector}'
        
        return selector
    
    def _needs_scoping(self, selector_part: str) -> bool:
        """
        Check if a selector part needs scoping
        """
        # Element selectors
        if re.match(r'^(img|hr|table|tr|td|th|div|p|span|h[1-6])', selector_part):
            return True
        
        # Wildcard
        if selector_part.startswith('*'):
            return True
        
        # Already has scope class
        if self.scope_class in selector_part:
            return False
        
        # Classes and IDs generally need scoping
        if selector_part.startswith('.') or selector_part.startswith('#'):
            return True
        
        return False
    
    def _is_already_scoped(self, selector: str) -> bool:
        """
        Check if selector is already properly scoped
        """
        return self.scope_class in selector or self.scope_selector in selector
    
    def _has_scope_wrapper(self, soup: BeautifulSoup) -> bool:
        """
        Check if content already has scope wrapper
        """
        # Look for existing wrapper div
        wrapper = soup.find('div', class_=self.scope_class)
        return wrapper is not None
    
    def _process_inline_styles(self, soup: BeautifulSoup) -> List[Dict]:
        """
        Process inline styles on elements
        Note: Inline styles don't need scoping but we track them
        """
        changes = []
        
        # Find elements with problematic inline styles
        for elem in soup.find_all(style=True):
            style = elem.get('style', '')
            
            # Check for problematic patterns
            if 'position: fixed' in style or 'position: absolute' in style:
                # These could break layout
                changes.append({
                    'type': 'inline_style_warning',
                    'element': elem.name,
                    'style': style[:100],
                    'issue': 'absolute/fixed positioning'
                })
        
        return changes


def get_csv_files() -> List[Path]:
    """Get all product CSV files"""
    data_dir = Path(__file__).parent.parent.parent / "data"
    return sorted(data_dir.glob("products_export_*.csv"))


def process_products_for_css_scoping() -> List[Dict[str, Any]]:
    """
    Process all products to add CSS scoping
    """
    print("🔍 Analyzing products for CSS scoping issues...")
    
    scoper = CSSStyleScoper()
    all_records = []
    csv_files = get_csv_files()
    
    total_products = 0
    products_with_styles = 0
    products_needing_scoping = 0
    
    # Process only first file for testing 
    for csv_file in csv_files[:1]:
        print(f"   📄 Processing {csv_file.name}...")
        
        try:
            # Read CSV
            df = pd.read_csv(csv_file, encoding='utf-8', low_memory=False)
            total_products += len(df)
            
            # Process products with HTML content
            for idx, row in df.iterrows():
                if idx % 100 == 0:
                    print(f"      Processing row {idx}/{len(df)}...")
                
                handle = row.get('Handle', '')
                body_html = row.get('Body (HTML)', '')
                
                if pd.isna(body_html) or not body_html or pd.isna(handle) or not handle:
                    continue
                
                # Check if HTML contains style tags or problematic patterns
                html_str = str(body_html)
                
                if '<style' in html_str.lower():
                    products_with_styles += 1
                    
                    # Process HTML for scoping
                    result = scoper.process_html(html_str, handle)
                    
                    if result.get('processed') and result.get('changes_made'):
                        products_needing_scoping += 1
                        
                        # Create record for JSON output
                        record = {
                            'productHandle': handle,
                            'originalHtml': result['original_html'],
                            'cleanedHtml': result['modified_html'],  # Using same field name as other scripts
                            'bytesRemoved': -result['bytes_changed'],  # Negative because we're adding bytes
                            'changesCount': len(result['changes_made']),
                            'changes': result['changes_made'],
                            'styleTagsProcessed': result.get('style_tags_processed', 0),
                            'hasWrapper': result.get('has_wrapper', False)
                        }
                        
                        all_records.append(record)
            
            print(f"      ✅ Found {len([r for r in all_records if r['productHandle'] in df['Handle'].values])} products needing CSS scoping")
            
        except Exception as e:
            print(f"      ❌ Error processing {csv_file.name}: {e}")
            continue
    
    # Print summary
    print(f"\n📊 Analysis Summary:")
    print(f"   Total products scanned: {total_products:,}")
    print(f"   Products with style tags: {products_with_styles:,}")
    print(f"   Products needing CSS scoping: {products_needing_scoping:,}")
    
    if all_records:
        # Analyze most common issues
        selector_issues = {}
        for record in all_records:
            for change in record['changes']:
                if change['type'] == 'selector_scoped':
                    original = change['original']
                    if original not in selector_issues:
                        selector_issues[original] = 0
                    selector_issues[original] += 1
        
        if selector_issues:
            print(f"\n   🎯 Most problematic selectors:")
            for selector, count in sorted(selector_issues.items(), key=lambda x: x[1], reverse=True)[:10]:
                print(f"      - '{selector}': {count} occurrences")
    
    return all_records


def main():
    """Main execution function"""
    print("=" * 70)
    print("🛡️ CSS STYLE SCOPING FOR PRODUCT DESCRIPTIONS")
    print("=" * 70)
    print("This script will:")
    print("1. Find products with CSS styles in descriptions")
    print("2. Add proper scoping to prevent theme conflicts")
    print("3. Wrap content in scoped container")
    print("4. Generate JSON for GraphQL updates")
    print("=" * 70)
    
    try:
        # Process products
        records = process_products_for_css_scoping()
        
        if not records:
            print("\n✅ No products need CSS scoping!")
            return 0
        
        # Sort by number of changes needed
        records.sort(key=lambda x: x['changesCount'], reverse=True)
        
        # Save JSON for Node.js processing
        print(f"\n💾 Saving JSON data for GraphQL processing...")
        
        json_data = {
            'timestamp': datetime.now().isoformat(),
            'summary': {
                'total_products': len(records),
                'total_changes': sum(r['changesCount'] for r in records),
                'scope_class': CSSStyleScoper().scope_class
            },
            'data': records
        }
        
        json_path = Path(__file__).parent.parent / "shared" / "css_scoped_descriptions.json"
        json_path.parent.mkdir(exist_ok=True)
        
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
        
        print(f"   ✅ Saved to: {json_path}")
        
        # Print examples
        print(f"\n📋 Sample products needing CSS scoping:")
        print(f"{'Handle':<40} {'Changes':<10} {'Style Tags':<12}")
        print("-" * 65)
        
        for record in records[:10]:
            handle = record['productHandle']
            if len(handle) > 37:
                handle = handle[:37] + "..."
            print(f"{handle:<40} {record['changesCount']:<10} {record['styleTagsProcessed']:<12}")
        
        if len(records) > 10:
            print(f"... and {len(records) - 10} more products")
        
        print(f"\n🎉 CSS scoping analysis completed successfully!")
        print(f"\n💡 Next steps:")
        print(f"   1. Review the generated JSON file")
        print(f"   2. Run: cd ../../node && node src/06_update_products_description.js --css-scoping")
        print(f"   3. Verify updated products in Shopify admin")
        
        return 0
        
    except KeyboardInterrupt:
        print("\n⏹️ Analysis interrupted by user")
        return 1
    except Exception as e:
        print(f"\n❌ Analysis failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())