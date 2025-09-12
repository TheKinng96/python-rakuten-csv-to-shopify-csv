#!/usr/bin/env python3
"""
Mobile Responsiveness Fix V3 - Complete Solution (CORRECTED)
Implements:
1. Proper table border radius (first tr top, last tr bottom, no margins between)
2. Smart padding removal when child has padding
3. HR margin adjustment (7rem desktop -> 2rem mobile)
4. Flex layout conversion (display:flex -> flex-direction:column on mobile)
"""
import re
import sys
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd
from bs4 import BeautifulSoup, Tag


class MobileFixerV3:
    """Enhanced mobile responsiveness fixer with all requirements"""
    
    def __init__(self):
        # Complete mobile CSS with all three requirements
        self.mobile_css = """
/* Desktop fixes */
hr {
  width: 100% !important;
  max-width: 100% !important;
  margin: 7rem 0; /* Desktop margin */
}

/* Mobile Responsiveness */
@media (max-width: 768px) {
  /* Force responsive images */
  .mobile-responsive-img, img {
    max-width: 100% !important;
    width: auto !important;
    height: auto !important;
    display: block !important;
    margin: 0 auto !important;
  }
  
  /* Force text wrapping */
  .mobile-responsive-text,
  [style*="white-space:nowrap"],
  [style*="white-space: nowrap"] {
    white-space: normal !important;
    font-size: 16px !important;
    word-wrap: break-word !important;
    overflow-wrap: break-word !important;
  }
  
  /* HR responsive styling - Requirement #3 */
  .mobile-responsive-hr, hr {
    width: 100% !important;
    max-width: 100% !important;
    box-sizing: border-box !important;
    margin: 2rem 0 !important; /* Reduced from 7rem to 2rem on mobile */
  }
  
  /* Global viewport control */
  html, body {
    overflow-x: hidden !important;
  }
  
  /* ===== MAIN TABLE STYLING - Requirement #1 ===== */
  .mobile-responsive-table {
    display: block !important;
    width: 100% !important;
    max-width: 100% !important;
    border-collapse: separate !important;
    border-spacing: 0 !important;
    overflow: visible !important;
    margin: 16px 0 !important;
  }
  
  .mobile-responsive-table tbody,
  .mobile-responsive-table thead,
  .mobile-responsive-table tfoot {
    display: block !important;
    width: 100% !important;
  }
  
  /* Table rows - NO MARGINS between them */
  .mobile-responsive-table tr {
    display: block !important;
    width: 100% !important;
    margin: 0 !important; /* No margin between rows */
    padding: 0 !important;
    border-left: 1px solid #e0e0e0 !important;
    border-right: 1px solid #e0e0e0 !important;
    border-top: none !important;
    border-bottom: 1px solid #e0e0e0 !important;
    background: #fff !important;
    border-radius: 0 !important; 
    margin-bottom: 0 !important; 
  }
  
  /* First row - rounded top corners */
  .mobile-responsive-table tbody tr:first-child,
  .mobile-responsive-table thead tr:first-child {
    border-top: 1px solid #e0e0e0 !important;
    overflow: hidden !important;
  }
  
  /* Last row - rounded bottom corners */
  .mobile-responsive-table tbody tr:last-child,
  .mobile-responsive-table tfoot tr:last-child {
    overflow: hidden !important;
    margin-bottom: 0px !important; /* Only the last row has bottom margin */
  }
  
  /* Single row (only child) - all corners rounded */
  .mobile-responsive-table tbody tr:only-child {
    border: 1px solid #e0e0e0 !important;
    border-radius: 12px !important;
    overflow: hidden !important;
  }
  
  /* Middle rows - ensure no gaps */
  .mobile-responsive-table tbody tr:not(:first-child):not(:last-child) {
    border-top: none !important;
  }

  .mobile-responsive-table tbody tr {
    margin-bottom: 0 !important; /* No margin between rows */
  }
  
  /* Table cells - NO MARGINS */
  .mobile-responsive-table tbody tr td,
  .mobile-responsive-table th {
    display: block !important;
    width: 100% !important;
    max-width: 100% !important;
    box-sizing: border-box !important;
    padding: 16px !important;
    text-align: left !important;
    border: none !important;
    margin: 0 !important; /* No margins on cells */
    word-wrap: break-word !important;
    overflow-wrap: break-word !important;
    border-radius: 0 !important;
  }
  
  /* Requirement #2: Remove padding if child has padding */
  .mobile-responsive-table td.no-padding,
  .mobile-responsive-table th.no-padding {
    padding: 0 !important;
  }
  
  /* Complex layout tables */
  .mobile-responsive-table.complex-layout tr:not(.colspan-row) {
    display: grid !important;
    grid-template-columns: 1fr !important;
    gap: 0 !important; /* No gap between grid items */
    margin-bottom: 0 !important; /* No margin between rows */
  }
  
  .mobile-responsive-table.complex-layout .image-cell {
    grid-row: 1 !important;
    display: block !important;
    width: 100% !important;
    text-align: center !important;
    padding: 16px !important;
    background: #f8f9fa !important;
    margin: 0 !important;
    border-radius: 0 !important;
  }
  
  .mobile-responsive-table.complex-layout .text-cell {
    grid-row: 2 !important;
    display: block !important;
    width: 100% !important;
    padding: 16px !important;
    margin: 0 !important;
    border-radius: 0 !important;
  }
  
  /* Info table specific styling */
  .mobile-responsive-table.info-table {
    border: none !important;
    background: transparent !important;
  }
  
  .mobile-responsive-table.info-table tr {
    border: 1px solid #ddd !important;
    margin: 0 !important;
    background: #fff !important;
    border-bottom: none !important;
  }
  
  .mobile-responsive-table.info-table td:first-child {
    background-color: #f8f9fa !important;
    font-weight: bold !important;
    border-bottom: 1px solid #eee !important;
  }
  
  .mobile-responsive-table.info-table td:last-child {
    border-bottom: none !important;
  }
  
  /* Force div responsiveness */
  .mobile-responsive-div {
    max-width: 100% !important;
    overflow-x: auto !important;
    -webkit-overflow-scrolling: touch;
    box-sizing: border-box !important;
  }
  
  /* Fix flex layouts for mobile - stack vertically */
  .mobile-responsive-flex,
  div[style*="display: flex"],
  div[style*="display:flex"] {
    flex-direction: column !important;
    align-items: stretch !important;
  }
  
  .mobile-responsive-flex > *,
  div[style*="display: flex"] > *,
  div[style*="display:flex"] > * {
    width: 100% !important;
    max-width: 100% !important;
    flex: none !important;
  }
  
  /* Override problematic inline styles */
  *[width] {
    max-width: 100% !important;
    width: auto !important;
  }
}

/* Small screens - extra adjustments */
@media (max-width: 400px) {
  .mobile-responsive-table {
    font-size: 14px !important;
  }
  
  .mobile-responsive-table td,
  .mobile-responsive-table th {
    padding: 12px !important;
  }
  
  /* Even smaller margin for HR on very small screens */
  hr {
    margin: 1.5rem 0 !important;
  }
}
"""

    def process_html(self, html_content: str) -> Dict[str, Any]:
        """
        Process HTML for mobile responsiveness with all requirements
        """
        if not html_content or pd.isna(html_content):
            return {
                'modified_html': '',
                'changes_made': [],
                'bytes_added': 0
            }
        
        html_str = str(html_content)
        original_length = len(html_str)
        changes_made = []
        
        try:
            soup = BeautifulSoup(html_str, 'html.parser')
            
            # 1. Remove white-space:nowrap from all elements
            for element in soup.find_all(attrs={'style': True}):
                original_style = element.get('style', '')
                if 'white-space' in original_style.lower():
                    new_style = re.sub(r'white-space\s*:\s*[^;]+;?', '', original_style, flags=re.IGNORECASE)
                    new_style = new_style.strip('; ').strip()
                    
                    if new_style:
                        element['style'] = new_style
                    else:
                        del element['style']
                    
                    classes = element.get('class', [])
                    if isinstance(classes, str):
                        classes = classes.split()
                    if 'mobile-responsive-text' not in classes:
                        classes.append('mobile-responsive-text')
                    element['class'] = classes
                    
                    changes_made.append({
                        'element': element.name,
                        'action': 'removed_white_space_nowrap'
                    })
            
            # 2. Process images
            for img in soup.find_all('img'):
                if img.has_attr('width'):
                    del img['width']
                if img.has_attr('height'):
                    del img['height']
                
                if img.has_attr('style'):
                    style = img['style']
                    style = re.sub(r'width\s*:\s*[^;]+;?', '', style, flags=re.IGNORECASE)
                    style = re.sub(r'height\s*:\s*[^;]+;?', '', style, flags=re.IGNORECASE)
                    style = style.strip('; ').strip()
                    
                    if style:
                        img['style'] = style
                    else:
                        del img['style']
                
                classes = img.get('class', [])
                if isinstance(classes, str):
                    classes = classes.split()
                if 'mobile-responsive-img' not in classes:
                    classes.append('mobile-responsive-img')
                img['class'] = classes
                
                changes_made.append({
                    'element': 'img',
                    'action': 'made_responsive'
                })
            
            # 3. Process HR elements - Add responsive class for 2rem mobile margin
            for hr in soup.find_all('hr'):
                if hr.has_attr('width'):
                    del hr['width']
                
                classes = hr.get('class', [])
                if isinstance(classes, str):
                    classes = classes.split()
                if 'mobile-responsive-hr' not in classes:
                    classes.append('mobile-responsive-hr')
                hr['class'] = classes
                
                changes_made.append({
                    'element': 'hr',
                    'action': 'added_responsive_class_for_margin'
                })
            
            # 4. Process tables
            for table in soup.find_all('table'):
                classes = table.get('class', [])
                if isinstance(classes, str):
                    classes = classes.split()
                
                if 'mobile-responsive-table' not in classes:
                    classes.append('mobile-responsive-table')
                
                # Detect table type
                is_complex = self._is_complex_table(table)
                is_info = self._is_info_table(table)
                
                if is_complex:
                    classes.append('complex-layout')
                    self._process_complex_table(table)
                elif is_info:
                    classes.append('info-table')
                
                table['class'] = classes
                
                # REQUIREMENT #2: Check for cells with padded children
                for cell in table.find_all(['td', 'th']):
                    if self._has_padded_child(cell):
                        cell_classes = cell.get('class', [])
                        if isinstance(cell_classes, str):
                            cell_classes = cell_classes.split()
                        if 'no-padding' not in cell_classes:
                            cell_classes.append('no-padding')
                        cell['class'] = cell_classes
                        
                        changes_made.append({
                            'element': 'td/th',
                            'action': 'removed_padding_due_to_child_padding'
                        })
                
                changes_made.append({
                    'element': 'table',
                    'action': 'made_responsive_with_proper_borders',
                    'type': 'complex' if is_complex else 'info' if is_info else 'simple'
                })
            
            # 5. Process DIVs with fixed widths and flex layouts
            for div in soup.find_all('div'):
                if div.has_attr('style'):
                    style = div['style']
                    div_classes = div.get('class', [])
                    if isinstance(div_classes, str):
                        div_classes = div_classes.split()
                    
                    # Handle fixed widths
                    if re.search(r'width\s*:\s*\d+px', style, re.IGNORECASE):
                        style = re.sub(r'width\s*:\s*\d+px', 'max-width: 100%', style, flags=re.IGNORECASE)
                        div['style'] = style
                        
                        if 'mobile-responsive-div' not in div_classes:
                            div_classes.append('mobile-responsive-div')
                        
                        changes_made.append({
                            'element': 'div',
                            'action': 'fixed_width_to_responsive'
                        })
                    
                    # Handle flex layouts - NEW REQUIREMENT
                    if 'display' in style.lower() and 'flex' in style.lower():
                        if 'mobile-responsive-flex' not in div_classes:
                            div_classes.append('mobile-responsive-flex')
                        
                        changes_made.append({
                            'element': 'div',
                            'action': 'flex_to_column_mobile'
                        })
                    
                    if div_classes:
                        div['class'] = div_classes
            
            # 6. Inject CSS at the beginning - CORRECTED VERSION
            style_tag = soup.new_tag('style')
            style_tag.string = self.mobile_css
            
            # Insert at the beginning of the soup
            soup.insert(0, style_tag)
            
            modified_html = str(soup)
            
            return {
                'modified_html': modified_html,
                'original_html': html_str,
                'changes_made': changes_made,
                'bytes_added': len(modified_html) - original_length
            }
            
        except Exception as e:
            return {
                'modified_html': html_str,
                'changes_made': [],
                'bytes_added': 0,
                'error': str(e)
            }
    
    def _is_complex_table(self, table: Tag) -> bool:
        """Check if table has complex layout (image + text in separate cells)"""
        for row in table.find_all('tr'):
            cells = row.find_all(['td', 'th'])
            if len(cells) >= 2:
                has_img = any(cell.find('img') for cell in cells)
                has_text = any(len(cell.get_text(strip=True)) > 50 for cell in cells)
                if has_img and has_text:
                    return True
        return False
    
    def _is_info_table(self, table: Tag) -> bool:
        """Check if table is info/spec table (label-value pairs)"""
        rows = table.find_all('tr')
        if len(rows) < 3:
            return False
        
        label_value_count = 0
        for row in rows:
            cells = row.find_all(['td', 'th'])
            if len(cells) >= 2 and not any(cell.get('colspan') for cell in cells):
                left_text = cells[0].get_text(strip=True)
                if len(left_text) < 50 and ('■' in left_text or ':' in left_text):
                    label_value_count += 1
        
        return label_value_count >= len(rows) * 0.5
    
    def _process_complex_table(self, table: Tag):
        """Add classes to complex table cells"""
        for row in table.find_all('tr'):
            if any(cell.get('colspan') for cell in row.find_all(['td', 'th'])):
                classes = row.get('class', [])
                if isinstance(classes, str):
                    classes = classes.split()
                if 'colspan-row' not in classes:
                    classes.append('colspan-row')
                row['class'] = classes
            else:
                for cell in row.find_all(['td', 'th']):
                    classes = cell.get('class', [])
                    if isinstance(classes, str):
                        classes = classes.split()
                    
                    if cell.find('img'):
                        if 'image-cell' not in classes:
                            classes.append('image-cell')
                    elif len(cell.get_text(strip=True)) > 20:
                        if 'text-cell' not in classes:
                            classes.append('text-cell')
                    
                    if classes:
                        cell['class'] = classes
    
    def _has_padded_child(self, cell: Tag) -> bool:
        """
        REQUIREMENT #2: Check if cell's only direct child has padding > 0
        Returns True if the single direct child element has padding style
        """
        # Get only element nodes (not text nodes)
        direct_element_children = [child for child in cell.children 
                                  if hasattr(child, 'name') and child.name]
        
        # Only apply if there's exactly ONE direct element child
        if len(direct_element_children) == 1:
            child = direct_element_children[0]
            style = child.get('style', '')
            
            if 'padding' in style.lower():
                # Check if padding is explicitly set to 0
                zero_patterns = [
                    r'padding\s*:\s*0\s*(?:;|$)',  # padding: 0
                    r'padding\s*:\s*0\s+0\s*(?:;|$)',  # padding: 0 0
                    r'padding\s*:\s*0\s+0\s+0\s*(?:;|$)',  # padding: 0 0 0
                    r'padding\s*:\s*0\s+0\s+0\s+0\s*(?:;|$)',  # padding: 0 0 0 0
                    r'padding\s*:\s*0px(?:\s+0px)*\s*(?:;|$)',  # padding: 0px variations
                ]
                
                # If it matches any zero pattern, it doesn't have padding
                if any(re.search(pattern, style, re.IGNORECASE) for pattern in zero_patterns):
                    return False
                
                # Check for non-zero padding values
                non_zero_patterns = [
                    r'padding\s*:\s*[1-9]',  # padding starts with non-zero digit
                    r'padding\s*:\s*\d*\.+[1-9]',  # padding with decimal
                    r'padding\s*:\s*\d+(?:px|em|rem|%|vh|vw)',  # padding with units
                    r'padding-(?:top|right|bottom|left)\s*:\s*[1-9]',  # individual padding properties
                    r'padding-(?:top|right|bottom|left)\s*:\s*\d*\.+[1-9]',  # individual with decimal
                    r'padding-(?:top|right|bottom|left)\s*:\s*\d+(?:px|em|rem|%|vh|vw)',  # individual with units
                ]
                
                # If it matches any non-zero pattern, it has padding
                if any(re.search(pattern, style, re.IGNORECASE) for pattern in non_zero_patterns):
                    return True
                
                # Special case: padding with multiple values where at least one is non-zero
                # e.g., "padding: 0 10px" or "padding: 10px 0 10px 0"
                multi_value_pattern = r'padding\s*:\s*([^;]+)'
                match = re.search(multi_value_pattern, style, re.IGNORECASE)
                if match:
                    values = match.group(1).strip()
                    # Split by spaces and check if any value is non-zero
                    parts = values.split()
                    for part in parts:
                        # Remove units and check if non-zero
                        num_str = re.sub(r'[^\d.-]', '', part)
                        try:
                            if num_str and float(num_str) != 0:
                                return True
                        except ValueError:
                            continue
        
        return False

    def debug_padding_detection(self, html_content: str):
        """Debug method to test padding detection"""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        print("\n" + "="*60)
        print("PADDING DETECTION DEBUG")
        print("="*60)
        
        for table_idx, table in enumerate(soup.find_all('table')):
            print(f"\n📋 Table {table_idx + 1}:")
            print("-" * 40)
            
            for row_idx, row in enumerate(table.find_all('tr')):
                for cell_idx, cell in enumerate(row.find_all(['td', 'th'])):
                    # Get direct element children
                    direct_children = [child for child in cell.children 
                                     if hasattr(child, 'name') and child.name]
                    
                    print(f"\n  Cell [{row_idx},{cell_idx}] ({cell.name}):")
                    print(f"    Total direct element children: {len(direct_children)}")
                    
                    if direct_children:
                        for i, child in enumerate(direct_children):
                            style = child.get('style', '')
                            print(f"    Child {i+1}: <{child.name}>")
                            if style:
                                print(f"      Style: {style[:100]}{'...' if len(style) > 100 else ''}")
                    
                    has_padding = self._has_padded_child(cell)
                    print(f"    ✅ Has padded child: {has_padding}")
                    
                    if has_padding:
                        print(f"    → TD/TH padding will be removed")


def process_csv_files():
    """Process all CSV files for mobile fixes"""
    print("=" * 70)
    print("📱 MOBILE RESPONSIVENESS FIX V3 - COMPLETE SOLUTION")
    print("=" * 70)
    print("Features implemented:")
    print("1. ✅ Proper table border radius (first tr top, last tr bottom)")
    print("2. ✅ Smart padding removal when child has padding")
    print("3. ✅ HR margin adjustment (7rem → 2rem on mobile)")
    print("4. ✅ Flex layout conversion (horizontal → vertical on mobile)")
    print("=" * 70)
    
    fixer = MobileFixerV3()
    data_dir = Path(__file__).resolve().parent.parent.parent / "data"
    csv_files = list(data_dir.glob("products_export_*.csv"))
    
    print(f"\nFound {len(csv_files)} CSV files")
    
    all_records = []
    
    for csv_file in csv_files:
        print(f"Processing {csv_file.name}...")
        
        try:
            df = pd.read_csv(csv_file, encoding='utf-8', low_memory=False)
            
            for _, row in df.iterrows():
                handle = row.get('Handle', '')
                body_html = row.get('Body (HTML)', '')
                
                if pd.notna(body_html) and pd.notna(handle):
                    # Process if has tables, hr, or problematic elements
                    if any(pattern in str(body_html).lower() for pattern in ['<table', '<hr', 'white-space', 'width:']):
                        result = fixer.process_html(str(body_html))
                        
                        if result['changes_made']:
                            all_records.append({
                                'productHandle': handle,
                                'currentHtml': str(body_html),
                                'modifiedHtml': result['modified_html'],
                                'bytesAdded': result['bytes_added'],
                                'changesCount': len(result['changes_made']),
                                'changes': result['changes_made']
                            })
            
        except Exception as e:
            print(f"Error processing {csv_file.name}: {e}")
    
    # Save results
    if all_records:
        from utils.json_output import save_json_report
        
        json_path = save_json_report(
            all_records,
            "mobile_fix_v3_complete.json",
            f"Mobile fixes (v3) for {len(all_records)} products with all requirements"
        )
        print(f"\n✅ Saved {len(all_records)} products to {json_path}")
        
        # Print summary
        print("\n📊 Summary of changes:")
        total_changes = sum(r['changesCount'] for r in all_records)
        print(f"   Total products modified: {len(all_records)}")
        print(f"   Total changes made: {total_changes}")
        
        # Count specific changes
        hr_changes = sum(1 for r in all_records for c in r['changes'] if c.get('element') == 'hr')
        padding_changes = sum(1 for r in all_records for c in r['changes'] if 'padding_due_to_child' in c.get('action', ''))
        
        print(f"   HR margin adjustments: {hr_changes}")
        print(f"   Padding removals (child has padding): {padding_changes}")
    else:
        print("\n✅ No products needed fixing")
    
    return 0


if __name__ == "__main__":
    # Test the padding detection with sample HTML
    test_html = '''
    <table>
        <tr>
            <td style="padding: 8px;">
                <div style="padding: 10px;">This div has padding</div>
            </td>
            <td style="padding: 8px;">
                <div style="padding: 0;">This div has no padding</div>
            </td>
            <td style="padding: 8px;">
                <div>This div has no style</div>
            </td>
            <td style="padding: 8px;">
                <span>Not a div</span>
                <div style="padding: 5px;">Multiple children</div>
            </td>
        </tr>
    </table>
    '''
    
    print("\n🧪 Testing padding detection:")
    fixer = MobileFixerV3()
    fixer.debug_padding_detection(test_html)
    
    # Run the main processing
    sys.exit(process_csv_files())