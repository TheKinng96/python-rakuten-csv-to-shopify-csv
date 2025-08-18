#!/usr/bin/env python3
"""
HTML Table Formatter for Shopify Product CSV Files - Improved Version

This script fixes HTML table formatting issues in Shopify product CSV exports,
with improved handling of image width constraints.
"""

import csv
import re
import os
import json
import glob
from datetime import datetime
from bs4 import BeautifulSoup
import argparse


def fix_table_html(html_content):
    """
    Fix HTML table formatting issues for better Shopify display
    
    Args:
        html_content (str): Original HTML content
        
    Returns:
        tuple: (fixed_html, was_changed, change_details)
    """
    if not html_content or html_content.strip() == '':
        return html_content, False, {}
    
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove or fix problematic CSS styles
        style_tags = soup.find_all('style')
        for style_tag in style_tags:
            style_tag.decompose()
        
        # Check if content is already wrapped (from previous processing)
        existing_wrapper = None
        for div in soup.find_all('div'):
            if div.get('style') and 'overflow-x: auto' in div.get('style', ''):
                existing_wrapper = div
                break
        
        if existing_wrapper:
            # Work with the content inside the wrapper
            soup = existing_wrapper
        
        # Fix table attributes for responsiveness
        tables = soup.find_all('table')
        original_table_count = len(tables)
        images_fixed = 0
        
        for table in tables:
            # Remove fixed widths
            if table.get('width'):
                del table['width']
            
            # Add responsive CSS classes and styles
            # Check if table contains images to determine width strategy
            table_has_images = len(table.find_all('img')) > 0
            
            # Preserve existing style and border attributes
            existing_style = table.get('style', '')
            border_attr = table.get('border', '')
            bordercolor_attr = table.get('bordercolor', '')
            
            # Build border styles from attributes if they exist
            border_styles = []
            if border_attr:
                border_styles.append(f'border: {border_attr}px solid')
            if bordercolor_attr:
                if border_attr:
                    border_styles[-1] = f'border: {border_attr}px solid {bordercolor_attr}'
                else:
                    border_styles.append(f'border: 1px solid {bordercolor_attr}')
            
            # Check if table has no border but cells do (like yamaya-miso413 case)
            if not border_attr and not bordercolor_attr and 'border:' not in existing_style:
                # Check if any cells have border styles
                first_cell = table.find(['td', 'th'])
                if first_cell:
                    cell_style = first_cell.get('style', '')
                    # Look for border style in cell
                    border_match = re.search(r'border:\s*(?:solid\s*)?(\d+)px\s+(?:solid\s+)?([#\w]+)', cell_style)
                    if not border_match:
                        # Try alternate format
                        border_match = re.search(r'border:\s*(\d+)px\s+solid\s+([#\w]+)', cell_style)
                    
                    if border_match:
                        # Cell has border, apply same to table
                        border_width = border_match.group(1)
                        border_color = border_match.group(2)
                        border_styles.append(f'border: {border_width}px solid {border_color}')
            
            # Remove width-related styles from existing style but keep border styles
            cleaned_existing_style = existing_style
            # Use word boundaries to ensure we match complete property names
            width_properties = ['table-layout', 'max-width', 'min-width', 'width']  # Order matters - longest first
            for width_property in width_properties:
                pattern = rf'\b{re.escape(width_property)}\s*:\s*[^;]+;?\s*'
                cleaned_existing_style = re.sub(pattern, '', cleaned_existing_style)
            
            # Clean up any leftover semicolons and spaces
            cleaned_existing_style = re.sub(r';\s*;+', ';', cleaned_existing_style)
            cleaned_existing_style = re.sub(r'^\s*;+', '', cleaned_existing_style)
            cleaned_existing_style = re.sub(r';+\s*$', '', cleaned_existing_style)
            cleaned_existing_style = cleaned_existing_style.strip()
            
            # Determine width strategy
            if table_has_images:
                # For tables with images, use auto width to preserve layout
                new_styles = 'width: auto; max-width: 100%; table-layout: auto; border-collapse: collapse;'
            else:
                # For text-only tables, use full width
                new_styles = 'width: 100%; max-width: 100%; table-layout: auto; border-collapse: collapse;'
            
            # Combine styles: existing (cleaned) + border attributes + new responsive styles
            combined_styles = []
            if cleaned_existing_style.strip():
                combined_styles.append(cleaned_existing_style.strip().rstrip(';'))
            if border_styles:
                combined_styles.extend(border_styles)
            combined_styles.append(new_styles)
            
            table['style'] = '; '.join(filter(None, combined_styles))
            
            # Fix cell attributes
            cells = table.find_all(['td', 'th'])
            for cell in cells:
                # Check if cell contains images
                images = cell.find_all('img')
                has_images = len(images) > 0
                
                # Get current style once at the beginning
                current_style = cell.get('style', '')
                
                # For cells containing images, ensure they maintain proper width
                if has_images:
                    max_img_width = 0
                    
                    # Process each image in the cell
                    for img in images:
                        # Get image width from various sources
                        img_width = None
                        
                        # Priority 1: Check the img style attribute for width
                        if img.get('style'):
                            width_match = re.search(r'width\s*:\s*(\d+)px', img.get('style', ''))
                            if width_match:
                                try:
                                    img_width = int(width_match.group(1))
                                except (ValueError, TypeError):
                                    pass
                        
                        # Priority 2: Check the width attribute
                        if not img_width and img.get('width'):
                            width_str = str(img.get('width'))
                            # Handle percentage widths differently
                            if '%' not in width_str:
                                try:
                                    img_width = int(re.sub(r'[^\d]', '', width_str))
                                except (ValueError, TypeError):
                                    pass
                        
                        # Priority 3: Try to get width from image src URL if available
                        if not img_width and img.get('src'):
                            src = img.get('src', '')
                            # Look for common size patterns in URL
                            size_match = re.search(r'_(\d+)x\d*[._]', src)
                            if not size_match:
                                size_match = re.search(r'[/_](\d{2,3})\.(?:jpg|jpeg|png|gif)', src)
                            if size_match:
                                try:
                                    potential_width = int(size_match.group(1))
                                    # Use this as a fallback if reasonable
                                    if 50 <= potential_width <= 800:
                                        img_width = potential_width
                                except (ValueError, TypeError):
                                    pass
                        
                        # Default to a reasonable width if we couldn't detect it
                        if not img_width or img_width < 50:
                            img_width = 200  # Default reasonable width (increased from 150)
                        
                        # Track the maximum image width in this cell
                        max_img_width = max(max_img_width, img_width)
                        
                        # Fix the image style to maintain its width
                        img_style = img.get('style', '')
                        # Remove any duplicate height: auto declarations
                        img_style = re.sub(r'(?:height:\s*auto;\s*display:\s*block;\s*)+', '', img_style)
                        # Remove any existing width styles to clean up
                        img_style = re.sub(r'width\s*:\s*[^;]+;?', '', img_style)
                        img_style = re.sub(r'max-width\s*:\s*[^;]+;?', '', img_style)
                        # Set proper width and display
                        img_style += f' width: {img_width}px; height: auto; display: block;'
                        img['style'] = img_style.strip()
                        images_fixed += 1
                    
                    # CRITICAL FIX: More aggressive removal of ALL width-related styles
                    # Remove any width-related styles with more flexible patterns
                    current_style = re.sub(r'min-width\s*:\s*[^;]+;?', '', current_style)
                    current_style = re.sub(r'max-width\s*:\s*[^;]+;?', '', current_style)
                    current_style = re.sub(r'width\s*:\s*[^;]+;?', '', current_style)
                    
                    # Clean up any leftover semicolons and spaces
                    current_style = re.sub(r';\s*;+', ';', current_style)
                    current_style = re.sub(r'^\s*;+', '', current_style)
                    current_style = re.sub(r';+\s*$', '', current_style)
                    
                    # Set cell width to accommodate the largest image with some padding
                    # Use auto width to let the cell expand to fit its content
                    cell_width_style = f'width: auto; min-width: {max_img_width}px;'
                    
                    # Combine styles, ensuring there's proper spacing
                    if current_style and not current_style.endswith(';'):
                        current_style += ';'
                    current_style = (current_style + ' ' + cell_width_style).strip()
                
                else:
                    # For non-image cells, handle width attribute
                    cell_width = cell.get('width')
                    if cell_width:
                        del cell['width']
                    
                    # Remove all width declarations for non-image cells
                    current_style = re.sub(r'width\s*:\s*[^;]+;?', '', current_style)
                
                # Add responsive styling
                if 'padding' not in current_style:
                    current_style += ' padding: 8px;'
                if 'word-wrap' not in current_style:
                    current_style += ' word-wrap: break-word;'
                if 'overflow-wrap' not in current_style:
                    current_style += ' overflow-wrap: break-word;'
                
                cell['style'] = current_style.strip()
        
        # Remove empty cells and rows
        empty_elements_removed = 0
        
        # First, collect empty td/th cells (don't remove while iterating)
        cells_to_remove = []
        for cell in soup.find_all(['td', 'th']):
            # Check if cell is empty (no text, no images, no meaningful content)
            cell_text = cell.get_text(strip=True)
            cell_images = cell.find_all('img')
            cell_tables = cell.find_all('table')
            
            # If cell has no text, no images, and no nested tables, it's empty
            if not cell_text and not cell_images and not cell_tables:
                cells_to_remove.append(cell)
        
        # Remove collected empty cells
        for cell in cells_to_remove:
            if cell and cell.parent:  # Check if cell still exists in the tree
                cell.decompose()
                empty_elements_removed += 1
        
        # Then, collect and remove empty tr rows
        rows_to_remove = []
        for row in soup.find_all('tr'):
            # Check if row has any remaining cells
            cells = row.find_all(['td', 'th'])
            if not cells:
                rows_to_remove.append(row)
        
        # Remove collected empty rows
        for row in rows_to_remove:
            if row and row.parent:  # Check if row still exists in the tree
                row.decompose()
                empty_elements_removed += 1
        
        # Remove empty or meaningless tables
        tables_to_remove = []
        for table in soup.find_all('table'):
            # Get all text content from the table, excluding whitespace
            table_text = table.get_text(strip=True)
            
            # Check if table is essentially empty (only whitespace, br tags, empty cells)
            if not table_text or table_text == '':
                tables_to_remove.append(table)
                continue
        
        # Remove identified empty tables
        for table in tables_to_remove:
            if table and table.parent:  # Check if table still exists in the tree
                table.decompose()
        
        # Update tables list after removal
        tables = soup.find_all('table')
        
        # Fix nested table structures - handle image layout tables specially
        for table in tables:
            parent_td = table.find_parent('td')
            if parent_td and parent_td.find_parent('table'):
                # This is a nested table
                # Check if this table contains images - if so, use auto width instead of 100%
                has_images = len(table.find_all('img')) > 0
                current_style = table.get('style', '')
                if has_images:
                    # For tables with images, use auto width to preserve image layout
                    table['style'] = current_style + ' margin: 0; width: auto;'
                else:
                    # For text-only tables, use 100% width
                    table['style'] = current_style + ' margin: 0; width: 100%;'
        
        # Add responsive wrapper div if not already present
        if tables and not existing_wrapper:
            # Convert current soup to string
            current_html = str(soup)
            
            # Create wrapper with the content
            wrapped_html = f'<div style="width: 100%; overflow-x: auto; -webkit-overflow-scrolling: touch;">{current_html}</div>'
            
            # Parse the wrapped HTML
            soup = BeautifulSoup(wrapped_html, 'html.parser')
        
        # Clean up extra whitespace and formatting
        cleaned_html = str(soup)
        
        # Remove extra blank lines and spaces
        cleaned_html = re.sub(r'\n\s*\n', '\n', cleaned_html)
        cleaned_html = re.sub(r'>\s+<', '><', cleaned_html)
        
        # Track changes
        change_details = {
            'tables_found': original_table_count,
            'empty_tables_removed': len(tables_to_remove),
            'empty_elements_removed': empty_elements_removed,
            'tables_remaining': len(tables),
            'style_tags_removed': len(style_tags),
            'images_fixed': images_fixed,
            'original_length': len(html_content),
            'fixed_length': len(cleaned_html),
            'has_nested_tables': any(table.find_parent('table') for table in tables)
        }
        
        was_changed = cleaned_html != html_content
        return cleaned_html, was_changed, change_details
        
    except Exception as e:
        print(f"Error processing HTML: {e}")
        import traceback
        traceback.print_exc()
        return html_content, False, {}


def _get_patterns_removed(change_details):
    """
    Determine which patterns were removed based on change details
    """
    patterns = []
    if change_details.get('tables_found', 0) > 0:
        patterns.append('fixed-width-tables')
    if change_details.get('style_tags_removed', 0) > 0:
        patterns.append('style-tags')
    if change_details.get('empty_tables_removed', 0) > 0:
        patterns.append('empty-tables')
    if change_details.get('empty_elements_removed', 0) > 0:
        patterns.append('empty-cells-rows')
    if change_details.get('images_fixed', 0) > 0:
        patterns.append('image-width-preservation')
    return patterns


def process_csv_file(input_file, output_file=None, test_handle=None):
    """
    Process a CSV file to fix HTML table formatting issues
    
    Args:
        input_file (str): Path to input CSV file
        output_file (str): Path to output CSV file (optional)
        test_handle (str): Optional handle to filter for testing
        
    Returns:
        dict: Processing results including affected products
    """
    if not output_file:
        name, ext = os.path.splitext(input_file)
        output_file = f"{name}_fixed{ext}"
    
    print(f"Processing {input_file}...")
    
    rows_processed = 0
    html_fixes_made = 0
    affected_products = []
    
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8', newline='') as outfile:
        
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        
        # Get headers
        headers = next(reader)
        writer.writerow(headers)
        
        # Find Body (HTML) and Handle column indices
        html_column_index = None
        handle_column_index = None
        for i, header in enumerate(headers):
            if 'Body (HTML)' in header:
                html_column_index = i
            elif header == 'Handle':
                handle_column_index = i
        
        if html_column_index is None:
            print("Warning: No 'Body (HTML)' column found in CSV")
            # Just copy the file as-is
            for row in reader:
                writer.writerow(row)
            return {
                'file': os.path.basename(input_file),
                'rows_processed': rows_processed,
                'html_fixes_made': 0,
                'affected_products': []
            }
        
        # Process each row
        for row in reader:
            rows_processed += 1
            
            # Skip row if we're testing and it doesn't match the test handle
            if test_handle and handle_column_index is not None:
                if len(row) > handle_column_index and row[handle_column_index] != test_handle:
                    writer.writerow(row)  # Write unchanged
                    continue
            
            if len(row) > html_column_index and row[html_column_index]:
                original_html = row[html_column_index]
                fixed_html, was_changed, change_details = fix_table_html(original_html)
                
                if was_changed:
                    html_fixes_made += 1
                    row[html_column_index] = fixed_html
                    
                    # Track affected product
                    handle = row[handle_column_index] if handle_column_index is not None and len(row) > handle_column_index else f"row_{rows_processed}"
                    affected_products.append({
                        'productHandle': handle,
                        'cleanedHtml': fixed_html,
                        'originalHtml': original_html,
                        'changeDetails': change_details,
                        'bytesRemoved': len(original_html) - len(fixed_html),
                        'patternsRemoved': _get_patterns_removed(change_details)
                    })
            
            writer.writerow(row)
            
            if rows_processed % 100 == 0:
                print(f"Processed {rows_processed} rows...")
    
    print(f"Completed processing {input_file}")
    print(f"Total rows processed: {rows_processed}")
    print(f"HTML fixes applied: {html_fixes_made}")
    print(f"Output saved to: {output_file}")
    
    return {
        'file': os.path.basename(input_file),
        'rows_processed': rows_processed,
        'html_fixes_made': html_fixes_made,
        'affected_products': affected_products
    }


def save_reports(all_results):
    """
    Save processing results to reports and shared folders
    """
    # Create directories if they don't exist
    reports_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'reports')
    shared_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'shared')
    os.makedirs(reports_dir, exist_ok=True)
    os.makedirs(shared_dir, exist_ok=True)
    
    # Prepare summary report
    total_rows = sum(r['rows_processed'] for r in all_results)
    total_fixes = sum(r['html_fixes_made'] for r in all_results)
    all_affected_products = []
    
    for result in all_results:
        all_affected_products.extend(result['affected_products'])
    
    # Count total images fixed and empty elements removed
    total_images_fixed = sum(
        p['changeDetails'].get('images_fixed', 0) 
        for r in all_results 
        for p in r['affected_products']
    )
    total_empty_elements = sum(
        p['changeDetails'].get('empty_elements_removed', 0) 
        for r in all_results 
        for p in r['affected_products']
    )
    
    summary_report = {
        'timestamp': datetime.now().isoformat(),
        'summary': {
            'total_files_processed': len(all_results),
            'total_rows_processed': total_rows,
            'total_html_fixes': total_fixes,
            'total_products_affected': len(all_affected_products),
            'total_images_fixed': total_images_fixed,
            'total_empty_elements_removed': total_empty_elements
        },
        'file_results': [
            {
                'file': r['file'],
                'rows_processed': r['rows_processed'],
                'html_fixes_made': r['html_fixes_made'],
                'affected_handles': [p['productHandle'] for p in r['affected_products']]
            }
            for r in all_results
        ]
    }
    
    # Save summary report
    report_path = os.path.join(reports_dir, 'html_table_fixes_report.json')
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(summary_report, f, indent=2, ensure_ascii=False)
    print(f"\n📊 Report saved to: {report_path}")
    
    # Save data for 06_update_products_description.js
    shared_data = {
        'timestamp': datetime.now().isoformat(),
        'source': 'fix_html_tables.py',
        'data': all_affected_products
    }
    
    shared_path = os.path.join(shared_dir, 'html_table_fixes_to_update.json')
    with open(shared_path, 'w', encoding='utf-8') as f:
        json.dump(shared_data, f, indent=2, ensure_ascii=False)
    print(f"📁 Shared data saved to: {shared_path}")
    print(f"   Total products to update: {len(all_affected_products)}")
    print(f"   Total images fixed: {total_images_fixed}")


def main():
    parser = argparse.ArgumentParser(description='Fix HTML table formatting in Shopify CSV files')
    parser.add_argument('input_files', nargs='*', help='Input CSV file(s) to process (optional)')
    parser.add_argument('--output-dir', default='fixed', help='Output directory for fixed files (default: fixed)')
    parser.add_argument('--data-dir', default='data', help='Directory containing CSV files (default: data)')
    parser.add_argument('--test-handle', help='Process only a specific product handle for testing')
    
    args = parser.parse_args()
    
    # If no input files specified, process all CSV files in data directory
    if not args.input_files:
        data_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), args.data_dir)
        if not os.path.exists(data_path):
            print(f"Error: Data directory {data_path} not found")
            return
        
        csv_files = glob.glob(os.path.join(data_path, '*.csv'))
        if not csv_files:
            print(f"No CSV files found in {data_path}")
            return
        
        print(f"Found {len(csv_files)} CSV files in {data_path}")
        args.input_files = csv_files
    
    # Create output directory
    output_base = os.path.join(os.path.dirname(os.path.dirname(__file__)), args.output_dir)
    os.makedirs(output_base, exist_ok=True)
    
    all_results = []
    
    # Process each file
    for input_file in sorted(args.input_files):
        if not os.path.exists(input_file):
            print(f"Error: File {input_file} not found")
            continue
        
        filename = os.path.basename(input_file)
        name, ext = os.path.splitext(filename)
        output_file = os.path.join(output_base, f"{name}_fixed{ext}")
        
        result = process_csv_file(input_file, output_file, test_handle=args.test_handle)
        if result:
            all_results.append(result)
    
    # Save reports
    if all_results:
        save_reports(all_results)
        
        # Print summary
        print("\n" + "="*70)
        print("📊 HTML TABLE FIXES SUMMARY")
        print("="*70)
        total_rows = sum(r['rows_processed'] for r in all_results)
        total_fixes = sum(r['html_fixes_made'] for r in all_results)
        total_images_fixed = sum(
            p['changeDetails'].get('images_fixed', 0) 
            for r in all_results 
            for p in r['affected_products']
        )
        print(f"Total files processed: {len(all_results)}")
        print(f"Total rows processed: {total_rows:,}")
        print(f"Total HTML fixes applied: {total_fixes:,}")
        print(f"Total images fixed: {total_images_fixed:,}")
        if total_rows > 0:
            print(f"Fix rate: {(total_fixes/total_rows*100):.1f}%")
        print("\n💡 Next steps:")
        print("   1. Review the report in reports/html_table_fixes_report.json")
        print("   2. Use node/src/06_update_products_description.js to update products")
        print("   3. The script will read from shared/html_table_fixes_to_update.json")


if __name__ == '__main__':
    main()