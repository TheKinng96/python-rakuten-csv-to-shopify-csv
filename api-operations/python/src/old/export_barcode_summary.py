#!/usr/bin/env python3
"""
Create a simplified export with only essential columns: Handle, SKU, Title, Barcode, Matching Status
"""

import pandas as pd
import os
from datetime import datetime

def create_simplified_export(detailed_report_file, output_file=None):
    """
    Create simplified export with only essential columns.
    
    Args:
        detailed_report_file: Path to the detailed barcode matching report
        output_file: Output file path (optional)
    
    Returns:
        str: Path to simplified export file
    """
    if output_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_dir = os.path.dirname(detailed_report_file)
        output_file = os.path.join(base_dir, f"barcode_summary_{timestamp}.csv")
    
    try:
        # Load the detailed report
        df = pd.read_csv(detailed_report_file, encoding='utf-8')
        
        # Select only the required columns
        simplified_df = df[['Handle', 'Variant SKU', 'Title', 'Variant Barcode', 'Barcode_Match_Status']].copy()
        
        # Rename columns for clarity
        simplified_df.columns = ['Handle', 'SKU', 'Title', 'Barcode', 'Barcode_Match_Status']
        
        # Sort by match status (matched items first) then by handle
        simplified_df = simplified_df.sort_values(['Barcode_Match_Status', 'Handle'], ascending=[False, True])
        
        # Save to CSV
        simplified_df.to_csv(output_file, index=False, encoding='utf-8')
        
        # Print summary
        total_count = len(simplified_df)
        matched_count = len(simplified_df[simplified_df['Barcode_Match_Status'] == 'Found'])
        not_matched_count = len(simplified_df[simplified_df['Barcode_Match_Status'] == 'Not Found'])
        
        print(f"Simplified export created: {output_file}")
        print(f"Total products: {total_count}")
        print(f"With barcodes: {matched_count}")
        print(f"Without barcodes: {not_matched_count}")
        
        return output_file
    
    except Exception as e:
        print(f"Error creating simplified export: {str(e)}")
        return None

def main():
    """Main function to create simplified export from latest report."""
    data_dir = "/Users/gen/corekara/rakuten-shopify/api-operations/data"
    
    # Find the latest barcode matching report
    import glob
    report_pattern = os.path.join(data_dir, "barcode_matching_report_*.csv")
    report_files = glob.glob(report_pattern)
    
    if not report_files:
        print("No barcode matching report found. Run export_missing_barcodes.py first.")
        return
    
    # Get the most recent report
    latest_report = max(report_files, key=os.path.getmtime)
    print(f"Using report: {os.path.basename(latest_report)}")
    
    # Create simplified export
    simplified_file = create_simplified_export(latest_report)
    
    if simplified_file:
        print(f"\nSimplified export ready: {os.path.basename(simplified_file)}")
    else:
        print("Failed to create simplified export")

if __name__ == "__main__":
    main()