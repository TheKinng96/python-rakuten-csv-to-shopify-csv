#!/usr/bin/env python3
"""
Extract products with -XXss.jpg or -XXss.png images from Shopify CSV
for removal via GraphQL mutations.
"""

import pandas as pd
import re
import csv
from pathlib import Path

def extract_ss_images(input_csv_path, output_csv_path):
    """
    Extract products with -XXss.jpg or -XXss.png images from Shopify CSV.
    
    Args:
        input_csv_path: Path to shopify_products.csv
        output_csv_path: Path to output CSV with SS images
    """
    
    # Read the CSV file
    print(f"Reading CSV file: {input_csv_path}")
    df = pd.read_csv(input_csv_path, encoding='utf-8')
    
    # Pattern to match -XXss.jpg or -XXss.png (where XX is one or more digits)
    ss_pattern = re.compile(r'-\d+ss\.(jpg|png)$', re.IGNORECASE)
    
    # Filter rows that have Image Src matching the SS pattern
    ss_rows = df[df['Image Src'].notna() & df['Image Src'].str.contains(ss_pattern, na=False)]
    
    if ss_rows.empty:
        print("No images with -XXss.jpg or -XXss.png pattern found.")
        return
    
    # Extract relevant columns
    result_df = ss_rows[['Handle', 'Image Src', 'Image Alt Text']].copy()
    
    # Remove duplicates
    result_df = result_df.drop_duplicates()
    
    # Sort by Handle for easier review
    result_df = result_df.sort_values('Handle')
    
    # Save to CSV
    result_df.to_csv(output_csv_path, index=False, encoding='utf-8')
    
    print(f"Found {len(result_df)} images with -XXss pattern")
    print(f"Affecting {result_df['Handle'].nunique()} unique products")
    print(f"Results saved to: {output_csv_path}")
    
    # Display summary
    print("\nSample of found SS images:")
    print(result_df.head(10).to_string(index=False))
    
    return result_df

def analyze_ss_images(df):
    """
    Analyze the SS images to provide insights for GraphQL operations.
    
    Args:
        df: DataFrame with SS images
    """
    print("\n" + "="*50)
    print("ANALYSIS FOR GRAPHQL OPERATIONS")
    print("="*50)
    
    # Group by handle to see how many SS images per product
    handle_counts = df.groupby('Handle').size()
    
    print(f"\nProducts with multiple SS images:")
    multiple_ss = handle_counts[handle_counts > 1]
    if not multiple_ss.empty:
        print(multiple_ss.head(10).to_string())
    else:
        print("All products have only 1 SS image each.")
    
    # Show unique alt text patterns
    alt_texts = df['Image Alt Text'].dropna().unique()
    print(f"\nUnique Alt Text patterns found ({len(alt_texts)} total):")
    for i, alt in enumerate(sorted(alt_texts)[:10]):
        print(f"  {i+1}. {alt}")
    if len(alt_texts) > 10:
        print(f"  ... and {len(alt_texts) - 10} more")
    
    # Show file extension breakdown
    jpg_count = df['Image Src'].str.contains(r'-\d+ss\.jpg$', case=False, na=False).sum()
    png_count = df['Image Src'].str.contains(r'-\d+ss\.png$', case=False, na=False).sum()
    
    print(f"\nFile extension breakdown:")
    print(f"  -XXss.jpg: {jpg_count} images")
    print(f"  -XXss.png: {png_count} images")
    
    print(f"\nNext steps for GraphQL operations:")
    print("1. Use the Handle column to query products by handle")
    print("2. Use the Image Alt Text to identify specific images to remove")
    print("3. The productCreateMedia mutation can recreate needed images")
    print("4. The productVariantBulkUpdate mutation can update variant images")

if __name__ == "__main__":
    # File paths
    input_path = Path("../manual/final/output/shopify_products.csv")
    output_path = Path("reports/ss_images_for_removal.csv")
    
    # Check if input file exists
    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}")
        print("Please ensure shopify_products.csv exists in the manual/final/output directory.")
        exit(1)
    
    # Extract SS images
    result_df = extract_ss_images(input_path, output_path)
    
    if result_df is not None and not result_df.empty:
        # Analyze the results
        analyze_ss_images(result_df)
        
        print(f"\n" + "="*50)
        print("OUTPUT FILE CREATED")
        print("="*50)
        print(f"CSV file created: {output_path}")
        print("This file contains all products with -XXss images that need to be removed from Shopify.")