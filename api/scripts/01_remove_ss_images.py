#!/usr/bin/env python3
"""
Script to analyze and remove product images ending with -XXss.jpg pattern

This script:
1. Scans all CSV files to identify products with -XXss images
2. Generates analysis report (01_ss_images_analysis.csv)
3. Connects to Shopify API to remove problematic images
4. Generates processing report (01_ss_images_removed.csv)
"""
import csv
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd
from tqdm import tqdm

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from shopify_manager.client import ShopifyClient
from shopify_manager.config import shopify_config, path_config
from shopify_manager.logger import get_script_logger
from shopify_manager.models import ShopifyProduct, SSImageRecord

logger = get_script_logger("01_remove_ss_images")

# Regex pattern for -XXss.jpg images
SS_IMAGE_PATTERN = re.compile(r'-(\d+)ss\.jpg', re.IGNORECASE)


def analyze_csv_for_ss_images() -> List[Dict[str, Any]]:
    """
    Analyze CSV files to find products with -XXss.jpg images
    Returns list of records for analysis report
    """
    logger.info("Starting analysis of CSV files for -XXss images...")
    
    analysis_records = []
    csv_files = path_config.get_csv_files()
    
    for csv_file in csv_files:
        logger.info(f"Analyzing {csv_file.name}...")
        
        try:
            # Read CSV in chunks to handle large files
            chunk_iter = pd.read_csv(
                csv_file, 
                chunksize=path_config.api_root.parent / shopify_config.chunk_size,
                encoding='utf-8',
                low_memory=False
            )
            
            for chunk_idx, chunk in enumerate(chunk_iter):
                logger.debug(f"Processing chunk {chunk_idx + 1} from {csv_file.name}")
                
                for _, row in chunk.iterrows():
                    handle = row.get('Handle', '')
                    image_src = row.get('Image Src', '')
                    image_position = row.get('Image Position', 0)
                    
                    if pd.isna(image_src) or not image_src:
                        continue
                    
                    # Check for -XXss pattern
                    ss_match = SS_IMAGE_PATTERN.search(str(image_src))
                    if ss_match:
                        analysis_records.append({
                            'product_handle': handle,
                            'shopify_product_id': None,  # Will be filled when connecting to API
                            'image_url': image_src,
                            'image_position': image_position,
                            'ss_pattern_found': ss_match.group(0),
                            'ss_number': ss_match.group(1),
                            'csv_source': csv_file.name
                        })
                        
        except Exception as e:
            logger.error(f"Error processing {csv_file.name}: {e}")
            continue
    
    logger.info(f"Found {len(analysis_records)} images with -XXss pattern")
    return analysis_records


def save_analysis_report(analysis_records: List[Dict[str, Any]]) -> None:
    """Save analysis results to CSV report"""
    report_path = path_config.get_report_path("01_ss_images_analysis.csv")
    
    logger.info(f"Saving analysis report to {report_path}")
    
    with open(report_path, 'w', newline='', encoding='utf-8') as f:
        if analysis_records:
            fieldnames = analysis_records[0].keys()
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(analysis_records)
        else:
            # Empty file with headers
            writer = csv.writer(f)
            writer.writerow([
                'product_handle', 'shopify_product_id', 'image_url', 
                'image_position', 'ss_pattern_found', 'ss_number', 'csv_source'
            ])
    
    logger.info(f"Analysis report saved with {len(analysis_records)} records")


def process_ss_images_via_api(analysis_records: List[Dict[str, Any]]) -> List[SSImageRecord]:
    """
    Connect to Shopify API and remove -XXss images
    Returns list of processing records
    """
    logger.info("Starting API processing to remove -XXss images...")
    
    # Initialize Shopify client
    client = ShopifyClient(shopify_config, use_test_store=True)
    processing_records = []
    
    # Group records by product handle for efficient processing
    handle_groups = {}
    for record in analysis_records:
        handle = record['product_handle']
        if handle not in handle_groups:
            handle_groups[handle] = []
        handle_groups[handle].append(record)
    
    logger.info(f"Processing {len(handle_groups)} unique products")
    
    for handle, handle_records in tqdm(handle_groups.items(), desc="Processing products"):
        try:
            # Get all products to find the one with this handle
            # Note: Shopify doesn't have direct handle lookup, so we need to search
            products = client.get_all_products(fields="id,handle,images")
            
            shopify_product = None
            for product_data in products:
                if product_data.get('handle') == handle:
                    shopify_product = ShopifyProduct.from_shopify_api(product_data)
                    break
            
            if not shopify_product:
                logger.warning(f"Product with handle '{handle}' not found in Shopify")
                for record in handle_records:
                    processing_records.append(SSImageRecord(
                        product_handle=handle,
                        shopify_product_id=None,
                        timestamp=datetime.now(),
                        status="error",
                        error_message="Product not found in Shopify",
                        image_url=record['image_url'],
                        image_position=record['image_position'],
                        ss_pattern_found=record['ss_pattern_found'],
                        removal_successful=False
                    ))
                continue
            
            # Process each SS image for this product
            for record in handle_records:
                image_url = record['image_url']
                
                # Find the image in the product's images
                target_image = None
                for img in shopify_product.images:
                    if img.src == image_url:
                        target_image = img
                        break
                
                if not target_image or not target_image.id:
                    logger.warning(f"Image {image_url} not found in product {handle}")
                    processing_records.append(SSImageRecord(
                        product_handle=handle,
                        shopify_product_id=shopify_product.id,
                        timestamp=datetime.now(),
                        status="error",
                        error_message="Image not found in product",
                        image_url=image_url,
                        image_position=record['image_position'],
                        ss_pattern_found=record['ss_pattern_found'],
                        removal_successful=False
                    ))
                    continue
                
                # Remove the image
                try:
                    client.delete_product_image(shopify_product.id, target_image.id)
                    
                    processing_records.append(SSImageRecord(
                        product_handle=handle,
                        shopify_product_id=shopify_product.id,
                        timestamp=datetime.now(),
                        status="success",
                        image_url=image_url,
                        image_position=record['image_position'],
                        ss_pattern_found=record['ss_pattern_found'],
                        removal_successful=True
                    ))
                    
                    logger.info(f"Removed image {target_image.id} from product {handle}")
                    
                except Exception as e:
                    logger.error(f"Failed to remove image {target_image.id} from product {handle}: {e}")
                    processing_records.append(SSImageRecord(
                        product_handle=handle,
                        shopify_product_id=shopify_product.id,
                        timestamp=datetime.now(),
                        status="error",
                        error_message=str(e),
                        image_url=image_url,
                        image_position=record['image_position'],
                        ss_pattern_found=record['ss_pattern_found'],
                        removal_successful=False
                    ))
                    
        except Exception as e:
            logger.error(f"Error processing product {handle}: {e}")
            for record in handle_records:
                processing_records.append(SSImageRecord(
                    product_handle=handle,
                    shopify_product_id=None,
                    timestamp=datetime.now(),
                    status="error",
                    error_message=str(e),
                    image_url=record['image_url'],
                    image_position=record['image_position'],
                    ss_pattern_found=record['ss_pattern_found'],
                    removal_successful=False
                ))
    
    logger.info(f"API processing completed. {len(processing_records)} records generated")
    return processing_records


def save_processing_report(processing_records: List[SSImageRecord]) -> None:
    """Save processing results to CSV report"""
    report_path = path_config.get_report_path("01_ss_images_removed.csv")
    
    logger.info(f"Saving processing report to {report_path}")
    
    with open(report_path, 'w', newline='', encoding='utf-8') as f:
        fieldnames = [
            'product_handle', 'shopify_product_id', 'image_url', 'image_position',
            'ss_pattern_found', 'removal_successful', 'timestamp', 'status', 'error_message'
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for record in processing_records:
            writer.writerow({
                'product_handle': record.product_handle,
                'shopify_product_id': record.shopify_product_id,
                'image_url': record.image_url,
                'image_position': record.image_position,
                'ss_pattern_found': record.ss_pattern_found,
                'removal_successful': record.removal_successful,
                'timestamp': record.timestamp.isoformat(),
                'status': record.status,
                'error_message': record.error_message
            })
    
    # Generate summary
    successful = sum(1 for r in processing_records if r.removal_successful)
    total = len(processing_records)
    logger.info(f"Processing report saved: {successful}/{total} images successfully removed")


def main():
    """Main execution function"""
    logger.info("=" * 60)
    logger.info("Starting SS Images Removal Script")
    logger.info("=" * 60)
    
    try:
        # Phase 1: Analyze CSV files
        logger.info("Phase 1: Analyzing CSV files...")
        analysis_records = analyze_csv_for_ss_images()
        save_analysis_report(analysis_records)
        
        if not analysis_records:
            logger.info("No -XXss images found in CSV files. Exiting.")
            return
        
        # Phase 2: Process via API (only if not dry run or user confirms)
        if shopify_config.dry_run:
            logger.info("DRY RUN mode enabled. Skipping API processing.")
            logger.info(f"Would process {len(analysis_records)} -XXss images")
        else:
            logger.info("Phase 2: Processing via Shopify API...")
            response = input(f"Process {len(analysis_records)} -XXss images via API? (y/N): ")
            if response.lower() == 'y':
                processing_records = process_ss_images_via_api(analysis_records)
                save_processing_report(processing_records)
            else:
                logger.info("API processing skipped by user")
        
    except KeyboardInterrupt:
        logger.info("Script interrupted by user")
    except Exception as e:
        logger.error(f"Script failed with error: {e}")
        raise
    finally:
        logger.info("SS Images Removal Script completed")


if __name__ == "__main__":
    main()