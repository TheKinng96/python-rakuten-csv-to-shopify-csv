#!/usr/bin/env python3
"""
Script to audit products and variants for missing images

This script:
1. Scans all CSV files to identify products and variants
2. Connects to Shopify API to check actual image availability
3. Generates comprehensive missing images audit report (04_missing_images_audit.csv)
4. Prioritizes products based on image availability and product importance
"""
import csv
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Set

import pandas as pd
from tqdm import tqdm

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from shopify_manager.client import ShopifyClient
from shopify_manager.config import shopify_config, path_config
from shopify_manager.logger import get_script_logger
from shopify_manager.models import ShopifyProduct, MissingImageRecord

logger = get_script_logger("04_audit_images")


class ImageAuditor:
    """Audits product and variant images for completeness"""
    
    def __init__(self):
        self.priority_keywords = {
            'high': ['限定', '特別', 'プレミアム', '人気', 'おすすめ', '新商品'],
            'medium': ['セット', 'お得', 'まとめ買い', 'スーパーSALE'],
            'low': ['訳あり', 'アウトレット', 'B級品']
        }
    
    def determine_priority(self, product_title: str, product_tags: str = "", variant_count: int = 1) -> str:
        """
        Determine priority level for missing image based on product characteristics
        """
        title_lower = product_title.lower()
        tags_lower = product_tags.lower() if product_tags else ""
        combined_text = f"{title_lower} {tags_lower}"
        
        # High priority criteria
        if any(keyword in combined_text for keyword in self.priority_keywords['high']):
            return 'high'
        
        # Products with multiple variants are generally more important
        if variant_count > 3:
            return 'high'
        
        # Low priority criteria
        if any(keyword in combined_text for keyword in self.priority_keywords['low']):
            return 'low'
        
        # Medium priority criteria or default
        if any(keyword in combined_text for keyword in self.priority_keywords['medium']):
            return 'medium'
        
        return 'medium'  # Default priority
    
    def audit_product_images(self, product: ShopifyProduct) -> List[Dict[str, Any]]:
        """
        Audit a single product's images
        Returns list of audit records for products/variants with missing images
        """
        audit_records = []
        
        # Count product-level images
        product_image_count = len([img for img in product.images if img.src])
        
        # Check each variant
        for variant in product.variants:
            variant_has_image = False
            
            # Check if variant has a specific image assigned
            if variant.image_id:
                variant_has_image = any(img.id == variant.image_id for img in product.images)
            
            # Determine if this variant needs attention
            needs_attention = False
            priority_reasons = []
            
            if product_image_count == 0:
                needs_attention = True
                priority_reasons.append("no_product_images")
            
            if not variant_has_image and len(product.variants) > 1:
                needs_attention = True
                priority_reasons.append("no_variant_image")
            
            if needs_attention:
                priority = self.determine_priority(
                    product.title, 
                    product.tags, 
                    len(product.variants)
                )
                
                audit_records.append({
                    'product_handle': product.handle,
                    'shopify_product_id': product.id,
                    'variant_sku': variant.sku,
                    'variant_title': variant.title,
                    'product_title': product.title,
                    'product_image_count': product_image_count,
                    'variant_has_image': variant_has_image,
                    'variant_count': len(product.variants),
                    'priority_level': priority,
                    'priority_reasons': ', '.join(priority_reasons),
                    'product_tags': product.tags,
                    'audit_timestamp': datetime.now()
                })
        
        return audit_records


def collect_products_from_csv() -> Set[str]:
    """
    Collect unique product handles from CSV files
    Returns set of handles to audit
    """
    logger.info("Collecting product handles from CSV files...")
    
    product_handles = set()
    csv_files = path_config.get_csv_files()
    
    for csv_file in csv_files:
        logger.info(f"Scanning {csv_file.name}...")
        
        try:
            # Read CSV in chunks to handle large files
            chunk_iter = pd.read_csv(
                csv_file,
                chunksize=shopify_config.chunk_size,
                encoding='utf-8',
                low_memory=False
            )
            
            for chunk_idx, chunk in enumerate(chunk_iter):
                logger.debug(f"Processing chunk {chunk_idx + 1} from {csv_file.name}")
                
                for _, row in chunk.iterrows():
                    handle = row.get('Handle', '')
                    
                    if handle and not pd.isna(handle):
                        product_handles.add(str(handle))
                        
        except Exception as e:
            logger.error(f"Error processing {csv_file.name}: {e}")
            continue
    
    logger.info(f"Collected {len(product_handles)} unique product handles")
    return product_handles


def audit_images_via_api(product_handles: Set[str]) -> List[MissingImageRecord]:
    """
    Connect to Shopify API and audit images for all products
    Returns list of missing image records
    """
    logger.info("Starting image audit via Shopify API...")
    
    # Initialize Shopify client and auditor
    client = ShopifyClient(shopify_config, use_test_store=True)
    auditor = ImageAuditor()
    missing_image_records = []
    
    # Get all products from Shopify
    logger.info("Retrieving all products from Shopify...")
    all_products = client.get_all_products()
    
    # Create handle lookup for efficient processing
    handle_to_product = {}
    for product_data in all_products:
        handle = product_data.get('handle')
        if handle:
            handle_to_product[handle] = ShopifyProduct.from_shopify_api(product_data)
    
    logger.info(f"Retrieved {len(handle_to_product)} products from Shopify")
    
    # Audit each product handle from CSV
    not_found_handles = []
    
    for handle in tqdm(product_handles, desc="Auditing products"):
        try:
            if handle not in handle_to_product:
                not_found_handles.append(handle)
                logger.debug(f"Product handle '{handle}' not found in Shopify")
                continue
            
            product = handle_to_product[handle]
            
            # Audit this product's images
            audit_results = auditor.audit_product_images(product)
            
            # Convert to MissingImageRecord objects
            for audit_result in audit_results:
                record = MissingImageRecord(
                    product_handle=audit_result['product_handle'],
                    shopify_product_id=audit_result['shopify_product_id'],
                    variant_sku=audit_result['variant_sku'],
                    product_image_count=audit_result['product_image_count'],
                    variant_has_image=audit_result['variant_has_image'],
                    priority_level=audit_result['priority_level'],
                    product_title=audit_result['product_title'],
                    audit_timestamp=audit_result['audit_timestamp']
                )
                missing_image_records.append(record)
                
        except Exception as e:
            logger.error(f"Error auditing product {handle}: {e}")
            continue
    
    if not_found_handles:
        logger.warning(f"{len(not_found_handles)} product handles from CSV not found in Shopify")
        
        # Save not found handles for reference
        not_found_path = path_config.get_report_path("04_products_not_found_in_shopify.csv")
        with open(not_found_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['product_handle', 'note'])
            for handle in not_found_handles:
                writer.writerow([handle, 'Product exists in CSV but not found in Shopify'])
    
    logger.info(f"Image audit completed. {len(missing_image_records)} issues found")
    return missing_image_records


def save_audit_report(missing_image_records: List[MissingImageRecord]) -> None:
    """Save audit results to CSV report"""
    report_path = path_config.get_report_path("04_missing_images_audit.csv")
    
    logger.info(f"Saving audit report to {report_path}")
    
    with open(report_path, 'w', newline='', encoding='utf-8') as f:
        fieldnames = [
            'product_handle', 'shopify_product_id', 'variant_sku', 'product_title',
            'product_image_count', 'variant_has_image', 'priority_level', 
            'audit_timestamp'
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        # Sort by priority (high -> medium -> low) and then by product title
        priority_order = {'high': 0, 'medium': 1, 'low': 2}
        sorted_records = sorted(
            missing_image_records,
            key=lambda r: (priority_order.get(r.priority_level, 1), r.product_title)
        )
        
        for record in sorted_records:
            writer.writerow({
                'product_handle': record.product_handle,
                'shopify_product_id': record.shopify_product_id,
                'variant_sku': record.variant_sku,
                'product_title': record.product_title,
                'product_image_count': record.product_image_count,
                'variant_has_image': record.variant_has_image,
                'priority_level': record.priority_level,
                'audit_timestamp': record.audit_timestamp.isoformat()
            })
    
    # Generate summary by priority
    priority_counts = {}
    for record in missing_image_records:
        priority = record.priority_level
        priority_counts[priority] = priority_counts.get(priority, 0) + 1
    
    logger.info("Audit Report Summary:")
    logger.info(f"  Total issues: {len(missing_image_records)}")
    for priority in ['high', 'medium', 'low']:
        count = priority_counts.get(priority, 0)
        logger.info(f"  {priority.capitalize()} priority: {count}")


def generate_summary_report(missing_image_records: List[MissingImageRecord]) -> None:
    """Generate additional summary reports for different use cases"""
    
    # High priority products only
    high_priority_path = path_config.get_report_path("04_high_priority_missing_images.csv")
    high_priority_records = [r for r in missing_image_records if r.priority_level == 'high']
    
    with open(high_priority_path, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['product_handle', 'product_title', 'variant_sku', 'issue_type']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for record in high_priority_records:
            issue_type = "No product images" if record.product_image_count == 0 else "No variant image"
            writer.writerow({
                'product_handle': record.product_handle,
                'product_title': record.product_title,
                'variant_sku': record.variant_sku,
                'issue_type': issue_type
            })
    
    logger.info(f"High priority report saved: {len(high_priority_records)} products")
    
    # Products with no images at all
    no_images_path = path_config.get_report_path("04_products_with_no_images.csv")
    no_images_records = [r for r in missing_image_records if r.product_image_count == 0]
    
    with open(no_images_path, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['product_handle', 'product_title']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        # Deduplicate by product handle
        seen_handles = set()
        for record in no_images_records:
            if record.product_handle not in seen_handles:
                writer.writerow({
                    'product_handle': record.product_handle,
                    'product_title': record.product_title
                })
                seen_handles.add(record.product_handle)
    
    logger.info(f"No images report saved: {len(seen_handles)} products")


def main():
    """Main execution function"""
    logger.info("=" * 60)
    logger.info("Starting Missing Images Audit Script")
    logger.info("=" * 60)
    
    try:
        # Phase 1: Collect product handles from CSV
        logger.info("Phase 1: Collecting product handles from CSV files...")
        product_handles = collect_products_from_csv()
        
        if not product_handles:
            logger.info("No product handles found in CSV files. Exiting.")
            return
        
        # Phase 2: Audit via API
        logger.info("Phase 2: Auditing images via Shopify API...")
        missing_image_records = audit_images_via_api(product_handles)
        
        # Phase 3: Generate reports
        logger.info("Phase 3: Generating audit reports...")
        save_audit_report(missing_image_records)
        generate_summary_report(missing_image_records)
        
        if not missing_image_records:
            logger.info("🎉 All products have adequate images! No issues found.")
        else:
            logger.info(f"📋 Audit completed. {len(missing_image_records)} products/variants need images.")
            logger.info("Check the generated reports in the reports/ folder:")
            logger.info("  - 04_missing_images_audit.csv (complete audit)")
            logger.info("  - 04_high_priority_missing_images.csv (urgent fixes needed)")
            logger.info("  - 04_products_with_no_images.csv (products with zero images)")
        
    except KeyboardInterrupt:
        logger.info("Script interrupted by user")
    except Exception as e:
        logger.error(f"Script failed with error: {e}")
        raise
    finally:
        logger.info("Missing Images Audit Script completed")


if __name__ == "__main__":
    main()