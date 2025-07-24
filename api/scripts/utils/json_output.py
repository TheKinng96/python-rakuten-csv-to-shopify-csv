"""
Utility functions for generating JSON output files for Node.js GraphQL operations
"""
import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional


def save_json_report(data: List[Dict[str, Any]], filename: str, description: str = "") -> Path:
    """
    Save data as JSON report in shared directory
    """
    # Create shared directory if it doesn't exist
    shared_dir = Path(__file__).parent.parent.parent / "shared"
    shared_dir.mkdir(exist_ok=True)
    
    # Add metadata
    report = {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "description": description,
            "count": len(data),
            "version": "1.0"
        },
        "data": data
    }
    
    # Save JSON file
    json_path = shared_dir / filename
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Saved JSON report: {json_path}")
    print(f"   📊 {len(data)} records")
    print(f"   📝 {description}")
    
    return json_path


def create_ss_image_removal_record(
    product_handle: str,
    product_id: Optional[str],
    images_to_remove: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """Create a record for SS image removal"""
    return {
        "productHandle": product_handle,
        "productId": product_id,  # Will be populated by Node.js after product lookup
        "imagesToRemove": images_to_remove,
        "totalImages": len(images_to_remove)
    }


def create_html_table_fix_record(
    product_handle: str,
    product_id: Optional[str],
    current_html: str,
    issues_found: List[str],
    suggested_fixes: Optional[str] = None
) -> Dict[str, Any]:
    """Create a record for HTML table fixing"""
    return {
        "productHandle": product_handle,
        "productId": product_id,  # Will be populated by Node.js
        "currentHtml": current_html,
        "htmlLength": len(current_html),
        "issuesFound": issues_found,
        "suggestedFixes": suggested_fixes,
        "priority": "high" if len(issues_found) > 2 else "medium"
    }


def create_rakuten_cleanup_record(
    product_handle: str,
    product_id: Optional[str],
    patterns_found: List[Dict[str, Any]],
    current_html: str
) -> Dict[str, Any]:
    """Create a record for Rakuten content cleanup"""
    return {
        "productHandle": product_handle,
        "productId": product_id,  # Will be populated by Node.js
        "currentHtml": current_html,
        "htmlLength": len(current_html),
        "patternsFound": patterns_found,
        "totalPatterns": len(patterns_found),
        "estimatedCleanupSize": sum(p.get("contentLength", 0) for p in patterns_found)
    }


def create_missing_image_record(
    product_handle: str,
    product_id: Optional[str],
    variant_sku: str,
    product_title: str,
    priority_level: str,
    issues: List[str]
) -> Dict[str, Any]:
    """Create a record for missing image audit"""
    return {
        "productHandle": product_handle,
        "productId": product_id,  # Will be populated by Node.js
        "variantSku": variant_sku,
        "productTitle": product_title,
        "priorityLevel": priority_level,
        "issues": issues,
        "needsAttention": True
    }


def create_product_import_record(
    handle: str,
    title: str,
    body_html: str,
    vendor: str,
    product_type: str,
    tags: str,
    variants: List[Dict[str, Any]],
    images: List[Dict[str, Any]],
    options: List[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Create a record for product import"""
    return {
        "handle": handle,
        "title": title,
        "bodyHtml": body_html,
        "vendor": vendor,
        "productType": product_type,
        "tags": tags,
        "published": True,
        "variants": variants,
        "images": images,
        "options": options or [],
        "variantCount": len(variants),
        "imageCount": len(images)
    }


def log_processing_summary(operation: str, total_found: int, processed: int, errors: int):
    """Log processing summary"""
    print(f"\n📊 {operation} Summary:")
    print(f"   🔍 Total found: {total_found}")
    print(f"   ✅ Processed: {processed}")
    print(f"   ❌ Errors: {errors}")
    print(f"   📈 Success rate: {(processed/(total_found or 1)*100):.1f}%")


def validate_json_structure(data: List[Dict[str, Any]], required_fields: List[str]) -> bool:
    """Validate JSON structure before saving"""
    if not isinstance(data, list):
        print("❌ Data must be a list")
        return False
    
    for i, record in enumerate(data):
        if not isinstance(record, dict):
            print(f"❌ Record {i} must be a dictionary")
            return False
        
        for field in required_fields:
            if field not in record:
                print(f"❌ Record {i} missing required field: {field}")
                return False
    
    return True