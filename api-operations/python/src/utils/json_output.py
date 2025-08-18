"""
Utility functions for generating JSON output files for Node.js GraphQL operations
"""
import json
import math
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
import pandas as pd


class SafeJSONEncoder(json.JSONEncoder):
    """JSON encoder that handles NaN, inf, and pandas NA values"""
    def encode(self, obj):
        # Clean the data before encoding
        cleaned_obj = self._clean_data(obj)
        return super().encode(cleaned_obj)
    
    def _clean_data(self, obj):
        """Recursively clean data to handle NaN and other problematic values"""
        if isinstance(obj, dict):
            return {key: self._clean_data(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self._clean_data(item) for item in obj]
        elif pd.isna(obj):
            return None
        elif isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
        else:
            return obj


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
        json.dump(report, f, indent=2, ensure_ascii=False, cls=SafeJSONEncoder)
    
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
    options: List[Dict[str, Any]] = None,
    seo_title: str = "",
    seo_description: str = ""
) -> Dict[str, Any]:
    """Create a record for product import matching 2025-07 ProductCreateInput structure"""
    
    # Convert tags string to array format
    tags_array = [tag.strip() for tag in tags.split(',') if tag.strip()] if tags else []
    
    # Convert options to productOptions format
    product_options = []
    if options:
        for position, option in enumerate(options, 1):
            option_name = option.get('name')
            # Skip if option name is NaN or empty
            if option_name and not pd.isna(option_name) and option_name.strip():
                option_values = option.get('values', [])
                # Filter out NaN values and create value objects
                valid_values = []
                for value in option_values:
                    if value and not pd.isna(value) and str(value).strip():
                        valid_values.append({"name": str(value).strip()})
                
                # Only add option if it has valid values
                if valid_values:
                    product_options.append({
                        "name": option_name.strip(),
                        "values": valid_values,
                        "position": position
                    })
    
    # Convert images to media format
    media = []
    for image in images:
        if image.get('src'):
            media.append({
                "originalSource": image['src'],
                "alt": image.get('alt', ''),
                "mediaContentType": "IMAGE"
            })
    
    # Build SEO structure
    seo = {}
    if seo_title:
        seo["title"] = seo_title
    if seo_description:
        seo["description"] = seo_description
    
    return {
        "product": {
            "handle": handle,
            "title": title,
            "descriptionHtml": body_html,
            "vendor": vendor,
            "productType": product_type,
            "tags": tags_array,
            "status": "ACTIVE",
            "productOptions": product_options,
            "seo": seo if seo else None
        },
        "media": media,
        "variants": variants,  # Keep for backward compatibility
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