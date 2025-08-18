"""
Data models for Shopify products and processing records
"""
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Dict, Any


@dataclass
class ProductImage:
    """Represents a product image"""
    id: Optional[int]
    position: int
    src: str
    alt: Optional[str] = None
    
    @property
    def has_ss_pattern(self) -> bool:
        """Check if image URL contains -XXss.jpg pattern"""
        import re
        return bool(re.search(r'-\d+ss\.jpg', self.src))


@dataclass
class ProductVariant:
    """Represents a product variant"""
    id: Optional[int]
    sku: str
    title: str
    price: str
    inventory_quantity: int
    image_id: Optional[int] = None


@dataclass
class ShopifyProduct:
    """Represents a Shopify product"""
    id: Optional[int]
    handle: str
    title: str
    body_html: str
    vendor: str
    product_type: str
    tags: str
    images: List[ProductImage]
    variants: List[ProductVariant]
    
    @classmethod
    def from_shopify_api(cls, data: Dict[str, Any]) -> "ShopifyProduct":
        """Create product from Shopify API response"""
        images = [
            ProductImage(
                id=img.get("id"),
                position=img.get("position", 0),
                src=img.get("src", ""),
                alt=img.get("alt")
            )
            for img in data.get("images", [])
        ]
        
        variants = [
            ProductVariant(
                id=var.get("id"),
                sku=var.get("sku", ""),  
                title=var.get("title", ""),
                price=str(var.get("price", "0")),
                inventory_quantity=var.get("inventory_quantity", 0),
                image_id=var.get("image_id")
            )
            for var in data.get("variants", [])
        ]
        
        return cls(
            id=data.get("id"),
            handle=data.get("handle", ""),
            title=data.get("title", ""),
            body_html=data.get("body_html", ""),
            vendor=data.get("vendor", ""),
            product_type=data.get("product_type", ""),
            tags=data.get("tags", ""),
            images=images,
            variants=variants
        )
    
    @property
    def has_ss_images(self) -> bool:
        """Check if product has any images with -XXss pattern"""
        return any(img.has_ss_pattern for img in self.images)
    
    @property
    def ss_images(self) -> List[ProductImage]:
        """Get all images with -XXss pattern"""
        return [img for img in self.images if img.has_ss_pattern]
    
    @property
    def has_ec_up_content(self) -> bool:
        """Check if body_html contains EC-UP content"""
        return "EC-UP" in self.body_html
    
    @property
    def has_nested_tables(self) -> bool:
        """Check if body_html contains nested table structures"""
        import re
        table_pattern = r'<table[^>]*>.*?<table[^>]*>'
        return bool(re.search(table_pattern, self.body_html, re.IGNORECASE | re.DOTALL))


@dataclass
class ProcessingRecord:
    """Base class for processing records"""
    product_handle: str
    shopify_product_id: Optional[int]
    timestamp: datetime
    status: str  # success, error, skipped
    error_message: Optional[str] = None


@dataclass
class SSImageRecord(ProcessingRecord):
    """Record for SS image processing"""
    image_url: str
    image_position: int
    ss_pattern_found: str
    removal_successful: bool = False


@dataclass
class ECUpRecord(ProcessingRecord):
    """Record for EC-UP content processing"""
    ec_up_pattern: str
    pattern_count: int
    associated_content_preview: str
    has_styling: bool
    content_length_before: int
    content_length_after: Optional[int] = None
    patterns_removed: Optional[str] = None


@dataclass
class HTMLTableRecord(ProcessingRecord):
    """Record for HTML table processing"""
    table_issue_type: str
    nested_depth: int
    table_count: int
    has_width_conflicts: bool
    has_overlapping_structure: bool
    html_length_before: int
    html_length_after: Optional[int] = None
    tables_restructured_count: Optional[int] = None
    fix_applied: Optional[str] = None


@dataclass
class MissingImageRecord:
    """Record for missing image audit"""
    product_handle: str
    shopify_product_id: Optional[int]
    variant_sku: str
    product_image_count: int
    variant_has_image: bool
    priority_level: str  # high, medium, low
    product_title: str
    audit_timestamp: datetime