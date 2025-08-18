"""
Constants for Shopify CSV headers.
This module contains all the column names used in the Shopify CSV format.
"""

from typing import Final

# Basic Product Information
HANDLE: Final[str] = "Handle"
TITLE: Final[str] = "Title"
BODY_HTML: Final[str] = "Body (HTML)"
VENDOR: Final[str] = "Vendor"
PRODUCT_CATEGORY: Final[str] = "Product Category"
TYPE: Final[str] = "Type"
TAGS: Final[str] = "Tags"
PUBLISHED: Final[str] = "Published"

# Options and Variants
OPTION1_NAME: Final[str] = "Option1 Name"
OPTION1_VALUE: Final[str] = "Option1 Value"
OPTION1_LINKED_TO: Final[str] = "Option1 Linked To"
OPTION2_NAME: Final[str] = "Option2 Name"
OPTION2_VALUE: Final[str] = "Option2 Value"
OPTION2_LINKED_TO: Final[str] = "Option2 Linked To"
OPTION3_NAME: Final[str] = "Option3 Name"
OPTION3_VALUE: Final[str] = "Option3 Value"
OPTION3_LINKED_TO: Final[str] = "Option3 Linked To"

# Variant Details
VARIANT_SKU: Final[str] = "Variant SKU"
VARIANT_GRAMS: Final[str] = "Variant Grams"
VARIANT_INVENTORY_QUANTITY: Final[str] = "Variant Inventory Quantity"
VARIANT_INVENTORY_TRACKER: Final[str] = "Variant Inventory Tracker"
VARIANT_INVENTORY_POLICY: Final[str] = "Variant Inventory Policy"
VARIANT_FULFILLMENT_SERVICE: Final[str] = "Variant Fulfillment Service"
VARIANT_PRICE: Final[str] = "Variant Price"
VARIANT_COMPARE_AT_PRICE: Final[str] = "Variant Compare At Price"
VARIANT_REQUIRES_SHIPPING: Final[str] = "Variant Requires Shipping"
VARIANT_TAXABLE: Final[str] = "Variant Taxable"
VARIANT_BARCODE: Final[str] = "Variant Barcode"
VARIANT_IMAGE: Final[str] = "Variant Image"
VARIANT_WEIGHT_UNIT: Final[str] = "Variant Weight Unit"
VARIANT_TAX_CODE: Final[str] = "Variant Tax Code"

# SEO Fields
SEO_TITLE: Final[str] = "SEO Title"
SEO_DESCRIPTION: Final[str] = "SEO Description"

# Status Fields
STATUS: Final[str] = "Status"
GIFT_CARD: Final[str] = "Gift Card"

# Image Fields
IMAGE_SRC: Final[str] = "Image Src"
IMAGE_POSITION: Final[str] = "Image Position"
IMAGE_ALT_TEXT: Final[str] = "Image Alt Text"

# Custom Fields
COST_PER_ITEM: Final[str] = "Cost per item"

# Included / Japan, must be true
INCLUDED_JAPAN: Final[str] = "Included / Japan"

# Price / Japan
PRICE_JAPAN: Final[str] = "Price / Japan"

# Collection
COLLECTION: Final[str] = "Collection"

class WeightUnit:
    GRAMS: Final[str] = "g"
    KILOGRAMS: Final[str] = "kg"
    POUNDS: Final[str] = "lb"
    OUNCES: Final[str] = "oz"

class Status:
    ACTIVE: Final[str] = "active"
    DRAFT: Final[str] = "draft"
    ARCHIVED: Final[str] = "archived" 