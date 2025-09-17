#!/bin/bash

# Rakuten to Shopify Conversion Script
# Usage: ./scripts/run_conversion.sh

set -e  # Exit on any error

echo "🚀 Rakuten to Shopify CSV Converter"
echo "=================================="

# Check if we're in the right directory
if [ ! -f "config.json" ]; then
    echo "❌ Error: Run this script from the rakuten-to-shopify root directory"
    echo "   Usage: ./scripts/run_conversion.sh"
    exit 1
fi

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Error: Python3 is required but not found"
    exit 1
fi

# Check if input files exist
if [ ! -f "data/input/rakuten_item.csv" ]; then
    echo "❌ Error: rakuten_item.csv not found in data/input/"
    echo "   Please place your Rakuten CSV files in data/input/ first"
    echo "   See data/input/README.md for details"
    exit 1
fi

# Install dependencies if needed
if [ ! -d "venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv venv
fi

echo "📦 Installing dependencies..."
source venv/bin/activate
pip install -q -r requirements.txt

# Set default values
INPUT_FILE="data/input/rakuten_item.csv"
COLLECTION_FILE="data/input/rakuten_collection.csv"
OUTPUT_FILE="output/csv/shopify_products.csv"

# Check for optional files
if [ ! -f "$COLLECTION_FILE" ]; then
    echo "⚠️  Warning: rakuten_collection.csv not found, proceeding without collection data"
    COLLECTION_ARG=""
else
    COLLECTION_ARG="--collection $COLLECTION_FILE"
fi

echo ""
echo "🔄 Starting conversion..."
echo "  Input: $INPUT_FILE"
echo "  Output: $OUTPUT_FILE"
echo ""

# Run the main conversion
python src/main.py \
    --input "$INPUT_FILE" \
    $COLLECTION_ARG \
    --output "$OUTPUT_FILE" \
    --config config.json

# Check if conversion succeeded
if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Conversion completed successfully!"
    echo ""

    # Show results
    if [ -f "$OUTPUT_FILE" ]; then
        LINES=$(wc -l < "$OUTPUT_FILE")
        SIZE=$(du -h "$OUTPUT_FILE" | cut -f1)
        echo "📊 Results:"
        echo "  Generated CSV: $OUTPUT_FILE"
        echo "  Lines: $LINES"
        echo "  Size: $SIZE"
    fi

    # Show reports
    if [ -f "output/reports/conversion_summary.json" ]; then
        echo "📋 Reports:"
        echo "  Summary: output/reports/conversion_summary.json"
        echo "  Logs: output/logs/conversion.log"
    fi

    # Check for description images
    if [ -f "output/reports/description_images.json" ]; then
        IMAGE_COUNT=$(grep -c '"source_url"' output/reports/description_images.json || echo "0")
        echo ""
        echo "📸 Description Images Found: $IMAGE_COUNT"
        echo "⚠️  IMPORTANT: Description images need separate handling!"
        echo "   1. cd extras/"
        echo "   2. python download_description_images.py"
        echo "   3. Upload to Shopify Admin → Settings → Files"
        echo "   See extras/README.md for details"
    fi

    echo ""
    echo "🎉 Ready to import to Shopify!"

else
    echo ""
    echo "❌ Conversion failed. Check output/logs/conversion.log for details"
    exit 1
fi