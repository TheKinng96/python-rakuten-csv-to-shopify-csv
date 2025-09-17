# Troubleshooting Guide

Common issues and solutions for the Rakuten to Shopify converter.

## Input File Issues

### Error: "UnicodeDecodeError" when reading Rakuten CSV
**Cause**: Incorrect encoding detection
**Solution**:
```bash
# Check encoding
file -I data/input/rakuten_item.csv

# If not Shift-JIS, convert it first
iconv -f UTF-8 -t SHIFT-JIS input.csv > rakuten_item.csv
```

### Error: "rakuten_item.csv not found"
**Cause**: File not in correct location
**Solution**:
```bash
# Ensure files are in the right place
ls data/input/
# Should show: rakuten_item.csv, rakuten_collection.csv

# Copy files if needed
cp /path/to/your/files/* data/input/
```

### Error: "No products found after filtering"
**Cause**: All products filtered out (likely -ss SKUs or excluded categories)
**Solution**:
- Check if input file contains valid product data
- Verify 商品管理番号 column exists and has data
- Review `output/logs/conversion.log` for filtering details

## Processing Issues

### Error: "HTML processing failed"
**Cause**: Malformed HTML in PC用商品説明文
**Solution**: Converter includes fallback processing - check logs for details

### Warning: "No tax data found for product"
**Cause**: Product not in tax master files and no keywords matched
**Solution**:
- Verify tax master files in `data/tax_master/`
- Check `output/reports/unmatched_tax_products.csv` for manual review
- Products will default to 10% tax rate

### Error: "Memory error processing large CSV"
**Cause**: Insufficient memory for large Rakuten files
**Solution**:
```bash
# Use chunked processing
python src/main.py --chunk-size 5000 ...

# Or process smaller files
python scripts/split_large_csv.py data/input/rakuten_item.csv
```

## Output Issues

### Generated CSV won't import to Shopify
**Cause**: Validation errors in generated CSV
**Solution**:
```bash
# Validate output
python scripts/validate_output.py output/csv/shopify_products.csv

# Check for common issues:
# - Empty required fields
# - Invalid characters
# - Incorrect encoding
```

### Images don't display after import
**Cause**: Description images not uploaded to Shopify Files
**Solution**:
```bash
# Download and upload images
cd extras/
python download_description_images.py
# Then upload to Shopify Admin → Settings → Files
```

### Tables break on mobile after import
**Cause**: HTML table fixes not applied
**Solution**: Check conversion logs - tables should be wrapped with responsive divs

## Performance Issues

### Conversion takes too long (>1 hour)
**Cause**: Large file or complex HTML processing
**Solution**:
```bash
# Disable intensive processing for testing
python src/main.py --skip-html-processing --skip-image-processing ...

# Or use smaller chunk size
python src/main.py --chunk-size 1000 ...
```

### Out of memory errors
**Cause**: Processing entire large CSV in memory
**Solution**:
```bash
# Enable memory optimization
python src/main.py --low-memory ...

# Or split input file
python scripts/split_by_size.py data/input/rakuten_item.csv 100MB
```

## Tax Classification Issues

### Many products have wrong tax rates
**Cause**: Keyword classification not matching product types
**Solution**:
1. Review `output/reports/tax_classification_report.csv`
2. Update tax keywords in `data/mappings/tax_keywords.json`
3. Add products to tax master files for exact matches

### Tax master file not loading
**Cause**: Encoding or format issues
**Solution**:
```bash
# Check file encoding
file -I data/tax_master/商品マスタ_*.csv

# Verify CSV structure has required columns:
# - 商品コード
# - 消費税率（%）
```

## Image Processing Issues

### Gold image URLs not fixed
**Cause**: Pattern matching failed
**Solution**: Check logs for specific URLs that failed to match pattern

### Description images download fails
**Cause**: Rakuten URLs no longer accessible
**Solution**:
```bash
# Check failed downloads
cat output/reports/image_download_errors.csv

# Use keep-rakuten-images as fallback
python src/main.py --keep-rakuten-images ...
```

## Configuration Issues

### Config file not found
**Cause**: Missing or invalid config.json
**Solution**:
```bash
# Verify config exists
ls config.json

# Reset to default if corrupted
cp config.json.example config.json
```

### CDN base URL is wrong
**Cause**: Incorrect Shopify store configuration
**Solution**: Update `cdn_base_url` in config.json with your store's CDN URL

## Getting Help

1. **Check logs first**: `output/logs/conversion.log`
2. **Review reports**: Files in `output/reports/`
3. **Validate input**: Ensure Rakuten CSV files are complete and properly encoded
4. **Test with small sample**: Use first 100 lines to isolate issues
5. **Check configuration**: Verify all paths and settings in config.json

## Debug Mode

Enable detailed logging:
```bash
python src/main.py --debug --verbose ...
```

This will create detailed logs showing:
- Every processing step
- HTML transformations
- Tax classification decisions
- Image URL processing
- Validation results