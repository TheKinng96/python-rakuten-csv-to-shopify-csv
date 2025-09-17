# Description Image Handler

Separate tools for handling images embedded in product descriptions (PC用商品説明文).

## Why Separate?

- **Main CSV conversion**: Fast (minutes) and always required
- **Image downloading**: Slow (hours) and optional for testing
- **Independent processes**: Can run image handling when needed
- **Flexible workflow**: Test CSV import before investing in images

## How It Works

### 1. Main Converter Creates Image Manifest
When you run the main converter, it:
- Finds all images in product descriptions
- Fixes gold URL patterns (`tsutsu-uraura/gold/` → `gold/tsutsu-uraura/`)
- Updates HTML to reference Shopify CDN URLs
- Creates `../output/reports/description_images.json` with download list

### 2. Download Images (This Folder)
```bash
# Download all description images
python download_description_images.py \
  --manifest ../output/reports/description_images.json \
  --output ../images/description_images/

# Options:
# --batch-size 100          # Download in batches
# --retry-failed            # Retry failed downloads
# --skip-existing           # Skip already downloaded
```

### 3. Upload to Shopify Files
1. Navigate to: **Shopify Admin → Settings → Files**
2. Click **"Upload files"**
3. Select all images from: `../images/description_images/`
4. Wait for upload completion
5. Shopify will automatically create CDN URLs matching the CSV

### 4. Import CSV
Now you can import the CSV - all image URLs will work!

## Image Manifest Format

```json
{
  "total_images": 1234,
  "unique_images": 987,
  "total_size_estimate_mb": 456,
  "images": [
    {
      "source_url": "https://image.rakuten.co.jp/gold/tsutsu-uraura/detail.jpg",
      "filename": "xl-ekjd-8f7a_detail.jpg",
      "cdn_url": "https://cdn.shopify.com/s/files/1/0637/6059/7127/files/xl-ekjd-8f7a_detail.jpg",
      "handles": ["xl-ekjd-8f7a", "xl-ekjd-8f7a-3s"],
      "filesize_estimate_kb": 45
    }
  ]
}
```

## Alternative: Keep Rakuten URLs

For testing, you can skip image downloading:

```bash
# Use this flag in main converter to keep original Rakuten URLs
cd ..
python src/main.py --keep-rakuten-images \
  --input data/input/rakuten_item.csv \
  --output output/csv/shopify_products.csv
```

**Pros:**
- No download/upload needed
- Instant testing
- Images still display

**Cons:**
- Slower loading from Rakuten servers
- External dependency
- Not recommended for production

## Troubleshooting

### Failed Downloads
```bash
# Check download log
cat ../output/logs/image_download.log

# Retry failed downloads only
python download_description_images.py --retry-failed
```

### Large Files
Images are typically small (20-100KB) but some might be large:
- Total size is usually < 500MB for full product catalog
- Upload to Shopify in batches if > 100MB
- Large images (>2MB) may need compression

### Missing Images
If some images don't download:
1. Check if URLs are accessible (some might be removed from Rakuten)
2. Review `image_download_errors.csv` for specific failures
3. These will show as broken images in Shopify until fixed

### Verification
```bash
# Verify all images downloaded correctly
python verify_downloads.py --manifest ../output/reports/description_images.json

# Check final CSV has working image URLs
python ../scripts/validate_output.py ../output/csv/shopify_products.csv
```

## Files in This Directory

- `download_description_images.py` - Main download script
- `verify_downloads.py` - Verify downloaded images
- `upload_instructions.txt` - Step-by-step Shopify upload guide
- `batch_uploader.py` - (Future) Automated Shopify upload via API