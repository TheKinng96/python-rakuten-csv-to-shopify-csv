#!/usr/bin/env node
/**
 * Insert correct images to Shopify products via GraphQL
 * 
 * This script:
 * 1. Reads a CSV file with image URLs to add to products
 * 2. Finds products by handle and adds specified images
 * 3. Provides progress logging and error handling
 * 4. Saves insertion results for audit
 * 
 * CSV format expected:
 * product_handle,image_url,alt_text,position
 */

import { readFileSync, writeFileSync, existsSync, createReadStream } from 'fs';
import { join } from 'path';
import csv from 'csv-parser';
import { shopifyConfig, pathConfig, validateConfig } from './config.js';
import { ShopifyGraphQLClient } from './shopify-client.js';

const GET_PRODUCT_QUERY = `
  query getProduct($handle: String!) {
    productByIdentifier(identifier: {handle: $handle}) {
      id
      handle
      title
      media(first: 10) {
        edges {
          node {
            id
            alt
          }
        }
      }
      variants(first: 10) {
        edges {
          node {
            id
            title
            price
          }
        }
      }
    }
  }
`;

const ASSOCIATE_IMAGE_WITH_VARIANT_MUTATION = `
  mutation associateImageWithVariant($productId: ID!, $variantUpdates: [ProductVariantsBulkInput!]!) {
    productVariantsBulkUpdate(productId: $productId, variants: $variantUpdates) {
      productVariants {
        id
        title
        image {
          id
          url
        }
      }
      userErrors {
        field
        message
      }
    }
  }
`;

class ImageInserter {
  constructor() {
    this.client = null;
    this.insertResults = {
      successful: [],
      failed: [],
      notFound: [],
      skipped: [],
      mismatchReports: []
    };
  }

  async initialize() {
    console.log('🔧 Initializing Shopify GraphQL client...');
    
    if (!validateConfig()) {
      throw new Error('Configuration validation failed');
    }

    this.client = new ShopifyGraphQLClient(true); // Use test store
    
    await this.client.testConnection();
    console.log('✅ Connected to Shopify test store');
  }

  async loadImageDataFromCSV(csvFilePath) {
    return new Promise((resolve, reject) => {
      const imageData = [];
      const groupedData = {};

      if (!existsSync(csvFilePath)) {
        reject(new Error(`CSV file not found: ${csvFilePath}`));
        return;
      }

      console.log(`📂 Loading image data from ${csvFilePath}...`);
      console.log(`ℹ️  URL transformation will be applied for tsutsu-uraura gold URLs`);

      createReadStream(csvFilePath)
        .pipe(csv())
        .on('data', (row) => {
          const handle = row.product_handle?.trim();
          const variantSku = row.variant_sku?.trim();
          const imageUrl = row.image_url?.trim();
          const imageAlt = row.image_alt?.trim() || '';
          let variantTitleMatch = row.variant_title_match?.trim() || '';

          // Fix numeric conversion issue - ensure integers don't get .0 added
          if (variantTitleMatch && !isNaN(variantTitleMatch)) {
            const numValue = parseFloat(variantTitleMatch);
            if (Number.isInteger(numValue)) {
              variantTitleMatch = String(Math.floor(numValue));
            }
          }


          if (handle && variantSku && imageUrl) {
            if (!groupedData[handle]) {
              groupedData[handle] = [];
            }
            
            groupedData[handle].push({
              variantSku: variantSku,
              src: imageUrl,
              altText: imageAlt,
              variantTitleMatch: variantTitleMatch
            });
          }
        })
        .on('end', () => {
          // Convert to array format
          for (const [handle, variants] of Object.entries(groupedData)) {
            imageData.push({
              productHandle: handle,
              variantsToUpdate: variants
            });
          }

          console.log(`✅ Loaded ${imageData.length} products with images from CSV`);
          resolve(imageData);
        })
        .on('error', (error) => {
          reject(error);
        });
    });
  }

  async findProductByHandle(handle) {
    try {
      const result = await this.client.query(GET_PRODUCT_QUERY, { handle });
      return result.productByIdentifier;
    } catch (error) {
      throw new Error(`Failed to find product ${handle}: ${error.message}`);
    }
  }

  findVariantByTitleMatch(variants, titleMatch) {
    // If no title match (empty string), return first variant
    if (!titleMatch) {
      return variants.edges[0]?.node || null;
    }
    
    // Normalize the titleMatch for comparison
    const normalizedTitleMatch = this.normalizeVariantTitle(titleMatch);
    
    // Find variant where title matches the titleMatch (with normalization)
    return variants.edges.find(edge => {
      const normalizedVariantTitle = this.normalizeVariantTitle(edge.node.title);
      return normalizedVariantTitle === normalizedTitleMatch;
    })?.node || null;
  }

  normalizeVariantTitle(title) {
    if (!title) return '';
    
    const titleStr = String(title).trim();
    
    // If it's a number, ensure it's normalized (remove .0 from integers)
    if (!isNaN(titleStr)) {
      const numValue = parseFloat(titleStr);
      if (Number.isInteger(numValue)) {
        return String(Math.floor(numValue));
      }
    }
    
    return titleStr;
  }

  findExistingMediaByAlt(media, altText) {
    // Try to find existing media with matching alt text
    return media.edges.find(edge => edge.node.alt === altText)?.node || null;
  }

  async associateVariantImages(productHandle, variantsToUpdate, index, total) {
    try {
      // Find product in Shopify
      const product = await this.findProductByHandle(productHandle);
      
      if (!product) {
        console.log(`   ⚠️  [${index}/${total}] Product not found: ${productHandle}`);
        this.insertResults.notFound.push({
          handle: productHandle,
          reason: 'Product not found in Shopify'
        });
        return;
      }

      // Check media/variant count mismatch
      const mediaCount = product.media.edges.length;
      const variantCount = product.variants.edges.length;
      if (mediaCount !== variantCount) {
        console.log(`   ⚠️  Media/variant count mismatch for ${productHandle}: ${mediaCount} media, ${variantCount} variants`);
        this.insertResults.mismatchReports = this.insertResults.mismatchReports || [];
        this.insertResults.mismatchReports.push({
          handle: productHandle,
          mediaCount,
          variantCount,
          title: product.title
        });
      }

      const variantUpdates = [];
      let processedCount = 0;
      let reusedCount = 0;
      let newUploadsCount = 0;

      for (const variantData of variantsToUpdate) {
        const { variantSku, src, altText, variantTitleMatch } = variantData;
        
        // Find the matching variant
        const variant = this.findVariantByTitleMatch(product.variants, variantTitleMatch);
        if (!variant) {
          const normalizedSearch = this.normalizeVariantTitle(variantTitleMatch);
          const availableNormalized = product.variants.edges.map(e => 
            `"${e.node.title}" (normalized: "${this.normalizeVariantTitle(e.node.title)}")`
          ).join(', ');
          
          console.log(`   ⚠️  Variant not found for title match "${variantTitleMatch}" in ${productHandle}`);
          console.log(`   🔍 Available variants: ${availableNormalized}`);
          console.log(`   🔍 Looking for normalized: "${normalizedSearch}"`);
          continue;
        }

        // Check if image URL ends with ss.jpg (invalid image)
        const isSsImage = src && src.toLowerCase().endsWith('ss.jpg');
        
        if (isSsImage) {
          // Use existing Shopify media based on variant position
          const variantIndex = product.variants.edges.findIndex(edge => edge.node.id === variant.id);
          const correspondingMedia = product.media.edges[variantIndex]?.node;
          
          if (correspondingMedia) {
            variantUpdates.push({
              id: variant.id,
              mediaId: correspondingMedia.id
            });
            reusedCount++;
            console.log(`   🔄 Using existing Shopify media for variant ${variantTitleMatch || '1'} (ss.jpg detected): ${correspondingMedia.alt || 'No alt'}`);
          } else {
            console.log(`   ⚠️  No corresponding media found for variant ${variantTitleMatch || '1'} with ss.jpg image`);
            continue;
          }
        } else {
          // Check if existing media with same alt text exists (only if altText is provided)
          let existingMedia = null;
          if (altText && altText.trim()) {
            existingMedia = this.findExistingMediaByAlt(product.media, altText);
          }
          
          if (existingMedia) {
            // Reuse existing media by matching alt text
            variantUpdates.push({
              id: variant.id,
              mediaId: existingMedia.id
            });
            reusedCount++;
            console.log(`   🔄 Reusing existing media for variant ${variantTitleMatch || '1'}: ${altText}`);
          } else {
            // Upload new media using mediaSrc (for products like bos-toilet15 with no matching alt)
            variantUpdates.push({
              id: variant.id,
              mediaSrc: [src]
            });
            newUploadsCount++;
            console.log(`   📤 Will upload new media for variant ${variantTitleMatch || '1'}: ${src}`);
          }
        }
        
        processedCount++;
      }

      if (variantUpdates.length === 0) {
        console.log(`   ℹ️  [${index}/${total}] No valid variant updates for: ${productHandle}`);
        this.insertResults.skipped.push({
          handle: productHandle,
          title: product.title,
          shopifyId: product.id,
          reason: 'No valid variant updates found'
        });
        return;
      }

      // Execute the mutation
      if (shopifyConfig.dryRun) {
        console.log(`   🔍 [DRY RUN] Would update ${variantUpdates.length} variants in: ${productHandle}`);
        this.insertResults.successful.push({
          handle: productHandle,
          title: product.title,
          status: 'dry_run',
          variantsUpdated: variantUpdates.length,
          reusedMedia: reusedCount,
          newUploads: newUploadsCount,
          shopifyId: product.id
        });
        return;
      }

      const result = await this.client.mutate(ASSOCIATE_IMAGE_WITH_VARIANT_MUTATION, {
        productId: product.id,
        variantUpdates: variantUpdates
      });

      if (result.productVariantsBulkUpdate.userErrors.length > 0) {
        const errors = result.productVariantsBulkUpdate.userErrors.map(e => `${e.field}: ${e.message}`);
        throw new Error(`Shopify errors: ${errors.join(', ')}`);
      }

      const updatedVariants = result.productVariantsBulkUpdate.productVariants;
      
      console.log(`   ✅ [${index}/${total}] Updated ${updatedVariants.length} variants in: ${productHandle} (${reusedCount} reused, ${newUploadsCount} new)`);
      
      this.insertResults.successful.push({
        handle: productHandle,
        title: product.title,
        status: 'variants_updated',
        variantsUpdated: updatedVariants.length,
        reusedMedia: reusedCount,
        newUploads: newUploadsCount,
        shopifyId: product.id,
        updatedVariants: updatedVariants.map(variant => ({
          id: variant.id,
          title: variant.title,
          imageId: variant.image?.id,
          imageUrl: variant.image?.url
        }))
      });

    } catch (error) {
      console.error(`   ❌ [${index}/${total}] Failed: ${productHandle} - ${error.message}`);
      
      this.insertResults.failed.push({
        handle: productHandle,
        status: 'failed',
        error: error.message,
        attemptedVariants: variantsToUpdate.length
      });
    }
  }

  async processProducts(imageData) {
    console.log(`\n🚀 Starting variant image association (${imageData.length} products)...`);
    
    const batchSize = shopifyConfig.batchSize;
    const total = imageData.length;
    let processed = 0;

    // Process in batches to respect rate limits
    for (let i = 0; i < imageData.length; i += batchSize) {
      const batch = imageData.slice(i, i + batchSize);
      
      console.log(`\n📦 Processing batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(imageData.length/batchSize)}`);
      
      // Process batch with concurrency limit
      const promises = batch.map((productData, batchIndex) => 
        this.associateVariantImages(productData.productHandle, productData.variantsToUpdate, i + batchIndex + 1, total)
      );
      
      await Promise.all(promises);
      processed += batch.length;
      
      // Progress update
      const successRate = (this.insertResults.successful.length / processed * 100).toFixed(1);
      console.log(`📈 Progress: ${processed}/${total} processed (${successRate}% success rate)`);
      
      // Rate limiting delay between batches
      if (i + batchSize < imageData.length) {
        const delay = Math.ceil(1000 / shopifyConfig.maxRequestsPerSecond * batchSize);
        console.log(`⏱️  Waiting ${delay}ms for rate limiting...`);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
  }

  saveInsertResults() {
    const resultsPath = join(pathConfig.reportsPath, '04_image_insert_results.json');
    
    const report = {
      timestamp: new Date().toISOString(),
      summary: {
        total: this.insertResults.successful.length + this.insertResults.failed.length + this.insertResults.notFound.length + this.insertResults.skipped.length,
        successful: this.insertResults.successful.length,
        failed: this.insertResults.failed.length,
        notFound: this.insertResults.notFound.length,
        skipped: this.insertResults.skipped.length,
        mismatchReports: this.insertResults.mismatchReports.length,
        totalVariantsUpdated: this.insertResults.successful.reduce((sum, result) => 
          sum + (result.variantsUpdated || 0), 0
        ),
        totalMediaReused: this.insertResults.successful.reduce((sum, result) => 
          sum + (result.reusedMedia || 0), 0
        ),
        totalNewUploads: this.insertResults.successful.reduce((sum, result) => 
          sum + (result.newUploads || 0), 0
        )
      },
      results: this.insertResults,
      config: {
        dryRun: shopifyConfig.dryRun,
        batchSize: shopifyConfig.batchSize,
        store: 'test'
      }
    };

    writeFileSync(resultsPath, JSON.stringify(report, null, 2));
    console.log(`📄 Insert results saved to: ${resultsPath}`);
  }

  printSummary() {
    const { successful, failed, notFound, skipped, mismatchReports } = this.insertResults;
    const total = successful.length + failed.length + notFound.length + skipped.length;
    const totalVariantsUpdated = successful.reduce((sum, result) => sum + (result.variantsUpdated || 0), 0);
    const totalMediaReused = successful.reduce((sum, result) => sum + (result.reusedMedia || 0), 0);
    const totalNewUploads = successful.reduce((sum, result) => sum + (result.newUploads || 0), 0);
    
    console.log('\n' + '='.repeat(70));
    console.log('📊 VARIANT IMAGE ASSOCIATION SUMMARY');
    console.log('='.repeat(70));
    console.log(`Total products: ${total}`);
    console.log(`✅ Successfully processed: ${successful.length}`);
    console.log(`❌ Failed: ${failed.length}`);
    console.log(`🔍 Not found: ${notFound.length}`);
    console.log(`⏭️  Skipped: ${skipped.length}`);
    console.log(`⚠️  Media/variant count mismatches: ${mismatchReports.length}`);
    console.log(`🔗 Total variants updated: ${totalVariantsUpdated}`);
    console.log(`🔄 Media reused (existing): ${totalMediaReused}`);
    console.log(`📤 New media uploads: ${totalNewUploads}`);
    
    if (total > 0) {
      const successRate = ((successful.length + skipped.length) / total * 100).toFixed(1);
      console.log(`📈 Success rate: ${successRate}%`);
    }
    
    if (mismatchReports.length > 0) {
      console.log('\n⚠️  Products with media/variant count mismatches:');
      mismatchReports.forEach(report => {
        console.log(`   ${report.handle}: ${report.mediaCount} media, ${report.variantCount} variants`);
      });
    }
    
    if (shopifyConfig.dryRun) {
      console.log('\n🔍 This was a DRY RUN - no actual associations performed');
      console.log('💡 Set DRY_RUN=false in .env to perform actual variant image association');
    }
    
    console.log('\n💡 Next steps:');
    console.log('   1. Review association results in reports/04_image_insert_results.json');
    console.log('   2. Check product variants with new images in Shopify admin');
    console.log('   3. Verify variant-specific image associations');
    if (mismatchReports.length > 0) {
      console.log('   4. Review products with media/variant count mismatches');
    }
  }
}

async function main() {
  console.log('='.repeat(70));
  console.log('🔗 VARIANT IMAGE ASSOCIATION');
  console.log('='.repeat(70));

  const inserter = new ImageInserter();

  try {
    await inserter.initialize();
    
    // Look for filtered CSV file first, then fallback to original
    const filteredCsvPath = join(pathConfig.reportsPath, 'images_to_insert_filtered.csv');
    const originalCsvPath = join(pathConfig.reportsPath, 'images_to_insert.csv');
    
    let csvPath;
    if (existsSync(filteredCsvPath)) {
      csvPath = filteredCsvPath;
      console.log('📂 Using filtered CSV (broken URLs excluded)');
    } else if (existsSync(originalCsvPath)) {
      csvPath = originalCsvPath;
      console.log('⚠️  Using original CSV (may contain broken URLs)');
    } else {
      console.log(`⚠️ No CSV file found at ${originalCsvPath}`);
      console.log('\n📝 Expected CSV format:');
      console.log('product_handle,variant_sku,image_url,image_alt,variant_title_match');
      console.log('example-product,example-sku-3s,https://example.com/image.jpg,Product Image,3');
      console.log('\n💡 Run python api/scripts/04_audit_images.py to generate this CSV file.');
      return;
    }
    
    const imageData = await inserter.loadImageDataFromCSV(csvPath);
    
    if (imageData.length === 0) {
      console.log('⚠️ No valid image data found in CSV file');
      return;
    }

    // Confirmation for live association
    if (!shopifyConfig.dryRun) {
      const totalVariants = imageData.reduce((sum, product) => sum + product.variantsToUpdate.length, 0);
      console.log(`\n⚠️  LIVE ASSOCIATION MODE - This will associate images with ${totalVariants} variants across ${imageData.length} products!`);
      console.log('Press Ctrl+C to cancel or wait 5 seconds to continue...');
      await new Promise(resolve => setTimeout(resolve, 5000));
    }

    await inserter.processProducts(imageData);
    inserter.saveInsertResults();
    inserter.printSummary();

    console.log('\n🎉 Variant image association completed!');

  } catch (error) {
    console.error(`\n❌ Variant image association failed: ${error.message}`);
    console.error(error.stack);
    process.exit(1);
  }
}

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.log('\n⏹️ Variant image association interrupted by user');
  process.exit(0);
});

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}