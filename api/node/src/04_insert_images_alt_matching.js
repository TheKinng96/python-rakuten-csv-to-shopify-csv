#!/usr/bin/env node
/**
 * Insert correct images to Shopify products via GraphQL (Alt-Text + Filename Matching Version)
 * 
 * This script:
 * 1. Reads a CSV file with image URLs to add to products
 * 2. Finds products by handle and adds specified images
 * 3. Uses intelligent matching: alt patterns → filename digits → position fallback
 * 4. Provides progress logging and error handling
 * 5. Saves insertion results for audit
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
            ... on MediaImage {
              image {
                url
              }        
            }
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
    this.matchingStats = {
      altPatternMatches: 0,
      filenameDigitMatches: 0,
      positionFallbacks: 0,
      unmatchedVariants: 0
    };
  }

  async initialize() {
    console.log('🔧 Initializing Shopify GraphQL client...');
    
    if (!validateConfig()) {
      throw new Error('Configuration validation failed');
    }

    this.client = new ShopifyGraphQLClient(false); // Use test store
    
    await this.client.testConnection();
    console.log('✅ Connected to Shopify test store');
  }

  async loadMissingVariantsFromJSON(jsonFilePath) {
    if (!existsSync(jsonFilePath)) {
      throw new Error(`JSON file not found: ${jsonFilePath}`);
    }

    console.log(`📂 Loading missing variant data from ${jsonFilePath}...`);
    
    const jsonData = JSON.parse(readFileSync(jsonFilePath, 'utf8'));
    const missingVariants = jsonData.missing_variant_images || [];
    
    console.log(`📊 Found ${missingVariants.length} variants missing images`);
    
    // Group by handle to reduce API calls
    const groupedData = {};
    
    for (const variant of missingVariants) {
      const handle = variant.Handle?.trim();
      const variantSku = variant['Variant SKU']?.trim();
      
      if (handle && variantSku) {
        if (!groupedData[handle]) {
          groupedData[handle] = [];
        }
        
        // Extract variant title from SKU suffix
        const variantTitleMatch = this.extractVariantTitleFromSku(variantSku);
        
        groupedData[handle].push({
          variantSku: variantSku,
          variantTitleMatch: variantTitleMatch,
          originalTitle: variant.Title || '',
          sourceFile: variant.File || ''
        });
      }
    }
    
    // Convert to expected format
    const imageData = Object.entries(groupedData).map(([handle, variants]) => ({
      productHandle: handle,
      variantsToUpdate: variants
    }));
    
    console.log(`✅ Grouped into ${imageData.length} products with missing variant images`);
    return imageData;
  }

  extractVariantTitleFromSku(sku) {
    if (!sku) return '1';
    
    // Extract number from SKU suffixes like "-3s", "-6", "-4s", "-2", etc.
    const match = sku.match(/-(\d+)s?$/);
    return match ? match[1] : '1'; // Default to "1" if no suffix found
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

  extractFilename(url) {
    if (!url) return '';
    return url.split('/').pop().split('?')[0].split('.')[0];
  }

  extractFilenameDigit(url) {
    if (!url) return null;
    const filename = this.extractFilename(url);
    // Get last digit from filename
    const match = filename.match(/(\d)(?!.*\d)/);
    return match ? match[1] : null;
  }

  allMediaAltsIdentical(media) {
    if (media.edges.length <= 1) return false;
    
    const firstAlt = media.edges[0]?.node?.alt || '';
    return media.edges.every(edge => (edge.node.alt || '') === firstAlt);
  }

  findMediaByAltPattern(media, variantTitle) {
    if (!variantTitle) return null;
    
    const normalizedVariant = this.normalizeVariantTitle(variantTitle);
    
    // Pattern matching for Japanese text with numbers
    const patterns = [
      new RegExp(`×${normalizedVariant}[箱袋個kg]?`, 'i'),
      new RegExp(`x${normalizedVariant}[箱袋個kg]?`, 'i'),
      new RegExp(`X${normalizedVariant}[箱袋個kg]?`, 'i'),
      new RegExp(`${normalizedVariant}[箱袋個セット]`, 'i'),
      new RegExp(`${normalizedVariant}kg`, 'i'),
      new RegExp(`${normalizedVariant}個入り`, 'i'),
      new RegExp(`[×xX]${normalizedVariant}`, 'i'),
      new RegExp(`${normalizedVariant}[袋箱個]セット`, 'i')
    ];
    
    for (const edge of media.edges) {
      const alt = edge.node.alt || '';
      for (const pattern of patterns) {
        if (pattern.test(alt)) {
          return edge.node;
        }
      }
    }
    
    return null;
  }

  findMediaByFilenameDigit(media, variantTitle) {
    if (!variantTitle) return null;
    
    const normalizedVariant = this.normalizeVariantTitle(variantTitle);
    
    for (const edge of media.edges) {
      const url = edge.node.image?.url;
      if (url) {
        const digit = this.extractFilenameDigit(url);
        if (digit === normalizedVariant) {
          return edge.node;
        }
      }
    }
    
    return null;
  }

  findMediaByVariantTitle(media, variantTitle) {
    // Step 1: Try alt text pattern matching
    let match = this.findMediaByAltPattern(media, variantTitle);
    if (match) {
      return { media: match, method: 'alt_pattern' };
    }
    
    // Step 2: If all alts are identical, try filename digit matching
    if (this.allMediaAltsIdentical(media)) {
      match = this.findMediaByFilenameDigit(media, variantTitle);
      if (match) {
        return { media: match, method: 'filename_digit' };
      }
    }
    
    // Step 3: Position-based fallback (existing logic)
    const normalizedVariant = this.normalizeVariantTitle(variantTitle);
    let variantIndex = 0;
    
    if (normalizedVariant && !isNaN(normalizedVariant)) {
      const num = parseInt(normalizedVariant);
      if (num > 1) {
        variantIndex = num - 1; // Convert 1-based to 0-based index
      }
    }
    
    match = media.edges[variantIndex]?.node;
    if (match) {
      return { media: match, method: 'position_fallback' };
    }
    
    return { media: null, method: 'unmatched' };
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
          type: 'media_variant_count_mismatch',
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
        const { variantSku, variantTitleMatch, originalTitle, sourceFile } = variantData;
        
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

        // Try smart matching to find existing media for this variant
        const matchResult = this.findMediaByVariantTitle(product.media, variantTitleMatch);
        
        if (matchResult.media) {
          variantUpdates.push({
            id: variant.id,
            mediaId: matchResult.media.id
          });
          reusedCount++;
          this.matchingStats[matchResult.method + 's']++;
          console.log(`   🎯 Smart match for variant ${variantTitleMatch || '1'} (${matchResult.method}): ${matchResult.media.alt || 'No alt'}`);
        } else {
          console.log(`   ⚠️  No media match found for variant ${variantTitleMatch || '1'} SKU: ${variantSku}`);
          this.matchingStats.unmatchedVariants++;
          
          // Add to mismatch reports for manual review
          this.insertResults.mismatchReports.push({
            handle: productHandle,
            type: 'variant_no_media_match',
            variantTitle: variantTitleMatch,
            variantSku: variantSku,
            originalTitle: originalTitle,
            sourceFile: sourceFile,
            availableMediaAlts: product.media.edges.map(e => e.node.alt || 'No alt'),
            availableMediaFilenames: product.media.edges.map(e => this.extractFilename(e.node.image?.url)),
            reason: 'No alt pattern or filename digit match found - needs manual review'
          });
          continue;
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
          newUploads: 0, // No new uploads in this version
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
      
      console.log(`   ✅ [${index}/${total}] Updated ${updatedVariants.length} variants in: ${productHandle} (${reusedCount} reused)`);
      
      this.insertResults.successful.push({
        handle: productHandle,
        title: product.title,
        status: 'variants_updated',
        variantsUpdated: updatedVariants.length,
        reusedMedia: reusedCount,
        newUploads: 0, // No new uploads in this version
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
    console.log(`\n🚀 Starting variant image association with smart matching (${imageData.length} products)...`);
    
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
    const resultsPath = join(pathConfig.reportsPath, '04_image_insert_alt_matching_results.json');
    
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
        ),
        // Enhanced matching statistics
        matchingMethods: {
          altPatternMatches: this.matchingStats.altPatternMatches,
          filenameDigitMatches: this.matchingStats.filenameDigitMatches,
          positionFallbacks: this.matchingStats.positionFallbacks,
          unmatchedVariants: this.matchingStats.unmatchedVariants
        }
      },
      results: this.insertResults,
      config: {
        dryRun: shopifyConfig.dryRun,
        batchSize: shopifyConfig.batchSize,
        store: 'test',
        matchingVersion: 'alt_and_filename'
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
    console.log('📊 VARIANT IMAGE ASSOCIATION SUMMARY (Alt + Filename Matching)');
    console.log('='.repeat(70));
    console.log(`Total products: ${total}`);
    console.log(`✅ Successfully processed: ${successful.length}`);
    console.log(`❌ Failed: ${failed.length}`);
    console.log(`🔍 Not found: ${notFound.length}`);
    console.log(`⏭️  Skipped: ${skipped.length}`);
    console.log(`⚠️  Mismatch reports: ${mismatchReports.length}`);
    console.log(`🔗 Total variants updated: ${totalVariantsUpdated}`);
    console.log(`🔄 Media reused (existing): ${totalMediaReused}`);
    console.log(`📤 New media uploads: ${totalNewUploads}`);
    
    // Enhanced matching method breakdown
    console.log('\n🎯 Matching Method Breakdown:');
    console.log(`   🔍 Alt pattern matches: ${this.matchingStats.altPatternMatches}`);
    console.log(`   📄 Filename digit matches: ${this.matchingStats.filenameDigitMatches}`);
    console.log(`   📍 Position fallbacks: ${this.matchingStats.positionFallbacks}`);
    console.log(`   ❓ Unmatched variants: ${this.matchingStats.unmatchedVariants}`);
    
    if (total > 0) {
      const successRate = ((successful.length + skipped.length) / total * 100).toFixed(1);
      console.log(`📈 Success rate: ${successRate}%`);
    }
    
    if (mismatchReports.length > 0) {
      console.log('\n⚠️  Mismatch Reports:');
      const mediaVariantMismatches = mismatchReports.filter(r => r.type === 'media_variant_count_mismatch');
      const noMediaMatches = mismatchReports.filter(r => r.type === 'variant_no_media_match');
      
      if (mediaVariantMismatches.length > 0) {
        console.log(`   📊 Media/variant count mismatches: ${mediaVariantMismatches.length}`);
        mediaVariantMismatches.slice(0, 3).forEach(report => {
          console.log(`      ${report.handle}: ${report.mediaCount} media, ${report.variantCount} variants`);
        });
      }
      
      if (noMediaMatches.length > 0) {
        console.log(`   🎯 Variants with no media match: ${noMediaMatches.length}`);
        noMediaMatches.slice(0, 3).forEach(report => {
          console.log(`      ${report.handle}: variant "${report.variantTitle}" - ${report.reason}`);
        });
      }
    }
    
    if (shopifyConfig.dryRun) {
      console.log('\n🔍 This was a DRY RUN - no actual associations performed');
      console.log('💡 Set DRY_RUN=false in .env to perform actual variant image association');
    }
    
    console.log('\n💡 Next steps:');
    console.log('   1. Review association results in reports/04_image_insert_alt_matching_results.json');
    console.log('   2. Check unmatched variants in mismatchReports for manual review');
    console.log('   3. Verify variant-specific image associations in Shopify admin');
    console.log('   4. Compare alt pattern vs filename digit matching effectiveness');
  }
}

async function main() {
  console.log('='.repeat(70));
  console.log('🔗 VARIANT IMAGE ASSOCIATION (Alt + Filename Matching)');
  console.log('='.repeat(70));

  const inserter = new ImageInserter();

  try {
    await inserter.initialize();
    
    // Load from JSON report instead of CSV
    const jsonReportPath = join(pathConfig.reportsPath, 'variant_images_check_report.json');
    
    if (!existsSync(jsonReportPath)) {
      console.log(`⚠️ JSON report not found at ${jsonReportPath}`);
      console.log('\n📝 Expected JSON structure:');
      console.log('{"missing_variant_images": [{"Handle": "product-handle", "Variant SKU": "sku-3s", "Title": "Product Title"}]}');
      console.log('\n💡 Run python api/scripts/check_variant_images.py to generate this JSON file.');
      return;
    }
    
    const imageData = await inserter.loadMissingVariantsFromJSON(jsonReportPath);
    
    if (imageData.length === 0) {
      console.log('⚠️ No valid missing variant data found in JSON file');
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