#!/usr/bin/env node
/**
 * Remove SS images from Shopify products using fileDelete API
 * 
 * This script:
 * 1. Reads ss_images_for_removal.csv from reports directory
 * 2. Finds products by handle and removes specified SS images
 * 3. Uses fileDelete mutation to permanently delete images
 * 4. Provides progress logging and error handling
 * 5. Saves deletion results for audit
 */

import { readFileSync, writeFileSync, existsSync } from 'fs';
import { join } from 'path';
import { shopifyConfig, pathConfig, validateConfig } from './config.js';
import { ShopifyGraphQLClient } from './shopify-client.js';

const GET_PRODUCT_MEDIA_QUERY = `
  query getProduct($handle: String!) {
    productByIdentifier(identifier: {handle: $handle}) {
      id
      handle
      title
      media(first: 250) {
        edges {
          node {
            id
            alt
            ... on MediaImage {
              id
              alt
            }
          }
        }
      }
    }
  }
`;

const FILE_DELETE_MUTATION = `
  mutation fileDelete($fileIds: [ID!]!) {
    fileDelete(fileIds: $fileIds) {
      deletedFileIds
      userErrors {
        field
        message
      }
    }
  }
`;

class SSImageDeleter {
  constructor() {
    this.client = null;
    this.deletionResults = {
      successful: [],
      failed: [],
      notFound: []
    };
    this.deletedImageIds = [];
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

  loadSSImagesFromCSV() {
    const csvPath = join(pathConfig.reportsPath, 'ss_images_for_removal.csv');
    
    if (!existsSync(csvPath)) {
      throw new Error(`SS images CSV file not found: ${csvPath}`);
    }

    console.log(`📂 Loading SS images data from ${csvPath}...`);
    
    const csvContent = readFileSync(csvPath, 'utf-8');
    const lines = csvContent.trim().split('\n');
    const headers = lines[0].split(',');
    
    const ssImages = [];
    for (let i = 1; i < lines.length; i++) {
      const values = lines[i].split(',');
      const row = {};
      headers.forEach((header, index) => {
        row[header.trim()] = values[index] ? values[index].trim() : '';
      });
      ssImages.push(row);
    }

    console.log(`✅ Loaded ${ssImages.length} SS images from CSV`);
    return ssImages;
  }

  async findProductByHandle(handle) {
    try {
      const result = await this.client.query(GET_PRODUCT_MEDIA_QUERY, { handle });
      
      if (!result || !result.productByIdentifier) {
        console.log(`   ⚠️  Product not found in Shopify: ${handle}`);
        return null;
      }
      
      return result.productByIdentifier;
    } catch (error) {
      throw new Error(`Failed to find product ${handle}: ${error.message}`);
    }
  }

  matchSSImagesByAlt(shopifyMedia, ssImageAltText) {
    const matchedImages = [];
    
    if (!shopifyMedia || !Array.isArray(shopifyMedia)) {
      console.log(`   ⚠️  Invalid shopifyMedia provided to matchSSImagesByAlt`);
      return matchedImages;
    }
    
    // Find matching Shopify media by alt text
    const matchingMedia = shopifyMedia.filter(media => {
      if (!media || !media.alt) {
        return false;
      }
      return media.alt.trim() === ssImageAltText.trim();
    });
    
    for (const media of matchingMedia) {
      if (media && media.id) {
        matchedImages.push({
          shopifyImageId: media.id,
          shopifyImageAlt: media.alt,
          ssImageAltText: ssImageAltText,
          reason: 'SS image matched by alt text'
        });
      }
    }
    
    return matchedImages;
  }

  async deleteImages(imageIds) {
    if (!imageIds || imageIds.length === 0) {
      return { success: false, error: 'No image IDs provided' };
    }

    try {
      console.log(`   🗑️  Deleting ${imageIds.length} images...`);
      
      const result = await this.client.query(FILE_DELETE_MUTATION, { 
        fileIds: imageIds 
      });

      if (result.fileDelete.userErrors && result.fileDelete.userErrors.length > 0) {
        const errors = result.fileDelete.userErrors.map(err => err.message).join(', ');
        return { success: false, error: errors };
      }

      const deletedIds = result.fileDelete.deletedFileIds || [];
      console.log(`   ✅ Successfully deleted ${deletedIds.length} images`);
      
      return { 
        success: true, 
        deletedIds: deletedIds,
        deletedCount: deletedIds.length 
      };

    } catch (error) {
      return { success: false, error: error.message };
    }
  }

  async removeSSImages(productHandle, index, total) {
    try {
      // Find product in Shopify
      const product = await this.findProductByHandle(productHandle);
      
      if (!product) {
        console.log(`   ⚠️  [${index}/${total}] Product not found: ${productHandle}`);
        this.deletionResults.notFound.push({
          handle: productHandle,
          reason: 'Product not found in Shopify'
        });
        return;
      }

      // Check if product has media
      if (!product.media || !product.media.edges) {
        console.log(`   ⚠️  [${index}/${total}] Product has no media: ${productHandle}`);
        this.deletionResults.successful.push({
          handle: productHandle,
          title: product.title,
          status: 'no_media_found',
          imagesDeleted: 0,
          shopifyId: product.id
        });
        return;
      }

      const shopifyMedia = product.media.edges.map(edge => edge.node).filter(node => node != null);
      console.log(`   📊 [${index}/${total}] Found ${shopifyMedia.length} media items for: ${productHandle}`);
      
      // Find SS images for this product handle
      const ssImagesData = this.ssImagesCSV.filter(row => 
        row.Handle === productHandle
      );
      
      if (ssImagesData.length === 0) {
        console.log(`   ⚠️  [${index}/${total}] No SS images data for: ${productHandle}`);
        return;
      }

      console.log(`   🔍 [${index}/${total}] Looking for ${ssImagesData.length} SS images with alt text`);

      // Match SS images by alt text
      const matchedImages = [];
      for (const ssImage of ssImagesData) {
        const altText = ssImage['Image Alt Text'];
        if (altText && altText.trim()) {
          console.log(`   🔍 [${index}/${total}] Searching for alt text: "${altText}"`);
          const matches = this.matchSSImagesByAlt(shopifyMedia, altText.trim());
          console.log(JSON.stringify(matches, null, 2));
          if (matches.length > 0) {
            console.log(`   ✅ [${index}/${total}] Found ${matches.length} matches for alt text: "${altText}"`);
          }
          matchedImages.push(...matches);
        }
      }
      
      if (matchedImages.length === 0) {
        console.log(`   ⚠️  [${index}/${total}] No SS images found to delete: ${productHandle}`);
        this.deletionResults.successful.push({
          handle: productHandle,
          title: product.title,
          status: 'no_ss_images_found',
          imagesDeleted: 0,
          shopifyId: product.id
        });
        return;
      }

      // Delete matched images
      const imageIds = matchedImages.map(match => match.shopifyImageId);
      const deleteResult = await this.deleteImages(imageIds);

      if (deleteResult.success) {
        console.log(`   ✅ [${index}/${total}] Successfully deleted ${deleteResult.deletedCount} SS images for: ${productHandle}`);
        
        // Store deleted image IDs for audit
        this.deletedImageIds.push(...deleteResult.deletedIds);
        
        this.deletionResults.successful.push({
          handle: productHandle,
          title: product.title,
          status: 'images_deleted',
          imagesDeleted: deleteResult.deletedCount,
          shopifyId: product.id,
          deletedImageIds: deleteResult.deletedIds,
          matchedImages: matchedImages
        });
      } else {
        console.error(`   ❌ [${index}/${total}] Failed to delete images for: ${productHandle} - ${deleteResult.error}`);
        
        this.deletionResults.failed.push({
          handle: productHandle,
          title: product.title,
          status: 'deletion_failed',
          error: deleteResult.error,
          attemptedImageIds: imageIds,
          matchedImages: matchedImages
        });
      }

    } catch (error) {
      console.error(`   ❌ [${index}/${total}] Failed: ${productHandle} - ${error.message}`);
      
      this.deletionResults.failed.push({
        handle: productHandle,
        status: 'failed',
        error: error.message
      });
    }
  }

  async processProducts(uniqueHandles) {
    console.log(`\n🚀 Starting SS image deletion (${uniqueHandles.length} products)...`);
    
    const batchSize = shopifyConfig.batchSize;
    const total = uniqueHandles.length;
    let processed = 0;

    // Process in batches to respect rate limits
    for (let i = 0; i < uniqueHandles.length; i += batchSize) {
      const batch = uniqueHandles.slice(i, i + batchSize);
      
      console.log(`\n📦 Processing batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(uniqueHandles.length/batchSize)}`);
      
      // Process batch with concurrency limit
      const promises = batch.map((handle, batchIndex) => 
        this.removeSSImages(handle, i + batchIndex + 1, total)
      );
      
      await Promise.all(promises);
      processed += batch.length;
      
      // Progress update
      const successRate = (this.deletionResults.successful.length / processed * 100).toFixed(1);
      console.log(`📈 Progress: ${processed}/${total} processed (${successRate}% success rate)`);
      
      // Rate limiting delay between batches
      if (i + batchSize < uniqueHandles.length) {
        const delay = Math.ceil(1000 / shopifyConfig.maxRequestsPerSecond * batchSize);
        console.log(`⏱️  Waiting ${delay}ms for rate limiting...`);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
  }

  saveDeletionResults() {
    const resultsPath = join(pathConfig.reportsPath, '01_02_ss_deletion_results.json');
    
    const report = {
      timestamp: new Date().toISOString(),
      summary: {
        total: this.deletionResults.successful.length + this.deletionResults.failed.length + this.deletionResults.notFound.length,
        successful: this.deletionResults.successful.length,
        failed: this.deletionResults.failed.length,
        notFound: this.deletionResults.notFound.length,
        totalImagesDeleted: this.deletionResults.successful.reduce((sum, result) => 
          sum + (result.imagesDeleted || 0), 0
        ),
        deletedImageIds: this.deletedImageIds
      },
      results: this.deletionResults,
      config: {
        dryRun: shopifyConfig.dryRun,
        batchSize: shopifyConfig.batchSize,
        store: 'test'
      }
    };

    writeFileSync(resultsPath, JSON.stringify(report, null, 2));
    console.log(`📄 Deletion results saved to: ${resultsPath}`);
    
    // Save deleted image IDs to separate file
    this.saveDeletedImageIds();
  }

  saveDeletedImageIds() {
    if (this.deletedImageIds.length === 0) {
      console.log('⚠️  No deleted images to save');
      return;
    }

    // Save file with deleted image IDs (one per line)
    const deletedIdsPath = join(pathConfig.reportsPath, '01_02_deleted_image_ids.txt');
    const deletedIdsContent = this.deletedImageIds.join('\n');
    writeFileSync(deletedIdsPath, deletedIdsContent);
    console.log(`📄 Deleted image IDs saved to: 01_02_deleted_image_ids.txt (${this.deletedImageIds.length} items)`);

    // Save summary
    const summaryData = {
      timestamp: new Date().toISOString(),
      totalDeleted: this.deletedImageIds.length,
      uniqueHandles: [...new Set(this.deletionResults.successful.map(result => result.handle))].length,
      files: {
        deletedIds: '01_02_deleted_image_ids.txt',
        fullResults: '01_02_ss_deletion_results.json'
      },
      instructions: {
        deletedIdsFile: "Contains only the deleted image IDs, one per line",
        fullResultsFile: "Contains complete deletion results with success/failure details"
      }
    };

    const summaryPath = join(pathConfig.reportsPath, '01_02_deletion_summary.json');
    writeFileSync(summaryPath, JSON.stringify(summaryData, null, 2));
    console.log(`📄 Summary saved to: 01_02_deletion_summary.json`);
  }

  printSummary() {
    const { successful, failed, notFound } = this.deletionResults;
    const total = successful.length + failed.length + notFound.length;
    const totalImagesDeleted = successful.reduce((sum, result) => sum + (result.imagesDeleted || 0), 0);
    
    console.log('\n' + '='.repeat(70));
    console.log('🗑️  SS IMAGE DELETION SUMMARY');
    console.log('='.repeat(70));
    console.log(`Total products processed: ${total}`);
    console.log(`✅ Successful: ${successful.length}`);
    console.log(`❌ Failed: ${failed.length}`);
    console.log(`🔍 Not found: ${notFound.length}`);
    console.log(`🗑️  Total images deleted: ${totalImagesDeleted}`);
    console.log(`📝 Deleted image IDs: ${this.deletedImageIds.length}`);
    
    if (total > 0) {
      const successRate = (successful.length / total * 100).toFixed(1);
      console.log(`📈 Success rate: ${successRate}%`);
    }
    
    if (this.deletedImageIds.length > 0) {
      const uniqueHandles = [...new Set(this.deletionResults.successful.map(result => result.handle))].length;
      
      console.log('\n' + '='.repeat(70));
      console.log('📁 OUTPUT FILES CREATED');
      console.log('='.repeat(70));
      console.log(`📁 Check the reports folder for these files:`);
      console.log(`   📄 01_02_deleted_image_ids.txt (${this.deletedImageIds.length} IDs)`);
      console.log(`   📄 01_02_ss_deletion_results.json (detailed results)`);
      console.log(`   📄 01_02_deletion_summary.json (summary)`);
      console.log(`\n📊 Deletion summary:`);
      console.log(`   🗑️  Deleted images: ${this.deletedImageIds.length}`);
      console.log(`   🏪 Affected products: ${uniqueHandles}`);
    }
    
    console.log('\n💡 Files created:');
    console.log('   📄 01_02_deleted_image_ids.txt - Just the deleted image IDs (one per line)');
    console.log('   📄 01_02_ss_deletion_results.json - Complete deletion results');
    console.log('   📄 01_02_deletion_summary.json - Summary information');
  }
}

async function main() {
  console.log('='.repeat(70));
  console.log('🗑️  SS IMAGES DELETION');
  console.log('='.repeat(70));

  const deleter = new SSImageDeleter();

  try {
    await deleter.initialize();
    
    const ssImageData = deleter.loadSSImagesFromCSV();
    deleter.ssImagesCSV = ssImageData; // Store for use in removeSSImages method
    
    if (ssImageData.length === 0) {
      console.log('⚠️ No SS images to process');
      return;
    }

    // Get unique handles
    const uniqueHandles = [...new Set(ssImageData.map(row => row.Handle))];
    console.log(`📊 Found ${ssImageData.length} SS images across ${uniqueHandles.length} products`);

    await deleter.processProducts(uniqueHandles);
    deleter.saveDeletionResults();
    deleter.printSummary();

    console.log('\n🎉 SS image deletion completed!');

  } catch (error) {
    console.error(`\n❌ SS image deletion failed: ${error.message}`);
    console.error(error.stack);
    process.exit(1);
  }
}

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.log('\n⏹️ SS image deletion interrupted by user');
  process.exit(0);
});

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}