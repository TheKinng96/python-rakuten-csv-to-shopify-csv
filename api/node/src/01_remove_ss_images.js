#!/usr/bin/env node
/**
 * Remove SS images from Shopify products via GraphQL
 * 
 * This script:
 * 1. Reads ss_images_to_remove.json from shared directory
 * 2. Finds products by handle and removes specified SS images
 * 3. Provides progress logging and error handling
 * 4. Saves removal results for audit
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

// Removed DELETE_FILES_MUTATION - now only collecting data

class SSImageRemover {
  constructor() {
    this.client = null;
    this.removalResults = {
      successful: [],
      failed: [],
      notFound: []
    };
    this.matchedImages = []; // Store matched images with details
    this.imageGids = []; // Store just the GIDs
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

  async removeSSImages(productHandle, index, total) {
    try {
      // Find product in Shopify
      const product = await this.findProductByHandle(productHandle);
      
      if (!product) {
        console.log(`   ⚠️  [${index}/${total}] Product not found: ${productHandle}`);
        this.removalResults.notFound.push({
          handle: productHandle,
          reason: 'Product not found in Shopify'
        });
        return;
      }

      // Check if product has media
      if (!product.media || !product.media.edges) {
        console.log(`   ⚠️  [${index}/${total}] Product has no media: ${productHandle}`);
        this.removalResults.successful.push({
          handle: productHandle,
          title: product.title,
          status: 'no_media_found',
          imagesRemoved: 0,
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
          if (matches.length > 0) {
            console.log(`   ✅ [${index}/${total}] Found ${matches.length} matches for alt text: "${altText}"`);
          }
          matchedImages.push(...matches);
        }
      }
      
      if (matchedImages.length === 0) {
        console.log(`   ⚠️  [${index}/${total}] No SS images found to remove: ${productHandle}`);
        this.removalResults.successful.push({
          handle: productHandle,
          title: product.title,
          status: 'no_ss_images_found',
          imagesRemoved: 0,
          shopifyId: product.id
        });
        return;
      }

      // Store matched images for file output
      if (matchedImages.length > 0) {
        console.log(`   ✅ [${index}/${total}] Found ${matchedImages.length} matching SS images for: ${productHandle}`);
        
        // Store matched images with full details
        for (const match of matchedImages) {
          this.matchedImages.push({
            gid: match.shopifyImageId,
            handle: productHandle,
            imageAlt: match.shopifyImageAlt
          });
          
          // Store just the GID
          this.imageGids.push(match.shopifyImageId);
        }
        
        console.log(`   📝 [${index}/${total}] Added ${matchedImages.length} images to collection`);
        
        this.removalResults.successful.push({
          handle: productHandle,
          title: product.title,
          status: 'images_found',
          imagesRemoved: matchedImages.length,
          shopifyId: product.id,
          matchedImages: matchedImages
        });
      } else {
        console.log(`   ⚠️  [${index}/${total}] No matching SS images found for: ${productHandle}`);
        this.removalResults.successful.push({
          handle: productHandle,
          title: product.title,
          status: 'no_matches',
          imagesRemoved: 0,
          shopifyId: product.id
        });
      }

    } catch (error) {
      console.error(`   ❌ [${index}/${total}] Failed: ${productHandle} - ${error.message}`);
      
      this.removalResults.failed.push({
        handle: productHandle,
        status: 'failed',
        error: error.message
      });
    }
  }

  async processProducts(uniqueHandles) {
    console.log(`\n🚀 Starting SS image removal (${uniqueHandles.length} products)...`);
    
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
      const successRate = (this.removalResults.successful.length / processed * 100).toFixed(1);
      console.log(`📈 Progress: ${processed}/${total} processed (${successRate}% success rate)`);
      
      // Rate limiting delay between batches
      if (i + batchSize < uniqueHandles.length) {
        const delay = Math.ceil(1000 / shopifyConfig.maxRequestsPerSecond * batchSize);
        console.log(`⏱️  Waiting ${delay}ms for rate limiting...`);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
  }

  saveRemovalResults() {
    const resultsPath = join(pathConfig.reportsPath, '01_ss_removal_results.json');
    
    const report = {
      timestamp: new Date().toISOString(),
      summary: {
        total: this.removalResults.successful.length + this.removalResults.failed.length + this.removalResults.notFound.length,
        successful: this.removalResults.successful.length,
        failed: this.removalResults.failed.length,
        notFound: this.removalResults.notFound.length,
        totalImagesFound: this.removalResults.successful.reduce((sum, result) => 
          sum + (result.imagesRemoved || 0), 0
        )
      },
      results: this.removalResults,
      config: {
        dryRun: shopifyConfig.dryRun,
        batchSize: shopifyConfig.batchSize,
        store: 'test'
      }
    };

    writeFileSync(resultsPath, JSON.stringify(report, null, 2));
    console.log(`📄 Removal results saved to: ${resultsPath}`);
    
    // Save matched images to files
    this.saveMatchedImagesToFiles();
  }

  saveMatchedImagesToFiles() {
    if (this.matchedImages.length === 0) {
      console.log('⚠️  No matched images to save');
      return;
    }

    // Save file with just GIDs (one per line)
    const gidsPath = join(pathConfig.reportsPath, '01_ss_image_gids.txt');
    const gidsContent = this.imageGids.join('\n');
    writeFileSync(gidsPath, gidsContent);
    console.log(`📄 Image GIDs saved to: 01_ss_image_gids.txt (${this.imageGids.length} items)`);

    // Save file with full image details (CSV format)
    const detailsPath = join(pathConfig.reportsPath, '01_ss_image_details.csv');
    const csvHeader = 'gid,handle,imageAlt\n';
    const csvRows = this.matchedImages.map(img => 
      `${img.gid},"${img.handle}","${img.imageAlt}"`
    ).join('\n');
    const csvContent = csvHeader + csvRows;
    writeFileSync(detailsPath, csvContent);
    console.log(`📄 Image details saved to: 01_ss_image_details.csv (${this.matchedImages.length} items)`);

    // Save summary
    const summaryData = {
      timestamp: new Date().toISOString(),
      totalImages: this.matchedImages.length,
      uniqueHandles: [...new Set(this.matchedImages.map(img => img.handle))].length,
      files: {
        gidsOnly: '01_ss_image_gids.txt',
        fullDetails: '01_ss_image_details.csv'
      },
      instructions: {
        gidsFile: "Contains only the image GIDs, one per line",
        detailsFile: "Contains GID, handle, and image alt text in CSV format"
      }
    };

    const summaryPath = join(pathConfig.reportsPath, '01_ss_matched_images_summary.json');
    writeFileSync(summaryPath, JSON.stringify(summaryData, null, 2));
    console.log(`📄 Summary saved to: 01_ss_matched_images_summary.json`);
  }

  printSummary() {
    const { successful, failed, notFound } = this.removalResults;
    const total = successful.length + failed.length + notFound.length;
    const totalImagesFound = successful.reduce((sum, result) => sum + (result.imagesRemoved || 0), 0);
    
    console.log('\n' + '='.repeat(70));
    console.log('📊 SS IMAGE MATCHING SUMMARY');
    console.log('='.repeat(70));
    console.log(`Total products processed: ${total}`);
    console.log(`✅ Successful: ${successful.length}`);
    console.log(`❌ Failed: ${failed.length}`);
    console.log(`🔍 Not found: ${notFound.length}`);
    console.log(`🖼️  Total matching images found: ${totalImagesFound}`);
    console.log(`🗂️  Unique images collected: ${this.matchedImages.length}`);
    
    if (total > 0) {
      const successRate = (successful.length / total * 100).toFixed(1);
      console.log(`📈 Success rate: ${successRate}%`);
    }
    
    if (this.matchedImages.length > 0) {
      const uniqueHandles = [...new Set(this.matchedImages.map(img => img.handle))].length;
      
      console.log('\n' + '='.repeat(70));
      console.log('🗂️  OUTPUT FILES CREATED');
      console.log('='.repeat(70));
      console.log(`📁 Check the reports folder for these files:`);
      console.log(`   📄 01_ss_image_gids.txt (${this.imageGids.length} GIDs)`);
      console.log(`   📄 01_ss_image_details.csv (${this.matchedImages.length} rows)`);
      console.log(`   📄 01_ss_matched_images_summary.json`);
      console.log(`   📄 01_ss_removal_results.json`);
      console.log(`\n📊 Data summary:`);
      console.log(`   🎯 Matched images: ${this.matchedImages.length}`);
      console.log(`   🏪 Unique products: ${uniqueHandles}`);
    }
    
    console.log('\n💡 Files created:');
    console.log('   📄 01_ss_image_gids.txt - Just the image GIDs (one per line)');
    console.log('   📄 01_ss_image_details.csv - Full details (GID, handle, image alt)');
    console.log('   📄 01_ss_matched_images_summary.json - Summary information');
  }
}

async function main() {
  console.log('='.repeat(70));
  console.log('🖼️  SS IMAGES MATCHING');
  console.log('='.repeat(70));

  const remover = new SSImageRemover();

  try {
    await remover.initialize();
    
    const ssImageData = remover.loadSSImagesFromCSV();
    remover.ssImagesCSV = ssImageData; // Store for use in removeSSImages method
    
    if (ssImageData.length === 0) {
      console.log('⚠️ No SS images to process');
      return;
    }

    // Get unique handles
    const uniqueHandles = [...new Set(ssImageData.map(row => row.Handle))];
    console.log(`📊 Found ${ssImageData.length} SS images across ${uniqueHandles.length} products`);

    await remover.processProducts(uniqueHandles);
    remover.saveRemovalResults();
    remover.printSummary();

    console.log('\n🎉 SS image matching completed!');

  } catch (error) {
    console.error(`\n❌ SS image matching failed: ${error.message}`);
    console.error(error.stack);
    process.exit(1);
  }
}

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.log('\n⏹️ SS image matching interrupted by user');
  process.exit(0);
});

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}