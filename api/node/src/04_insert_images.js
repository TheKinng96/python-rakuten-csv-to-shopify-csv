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
    productByHandle(handle: $handle) {
      id
      handle
      title
      images(first: 250) {
        edges {
          node {
            id
            src
            altText
          }
        }
      }
    }
  }
`;

const CREATE_PRODUCT_IMAGES_MUTATION = `
  mutation productCreateImages($id: ID!, $images: [ImageInput!]!) {
    productCreateImages(id: $id, images: $images) {
      images {
        id
        src
        altText
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
      skipped: []
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

      createReadStream(csvFilePath)
        .pipe(csv())
        .on('data', (row) => {
          const handle = row.product_handle?.trim();
          const imageUrl = row.image_url?.trim();
          const altText = row.alt_text?.trim() || '';
          const position = parseInt(row.position) || 1;

          if (handle && imageUrl) {
            if (!groupedData[handle]) {
              groupedData[handle] = [];
            }
            
            groupedData[handle].push({
              src: imageUrl,
              altText: altText,
              position: position
            });
          }
        })
        .on('end', () => {
          // Convert to array format
          for (const [handle, images] of Object.entries(groupedData)) {
            imageData.push({
              productHandle: handle,
              imagesToAdd: images.sort((a, b) => a.position - b.position)
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
      return result.productByHandle;
    } catch (error) {
      throw new Error(`Failed to find product ${handle}: ${error.message}`);
    }
  }

  checkForDuplicateImages(existingImages, newImages) {
    const duplicateImages = [];
    const uniqueImages = [];

    for (const newImage of newImages) {
      const isDuplicate = existingImages.some(existing => {
        // Check for exact URL match or filename match
        const existingFileName = existing.src.split('/').pop();
        const newFileName = newImage.src.split('/').pop();
        
        return existing.src === newImage.src || existingFileName === newFileName;
      });

      if (isDuplicate) {
        duplicateImages.push(newImage);
      } else {
        uniqueImages.push(newImage);
      }
    }

    return { uniqueImages, duplicateImages };
  }

  async insertImages(productHandle, imagesToAdd, index, total) {
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

      const existingImages = product.images.edges.map(edge => edge.node);
      
      // Check for duplicate images
      const { uniqueImages, duplicateImages } = this.checkForDuplicateImages(existingImages, imagesToAdd);

      if (uniqueImages.length === 0) {
        console.log(`   ℹ️  [${index}/${total}] All images already exist: ${productHandle}`);
        this.insertResults.skipped.push({
          handle: productHandle,
          title: product.title,
          shopifyId: product.id,
          reason: 'All images already exist',
          duplicateCount: duplicateImages.length
        });
        return;
      }

      // Prepare images for insertion (remove position as it's not used in GraphQL)
      const imagesToInsert = uniqueImages.map(img => ({
        src: img.src,
        altText: img.altText || product.title
      }));

      // Insert images
      if (shopifyConfig.dryRun) {
        console.log(`   🔍 [DRY RUN] Would add ${uniqueImages.length} images to: ${productHandle}`);
        this.insertResults.successful.push({
          handle: productHandle,
          title: product.title,
          status: 'dry_run',
          imagesAdded: uniqueImages.length,
          duplicatesSkipped: duplicateImages.length,
          shopifyId: product.id,
          addedImages: imagesToInsert
        });
        return;
      }

      const result = await this.client.mutate(CREATE_PRODUCT_IMAGES_MUTATION, {
        id: product.id,
        images: imagesToInsert
      });

      if (result.productCreateImages.userErrors.length > 0) {
        const errors = result.productCreateImages.userErrors.map(e => `${e.field}: ${e.message}`);
        throw new Error(`Shopify errors: ${errors.join(', ')}`);
      }

      const addedImages = result.productCreateImages.images;
      
      console.log(`   ✅ [${index}/${total}] Added ${addedImages.length} images to: ${productHandle}${duplicateImages.length > 0 ? ` (${duplicateImages.length} duplicates skipped)` : ''}`);
      
      this.insertResults.successful.push({
        handle: productHandle,
        title: product.title,
        status: 'images_added',
        imagesAdded: addedImages.length,
        duplicatesSkipped: duplicateImages.length,
        shopifyId: product.id,
        addedImages: addedImages.map(img => ({
          id: img.id,
          src: img.src,
          altText: img.altText
        }))
      });

    } catch (error) {
      console.error(`   ❌ [${index}/${total}] Failed: ${productHandle} - ${error.message}`);
      
      this.insertResults.failed.push({
        handle: productHandle,
        status: 'failed',
        error: error.message,
        attemptedImages: imagesToAdd.length
      });
    }
  }

  async processProducts(imageData) {
    console.log(`\n🚀 Starting image insertion (${imageData.length} products)...`);
    
    const batchSize = shopifyConfig.batchSize;
    const total = imageData.length;
    let processed = 0;

    // Process in batches to respect rate limits
    for (let i = 0; i < imageData.length; i += batchSize) {
      const batch = imageData.slice(i, i + batchSize);
      
      console.log(`\n📦 Processing batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(imageData.length/batchSize)}`);
      
      // Process batch with concurrency limit
      const promises = batch.map((productData, batchIndex) => 
        this.insertImages(productData.productHandle, productData.imagesToAdd, i + batchIndex + 1, total)
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
        totalImagesAdded: this.insertResults.successful.reduce((sum, result) => 
          sum + (result.imagesAdded || 0), 0
        ),
        totalDuplicatesSkipped: this.insertResults.successful.reduce((sum, result) => 
          sum + (result.duplicatesSkipped || 0), 0
        ) + this.insertResults.skipped.reduce((sum, result) => 
          sum + (result.duplicateCount || 0), 0
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
    const { successful, failed, notFound, skipped } = this.insertResults;
    const total = successful.length + failed.length + notFound.length + skipped.length;
    const totalImagesAdded = successful.reduce((sum, result) => sum + (result.imagesAdded || 0), 0);
    const totalDuplicatesSkipped = successful.reduce((sum, result) => sum + (result.duplicatesSkipped || 0), 0) +
                                  skipped.reduce((sum, result) => sum + (result.duplicateCount || 0), 0);
    
    console.log('\n' + '='.repeat(70));
    console.log('📊 IMAGE INSERTION SUMMARY');
    console.log('='.repeat(70));
    console.log(`Total products: ${total}`);
    console.log(`✅ Successfully processed: ${successful.length}`);
    console.log(`❌ Failed: ${failed.length}`);
    console.log(`🔍 Not found: ${notFound.length}`);
    console.log(`⏭️  Skipped (duplicates): ${skipped.length}`);
    console.log(`🖼️  Total images added: ${totalImagesAdded}`);
    console.log(`🔄 Total duplicates skipped: ${totalDuplicatesSkipped}`);
    
    if (total > 0) {
      const successRate = ((successful.length + skipped.length) / total * 100).toFixed(1);
      console.log(`📈 Success rate: ${successRate}%`);
    }
    
    if (shopifyConfig.dryRun) {
      console.log('\n🔍 This was a DRY RUN - no actual insertions performed');
      console.log('💡 Set DRY_RUN=false in .env to perform actual insertion');
    }
    
    console.log('\n💡 Next steps:');
    console.log('   1. Review insert results in reports/04_image_insert_results.json');
    console.log('   2. Check products with new images in Shopify admin');
    console.log('   3. Verify image quality and positioning');
  }
}

async function main() {
  console.log('='.repeat(70));
  console.log('🖼️  IMAGE INSERTION');
  console.log('='.repeat(70));

  const inserter = new ImageInserter();

  try {
    await inserter.initialize();
    
    // Look for CSV file in reports directory
    const csvPath = join(pathConfig.reportsPath, 'images_to_insert.csv');
    
    if (!existsSync(csvPath)) {
      console.log(`⚠️ CSV file not found: ${csvPath}`);
      console.log('\n📝 Expected CSV format:');
      console.log('product_handle,image_url,alt_text,position');
      console.log('example-product,https://example.com/image.jpg,Product Image,1');
      console.log('\n💡 Create the CSV file with images to insert and run again.');
      return;
    }
    
    const imageData = await inserter.loadImageDataFromCSV(csvPath);
    
    if (imageData.length === 0) {
      console.log('⚠️ No valid image data found in CSV file');
      return;
    }

    // Confirmation for live insertion
    if (!shopifyConfig.dryRun) {
      const totalImages = imageData.reduce((sum, product) => sum + product.imagesToAdd.length, 0);
      console.log(`\n⚠️  LIVE INSERTION MODE - This will add ${totalImages} images to ${imageData.length} products!`);
      console.log('Press Ctrl+C to cancel or wait 5 seconds to continue...');
      await new Promise(resolve => setTimeout(resolve, 5000));
    }

    await inserter.processProducts(imageData);
    inserter.saveInsertResults();
    inserter.printSummary();

    console.log('\n🎉 Image insertion completed!');

  } catch (error) {
    console.error(`\n❌ Image insertion failed: ${error.message}`);
    console.error(error.stack);
    process.exit(1);
  }
}

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.log('\n⏹️ Image insertion interrupted by user');
  process.exit(0);
});

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}