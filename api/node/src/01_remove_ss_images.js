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

const GET_PRODUCT_IMAGES_QUERY = `
  query getProductImages($handle: String!) {
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

const DELETE_PRODUCT_IMAGES_MUTATION = `
  mutation productDeleteImages($id: ID!, $imageIds: [ID!]!) {
    productDeleteImages(id: $id, imageIds: $imageIds) {
      deletedImageIds
      userErrors {
        field
        message
      }
    }
  }
`;

class SSImageRemover {
  constructor() {
    this.client = null;
    this.removalResults = {
      successful: [],
      failed: [],
      notFound: []
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

  loadSSImagesFromJSON() {
    const jsonPath = join(pathConfig.sharedPath, 'ss_images_to_remove.json');
    
    if (!existsSync(jsonPath)) {
      throw new Error(`SS images JSON file not found: ${jsonPath}`);
    }

    console.log(`📂 Loading SS images data from ${jsonPath}...`);
    
    const jsonData = JSON.parse(readFileSync(jsonPath, 'utf-8'));
    
    if (!jsonData.data || !Array.isArray(jsonData.data)) {
      throw new Error('Invalid JSON structure: expected data array');
    }

    console.log(`✅ Loaded ${jsonData.data.length} products with SS images from JSON`);
    return jsonData.data;
  }

  async findProductByHandle(handle) {
    try {
      const result = await this.client.query(GET_PRODUCT_IMAGES_QUERY, { handle });
      return result.productByHandle;
    } catch (error) {
      throw new Error(`Failed to find product ${handle}: ${error.message}`);
    }
  }

  matchSSImages(shopifyImages, ssImagesToRemove) {
    const matchedImages = [];
    
    for (const ssImage of ssImagesToRemove) {
      const ssImageUrl = ssImage.imageUrl;
      
      // Find matching Shopify image by URL
      const shopifyImage = shopifyImages.find(img => {
        // Extract filename from URLs for comparison
        const ssFileName = ssImageUrl.split('/').pop();
        const shopifyFileName = img.src.split('/').pop();
        
        // Direct URL match or filename match
        return img.src === ssImageUrl || shopifyFileName === ssFileName;
      });
      
      if (shopifyImage) {
        matchedImages.push({
          shopifyImageId: shopifyImage.id,
          shopifyImageSrc: shopifyImage.src,
          ssImagePattern: ssImage.ssPattern,
          ssImageUrl: ssImageUrl,
          reason: ssImage.reason
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

      const shopifyImages = product.images.edges.map(edge => edge.node);
      
      // Find matching SS images to remove
      const ssImagesData = this.loadSSImagesFromJSON().find(item => 
        item.productHandle === productHandle
      );
      
      if (!ssImagesData) {
        console.log(`   ⚠️  [${index}/${total}] No SS images data for: ${productHandle}`);
        return;
      }

      const matchedImages = this.matchSSImages(shopifyImages, ssImagesData.imagesToRemove);
      
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

      // Remove matched SS images
      if (shopifyConfig.dryRun) {
        console.log(`   🔍 [DRY RUN] Would remove ${matchedImages.length} SS images from: ${productHandle}`);
        this.removalResults.successful.push({
          handle: productHandle,
          title: product.title,
          status: 'dry_run',
          imagesRemoved: matchedImages.length,
          shopifyId: product.id,
          removedImages: matchedImages
        });
        return;
      }

      const imageIdsToRemove = matchedImages.map(img => img.shopifyImageId);
      
      const result = await this.client.mutate(DELETE_PRODUCT_IMAGES_MUTATION, {
        id: product.id,
        imageIds: imageIdsToRemove
      });

      if (result.productDeleteImages.userErrors.length > 0) {
        const errors = result.productDeleteImages.userErrors.map(e => `${e.field}: ${e.message}`);
        throw new Error(`Shopify errors: ${errors.join(', ')}`);
      }

      const deletedImageIds = result.productDeleteImages.deletedImageIds;
      
      console.log(`   ✅ [${index}/${total}] Removed ${deletedImageIds.length} SS images from: ${productHandle}`);
      
      this.removalResults.successful.push({
        handle: productHandle,
        title: product.title,
        status: 'images_removed',
        imagesRemoved: deletedImageIds.length,
        shopifyId: product.id,
        removedImages: matchedImages.filter(img => 
          deletedImageIds.includes(img.shopifyImageId)
        )
      });

    } catch (error) {
      console.error(`   ❌ [${index}/${total}] Failed: ${productHandle} - ${error.message}`);
      
      this.removalResults.failed.push({
        handle: productHandle,
        status: 'failed',
        error: error.message
      });
    }
  }

  async processProducts(ssImageData) {
    console.log(`\n🚀 Starting SS image removal (${ssImageData.length} products)...`);
    
    const batchSize = shopifyConfig.batchSize;
    const total = ssImageData.length;
    let processed = 0;

    // Process in batches to respect rate limits
    for (let i = 0; i < ssImageData.length; i += batchSize) {
      const batch = ssImageData.slice(i, i + batchSize);
      
      console.log(`\n📦 Processing batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(ssImageData.length/batchSize)}`);
      
      // Process batch with concurrency limit
      const promises = batch.map((productData, batchIndex) => 
        this.removeSSImages(productData.productHandle, i + batchIndex + 1, total)
      );
      
      await Promise.all(promises);
      processed += batch.length;
      
      // Progress update
      const successRate = (this.removalResults.successful.length / processed * 100).toFixed(1);
      console.log(`📈 Progress: ${processed}/${total} processed (${successRate}% success rate)`);
      
      // Rate limiting delay between batches
      if (i + batchSize < ssImageData.length) {
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
        totalImagesRemoved: this.removalResults.successful.reduce((sum, result) => 
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
  }

  printSummary() {
    const { successful, failed, notFound } = this.removalResults;
    const total = successful.length + failed.length + notFound.length;
    const totalImagesRemoved = successful.reduce((sum, result) => sum + (result.imagesRemoved || 0), 0);
    
    console.log('\n' + '='.repeat(70));
    console.log('📊 SS IMAGE REMOVAL SUMMARY');
    console.log('='.repeat(70));
    console.log(`Total products: ${total}`);
    console.log(`✅ Successful: ${successful.length}`);
    console.log(`❌ Failed: ${failed.length}`);
    console.log(`🔍 Not found: ${notFound.length}`);
    console.log(`🖼️  Total images removed: ${totalImagesRemoved}`);
    
    if (total > 0) {
      const successRate = (successful.length / total * 100).toFixed(1);
      console.log(`📈 Success rate: ${successRate}%`);
    }
    
    if (shopifyConfig.dryRun) {
      console.log('\n🔍 This was a DRY RUN - no actual removals performed');
      console.log('💡 Set DRY_RUN=false in .env to perform actual removal');
    }
    
    console.log('\n💡 Next steps:');
    console.log('   1. Review removal results in reports/01_ss_removal_results.json');
    console.log('   2. Check affected products in Shopify admin');
    console.log('   3. Continue with other processing scripts if needed');
  }
}

async function main() {
  console.log('='.repeat(70));
  console.log('🖼️  SS IMAGES REMOVAL');
  console.log('='.repeat(70));

  const remover = new SSImageRemover();

  try {
    await remover.initialize();
    
    const ssImageData = remover.loadSSImagesFromJSON();
    
    if (ssImageData.length === 0) {
      console.log('⚠️ No products with SS images to process');
      return;
    }

    // Confirmation for live removal
    if (!shopifyConfig.dryRun) {
      console.log(`\n⚠️  LIVE REMOVAL MODE - This will remove SS images from ${ssImageData.length} products!`);
      console.log('Press Ctrl+C to cancel or wait 5 seconds to continue...');
      await new Promise(resolve => setTimeout(resolve, 5000));
    }

    await remover.processProducts(ssImageData);
    remover.saveRemovalResults();
    remover.printSummary();

    console.log('\n🎉 SS image removal completed!');

  } catch (error) {
    console.error(`\n❌ SS image removal failed: ${error.message}`);
    console.error(error.stack);
    process.exit(1);
  }
}

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.log('\n⏹️ SS image removal interrupted by user');
  process.exit(0);
});

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}