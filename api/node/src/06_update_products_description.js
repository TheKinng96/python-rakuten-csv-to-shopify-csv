#!/usr/bin/env node
/**
 * Update Shopify product descriptions by removing EC-UP content
 * 
 * This script:
 * 1. Reads extracted SKU rows from CSV (from 05_extract_sku_rows.py)
 * 2. For each product handle, removes EC-UP patterns from descriptionHtml
 * 3. Updates products via GraphQL using 2025-07 API
 * 4. Provides progress logging and error handling
 * 
 * CHECK: DONE
 */

import { readFileSync, writeFileSync, existsSync } from 'fs';
import { join } from 'path';
import { shopifyConfig, pathConfig, validateConfig } from './config.js';
import { ShopifyGraphQLClient } from './shopify-client.js';

const GET_PRODUCT_QUERY = `
  query getProduct($handle: String!) {
    productByHandle(handle: $handle) {
      id
      handle
      title
      descriptionHtml
    }
  }
`;

const UPDATE_PRODUCT_MUTATION = `
  mutation productUpdate($product: ProductUpdateInput!) {
    productUpdate(product: $product) {
      product {
        id
        handle
        title
        descriptionHtml
      }
      userErrors {
        field
        message
      }
    }
  }
`;

class ProductDescriptionUpdater {
  constructor() {
    this.client = null;
    this.updateResults = {
      successful: [],
      failed: [],
      notFound: [],
      noChangesNeeded: [],
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

  loadProductDataFromJSON() {
    const jsonPath = join(pathConfig.sharedPath, 'rakuten_content_to_clean.json');
    
    if (!existsSync(jsonPath)) {
      throw new Error(`Rakuten content JSON file not found: ${jsonPath}`);
    }

    console.log(`📂 Loading product data from ${jsonPath}...`);
    
    const jsonData = JSON.parse(readFileSync(jsonPath, 'utf-8'));
    
    if (!jsonData.data || !Array.isArray(jsonData.data)) {
      throw new Error('Invalid JSON structure: expected data array');
    }

    console.log(`✅ Loaded ${jsonData.data.length} products with cleaned HTML from JSON`);
    return jsonData.data;
  }

  async findProductByHandle(handle) {
    try {
      const result = await this.client.query(GET_PRODUCT_QUERY, { handle });
      return result.productByHandle;
    } catch (error) {
      throw new Error(`Failed to find product ${handle}: ${error.message}`);
    }
  }


  async updateProductDescription(productData, index, total) {
    try {
      const { productHandle, cleanedHtml, patternsRemoved, bytesRemoved } = productData;
      
      // Find product in Shopify
      const product = await this.findProductByHandle(productHandle);
      
      if (!product) {
        console.log(`   ⚠️  [${index}/${total}] Product not found: ${productHandle}`);
        this.updateResults.notFound.push({
          handle: productHandle,
          reason: 'Product not found in Shopify'
        });
        return;
      }

      const currentHtml = product.descriptionHtml || '';
      
      // Check if there are any changes to make
      if (currentHtml === cleanedHtml) {
        console.log(`   ℹ️  [${index}/${total}] No changes needed: ${productHandle}`);
        this.updateResults.noChangesNeeded.push({
          handle: productHandle,
          title: product.title,
          shopifyId: product.id,
          reason: 'HTML already matches cleaned version'
        });
        return;
      }

      // Update product with cleaned HTML
      if (shopifyConfig.dryRun) {
        console.log(`   🔍 [DRY RUN] Would remove ${patternsRemoved.length} EC-UP patterns (${bytesRemoved} bytes) from: ${productHandle}`);
        this.updateResults.successful.push({
          handle: productHandle,
          title: product.title,
          status: 'dry_run',
          patternsRemoved: patternsRemoved.length,
          bytesRemoved,
          shopifyId: product.id,
          patterns: patternsRemoved
        });
        return;
      }

      const result = await this.client.mutate(UPDATE_PRODUCT_MUTATION, {
        product: {
          id: product.id,
          descriptionHtml: cleanedHtml
        }
      });

      if (result.productUpdate.userErrors.length > 0) {
        const errors = result.productUpdate.userErrors.map(e => `${e.field}: ${e.message}`);
        throw new Error(`Shopify errors: ${errors.join(', ')}`);
      }

      console.log(`   ✅ [${index}/${total}] Removed ${patternsRemoved.length} EC-UP patterns (${bytesRemoved} bytes) from: ${productHandle}`);
      
      this.updateResults.successful.push({
        handle: productHandle,
        title: product.title,
        status: 'description_updated',
        patternsRemoved: patternsRemoved.length,
        bytesRemoved,
        shopifyId: product.id,
        patterns: patternsRemoved,
        originalHtmlLength: currentHtml.length,
        cleanedHtmlLength: cleanedHtml.length,
        compressionRatio: ((1 - cleanedHtml.length / currentHtml.length) * 100).toFixed(1) + '%'
      });

    } catch (error) {
      console.error(`   ❌ [${index}/${total}] Failed: ${productData.productHandle} - ${error.message}`);
      
      this.updateResults.failed.push({
        handle: productData.productHandle,
        status: 'failed',
        error: error.message
      });
    }
  }

  async processProducts(productDataArray) {
    console.log(`\n🚀 Starting product description updates (${productDataArray.length} products)...`);
    
    const batchSize = shopifyConfig.batchSize;
    const total = productDataArray.length;
    let processed = 0;

    // Process in batches to respect rate limits
    for (let i = 0; i < productDataArray.length; i += batchSize) {
      const batch = productDataArray.slice(i, i + batchSize);
      
      console.log(`\n📦 Processing batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(productDataArray.length/batchSize)}`);
      
      // Process batch with concurrency limit
      const promises = batch.map((productData, batchIndex) => 
        this.updateProductDescription(productData, i + batchIndex + 1, total)
      );
      
      await Promise.all(promises);
      processed += batch.length;
      
      // Progress update
      const successRate = (this.updateResults.successful.length / processed * 100).toFixed(1);
      console.log(`📈 Progress: ${processed}/${total} processed (${successRate}% success rate)`);
      
      // Rate limiting delay between batches
      if (i + batchSize < productDataArray.length) {
        const delay = Math.ceil(1000 / shopifyConfig.maxRequestsPerSecond * batchSize);
        console.log(`⏱️  Waiting ${delay}ms for rate limiting...`);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
  }

  saveUpdateResults() {
    const resultsPath = join(pathConfig.reportsPath, '06_description_update_results.json');
    
    const report = {
      timestamp: new Date().toISOString(),
      summary: {
        total: this.updateResults.successful.length + this.updateResults.failed.length + this.updateResults.notFound.length + this.updateResults.noChangesNeeded.length,
        successful: this.updateResults.successful.length,
        failed: this.updateResults.failed.length,
        notFound: this.updateResults.notFound.length,
        noChangesNeeded: this.updateResults.noChangesNeeded.length,
        totalPatternsRemoved: this.updateResults.successful.reduce((sum, result) => 
          sum + (result.patternsRemoved || 0), 0
        ),
        totalBytesRemoved: this.updateResults.successful.reduce((sum, result) => 
          sum + (result.bytesRemoved || 0), 0
        )
      },
      results: this.updateResults,
      config: {
        dryRun: shopifyConfig.dryRun,
        batchSize: shopifyConfig.batchSize,
        store: 'test',
        apiVersion: '2025-07'
      }
    };

    writeFileSync(resultsPath, JSON.stringify(report, null, 2));
    console.log(`📄 Update results saved to: ${resultsPath}`);
  }

  printSummary() {
    const { successful, failed, notFound, noChangesNeeded } = this.updateResults;
    const total = successful.length + failed.length + notFound.length + noChangesNeeded.length;
    const totalPatternsRemoved = successful.reduce((sum, result) => sum + (result.patternsRemoved || 0), 0);
    const totalBytesRemoved = successful.reduce((sum, result) => sum + (result.bytesRemoved || 0), 0);
    
    console.log('\n' + '='.repeat(70));
    console.log('📊 PRODUCT DESCRIPTION UPDATE SUMMARY');
    console.log('='.repeat(70));
    console.log(`Total products: ${total}`);
    console.log(`✅ Successfully updated: ${successful.length}`);
    console.log(`❌ Failed: ${failed.length}`);
    console.log(`🔍 Not found: ${notFound.length}`);
    console.log(`ℹ️  No EC-UP content: ${noChangesNeeded.length}`);
    console.log(`🏷️  Total EC-UP patterns removed: ${totalPatternsRemoved}`);
    console.log(`💾 Total bytes removed: ${(totalBytesRemoved / 1024).toFixed(1)} KB`);
    
    if (total > 0) {
      const successRate = ((successful.length + noChangesNeeded.length) / total * 100).toFixed(1);
      console.log(`📈 Success rate: ${successRate}%`);
    }
    
    if (shopifyConfig.dryRun) {
      console.log('\n🔍 This was a DRY RUN - no actual updates performed');
      console.log('💡 Set DRY_RUN=false in .env to perform actual updates');
    }
    
    console.log('\n💡 Next steps:');
    console.log('   1. Review update results in reports/06_description_update_results.json');
    console.log('   2. Check updated products in Shopify admin');
    console.log('   3. Continue with image attachment if needed');
  }
}

async function main() {
  console.log('='.repeat(70));
  console.log('📝 PRODUCT DESCRIPTION UPDATE (EC-UP REMOVAL)');
  console.log('='.repeat(70));

  const updater = new ProductDescriptionUpdater();

  try {
    await updater.initialize();
    
    const productDataArray = updater.loadProductDataFromJSON();
    
    if (productDataArray.length === 0) {
      console.log('⚠️ No product data found to process');
      return;
    }

    // Confirmation for live updates
    if (!shopifyConfig.dryRun) {
      console.log(`\n⚠️  LIVE UPDATE MODE - This will update descriptions for ${productDataArray.length} products!`);
      console.log('Press Ctrl+C to cancel or wait 5 seconds to continue...');
      await new Promise(resolve => setTimeout(resolve, 5000));
    }

    await updater.processProducts(productDataArray);
    updater.saveUpdateResults();
    updater.printSummary();

    console.log('\n🎉 Product description updates completed!');

  } catch (error) {
    console.error(`\n❌ Product description update failed: ${error.message}`);
    console.error(error.stack);
    process.exit(1);
  }
}

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.log('\n⏹️ Product description update interrupted by user');
  process.exit(0);
});

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}