#!/usr/bin/env node
/**
 * Update Shopify product descriptions
 * 
 * This script can handle two types of updates:
 * 1. EC-UP content removal (default) - reads from rakuten_content_to_clean.json
 * 2. HTML table fixes - reads from html_table_fixes_to_update.json (use --html-table-fix flag)
 * 
 * Usage:
 *   node 06_update_products_description.js                  # EC-UP removal
 *   node 06_update_products_description.js --html-table-fix # HTML table fixes
 *   node 06_update_products_description.js --test-handle <handle> # Test with single product
 *   node 06_update_products_description.js --html-table-fix --test-handle <handle> # Test HTML fix with single product
 *   node 06_update_products_description.js --html-table-fix --resume-from 6250 # Resume from product 6250
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
         
  loadProductDataFromJSON(jsonFile = 'html_table_fixes_to_update.json') {
    const jsonPath = join(pathConfig.sharedPath, jsonFile);
    
    if (!existsSync(jsonPath)) {
      throw new Error(`JSON file not found: ${jsonPath}`);
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
        const dryRunMessage = patternsRemoved && patternsRemoved.length > 0 
          ? `Would remove ${patternsRemoved.length} patterns (${bytesRemoved} bytes)`
          : `Would update HTML (${bytesRemoved} bytes changed)`;
        console.log(`   🔍 [DRY RUN] ${dryRunMessage} from: ${productHandle}`);
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

      const updateMessage = patternsRemoved && patternsRemoved.length > 0 
        ? `Removed ${patternsRemoved.length} patterns (${bytesRemoved} bytes)`
        : `Updated HTML (${bytesRemoved} bytes changed)`;
      console.log(`   ✅ [${index}/${total}] ${updateMessage} from: ${productHandle}`);
      
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

  async processProducts(productDataArray, resumeFrom = 0) {
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
        this.updateProductDescription(productData, resumeFrom + i + batchIndex + 1, resumeFrom + total)
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
          sum + (result.patternsRemoved || 1), 0
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
    console.log(`ℹ️  No changes needed: ${noChangesNeeded.length}`);
    console.log(`🏷️  Total patterns/changes: ${totalPatternsRemoved}`);
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
  // Check command line arguments
  const args = process.argv.slice(2);
  const isHtmlTableFix = args.includes('--html-table-fix');
  
  // Check for test handle argument
  let testHandle = null;
  const testHandleIndex = args.findIndex(arg => arg === '--test-handle');
  if (testHandleIndex !== -1 && args[testHandleIndex + 1]) {
    testHandle = args[testHandleIndex + 1];
  }
  
  // Check for resume-from argument
  let resumeFrom = 0;
  const resumeFromIndex = args.findIndex(arg => arg === '--resume-from');
  if (resumeFromIndex !== -1 && args[resumeFromIndex + 1]) {
    resumeFrom = parseInt(args[resumeFromIndex + 1], 10);
    if (isNaN(resumeFrom) || resumeFrom < 0) {
      console.error('❌ Invalid --resume-from value. Must be a positive number.');
      process.exit(1);
    }
  }
  
  const jsonFile = isHtmlTableFix ? 'html_table_fixes_to_update.json' : 'rakuten_content_to_clean.json';
  const updateType = isHtmlTableFix ? 'HTML TABLE FIX' : 'EC-UP REMOVAL';
  
  console.log('='.repeat(70));
  console.log(`📝 PRODUCT DESCRIPTION UPDATE (${updateType})`);
  console.log('='.repeat(70));

  const updater = new ProductDescriptionUpdater();

  try {
    await updater.initialize();
    
    let productDataArray = updater.loadProductDataFromJSON(jsonFile);
    
    // Filter by test handle if provided
    if (testHandle) {
      console.log(`\n🔍 Filtering for test handle: ${testHandle}`);
      productDataArray = productDataArray.filter(product => product.productHandle === testHandle);
      
      if (productDataArray.length === 0) {
        console.log(`⚠️ No products found with handle: ${testHandle}`);
        return;
      }
      console.log(`✅ Found ${productDataArray.length} product(s) with handle: ${testHandle}`);
    }
    
    if (productDataArray.length === 0) {
      console.log('⚠️ No product data found to process');
      return;
    }

    // Apply resume-from if specified
    if (resumeFrom > 0) {
      console.log(`\n📍 Resuming from product ${resumeFrom} (skipping first ${resumeFrom} products)`);
      productDataArray = productDataArray.slice(resumeFrom);
      
      if (productDataArray.length === 0) {
        console.log(`⚠️ No products to process after resuming from index ${resumeFrom}`);
        return;
      }
      console.log(`📦 Will process ${productDataArray.length} remaining products`);
    }

    // Confirmation for live updates
    if (!shopifyConfig.dryRun) {
      console.log(`\n⚠️  LIVE UPDATE MODE - This will update descriptions for ${productDataArray.length} products!`);
      console.log('Press Ctrl+C to cancel or wait 5 seconds to continue...');
      await new Promise(resolve => setTimeout(resolve, 5000));
    }

    await updater.processProducts(productDataArray, resumeFrom);
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