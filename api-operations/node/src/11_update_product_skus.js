#!/usr/bin/env node
/**
 * Update Shopify product variant SKUs from 管理番号 to 商品番号
 * 
 * This script:
 * 1. Reads rakuten-tax.csv to map current SKUs (管理番号) to new SKUs (商品番号)
 * 2. Groups variants by product handle for efficient bulk updates
 * 3. Uses productVariantsBulkUpdate mutation to update SKUs via inventoryItem.sku
 * 
 * Usage:
 *   node 11_update_product_skus.js                      # Live updates
 *   node 11_update_product_skus.js --dry-run            # Preview changes
 *   node 11_update_product_skus.js --test-handle <handle> # Test single product
 *   node 11_update_product_skus.js --resume-from 1000   # Resume from specific index
 *   node 11_update_product_skus.js --production         # Use production store
 */

import { readFileSync, writeFileSync, existsSync } from 'fs';
import { join } from 'path';
import { shopifyConfig, pathConfig, validateConfig } from './config.js';
import { ShopifyGraphQLClient } from './shopify-client.js';

const GET_PRODUCT_BY_HANDLE_QUERY = `
  query getProductByHandle($handle: String!) {
    productByHandle(handle: $handle) {
      id
      handle
      title
      variants(first: 100) {
        edges {
          node {
            id
            sku
            title
          }
        }
      }
    }
  }
`;

const UPDATE_VARIANT_SKUS_MUTATION = `
  mutation updateVariantSKUs($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
    productVariantsBulkUpdate(productId: $productId, variants: $variants) {
      productVariants {
        id
        sku
      }
      userErrors {
        field
        message
      }
    }
  }
`;

class SKUUpdater {
  constructor() {
    this.client = null;
    this.updateMapping = null; // Product handle -> variant update data
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

  loadSKUMappingFromJSON() {
    const jsonPath = join(pathConfig.sharedPath, 'sku_update_mapping.json');
    
    if (!existsSync(jsonPath)) {
      throw new Error(`Mapping file not found: ${jsonPath}. Please run prepare_sku_updates.py first.`);
    }

    console.log(`📂 Loading SKU update mapping from ${jsonPath}...`);
    
    const jsonData = JSON.parse(readFileSync(jsonPath, 'utf-8'));
    
    if (!jsonData.products) {
      throw new Error('Invalid mapping file: missing products data');
    }

    this.updateMapping = jsonData.products;
    
    console.log(`✅ Loaded mapping for ${Object.keys(this.updateMapping).length} products`);
    console.log(`✅ Total variants needing update: ${jsonData.metadata.variantsNeedingUpdate}`);
    console.log(`📊 Processing ${jsonData.metadata.productsNeedingUpdate}/${jsonData.metadata.totalProducts} products (${(jsonData.metadata.productsNeedingUpdate/jsonData.metadata.totalProducts*100).toFixed(1)}%)`);
  }


  async findProductByHandle(handle) {
    try {
      const result = await this.client.query(GET_PRODUCT_BY_HANDLE_QUERY, { handle });
      return result.productByHandle;
    } catch (error) {
      throw new Error(`Failed to find product ${handle}: ${error.message}`);
    }
  }

  async updateProductVariantSKUs(productHandle, productMapping, index, total) {
    try {
      console.log(`\n🔍 [${index}/${total}] Processing product: ${productHandle}`);
      console.log(`   📊 Will update ${productMapping.variantsToUpdate}/${productMapping.totalVariants} variants`);

      // Find product in Shopify
      const product = await this.findProductByHandle(productHandle);
      
      if (!product) {
        console.log(`   ⚠️  Product not found: ${productHandle}`);
        this.updateResults.notFound.push({
          handle: productHandle,
          reason: 'Product not found in Shopify'
        });
        return;
      }

      const variants = product.variants.edges.map(edge => edge.node);
      console.log(`   📦 Found ${variants.length} variants in Shopify`);

      // Build variant updates with new SKUs based on mapping
      const variantUpdates = [];
      const skuMap = new Map();
      
      // Create lookup map from mapping
      for (const variantMapping of productMapping.variants) {
        skuMap.set(variantMapping.currentSKU, variantMapping.newSKU);
      }

      for (const variant of variants) {
        const currentSKU = variant.sku;
        const newSKU = skuMap.get(currentSKU);

        if (newSKU) {
          console.log(`   📝 ${currentSKU} → ${newSKU}`);
          
          variantUpdates.push({
            id: variant.id,
            inventoryItem: {
              sku: newSKU
            }
          });
        } else {
          console.log(`   ℹ️  SKU unchanged: ${currentSKU}`);
        }
      }

      if (variantUpdates.length === 0) {
        console.log(`   ⚠️  No matching variants found for updates`);
        this.updateResults.noChangesNeeded.push({
          handle: productHandle,
          title: product.title,
          reason: 'No matching variants found in Shopify'
        });
        return;
      }

      if (shopifyConfig.dryRun) {
        console.log(`   🔍 [DRY RUN] Would update ${variantUpdates.length} variant SKUs`);
        this.updateResults.successful.push({
          handle: productHandle,
          title: product.title,
          status: 'dry_run',
          variantsUpdated: variantUpdates.length,
          shopifyId: product.id
        });
        return;
      }

      // Update variant SKUs using bulk update
      const result = await this.client.mutate(UPDATE_VARIANT_SKUS_MUTATION, {
        productId: product.id,
        variants: variantUpdates
      });

      if (result.productVariantsBulkUpdate.userErrors.length > 0) {
        const errors = result.productVariantsBulkUpdate.userErrors.map(e => `${e.field}: ${e.message}`);
        throw new Error(`Shopify errors: ${errors.join(', ')}`);
      }

      const updatedVariants = result.productVariantsBulkUpdate.productVariants;
      console.log(`   ✅ Successfully updated ${updatedVariants.length} variant SKUs`);
      
      this.updateResults.successful.push({
        handle: productHandle,
        title: product.title,
        status: 'skus_updated',
        variantsUpdated: updatedVariants.length,
        shopifyId: product.id,
        timestamp: new Date().toISOString()
      });

    } catch (error) {
      console.error(`   ❌ Failed: ${productHandle} - ${error.message}`);
      
      this.updateResults.failed.push({
        handle: productHandle,
        status: 'failed',
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
  }

  async processProducts(productsToProcess, resumeFrom = 0) {
    console.log(`\n🚀 Starting SKU updates (${productsToProcess.length} products)...`);
    
    const batchSize = shopifyConfig.batchSize;
    const total = productsToProcess.length;
    let processed = 0;

    // Process in batches to respect rate limits
    for (let i = 0; i < productsToProcess.length; i += batchSize) {
      const batch = productsToProcess.slice(i, i + batchSize);
      
      console.log(`\n📦 Processing batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(productsToProcess.length/batchSize)}`);
      
      // Process batch with concurrency limit
      const promises = batch.map(({ handle, mapping }, batchIndex) => 
        this.updateProductVariantSKUs(handle, mapping, resumeFrom + i + batchIndex + 1, resumeFrom + total)
      );
      
      await Promise.all(promises);
      processed += batch.length;
      
      // Progress update
      const successRate = (this.updateResults.successful.length / processed * 100).toFixed(1);
      console.log(`📈 Progress: ${processed}/${total} processed (${successRate}% success rate)`);
      
      // Rate limiting delay between batches
      if (i + batchSize < productsToProcess.length) {
        const delay = Math.ceil(1000 / shopifyConfig.maxRequestsPerSecond * batchSize);
        console.log(`⏱️  Waiting ${delay}ms for rate limiting...`);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
  }

  saveUpdateResults() {
    const resultsPath = join(pathConfig.reportsPath, '11_sku_update_results.json');
    
    const report = {
      timestamp: new Date().toISOString(),
      summary: {
        total: this.updateResults.successful.length + this.updateResults.failed.length + this.updateResults.notFound.length + this.updateResults.noChangesNeeded.length,
        successful: this.updateResults.successful.length,
        failed: this.updateResults.failed.length,
        notFound: this.updateResults.notFound.length,
        noChangesNeeded: this.updateResults.noChangesNeeded.length,
        totalProductsInMapping: Object.keys(this.updateMapping).length,
        totalVariantsInMapping: Object.values(this.updateMapping).reduce((sum, product) => sum + product.variantsToUpdate, 0)
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
    
    console.log('\n' + '='.repeat(70));
    console.log('📊 SKU UPDATE SUMMARY');
    console.log('='.repeat(70));
    console.log(`Total products: ${total}`);
    console.log(`✅ Successfully updated: ${successful.length}`);
    console.log(`❌ Failed: ${failed.length}`);
    console.log(`🔍 Not found: ${notFound.length}`);
    console.log(`ℹ️  No changes needed: ${noChangesNeeded.length}`);
    console.log(`📊 Products in mapping: ${Object.keys(this.updateMapping).length}`);
    
    if (total > 0) {
      const successRate = ((successful.length + noChangesNeeded.length) / total * 100).toFixed(1);
      console.log(`📈 Success rate: ${successRate}%`);
    }
    
    if (shopifyConfig.dryRun) {
      console.log('\n🔍 This was a DRY RUN - no actual updates performed');
      console.log('💡 Set DRY_RUN=false in .env to perform actual updates');
    }
    
    console.log('\n💡 Next steps:');
    console.log('   1. Review update results in reports/11_sku_update_results.json');
    console.log('   2. Check updated products in Shopify admin');
    console.log('   3. Verify SKU changes are applied correctly');
  }
}

async function main() {
  // Check command line arguments
  const args = process.argv.slice(2);
  const isDryRun = args.includes('--dry-run');
  const useProductionStore = args.includes('--production');
  
  // Override shopifyConfig.dryRun if dry-run flag is provided
  if (isDryRun) {
    shopifyConfig.dryRun = true;
  }
  
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
  
  console.log('='.repeat(70));
  console.log('📝 SHOPIFY SKU UPDATER (管理番号 → 商品番号)');
  console.log('='.repeat(70));

  const updater = new SKUUpdater();

  try {
    await updater.initialize();
    updater.loadSKUMappingFromJSON();
    
    // Convert mapping to processable format
    let productsToProcess = Object.entries(updater.updateMapping).map(([handle, mapping]) => ({
      handle,
      mapping
    }));
    
    // Filter by test handle if provided
    if (testHandle) {
      console.log(`\n🔍 Filtering for test handle: ${testHandle}`);
      productsToProcess = productsToProcess.filter(product => product.handle === testHandle);
      
      if (productsToProcess.length === 0) {
        console.log(`⚠️ No products found with handle: ${testHandle}`);
        return;
      }
      console.log(`✅ Found ${productsToProcess.length} product(s) with handle: ${testHandle}`);
    }
    
    if (productsToProcess.length === 0) {
      console.log('⚠️ No products found to process');
      return;
    }

    // Apply resume-from if specified
    if (resumeFrom > 0) {
      console.log(`\n📍 Resuming from product ${resumeFrom} (skipping first ${resumeFrom} products)`);
      productsToProcess = productsToProcess.slice(resumeFrom);
      
      if (productsToProcess.length === 0) {
        console.log(`⚠️ No products to process after resuming from index ${resumeFrom}`);
        return;
      }
      console.log(`📦 Will process ${productsToProcess.length} remaining products`);
    }

    // Confirmation for live updates
    if (!shopifyConfig.dryRun) {
      console.log(`\n⚠️  LIVE UPDATE MODE - This will update SKUs for ${productsToProcess.length} products!`);
      console.log('Press Ctrl+C to cancel or wait 5 seconds to continue...');
      await new Promise(resolve => setTimeout(resolve, 5000));
    }

    await updater.processProducts(productsToProcess, resumeFrom);
    updater.saveUpdateResults();
    updater.printSummary();

    console.log('\n🎉 SKU updates completed!');

  } catch (error) {
    console.error(`\n❌ SKU update failed: ${error.message}`);
    console.error(error.stack);
    process.exit(1);
  }
}

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.log('\n⏹️ SKU update interrupted by user');
  process.exit(0);
});

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}