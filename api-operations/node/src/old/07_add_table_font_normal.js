#!/usr/bin/env node
/**
 * Add font-weight: normal styles to table elements in Shopify products
 * 
 * This script:
 * 1. Reads table_font_weight_to_normalize.json from shared directory
 * 2. Updates products with the pre-processed HTML from Python analysis
 * 
 * Usage:
 *   node 07_add_table_font_normal.js                     # Live updates
 *   node 07_add_table_font_normal.js --dry-run           # Preview changes
 *   node 07_add_table_font_normal.js --test-handle <handle> # Test single product
 */

import { readFileSync } from 'fs';
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
      }
      userErrors {
        field
        message
      }
    }
  }
`;

async function main() {
  // Check command line arguments
  const args = process.argv.slice(2);
  
  // Check for test handle argument
  let testHandle = null;
  const testHandleIndex = args.findIndex(arg => arg === '--test-handle');
  if (testHandleIndex !== -1 && args[testHandleIndex + 1]) {
    testHandle = args[testHandleIndex + 1];
  }
  
  console.log('='.repeat(70));
  console.log('🎨 TABLE FONT-WEIGHT: NORMAL ADDITION');
  console.log('='.repeat(70));

  // Validate config and connect to Shopify
  if (!validateConfig()) {
    throw new Error('Configuration validation failed');
  }

  const client = new ShopifyGraphQLClient(false); // Use test store
  await client.testConnection();
  console.log('✅ Connected to Shopify test store\n');

  // Load JSON data
  const jsonPath = '/Users/gen/corekara/rakuten-shopify/api-operations/python/shared/mobile_fix_v3_complete.json';
  const jsonData = JSON.parse(readFileSync(jsonPath, 'utf-8'));
  
  if (!jsonData.data || !Array.isArray(jsonData.data)) {
    throw new Error('Invalid JSON structure: expected data array');
  }

  let products = jsonData.data;
  console.log(`📂 Loaded ${products.length} products from JSON\n`);

  // Filter by test handle if provided
  if (testHandle) {
    products = products.filter(product => product.productHandle === testHandle);
    console.log(`🔍 Filtered to ${products.length} product(s) with handle: ${testHandle}\n`);
  }

  if (products.length === 0) {
    console.log('⚠️ No products to process');
    return;
  }

  // Process products
  let successful = 0;
  let failed = 0;
  let notFound = 0;

  for (let i = 0; i < products.length; i++) {
    const productData = products[i];
    const { productHandle, modifiedHtml } = productData;
    
    try {
      // Find product in Shopify
      const result = await client.query(GET_PRODUCT_QUERY, { handle: productHandle });
      const product = result.productByHandle;
      
      if (!product) {
        console.log(`❌ Not found: ${productHandle}`);
        notFound++;
        continue;
      }

      // Skip if no modified HTML
      if (!modifiedHtml) {
        console.log(`⚠️  No modified HTML for: ${productHandle}`);
        continue;
      }

      // Update product with modified HTML (dry run check)
      if (shopifyConfig.dryRun) {
        console.log(`🔍 [DRY RUN] Would update: ${productHandle}`);
        successful++;
      } else {
        const updateResult = await client.mutate(UPDATE_PRODUCT_MUTATION, {
          product: {
            id: product.id,
            descriptionHtml: modifiedHtml
          }
        });

        if (updateResult.productUpdate.userErrors.length > 0) {
          const errors = updateResult.productUpdate.userErrors.map(e => `${e.field}: ${e.message}`);
          throw new Error(`Shopify errors: ${errors.join(', ')}`);
        }

        console.log(`✅ Updated: ${productHandle}`);
        successful++;
      }

    } catch (error) {
      console.log(`❌ Failed: ${productHandle} - ${error.message}`);
      failed++;
    }

    // Progress indicator
    if ((i + 1) % 100 === 0 || i === products.length - 1) {
      console.log(`Progress: ${i + 1}/${products.length}`);
    }

    // Rate limiting - wait 200ms between requests (5 requests per second)
    // Skip delay on dry run or last item
    if (!shopifyConfig.dryRun && i < products.length - 1) {
      await new Promise(resolve => setTimeout(resolve, 200));
    }
  }

  // Summary
  console.log('\n' + '='.repeat(70));
  console.log('📊 SUMMARY');
  console.log('='.repeat(70));
  console.log(`Total products: ${products.length}`);
  console.log(`✅ Successfully updated: ${successful}`);
  console.log(`❌ Failed: ${failed}`);
  console.log(`🔍 Not found: ${notFound}`);
  
  if (shopifyConfig.dryRun) {
    console.log('\n🔍 This was a DRY RUN - no actual updates performed');
  }
  
  console.log('\n🎉 Done!');
}

// Run the script
main().catch(error => {
  console.error(`\n❌ Script failed: ${error.message}`);
  process.exit(1);
});