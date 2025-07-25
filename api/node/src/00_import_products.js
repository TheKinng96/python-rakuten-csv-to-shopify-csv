#!/usr/bin/env node
/**
 * Import products from JSON to Shopify via GraphQL
 * 
 * This script:
 * 1. Reads products_for_import.json from shared directory
 * 2. Imports products to Shopify test store using GraphQL mutations
 * 3. Provides progress logging and error handling
 * 4. Saves import results for audit
 */

import { readFileSync, writeFileSync, existsSync } from 'fs';
import { join } from 'path';
import { shopifyConfig, pathConfig, getStoreConfig, validateConfig } from './config.js';
import { ShopifyGraphQLClient } from './shopify-client.js';

const PRODUCT_CREATE_MUTATION = `
  mutation productCreate($input: ProductInput!) {
    productCreate(input: $input) {
      product {
        id
        handle
        title
        status
        variants(first: 10) {
          edges {
            node {
              id
              sku
              price
            }
          }
        }
        images(first: 20) {
          edges {
            node {
              id
              src
              altText
            }
          }
        }
      }
      userErrors {
        field
        message
      }
    }
  }
`;

class ProductImporter {
  constructor() {
    this.client = null;
    this.importResults = {
      successful: [],
      failed: [],
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

  loadProductsFromJSON() {
    const jsonPath = join(pathConfig.sharedPath, 'products_for_import.json');
    
    if (!existsSync(jsonPath)) {
      throw new Error(`Products JSON file not found: ${jsonPath}`);
    }

    console.log(`📂 Loading products from ${jsonPath}...`);
    
    const jsonData = JSON.parse(readFileSync(jsonPath, 'utf-8'));
    
    if (!jsonData.data || !Array.isArray(jsonData.data)) {
      throw new Error('Invalid JSON structure: expected data array');
    }

    console.log(`✅ Loaded ${jsonData.data.length} products from JSON`);
    return jsonData.data;
  }

  convertToShopifyInput(productData) {
    const input = {
      handle: productData.handle,
      title: productData.title,
      bodyHtml: productData.bodyHtml,
      vendor: productData.vendor,
      productType: productData.productType,
      tags: productData.tags ? productData.tags.split(',').map(tag => tag.trim()) : [],
      status: 'DRAFT', // Import as draft first
      variants: productData.variants.map(variant => ({
        sku: variant.sku,
        price: variant.price?.toString(),
        compareAtPrice: variant.compare_at_price?.toString(),
        inventoryQuantity: variant.inventory_quantity || 0,
        requiresShipping: variant.requires_shipping !== false,
        taxable: variant.taxable !== false,
        inventoryManagement: 'SHOPIFY',
        inventoryPolicy: 'DENY',
        ...(variant.option1 && { option1: variant.option1 })
      })),
      images: productData.images?.map(image => ({
        src: image.src,
        altText: image.alt || productData.title
      })) || [],
      options: productData.options?.map(option => ({
        name: option.name,
        values: [option.name] // Will be populated by variants
      })) || []
    };

    return input;
  }

  async importProduct(productData, index, total) {
    try {
      const input = this.convertToShopifyInput(productData);
      
      if (shopifyConfig.dryRun) {
        console.log(`   🔍 [DRY RUN] Would import: ${productData.handle}`);
        this.importResults.successful.push({
          handle: productData.handle,
          title: productData.title,
          status: 'dry_run',
          shopifyId: 'dry_run_id'
        });
        return;
      }

      const result = await this.client.mutate(PRODUCT_CREATE_MUTATION, { input });
      
      if (result.productCreate.userErrors.length > 0) {
        const errors = result.productCreate.userErrors.map(e => `${e.field}: ${e.message}`);
        throw new Error(`Shopify errors: ${errors.join(', ')}`);
      }

      const product = result.productCreate.product;
      this.importResults.successful.push({
        handle: productData.handle,
        title: productData.title,
        status: 'imported',
        shopifyId: product.id,
        variantCount: product.variants.edges.length,
        imageCount: product.images.edges.length
      });

      console.log(`   ✅ [${index}/${total}] Imported: ${productData.handle} (ID: ${product.id})`);

    } catch (error) {
      console.error(`   ❌ [${index}/${total}] Failed: ${productData.handle} - ${error.message}`);
      
      this.importResults.failed.push({
        handle: productData.handle,
        title: productData.title,
        status: 'failed',
        error: error.message
      });
    }
  }

  async processProducts(products) {
    console.log(`\n🚀 Starting product import (${products.length} products)...`);
    
    const batchSize = shopifyConfig.batchSize;
    const total = products.length;
    let processed = 0;

    // Process in batches to respect rate limits
    for (let i = 0; i < products.length; i += batchSize) {
      const batch = products.slice(i, i + batchSize);
      
      console.log(`\n📦 Processing batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(products.length/batchSize)}`);
      
      // Process batch with concurrency limit
      const promises = batch.map((product, batchIndex) => 
        this.importProduct(product, i + batchIndex + 1, total)
      );
      
      await Promise.all(promises);
      processed += batch.length;
      
      // Progress update
      const successRate = (this.importResults.successful.length / processed * 100).toFixed(1);
      console.log(`📈 Progress: ${processed}/${total} processed (${successRate}% success rate)`);
      
      // Rate limiting delay between batches
      if (i + batchSize < products.length) {
        const delay = Math.ceil(1000 / shopifyConfig.maxRequestsPerSecond * batchSize);
        console.log(`⏱️  Waiting ${delay}ms for rate limiting...`);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
  }

  saveImportResults() {
    const resultsPath = join(pathConfig.reportsPath, '00_import_results.json');
    
    const report = {
      timestamp: new Date().toISOString(),
      summary: {
        total: this.importResults.successful.length + this.importResults.failed.length + this.importResults.skipped.length,
        successful: this.importResults.successful.length,
        failed: this.importResults.failed.length,
        skipped: this.importResults.skipped.length,
        successRate: (this.importResults.successful.length / (this.importResults.successful.length + this.importResults.failed.length) * 100).toFixed(1) + '%'
      },
      results: this.importResults,
      config: {
        dryRun: shopifyConfig.dryRun,
        batchSize: shopifyConfig.batchSize,
        store: 'test'
      }
    };

    writeFileSync(resultsPath, JSON.stringify(report, null, 2));
    console.log(`📄 Import results saved to: ${resultsPath}`);
  }

  printSummary() {
    const { successful, failed, skipped } = this.importResults;
    const total = successful.length + failed.length + skipped.length;
    
    console.log('\n' + '='.repeat(70));
    console.log('📊 IMPORT SUMMARY');
    console.log('='.repeat(70));
    console.log(`Total products: ${total}`);
    console.log(`✅ Successful: ${successful.length}`);
    console.log(`❌ Failed: ${failed.length}`);
    console.log(`⏭️  Skipped: ${skipped.length}`);
    
    if (total > 0) {
      const successRate = (successful.length / total * 100).toFixed(1);
      console.log(`📈 Success rate: ${successRate}%`);
    }
    
    if (shopifyConfig.dryRun) {
      console.log('\n🔍 This was a DRY RUN - no actual imports performed');
      console.log('💡 Set DRY_RUN=false in .env to perform actual import');
    }
    
    console.log('\n💡 Next steps:');
    console.log('   1. Review import results in reports/00_import_results.json');
    console.log('   2. Check imported products in Shopify admin');
    console.log('   3. Run other processing scripts if needed');
  }
}

async function main() {
  console.log('='.repeat(70));
  console.log('🏪 SHOPIFY PRODUCT IMPORT');
  console.log('='.repeat(70));

  const importer = new ProductImporter();

  try {
    await importer.initialize();
    
    const products = importer.loadProductsFromJSON();
    
    if (products.length === 0) {
      console.log('⚠️ No products to import');
      return;
    }

    // Confirmation for live import
    if (!shopifyConfig.dryRun) {
      console.log(`\n⚠️  LIVE IMPORT MODE - This will import ${products.length} products to your test store!`);
      console.log('Press Ctrl+C to cancel or wait 5 seconds to continue...');
      await new Promise(resolve => setTimeout(resolve, 5000));
    }

    await importer.processProducts(products);
    importer.saveImportResults();
    importer.printSummary();

    console.log('\n🎉 Product import completed!');

  } catch (error) {
    console.error(`\n❌ Import failed: ${error.message}`);
    console.error(error.stack);
    process.exit(1);
  }
}

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.log('\n⏹️ Import interrupted by user');
  process.exit(0);
});

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}