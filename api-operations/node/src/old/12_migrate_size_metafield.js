#!/usr/bin/env node
/**
 * Migrate variant custom.size metafield to product custom.search_size metafield
 * 
 * Usage:
 *   node 12_migrate_size_metafield.js                  # Process all products
 *   node 12_migrate_size_metafield.js --test-handle <handle> # Test specific product
 *   node 12_migrate_size_metafield.js --batch-size 50  # Custom batch size
 */

import { writeFileSync } from 'fs';
import { join } from 'path';
import { pathConfig, shopifyConfig, validateConfig } from './config.js';
import { ShopifyGraphQLClient } from './shopify-client.js';

// GraphQL query to fetch products with variants and their metafields
const GET_PRODUCTS_WITH_VARIANTS = `
  query getProductsWithVariants($first: Int!, $after: String) {
    products(first: $first, after: $after) {
      edges {
        node {
          id
          handle
          title
          metafields(namespace: "custom", first: 20) {
            edges {
              node {
                key
                value
                namespace
              }
            }
          }
          variants(first: 20) {
            edges {
              node {
                id
                displayName
                metafields(namespace: "custom", first: 20) {
                  edges {
                    node {
                      key
                      value
                      namespace
                    }
                  }
                }
              }
            }
          }
        }
      }
      pageInfo {
        hasNextPage
        endCursor
      }
    }
  }
`;

const GET_PRODUCT_BY_HANDLE = `
  query getProductByHandle($handle: String!) {
    productByHandle(handle: $handle) {
      id
      handle
      title
      metafields(namespace: "custom", first: 20) {
        edges {
          node {
            key
            value
            namespace
          }
        }
      }
      variants(first: 20) {
        edges {
          node {
            id
            displayName
            metafields(namespace: "custom", first: 20) {
              edges {
                node {
                  key
                  value
                  namespace
                }
              }
            }
          }
        }
      }
    }
  }
`;

// Mutation to update product metafield
const UPDATE_PRODUCT_METAFIELD = `
  mutation productUpdate($product: ProductUpdateInput!) {
    productUpdate(product: $product) {
      product {
        id
        handle
        metafields(namespace: "custom", first: 20) {
          edges {
            node {
              key
              value
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

class SizeMetafieldMigrator {
  constructor() {
    this.client = null;
    this.migrationResults = {
      successful: [],
      failed: [],
      skipped: [],
      alreadyHasSearchSize: [],
      noVariantSize: []
    };
    
    this.stats = {
      totalProcessed: 0,
      productsWithVariantSize: 0,
      productsWithSearchSize: 0,
      productsWithBoth: 0,
      migrationAttempts: 0,
      successfulMigrations: 0
    };
  }

  async initialize() {
    console.log('🔧 Initializing Shopify GraphQL client...');
    
    if (!validateConfig()) {
      throw new Error('Configuration validation failed');
    }

    this.client = new ShopifyGraphQLClient(true);
    await this.client.testConnection();
    console.log('✅ Connected to Shopify test store');
  }

  async processSpecificProduct(handle) {
    console.log(`\n🔍 Processing specific product: ${handle}`);
    
    try {
      const result = await this.client.query(GET_PRODUCT_BY_HANDLE, { handle });
      const product = result.productByHandle;
      
      if (!product) {
        console.log(`❌ Product not found: ${handle}`);
        return;
      }
      
      await this.processProduct(product, 1, 1);
      
    } catch (error) {
      console.error(`❌ Failed to process product ${handle}: ${error.message}`);
    }
  }

  async processAllProducts(batchSize = 10) {
    console.log(`\n🚀 Processing all products (batch size: ${batchSize})...`);
    
    let cursor = null;
    let totalProcessed = 0;
    let batchNumber = 0;
    
    while (true) {
      batchNumber++;
      console.log(`\n📦 Processing batch ${batchNumber}...`);
      
      try {
        const result = await this.client.query(GET_PRODUCTS_WITH_VARIANTS, {
          first: batchSize,
          after: cursor
        });
        
        const products = result.products.edges.map(edge => edge.node);
        
        if (products.length === 0) {
          console.log(`✅ No more products to process`);
          break;
        }
        
        // Process products in batch
        const promises = products.map((product, index) => 
          this.processProduct(product, totalProcessed + index + 1, '?')
        );
        
        await Promise.all(promises);
        totalProcessed += products.length;
        
        console.log(`   📊 Batch ${batchNumber} completed: ${products.length} products processed`);
        console.log(`   📈 Total processed: ${totalProcessed}`);
        
        // Check if there are more pages
        if (!result.products.pageInfo.hasNextPage) {
          console.log(`✅ All products processed`);
          break;
        }
        
        cursor = result.products.pageInfo.endCursor;
        
        // Rate limiting between batches
        const delay = Math.ceil(1000 / shopifyConfig.maxRequestsPerSecond * batchSize);
        console.log(`   ⏱️  Rate limiting delay: ${delay}ms`);
        await new Promise(resolve => setTimeout(resolve, delay));
        
      } catch (error) {
        console.error(`❌ Failed to process batch ${batchNumber}: ${error.message}`);
        break;
      }
    }
    
    this.stats.totalProcessed = totalProcessed;
  }

  async processProduct(product, index, total) {
    try {
      const productHandle = product.handle;
      const productTitle = product.title;
      
      // Get product metafields
      const productMetafields = (product.metafields?.edges || []).map(edge => edge.node);
      const hasSearchSize = productMetafields.find(meta => meta.key === 'search_size');
      
      // Get variant metafields
      const variants = product.variants?.edges || [];
      let firstVariantSizeValue = null;
      let variantSizeCounts = 0;
      
      // Check all variants for size metafield
      for (const variantEdge of variants) {
        const variant = variantEdge.node;
        const variantMetafields = (variant.metafields?.edges || []).map(edge => edge.node);
        const sizeMetafield = variantMetafields.find(meta => meta.key === 'size');
        
        if (sizeMetafield && sizeMetafield.value && sizeMetafield.value.trim()) {
          if (!firstVariantSizeValue) {
            firstVariantSizeValue = sizeMetafield.value.trim();
          }
          variantSizeCounts++;
        }
      }
      
      // Update statistics
      if (firstVariantSizeValue) {
        this.stats.productsWithVariantSize++;
      }
      if (hasSearchSize && hasSearchSize.value && hasSearchSize.value.trim()) {
        this.stats.productsWithSearchSize++;
      }
      if (firstVariantSizeValue && hasSearchSize && hasSearchSize.value) {
        this.stats.productsWithBoth++;
      }
      
      console.log(`\n📦 [${index}/${total}] ${productTitle} (${productHandle})`);
      console.log(`   Product metafields: ${productMetafields.length}`);
      console.log(`   Variants with size: ${variantSizeCounts}`);
      
      // Decision logic
      if (hasSearchSize && hasSearchSize.value && hasSearchSize.value.trim()) {
        // Product already has search_size
        const currentValue = hasSearchSize.value.trim();
        
        if (firstVariantSizeValue) {
          const match = currentValue === firstVariantSizeValue ? '✅' : '❌';
          console.log(`   🔄 Already has search_size: "${currentValue}"`);
          console.log(`   🔍 First variant size: "${firstVariantSizeValue}" ${match}`);
          
          this.migrationResults.alreadyHasSearchSize.push({
            handle: productHandle,
            title: productTitle,
            id: product.id,
            currentSearchSize: currentValue,
            firstVariantSize: firstVariantSizeValue,
            matches: currentValue === firstVariantSizeValue,
            variantSizeCount: variantSizeCounts
          });
        } else {
          console.log(`   ✅ Already has search_size: "${currentValue}" (no variant size to migrate)`);
          this.migrationResults.alreadyHasSearchSize.push({
            handle: productHandle,
            title: productTitle,
            id: product.id,
            currentSearchSize: currentValue,
            firstVariantSize: null,
            matches: true,
            variantSizeCount: 0
          });
        }
        return;
      }
      
      if (!firstVariantSizeValue) {
        // No variant size to migrate
        console.log(`   ⚠️  No variant size metafield found`);
        this.migrationResults.noVariantSize.push({
          handle: productHandle,
          title: productTitle,
          id: product.id,
          reason: 'No variant size metafield found'
        });
        return;
      }
      
      // Migration needed
      console.log(`   📋 Migration candidate: "${firstVariantSizeValue}" (from ${variantSizeCounts} variants)`);
      
      if (shopifyConfig.dryRun) {
        console.log(`   🔍 [DRY RUN] Would migrate variant size to product search_size: "${firstVariantSizeValue}"`);
        this.migrationResults.successful.push({
          handle: productHandle,
          title: productTitle,
          id: product.id,
          status: 'dry_run',
          migratedValue: firstVariantSizeValue,
          variantSizeCount: variantSizeCounts
        });
        return;
      }
      
      // Perform actual migration
      await this.migrateSearchSize(product, firstVariantSizeValue, variantSizeCounts, index, total);
      
    } catch (error) {
      console.error(`   ❌ [${index}/${total}] Failed to process ${product.handle}: ${error.message}`);
      this.migrationResults.failed.push({
        handle: product.handle,
        title: product.title,
        id: product.id,
        error: error.message
      });
    }
  }

  async migrateSearchSize(product, sizeValue, variantSizeCount, index, total) {
    try {
      this.stats.migrationAttempts++;
      
      // Update product with search_size metafield
      const result = await this.client.mutate(UPDATE_PRODUCT_METAFIELD, {
        product: {
          id: product.id,
          metafields: [
            {
              namespace: 'custom',
              key: 'search_size',
              value: JSON.stringify([sizeValue]),
              type: 'list.single_line_text_field'
            }
          ]
        }
      });

      if (result.productUpdate.userErrors.length > 0) {
        const errors = result.productUpdate.userErrors.map(e => `${e.field}: ${e.message}`);
        throw new Error(`Shopify errors: ${errors.join(', ')}`);
      }

      console.log(`   ✅ [${index}/${total}] Migrated search_size: "${sizeValue}"`);
      
      this.stats.successfulMigrations++;
      this.migrationResults.successful.push({
        handle: product.handle,
        title: product.title,
        id: product.id,
        status: 'migrated',
        migratedValue: sizeValue,
        variantSizeCount: variantSizeCount
      });

    } catch (error) {
      console.error(`   ❌ [${index}/${total}] Migration failed for ${product.handle}: ${error.message}`);
      this.migrationResults.failed.push({
        handle: product.handle,
        title: product.title,
        id: product.id,
        error: error.message
      });
    }
  }

  saveMigrationResults() {
    const resultsPath = join(pathConfig.reportsPath, '12_size_metafield_migration_results.json');
    
    const report = {
      timestamp: new Date().toISOString(),
      summary: {
        totalProcessed: this.stats.totalProcessed,
        productsWithVariantSize: this.stats.productsWithVariantSize,
        productsWithSearchSize: this.stats.productsWithSearchSize,
        productsWithBoth: this.stats.productsWithBoth,
        migrationAttempts: this.stats.migrationAttempts,
        successfulMigrations: this.stats.successfulMigrations,
        failed: this.migrationResults.failed.length,
        alreadyHasSearchSize: this.migrationResults.alreadyHasSearchSize.length,
        noVariantSize: this.migrationResults.noVariantSize.length
      },
      results: this.migrationResults,
      config: {
        dryRun: shopifyConfig.dryRun,
        batchSize: shopifyConfig.batchSize,
        store: 'test',
        apiVersion: '2025-07'
      }
    };

    writeFileSync(resultsPath, JSON.stringify(report, null, 2));
    console.log(`📄 Migration results saved to: ${resultsPath}`);
  }

  printSummary() {
    console.log('\n' + '='.repeat(70));
    console.log('📊 SIZE METAFIELD MIGRATION SUMMARY');
    console.log('='.repeat(70));
    
    console.log(`Total products processed: ${this.stats.totalProcessed}`);
    console.log(`Products with variant size: ${this.stats.productsWithVariantSize}`);
    console.log(`Products with search_size: ${this.stats.productsWithSearchSize}`);
    console.log(`Products with both: ${this.stats.productsWithBoth}`);
    console.log(`\nMigration attempts: ${this.stats.migrationAttempts}`);
    console.log(`✅ Successful migrations: ${this.stats.successfulMigrations}`);
    console.log(`❌ Failed migrations: ${this.migrationResults.failed.length}`);
    console.log(`⚠️  Already had search_size: ${this.migrationResults.alreadyHasSearchSize.length}`);
    console.log(`ℹ️  No variant size to migrate: ${this.migrationResults.noVariantSize.length}`);
    
    if (this.stats.migrationAttempts > 0) {
      const successRate = (this.stats.successfulMigrations / this.stats.migrationAttempts * 100).toFixed(1);
      console.log(`📈 Migration success rate: ${successRate}%`);
    }
    
    if (shopifyConfig.dryRun) {
      console.log('\n🔍 This was a DRY RUN - no actual updates performed');
      console.log('💡 Set DRY_RUN=false in .env to perform actual migrations');
    }
    
    // Show mismatched values if any
    const mismatched = this.migrationResults.alreadyHasSearchSize.filter(item => !item.matches);
    if (mismatched.length > 0) {
      console.log(`\n⚠️ ${mismatched.length} products have mismatched search_size vs variant size:`);
      mismatched.slice(0, 5).forEach(item => {
        console.log(`   - ${item.handle}: search="${item.currentSearchSize}" vs variant="${item.firstVariantSize}"`);
      });
      if (mismatched.length > 5) {
        console.log(`   ... and ${mismatched.length - 5} more`);
      }
    }
    
    console.log('\n💡 Next steps:');
    console.log('   1. Review migration results in reports/12_size_metafield_migration_results.json');
    console.log('   2. Check migrated products in Shopify admin');
    console.log('   3. Verify search functionality uses the new search_size metafield');
  }
}

async function main() {
  const args = process.argv.slice(2);
  
  // Parse arguments
  let testHandle = null;
  let batchSize = 10;
  
  const testHandleIndex = args.findIndex(arg => arg === '--test-handle');
  if (testHandleIndex !== -1 && args[testHandleIndex + 1]) {
    testHandle = args[testHandleIndex + 1];
  }
  
  const batchSizeIndex = args.findIndex(arg => arg === '--batch-size');
  if (batchSizeIndex !== -1 && args[batchSizeIndex + 1]) {
    batchSize = parseInt(args[batchSizeIndex + 1], 10);
    if (isNaN(batchSize) || batchSize < 1 || batchSize > 100) {
      console.error('❌ Invalid --batch-size value. Must be between 1 and 100.');
      process.exit(1);
    }
  }
  
  console.log('='.repeat(70));
  console.log('📝 SIZE METAFIELD MIGRATION (variant custom.size → product custom.search_size)');
  console.log('='.repeat(70));

  const migrator = new SizeMetafieldMigrator();

  try {
    await migrator.initialize();
    
    if (testHandle) {
      await migrator.processSpecificProduct(testHandle);
    } else {
      // Confirmation for bulk processing
      if (!shopifyConfig.dryRun) {
        console.log(`\n⚠️  LIVE MIGRATION MODE - This will update products!`);
        console.log('Press Ctrl+C to cancel or wait 5 seconds to continue...');
        await new Promise(resolve => setTimeout(resolve, 5000));
      }
      
      await migrator.processAllProducts(batchSize);
    }
    
    migrator.saveMigrationResults();
    migrator.printSummary();
    
    console.log('\n🎉 Size metafield migration completed!');

  } catch (error) {
    console.error(`\n❌ Migration failed: ${error.message}`);
    console.error(error.stack);
    process.exit(1);
  }
}

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.log('\n⏹️ Migration interrupted by user');
  process.exit(0);
});

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}