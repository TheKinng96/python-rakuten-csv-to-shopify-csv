#!/usr/bin/env node
/**
 * Comprehensive scan for products with variant custom.size metafields
 * Saves results to file for batch migration processing
 * 
 * Usage:
 *   node 13_scan_variant_size_metafields.js                    # Scan all products
 *   node 13_scan_variant_size_metafields.js --batch-size 50    # Custom batch size
 *   node 13_scan_variant_size_metafields.js --resume           # Resume from last cursor
 */

import { writeFileSync, readFileSync, existsSync } from 'fs';
import { join } from 'path';
import { pathConfig, shopifyConfig, validateConfig } from '../config.js';
import { ShopifyGraphQLClient } from '../shopify-client.js';

// GraphQL query to scan products and variants for size metafields
const SCAN_PRODUCTS_VARIANTS = `
  query scanProductsVariants($first: Int!, $after: String) {
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
                metafields(namespace: "custom", first: 15) {
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

class VariantSizeScanner {
  constructor() {
    this.client = null;
    this.scanResults = {
      productsWithVariantSize: [],
      productsWithSearchSize: [],
      productsWithBoth: [],
      productsWithNeither: [],
      migrationCandidates: []
    };
    
    this.stats = {
      totalScanned: 0,
      withVariantSize: 0,
      withSearchSize: 0,
      withBoth: 0,
      migrationNeeded: 0,
      uniqueSizeValues: new Set()
    };
    
    this.progressFile = join(pathConfig.reportsPath, '13_scan_progress.json');
    this.resultsFile = join(pathConfig.reportsPath, '13_variant_size_scan_results.json');
  }

  async initialize() {
    console.log('🔧 Initializing Shopify GraphQL client...');
    
    if (!validateConfig()) {
      throw new Error('Configuration validation failed');
    }

    this.client = new ShopifyGraphQLClient(false);
    await this.client.testConnection();
    console.log('✅ Connected to Shopify test store');
  }

  loadProgress() {
    if (existsSync(this.progressFile)) {
      try {
        const progress = JSON.parse(readFileSync(this.progressFile, 'utf8'));
        console.log(`📂 Resuming from cursor: ${progress.lastCursor || 'start'}`);
        console.log(`📊 Previous progress: ${progress.totalScanned} products scanned`);
        return progress;
      } catch (error) {
        console.log('⚠️  Could not load progress file, starting fresh');
        return null;
      }
    }
    return null;
  }

  saveProgress(cursor, totalScanned) {
    const progress = {
      lastCursor: cursor,
      totalScanned: totalScanned,
      timestamp: new Date().toISOString()
    };
    writeFileSync(this.progressFile, JSON.stringify(progress, null, 2));
  }

  async scanAllProducts(batchSize = 20, resume = false) {
    console.log(`\n🔍 Starting comprehensive variant size scan (batch size: ${batchSize})...`);
    
    let cursor = null;
    let totalScanned = 0;
    let batchNumber = 0;
    
    // Resume from previous scan if requested
    if (resume) {
      const progress = this.loadProgress();
      if (progress) {
        cursor = progress.lastCursor;
        totalScanned = progress.totalScanned;
        console.log(`🔄 Resuming from ${totalScanned} products`);
      }
    }
    
    while (true) {
      batchNumber++;
      console.log(`\n📦 Processing batch ${batchNumber} (starting from product ${totalScanned + 1})...`);
      
      try {
        const result = await this.client.query(SCAN_PRODUCTS_VARIANTS, {
          first: batchSize,
          after: cursor
        });
        
        const products = result.products.edges.map(edge => edge.node);
        
        if (products.length === 0) {
          console.log(`✅ Scan complete - no more products`);
          break;
        }
        
        // Process each product
        for (const product of products) {
          this.analyzeProduct(product);
          totalScanned++;
        }
        
        console.log(`   📊 Batch ${batchNumber}: ${products.length} products processed`);
        console.log(`   📈 Total scanned: ${totalScanned}`);
        console.log(`   🎯 Migration candidates so far: ${this.stats.migrationNeeded}`);
        
        // Save progress every batch
        cursor = result.products.pageInfo.endCursor;
        this.saveProgress(cursor, totalScanned);
        
        // Check if there are more pages
        if (!result.products.pageInfo.hasNextPage) {
          console.log(`✅ Scan complete - all products processed`);
          break;
        }
        
        // Rate limiting between batches
        const delay = Math.ceil(1000 / shopifyConfig.maxRequestsPerSecond * batchSize);
        console.log(`   ⏱️  Rate limiting delay: ${delay}ms`);
        await new Promise(resolve => setTimeout(resolve, delay));
        
      } catch (error) {
        console.error(`❌ Failed to process batch ${batchNumber}: ${error.message}`);
        console.log(`💾 Progress saved. You can resume with --resume flag`);
        throw error;
      }
    }
    
    this.stats.totalScanned = totalScanned;
  }

  analyzeProduct(product) {
    const productHandle = product.handle;
    const productTitle = product.title;
    
    // Check product metafields for search_size
    const productMetafields = (product.metafields?.edges || []).map(edge => edge.node);
    const hasSearchSize = productMetafields.find(meta => meta.key === 'search_size');
    
    // Check all variants for size metafield
    const variants = product.variants?.edges || [];
    let firstVariantSizeValue = null;
    let variantSizeCounts = 0;
    let allVariantSizes = [];
    
    variants.forEach(variantEdge => {
      const variant = variantEdge.node;
      const variantMetafields = (variant.metafields?.edges || []).map(edge => edge.node);
      const sizeMetafield = variantMetafields.find(meta => meta.key === 'size');
      
      if (sizeMetafield && sizeMetafield.value && sizeMetafield.value.trim()) {
        const sizeValue = sizeMetafield.value.trim();
        if (!firstVariantSizeValue) {
          firstVariantSizeValue = sizeValue;
        }
        allVariantSizes.push({
          variantId: variant.id,
          variantName: variant.displayName,
          sizeValue: sizeValue
        });
        variantSizeCounts++;
        this.stats.uniqueSizeValues.add(sizeValue);
      }
    });
    
    // Categorize the product
    const productData = {
      id: product.id,
      handle: productHandle,
      title: productTitle,
      currentSearchSize: hasSearchSize ? hasSearchSize.value : null,
      firstVariantSize: firstVariantSizeValue,
      variantSizeCount: variantSizeCounts,
      allVariantSizes: allVariantSizes,
      timestamp: new Date().toISOString()
    };
    
    if (firstVariantSizeValue && hasSearchSize && hasSearchSize.value && hasSearchSize.value.trim()) {
      // Has both
      this.stats.withBoth++;
      this.scanResults.productsWithBoth.push(productData);
    } else if (firstVariantSizeValue) {
      // Has variant size only - MIGRATION CANDIDATE
      this.stats.withVariantSize++;
      this.stats.migrationNeeded++;
      this.scanResults.productsWithVariantSize.push(productData);
      this.scanResults.migrationCandidates.push({
        ...productData,
        migrationValue: firstVariantSizeValue
      });
    } else if (hasSearchSize && hasSearchSize.value && hasSearchSize.value.trim()) {
      // Has search_size only
      this.stats.withSearchSize++;
      this.scanResults.productsWithSearchSize.push(productData);
    } else {
      // Has neither
      this.scanResults.productsWithNeither.push(productData);
    }
  }

  saveScanResults() {
    const report = {
      timestamp: new Date().toISOString(),
      summary: {
        totalScanned: this.stats.totalScanned,
        withVariantSize: this.stats.withVariantSize,
        withSearchSize: this.stats.withSearchSize,
        withBoth: this.stats.withBoth,
        migrationNeeded: this.stats.migrationNeeded,
        uniqueSizeValues: [...this.stats.uniqueSizeValues].sort()
      },
      scanResults: this.scanResults,
      config: {
        store: 'test',
        apiVersion: '2025-07',
        scanDate: new Date().toISOString()
      }
    };

    writeFileSync(this.resultsFile, JSON.stringify(report, null, 2));
    console.log(`📄 Scan results saved to: ${this.resultsFile}`);
    
    // Also save migration candidates separately for easy processing
    const migrationFile = join(pathConfig.reportsPath, '13_migration_candidates.json');
    const migrationData = {
      timestamp: new Date().toISOString(),
      totalCandidates: this.scanResults.migrationCandidates.length,
      candidates: this.scanResults.migrationCandidates
    };
    writeFileSync(migrationFile, JSON.stringify(migrationData, null, 2));
    console.log(`🎯 Migration candidates saved to: ${migrationFile}`);
  }

  printSummary() {
    console.log('\n' + '='.repeat(70));
    console.log('📊 VARIANT SIZE SCAN SUMMARY');
    console.log('='.repeat(70));
    
    console.log(`Total products scanned: ${this.stats.totalScanned}`);
    console.log(`✅ Products with variant custom.size: ${this.stats.withVariantSize}`);
    console.log(`✅ Products with product custom.search_size: ${this.stats.withSearchSize}`);
    console.log(`🔄 Products with both: ${this.stats.withBoth}`);
    console.log(`❌ Products with neither: ${this.scanResults.productsWithNeither.length}`);
    
    console.log(`\n🎯 MIGRATION NEEDED: ${this.stats.migrationNeeded} products`);
    console.log(`📏 Unique size values found: ${this.stats.uniqueSizeValues.size}`);
    
    if (this.stats.uniqueSizeValues.size > 0 && this.stats.uniqueSizeValues.size <= 20) {
      console.log('\n📝 Size values found:');
      [...this.stats.uniqueSizeValues].sort().forEach(value => {
        console.log(`  - "${value}"`);
      });
    }
    
    if (this.stats.migrationNeeded > 0) {
      console.log(`\n💡 Next steps:`);
      console.log(`   1. Review migration candidates in reports/13_migration_candidates.json`);
      console.log(`   2. Run migration script: node 13_batch_migrate_size_metafields.js`);
      console.log(`   3. Verify results after migration`);
    } else {
      console.log(`\n✅ No migration needed - all products already have search_size or no size data`);
    }
  }
}

async function main() {
  const args = process.argv.slice(2);
  
  // Parse arguments
  let batchSize = 20;
  let resume = false;
  
  const batchSizeIndex = args.findIndex(arg => arg === '--batch-size');
  if (batchSizeIndex !== -1 && args[batchSizeIndex + 1]) {
    batchSize = parseInt(args[batchSizeIndex + 1], 10);
    if (isNaN(batchSize) || batchSize < 1 || batchSize > 50) {
      console.error('❌ Invalid --batch-size value. Must be between 1 and 50.');
      process.exit(1);
    }
  }
  
  resume = args.includes('--resume');
  
  console.log('='.repeat(70));
  console.log('🔍 VARIANT SIZE METAFIELD SCAN');
  console.log('='.repeat(70));

  const scanner = new VariantSizeScanner();

  try {
    await scanner.initialize();
    await scanner.scanAllProducts(batchSize, resume);
    
    scanner.saveScanResults();
    scanner.printSummary();
    
    console.log('\n🎉 Variant size scan completed!');

  } catch (error) {
    console.error(`\n❌ Scan failed: ${error.message}`);
    console.error(error.stack);
    process.exit(1);
  }
}

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.log('\n⏹️ Scan interrupted by user');
  console.log('💾 Progress has been saved. Resume with --resume flag');
  process.exit(0);
});

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}