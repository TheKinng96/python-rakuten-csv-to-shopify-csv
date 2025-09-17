#!/usr/bin/env node
/**
 * Batch migration script using scan results from 13_scan_variant_size_metafields.js
 * Migrates variant custom.size to product custom.search_size based on scan file
 * 
 * Usage:
 *   node 13_batch_migrate_size_metafields.js                        # Migrate all candidates
 *   node 13_batch_migrate_size_metafields.js --dry-run              # Preview only
 *   node 13_batch_migrate_size_metafields.js --batch-size 10        # Custom batch size
 *   node 13_batch_migrate_size_metafields.js --limit 50             # Limit migrations
 */

import { writeFileSync, readFileSync, existsSync } from 'fs';
import { join } from 'path';
import { pathConfig, shopifyConfig, validateConfig } from '../config.js';
import { ShopifyGraphQLClient } from '../shopify-client.js';

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

class BatchSizeMigrator {
  constructor() {
    this.client = null;
    this.migrationResults = {
      successful: [],
      failed: [],
      skipped: []
    };
    
    this.stats = {
      totalCandidates: 0,
      processed: 0,
      successful: 0,
      failed: 0,
      skipped: 0
    };
    
    this.candidatesFile = join(pathConfig.reportsPath, '13_migration_candidates.json');
    this.resultsFile = join(pathConfig.reportsPath, '13_batch_migration_results.json');
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

  loadMigrationCandidates() {
    if (!existsSync(this.candidatesFile)) {
      throw new Error(`❌ Migration candidates file not found: ${this.candidatesFile}`);
    }
    
    try {
      const data = JSON.parse(readFileSync(this.candidatesFile, 'utf8'));
      console.log(`📂 Loaded ${data.totalCandidates} migration candidates`);
      console.log(`📅 Scan date: ${data.timestamp}`);
      return data.candidates;
    } catch (error) {
      throw new Error(`❌ Failed to load migration candidates: ${error.message}`);
    }
  }

  async migrateBatch(candidates, batchSize = 10, dryRun = false, limit = null) {
    console.log(`\n🚀 Starting batch migration...`);
    console.log(`   Batch size: ${batchSize}`);
    console.log(`   Dry run: ${dryRun}`);
    console.log(`   Limit: ${limit || 'none'}`);
    
    const totalToProcess = limit ? Math.min(candidates.length, limit) : candidates.length;
    this.stats.totalCandidates = totalToProcess;
    
    let processed = 0;
    let batchNumber = 0;
    
    // Process in batches
    for (let i = 0; i < totalToProcess; i += batchSize) {
      batchNumber++;
      const batch = candidates.slice(i, Math.min(i + batchSize, totalToProcess));
      
      console.log(`\n📦 Processing batch ${batchNumber} (${batch.length} products)...`);
      
      // Process batch in parallel
      const promises = batch.map((candidate, index) => 
        this.migrateProduct(candidate, processed + index + 1, totalToProcess, dryRun)
      );
      
      const results = await Promise.allSettled(promises);
      processed += batch.length;
      
      // Count results
      const batchSuccessful = results.filter(r => r.status === 'fulfilled').length;
      const batchFailed = results.filter(r => r.status === 'rejected').length;
      
      console.log(`   ✅ Batch ${batchNumber} completed: ${batchSuccessful} successful, ${batchFailed} failed`);
      console.log(`   📈 Progress: ${processed}/${totalToProcess} (${((processed/totalToProcess)*100).toFixed(1)}%)`);
      
      // Rate limiting between batches
      if (i + batchSize < totalToProcess) {
        const delay = Math.ceil(1000 / shopifyConfig.maxRequestsPerSecond * batchSize);
        console.log(`   ⏱️  Rate limiting delay: ${delay}ms`);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
    
    this.stats.processed = processed;
  }

  async migrateProduct(candidate, index, total, dryRun) {
    try {
      const productId = candidate.id;
      const productHandle = candidate.handle;
      const migrationValue = candidate.migrationValue;
      
      console.log(`   [${index}/${total}] ${productHandle}: "${migrationValue}"`);
      
      if (dryRun) {
        console.log(`   🔍 [DRY RUN] Would migrate: ${productHandle} → "${migrationValue}"`);
        this.migrationResults.successful.push({
          handle: productHandle,
          id: productId,
          status: 'dry_run',
          migrationValue: migrationValue,
          timestamp: new Date().toISOString()
        });
        this.stats.successful++;
        return;
      }
      
      // Perform actual migration
      const result = await this.client.mutate(UPDATE_PRODUCT_METAFIELD, {
        product: {
          id: productId,
          metafields: [
            {
              namespace: 'custom',
              key: 'search_size',
              value: JSON.stringify([migrationValue]),
              type: 'list.single_line_text_field'
            }
          ]
        }
      });

      if (result.productUpdate.userErrors.length > 0) {
        const errors = result.productUpdate.userErrors.map(e => `${e.field}: ${e.message}`);
        throw new Error(`Shopify errors: ${errors.join(', ')}`);
      }

      console.log(`   ✅ [${index}/${total}] Migrated: ${productHandle} → "${migrationValue}"`);
      
      this.migrationResults.successful.push({
        handle: productHandle,
        id: productId,
        status: 'migrated',
        migrationValue: migrationValue,
        timestamp: new Date().toISOString()
      });
      this.stats.successful++;

    } catch (error) {
      console.error(`   ❌ [${index}/${total}] Failed: ${candidate.handle} - ${error.message}`);
      
      this.migrationResults.failed.push({
        handle: candidate.handle,
        id: candidate.id,
        error: error.message,
        timestamp: new Date().toISOString()
      });
      this.stats.failed++;
    }
  }

  saveMigrationResults() {
    const report = {
      timestamp: new Date().toISOString(),
      summary: {
        totalCandidates: this.stats.totalCandidates,
        processed: this.stats.processed,
        successful: this.stats.successful,
        failed: this.stats.failed,
        skipped: this.stats.skipped,
        successRate: this.stats.processed > 0 ? 
          ((this.stats.successful / this.stats.processed) * 100).toFixed(1) + '%' : '0%'
      },
      results: this.migrationResults,
      config: {
        dryRun: shopifyConfig.dryRun,
        store: 'test',
        apiVersion: '2025-07'
      }
    };

    writeFileSync(this.resultsFile, JSON.stringify(report, null, 2));
    console.log(`📄 Migration results saved to: ${this.resultsFile}`);
  }

  printSummary() {
    console.log('\n' + '='.repeat(70));
    console.log('📊 BATCH MIGRATION SUMMARY');
    console.log('='.repeat(70));
    
    console.log(`Total candidates: ${this.stats.totalCandidates}`);
    console.log(`Products processed: ${this.stats.processed}`);
    console.log(`✅ Successful migrations: ${this.stats.successful}`);
    console.log(`❌ Failed migrations: ${this.stats.failed}`);
    console.log(`⏭️  Skipped: ${this.stats.skipped}`);
    
    if (this.stats.processed > 0) {
      const successRate = (this.stats.successful / this.stats.processed * 100).toFixed(1);
      console.log(`📈 Success rate: ${successRate}%`);
    }
    
    if (this.stats.failed > 0) {
      console.log(`\n❌ Failed migrations:`);
      this.migrationResults.failed.slice(0, 5).forEach(failure => {
        console.log(`   - ${failure.handle}: ${failure.error}`);
      });
      if (this.migrationResults.failed.length > 5) {
        console.log(`   ... and ${this.migrationResults.failed.length - 5} more`);
      }
    }
    
    console.log('\n💡 Next steps:');
    console.log('   1. Review migration results in reports/13_batch_migration_results.json');
    console.log('   2. Verify migrated products in Shopify admin');
    console.log('   3. Test search functionality with new search_size metafields');
    
    if (this.stats.failed > 0) {
      console.log('   4. Investigate and retry failed migrations if needed');
    }
  }
}

async function main() {
  const args = process.argv.slice(2);
  
  // Parse arguments
  let batchSize = 10;
  let dryRun = args.includes('--dry-run');
  let limit = null;
  
  const batchSizeIndex = args.findIndex(arg => arg === '--batch-size');
  if (batchSizeIndex !== -1 && args[batchSizeIndex + 1]) {
    batchSize = parseInt(args[batchSizeIndex + 1], 10);
    if (isNaN(batchSize) || batchSize < 1 || batchSize > 20) {
      console.error('❌ Invalid --batch-size value. Must be between 1 and 20.');
      process.exit(1);
    }
  }
  
  const limitIndex = args.findIndex(arg => arg === '--limit');
  if (limitIndex !== -1 && args[limitIndex + 1]) {
    limit = parseInt(args[limitIndex + 1], 10);
    if (isNaN(limit) || limit < 1) {
      console.error('❌ Invalid --limit value. Must be greater than 0.');
      process.exit(1);
    }
  }
  
  console.log('='.repeat(70));
  console.log('🚀 BATCH SIZE METAFIELD MIGRATION');
  console.log('='.repeat(70));

  const migrator = new BatchSizeMigrator();

  try {
    await migrator.initialize();
    
    const candidates = migrator.loadMigrationCandidates();
    
    if (candidates.length === 0) {
      console.log('✅ No migration candidates found. Run scan first: node 13_scan_variant_size_metafields.js');
      return;
    }
    
    // Confirmation for live migration
    if (!dryRun) {
      console.log(`\n⚠️  LIVE MIGRATION MODE - This will update ${limit || candidates.length} products!`);
      console.log('Press Ctrl+C to cancel or wait 5 seconds to continue...');
      await new Promise(resolve => setTimeout(resolve, 5000));
    }
    
    await migrator.migrateBatch(candidates, batchSize, dryRun, limit);
    
    migrator.saveMigrationResults();
    migrator.printSummary();
    
    console.log('\n🎉 Batch migration completed!');

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