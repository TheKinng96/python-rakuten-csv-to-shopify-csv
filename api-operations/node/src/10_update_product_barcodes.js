#!/usr/bin/env node
/**
 * Update Shopify product variant barcodes using catalog IDs from Rakuten data.
 * 
 * This script:
 * 1. Reads catalog_id_updates.json from data directory
 * 2. Updates product variants with catalog IDs as barcodes
 * 
 * Usage:
 *   node 10_update_product_barcodes.js                      # Live updates
 *   node 10_update_product_barcodes.js --dry-run            # Preview changes
 *   node 10_update_product_barcodes.js --test               # Test mode: update 1 variant only
 *   node 10_update_product_barcodes.js --production         # Use production store
 *   node 10_update_product_barcodes.js --use-recovered      # Use only recovered catalog IDs (separate from main)
 *   node 10_update_product_barcodes.js --use-merged         # Use merged catalog updates (includes recovered SKUs)
 *   node 10_update_product_barcodes.js --force              # Force update even if barcode already exists
 */

import { readFileSync, writeFileSync, existsSync, mkdirSync } from 'fs';
import { join } from 'path';
import { shopifyConfig, pathConfig, validateConfig } from './config.js';
import { ShopifyGraphQLClient } from './shopify-client.js';

const GET_VARIANT_BY_SKU_QUERY = `
  query getVariantBySku($query: String!) {
    productVariants(first: 1, query: $query) {
      edges {
        node {
          id
          sku
          barcode
          product {
            handle
            title
          }
        }
      }
    }
  }
`;

// Configuration  
const DATA_DIR = join('/Users/gen/corekara/rakuten-shopify/api-operations/data');
const REPORTS_DIR = join('/Users/gen/corekara/rakuten-shopify/api-operations/node/reports');
const BATCH_SIZE = 50; // API requests per batch
const DELAY_BETWEEN_BATCHES = 1000; // 1 second delay between batches

async function main() {
  // Check command line arguments
  const args = process.argv.slice(2);
  const isDryRun = args.includes('--dry-run');
  const isTestMode = args.includes('--test');
  const useProductionStore = args.includes('--production');
  const forceUpdate = args.includes('--force');

  console.log('='.repeat(70));
  console.log('📦 SHOPIFY PRODUCT BARCODE UPDATER');
  console.log('='.repeat(70));

  // Validate config and connect to Shopify
  if (!validateConfig()) {
    throw new Error('Configuration validation failed');
  }

  const client = new ShopifyGraphQLClient(!useProductionStore); // true = test store, false = production store
  await client.testConnection();
  console.log(`✅ Connected to Shopify ${useProductionStore ? 'production' : 'test'} store\\n`);

  // Check for data source flags
  const useMerged = args.includes('--use-merged');
  const useRecovered = args.includes('--use-recovered');
  
  // Load catalog ID updates
  let updatesFile;
  if (useRecovered) {
    updatesFile = join(DATA_DIR, 'catalog_id_updates_found.json');
  } else if (useMerged) {
    updatesFile = join(DATA_DIR, 'catalog_id_updates_merged.json');
  } else if (isTestMode) {
    updatesFile = join(DATA_DIR, 'catalog_id_updates_test.json');
  } else {
    updatesFile = join(DATA_DIR, 'catalog_id_updates.json');
  }

  if (!existsSync(updatesFile)) {
    throw new Error(`Updates file not found: ${updatesFile}`);
  }

  const updatesData = JSON.parse(readFileSync(updatesFile, 'utf-8'));
  
  if (!updatesData.updates || !Array.isArray(updatesData.updates)) {
    throw new Error('Invalid JSON structure: expected updates array');
  }

  let updates = updatesData.updates;
  console.log(`📂 Loaded ${updates.length} barcode updates from JSON\\n`);

  // Show summary
  if (updatesData.summary) {
    console.log('📊 Summary from mapping process:');
    console.log(`   • Total Shopify variants: ${updatesData.summary.total_shopify_variants?.toLocaleString()}`);
    console.log(`   • Mapped count: ${updatesData.summary.mapped_count?.toLocaleString()}`);
    console.log(`   • Unmapped count: ${updatesData.summary.unmapped_count?.toLocaleString()}\\n`);
  }

  // Filter for test mode (just 1 update)
  if (isTestMode && updates.length > 0) {
    updates = [updates[0]];
    console.log(`🧪 Test mode: Processing only first update (${updates[0].variant_sku})\\n`);
  }

  // Warning for production
  if (useProductionStore && !isDryRun) {
    console.log('⚠️  WARNING: You are about to update barcodes in PRODUCTION store!');
    console.log('Press Ctrl+C to cancel or wait 5 seconds to continue...');
    await new Promise(resolve => setTimeout(resolve, 5000));
    console.log();
  }

  // Processing stats
  const results = {
    successful: [],
    failed: [],
    skipped: [],
    missing_skus: [], // Track missing SKUs
    stats: {
      total: updates.length,
      processed: 0,
      successful: 0,
      failed: 0,
      skipped: 0,
      missing: 0
    }
  };

  console.log(`🚀 Starting ${isDryRun ? 'DRY RUN' : 'barcode updates'}...`);
  console.log(`📋 Processing ${updates.length} variants`);
  if (forceUpdate) {
    console.log(`🔄 FORCE UPDATE MODE: Will overwrite existing barcodes`);
  }
  console.log();

  // Process updates in batches
  for (let i = 0; i < updates.length; i += BATCH_SIZE) {
    const batch = updates.slice(i, i + BATCH_SIZE);
    const batchNum = Math.floor(i / BATCH_SIZE) + 1;
    const totalBatches = Math.ceil(updates.length / BATCH_SIZE);
    
    console.log(`\\nProcessing batch ${batchNum}/${totalBatches} (${batch.length} items)...`);
    
    for (const update of batch) {
      try {
        results.stats.processed++;
        
        console.log(`[${results.stats.processed}/${updates.length}] Processing SKU: ${update.variant_sku}`);
        
        // Add debug info for problematic SKUs
        if (update.variant_sku === 'zavida-hv-rfa907-3s' || update.variant_sku.includes('zavida')) {
          console.log(`  🔍 Debug info for ${update.variant_sku}:`, JSON.stringify(update, null, 2));
        }
        
        // Get variant by SKU
        const variables = { query: `sku:${update.variant_sku}` };
        const result = await client.query(GET_VARIANT_BY_SKU_QUERY, variables);
        
        const variants = result.productVariants.edges;
        if (variants.length === 0) {
          console.log(`  ❌ Variant not found for SKU: ${update.variant_sku}`);
          results.missing_skus.push({
            variant_sku: update.variant_sku,
            catalog_id: update.catalog_id,
            handle: update.handle || '',
            timestamp: new Date().toISOString()
          });
          results.failed.push({
            ...update,
            error: 'Variant not found',
            timestamp: new Date().toISOString()
          });
          results.stats.failed++;
          results.stats.missing++;
          continue;
        }
        
        const variant = variants[0].node;
        
        // Check if barcode already exists (skip unless --force is used)
        if (!forceUpdate && variant.barcode && variant.barcode.trim() !== '') {
          console.log(`  ⚠️  SKU ${update.variant_sku} already has barcode: ${variant.barcode}`);
          results.skipped.push({
            ...update,
            existing_barcode: variant.barcode,
            reason: 'Already has barcode',
            timestamp: new Date().toISOString()
          });
          results.stats.skipped++;
          continue;
        }
        
        // Log when force update is being used
        if (forceUpdate && variant.barcode && variant.barcode.trim() !== '') {
          console.log(`  🔄 FORCE UPDATE: Overwriting existing barcode "${variant.barcode}" for SKU ${update.variant_sku}`);
        }
        
        // Clean the barcode - convert various forms of zero to empty string
        let cleanedBarcode = String(update.catalog_id || '').trim();
        if (cleanedBarcode === '0' || cleanedBarcode === '.0' || cleanedBarcode === '0.0' || cleanedBarcode === '') {
          cleanedBarcode = '';
        }
        
        if (isDryRun) {
          console.log(`  📝 [DRY RUN] Would update barcode to: ${cleanedBarcode}`);
          results.successful.push({
            ...update,
            variant_id: variant.id,
            product_handle: variant.product.handle,
            product_title: variant.product.title,
            old_barcode: variant.barcode || '',
            new_barcode: cleanedBarcode,
            dry_run: true,
            timestamp: new Date().toISOString()
          });
          results.stats.successful++;
        } else {
          // Update barcode using REST API (more reliable for simple updates)
          console.log(`  📝 Updating barcode to: ${cleanedBarcode}`);
          
          const numericId = variant.id.split('/').pop();
          const storeConfig = client.client.config;
          
          // Extract just the hostname from storeDomain (it may contain https://)
          const storeDomain = storeConfig.storeDomain.replace(/^https?:\/\//, '');
          
          const url = `https://${storeDomain}/admin/api/2024-01/variants/${numericId}.json`;
          
          // Add URL validation to catch malformed URLs
          try {
            new URL(url);
          } catch (urlError) {
            throw new Error(`Invalid URL constructed: '${url}' - Store domain: '${storeDomain}', Numeric ID: '${numericId}', Variant ID: '${variant.id}'`);
          }
          
          // Add additional validation for numeric ID
          if (!numericId || isNaN(numericId)) {
            throw new Error(`Invalid numeric ID extracted: '${numericId}' from variant ID: '${variant.id}'`);
          }
          
          // Retry logic for network issues
          let response;
          let retryCount = 0;
          const maxRetries = 3;
          
          while (retryCount <= maxRetries) {
            try {
              response = await fetch(url, {
                method: 'PUT',
                headers: {
                  'Content-Type': 'application/json',
                  'X-Shopify-Access-Token': storeConfig.accessToken
                },
                body: JSON.stringify({
                  variant: {
                    id: parseInt(numericId),
                    barcode: cleanedBarcode
                  }
                }),
                timeout: 30000  // 30 second timeout
              });
              
              if (!response.ok) {
                const errorText = await response.text();
                throw new Error(`REST request failed: ${response.status} ${response.statusText} - ${errorText}`);
              }
              
              // Success - break out of retry loop
              break;
              
            } catch (fetchError) {
              retryCount++;
              
              if (retryCount > maxRetries) {
                // Max retries exceeded, throw the error
                throw fetchError;
              }
              
              console.log(`  ⚠️  Fetch failed (attempt ${retryCount}/${maxRetries}), retrying in 2 seconds...`);
              await new Promise(resolve => setTimeout(resolve, 2000));
            }
          }
          
          const updatedVariant = await response.json();
          
          console.log(`  ✅ Successfully updated SKU: ${update.variant_sku}`);
          results.successful.push({
            ...update,
            variant_id: variant.id,
            product_handle: variant.product.handle,
            product_title: variant.product.title,
            old_barcode: variant.barcode || '',
            new_barcode: updatedVariant.variant.barcode,
            timestamp: new Date().toISOString()
          });
          results.stats.successful++;
        }
        
        // Small delay between individual requests
        await new Promise(resolve => setTimeout(resolve, 100));
        
      } catch (error) {
        console.log(`  ❌ Failed to update SKU ${update.variant_sku}: ${error.message}`);
        console.log(`  🔍 Error details:`, error);
        if (error.cause) {
          console.log(`  🔍 Error cause:`, error.cause);
        }
        results.failed.push({
          ...update,
          error: error.message,
          error_details: error.toString(),
          timestamp: new Date().toISOString()
        });
        results.stats.failed++;
      }
    }
    
    // Delay between batches (except for the last batch)
    if (i + BATCH_SIZE < updates.length) {
      console.log(`Waiting ${DELAY_BETWEEN_BATCHES}ms before next batch...`);
      await new Promise(resolve => setTimeout(resolve, DELAY_BETWEEN_BATCHES));
    }
  }

  // Save results
  if (!existsSync(REPORTS_DIR)) {
    mkdirSync(REPORTS_DIR, { recursive: true });
  }
  
  const suffix = useProductionStore ? 
    (isTestMode ? '_test_prod' : '_prod') : 
    (isTestMode ? '_test' : '');
  
  const reportFile = join(REPORTS_DIR, `barcode_update_results${suffix}.json`);
  
  const reportData = {
    ...results,
    metadata: {
      dry_run: isDryRun,
      test_mode: isTestMode,
      production_store: useProductionStore,
      force_update: forceUpdate,
      processed_at: new Date().toISOString(),
      batch_size: BATCH_SIZE,
      delay_between_batches: DELAY_BETWEEN_BATCHES
    }
  };
  
  writeFileSync(reportFile, JSON.stringify(reportData, null, 2));
  
  // Save missing SKUs to CSV if any found
  if (results.missing_skus.length > 0) {
    const csvFile = join(REPORTS_DIR, `missing_skus${suffix}.csv`);
    
    // Create CSV header
    const csvHeader = 'variant_sku,catalog_id,handle,timestamp\n';
    
    // Create CSV rows
    const csvRows = results.missing_skus.map(item => 
      `"${item.variant_sku}","${item.catalog_id}","${item.handle}","${item.timestamp}"`
    ).join('\n');
    
    const csvContent = csvHeader + csvRows;
    writeFileSync(csvFile, csvContent);
    
    console.log(`📄 Missing SKUs saved to: ${csvFile}`);
  }
  
  // Final summary
  console.log('\\n' + '='.repeat(70));
  console.log('📊 PROCESSING COMPLETE');
  console.log('='.repeat(70));
  console.log(`📄 Results saved to: ${reportFile}`);
  console.log();
  console.log('📈 Summary:');
  console.log(`   • Total: ${results.stats.total}`);
  console.log(`   • Processed: ${results.stats.processed}`);
  console.log(`   • Successful: ${results.stats.successful}`);
  console.log(`   • Failed: ${results.stats.failed}`);
  console.log(`   • Skipped: ${results.stats.skipped}`);
  console.log(`   • Missing SKUs: ${results.stats.missing}`);
  
  if (results.missing_skus.length > 0) {
    console.log('\\n❌ Missing SKUs (not found in Shopify):');
    results.missing_skus.slice(0, 10).forEach(missing => {
      console.log(`   • ${missing.variant_sku} → ${missing.catalog_id}`);
    });
    if (results.missing_skus.length > 10) {
      console.log(`   ... and ${results.missing_skus.length - 10} more (see CSV file)`);
    }
  }
  
  if (results.failed.length > 0) {
    console.log('\\n❌ Other failed updates:');
    results.failed.filter(f => f.error !== 'Variant not found').forEach(failure => {
      console.log(`   • ${failure.variant_sku}: ${failure.error}`);
    });
  }
  
  if (results.successful.length > 0) {
    console.log('\\n✅ Recent successful updates:');
    results.successful.slice(-5).forEach(success => {
      const dryRunLabel = success.dry_run ? ' [DRY RUN]' : '';
      console.log(`   • ${success.variant_sku} → ${success.new_barcode}${dryRunLabel}`);
    });
    if (results.successful.length > 5) {
      console.log(`   ... and ${results.successful.length - 5} more`);
    }
  }
  
  console.log();
  console.log(`🎉 ${isDryRun ? 'Dry run' : 'Processing'} completed!`);
}

// Handle uncaught errors
process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
  process.exit(1);
});

// Run main function
if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch(error => {
    console.error('❌ Error:', error.message);
    process.exit(1);
  });
}