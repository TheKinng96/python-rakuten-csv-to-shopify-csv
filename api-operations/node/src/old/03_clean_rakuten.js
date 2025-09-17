#!/usr/bin/env node
/**
 * Clean Rakuten EC-UP content from Shopify products via GraphQL
 * 
 * This script:
 * 1. Reads rakuten_content_to_clean.json from shared directory
 * 2. Finds products by handle and removes EC-UP patterns
 * 3. Provides progress logging and error handling
 * 4. Saves cleaning results for audit
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
      bodyHtml
    }
  }
`;

const UPDATE_PRODUCT_MUTATION = `
  mutation productUpdate($input: ProductInput!) {
    productUpdate(input: $input) {
      product {
        id
        handle
        title
        bodyHtml
      }
      userErrors {
        field
        message
      }
    }
  }
`;

class RakutenContentCleaner {
  constructor() {
    this.client = null;
    this.cleanResults = {
      successful: [],
      failed: [],
      notFound: [],
      noChangesNeeded: []
    };
    
    // EC-UP pattern regex (matches all EC-UP blocks)
    this.ecUpPattern = /<!--EC-UP_([^_]+)_(\d+)_START-->(.*?)<!--EC-UP_\1_\2_END-->/gis;
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

  loadRakutenDataFromJSON() {
    const jsonPath = join(pathConfig.sharedPath, 'rakuten_content_to_clean.json');
    
    if (!existsSync(jsonPath)) {
      throw new Error(`Rakuten content JSON file not found: ${jsonPath}`);
    }

    console.log(`📂 Loading Rakuten content data from ${jsonPath}...`);
    
    const jsonData = JSON.parse(readFileSync(jsonPath, 'utf-8'));
    
    if (!jsonData.data || !Array.isArray(jsonData.data)) {
      throw new Error('Invalid JSON structure: expected data array');
    }

    console.log(`✅ Loaded ${jsonData.data.length} products with Rakuten content from JSON`);
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

  cleanRakutenContent(htmlContent, expectedPatterns = []) {
    if (!htmlContent) return { cleanedHtml: '', patternsRemoved: [], bytesRemoved: 0 };

    let cleanedHtml = htmlContent;
    const patternsRemoved = [];
    let bytesRemoved = 0;
    
    // Remove all EC-UP patterns
    cleanedHtml = cleanedHtml.replace(this.ecUpPattern, (match, patternName, patternNumber, content) => {
      const patternId = `EC-UP_${patternName}_${patternNumber}`;
      
      patternsRemoved.push({
        patternId,
        patternName,
        patternNumber,
        contentLength: content.length,
        fullMatchLength: match.length,
        contentPreview: content.substring(0, 100) + (content.length > 100 ? '...' : '')
      });
      
      bytesRemoved += match.length;
      
      return ''; // Remove the entire EC-UP block
    });

    // Clean up any remaining EC-UP artifacts
    const artifacts = [
      /<!--EC-UP_[^>]*-->/gi,  // Orphaned EC-UP comments
      /\s*<!\[endif\]-->/gi,   // IE conditional comments often used with EC-UP
      /<!--\[if[^>]*>\s*<!\[endif\]-->/gi  // Empty IE conditionals
    ];

    artifacts.forEach(pattern => {
      const matches = cleanedHtml.match(pattern);
      if (matches) {
        matches.forEach(match => bytesRemoved += match.length);
        cleanedHtml = cleanedHtml.replace(pattern, '');
      }
    });

    // Clean up excessive whitespace left behind
    cleanedHtml = cleanedHtml
      .replace(/\n\s*\n\s*\n/g, '\n\n')  // Multiple empty lines to double
      .replace(/^\s+|\s+$/g, '')         // Trim start/end whitespace
      .replace(/\s+(<\/)/g, '$1');       // Remove space before closing tags

    return {
      cleanedHtml,
      patternsRemoved,
      bytesRemoved,
      changesMade: patternsRemoved.length > 0
    };
  }

  async cleanProductRakuten(productHandle, expectedPatterns, index, total) {
    try {
      // Find product in Shopify
      const product = await this.findProductByHandle(productHandle);
      
      if (!product) {
        console.log(`   ⚠️  [${index}/${total}] Product not found: ${productHandle}`);
        this.cleanResults.notFound.push({
          handle: productHandle,
          reason: 'Product not found in Shopify'
        });
        return;
      }

      const currentHtml = product.bodyHtml || '';
      
      // Clean Rakuten content
      const { cleanedHtml, patternsRemoved, bytesRemoved, changesMade } = 
        this.cleanRakutenContent(currentHtml, expectedPatterns);
      
      if (!changesMade) {
        console.log(`   ℹ️  [${index}/${total}] No Rakuten content found: ${productHandle}`);
        this.cleanResults.noChangesNeeded.push({
          handle: productHandle,
          title: product.title,
          shopifyId: product.id,
          reason: 'No EC-UP patterns found'
        });
        return;
      }

      // Update product with cleaned HTML
      if (shopifyConfig.dryRun) {
        console.log(`   🔍 [DRY RUN] Would remove ${patternsRemoved.length} EC-UP patterns (${bytesRemoved} bytes) from: ${productHandle}`);
        this.cleanResults.successful.push({
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
        input: {
          id: product.id,
          bodyHtml: cleanedHtml
        }
      });

      if (result.productUpdate.userErrors.length > 0) {
        const errors = result.productUpdate.userErrors.map(e => `${e.field}: ${e.message}`);
        throw new Error(`Shopify errors: ${errors.join(', ')}`);
      }

      console.log(`   ✅ [${index}/${total}] Removed ${patternsRemoved.length} EC-UP patterns (${bytesRemoved} bytes) from: ${productHandle}`);
      
      this.cleanResults.successful.push({
        handle: productHandle,
        title: product.title,
        status: 'content_cleaned',
        patternsRemoved: patternsRemoved.length,
        bytesRemoved,
        shopifyId: product.id,
        patterns: patternsRemoved,
        originalHtmlLength: currentHtml.length,
        cleanedHtmlLength: cleanedHtml.length,
        compressionRatio: ((1 - cleanedHtml.length / currentHtml.length) * 100).toFixed(1) + '%'
      });

    } catch (error) {
      console.error(`   ❌ [${index}/${total}] Failed: ${productHandle} - ${error.message}`);
      
      this.cleanResults.failed.push({
        handle: productHandle,
        status: 'failed',
        error: error.message
      });
    }
  }

  async processProducts(rakutenData) {
    console.log(`\n🚀 Starting Rakuten content cleaning (${rakutenData.length} products)...`);
    
    const batchSize = shopifyConfig.batchSize;
    const total = rakutenData.length;
    let processed = 0;

    // Process in batches to respect rate limits
    for (let i = 0; i < rakutenData.length; i += batchSize) {
      const batch = rakutenData.slice(i, i + batchSize);
      
      console.log(`\n📦 Processing batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(rakutenData.length/batchSize)}`);
      
      // Process batch with concurrency limit
      const promises = batch.map((productData, batchIndex) => 
        this.cleanProductRakuten(
          productData.productHandle, 
          productData.patternsFound,
          i + batchIndex + 1, 
          total
        )
      );
      
      await Promise.all(promises);
      processed += batch.length;
      
      // Progress update
      const successRate = (this.cleanResults.successful.length / processed * 100).toFixed(1);
      console.log(`📈 Progress: ${processed}/${total} processed (${successRate}% success rate)`);
      
      // Rate limiting delay between batches
      if (i + batchSize < rakutenData.length) {
        const delay = Math.ceil(1000 / shopifyConfig.maxRequestsPerSecond * batchSize);
        console.log(`⏱️  Waiting ${delay}ms for rate limiting...`);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
  }

  saveCleanResults() {
    const resultsPath = join(pathConfig.reportsPath, '03_rakuten_clean_results.json');
    
    const report = {
      timestamp: new Date().toISOString(),
      summary: {
        total: this.cleanResults.successful.length + this.cleanResults.failed.length + this.cleanResults.notFound.length + this.cleanResults.noChangesNeeded.length,
        successful: this.cleanResults.successful.length,
        failed: this.cleanResults.failed.length,
        notFound: this.cleanResults.notFound.length,
        noChangesNeeded: this.cleanResults.noChangesNeeded.length,
        totalPatternsRemoved: this.cleanResults.successful.reduce((sum, result) => 
          sum + (result.patternsRemoved || 0), 0
        ),
        totalBytesRemoved: this.cleanResults.successful.reduce((sum, result) => 
          sum + (result.bytesRemoved || 0), 0
        )
      },
      results: this.cleanResults,
      config: {
        dryRun: shopifyConfig.dryRun,
        batchSize: shopifyConfig.batchSize,
        store: 'test'
      }
    };

    writeFileSync(resultsPath, JSON.stringify(report, null, 2));
    console.log(`📄 Clean results saved to: ${resultsPath}`);
  }

  printSummary() {
    const { successful, failed, notFound, noChangesNeeded } = this.cleanResults;
    const total = successful.length + failed.length + notFound.length + noChangesNeeded.length;
    const totalPatternsRemoved = successful.reduce((sum, result) => sum + (result.patternsRemoved || 0), 0);
    const totalBytesRemoved = successful.reduce((sum, result) => sum + (result.bytesRemoved || 0), 0);
    
    console.log('\n' + '='.repeat(70));
    console.log('📊 RAKUTEN CONTENT CLEANING SUMMARY');
    console.log('='.repeat(70));
    console.log(`Total products: ${total}`);
    console.log(`✅ Successfully cleaned: ${successful.length}`);
    console.log(`❌ Failed: ${failed.length}`);
    console.log(`🔍 Not found: ${notFound.length}`);
    console.log(`ℹ️  No Rakuten content: ${noChangesNeeded.length}`);
    console.log(`🏷️  Total EC-UP patterns removed: ${totalPatternsRemoved}`);
    console.log(`💾 Total bytes removed: ${(totalBytesRemoved / 1024).toFixed(1)} KB`);
    
    if (total > 0) {
      const successRate = ((successful.length + noChangesNeeded.length) / total * 100).toFixed(1);
      console.log(`📈 Success rate: ${successRate}%`);
    }
    
    if (shopifyConfig.dryRun) {
      console.log('\n🔍 This was a DRY RUN - no actual cleaning performed');
      console.log('💡 Set DRY_RUN=false in .env to perform actual cleaning');
    }
    
    console.log('\n💡 Next steps:');
    console.log('   1. Review clean results in reports/03_rakuten_clean_results.json');
    console.log('   2. Check cleaned products in Shopify admin');
    console.log('   3. Continue with other processing scripts if needed');
  }
}

async function main() {
  console.log('='.repeat(70));
  console.log('🧹 RAKUTEN CONTENT CLEANING');
  console.log('='.repeat(70));

  const cleaner = new RakutenContentCleaner();

  try {
    await cleaner.initialize();
    
    const rakutenData = cleaner.loadRakutenDataFromJSON();
    
    if (rakutenData.length === 0) {
      console.log('⚠️ No products with Rakuten content to process');
      return;
    }

    // Confirmation for live cleaning
    if (!shopifyConfig.dryRun) {
      console.log(`\n⚠️  LIVE CLEANING MODE - This will remove EC-UP content from ${rakutenData.length} products!`);
      console.log('Press Ctrl+C to cancel or wait 5 seconds to continue...');
      await new Promise(resolve => setTimeout(resolve, 5000));
    }

    await cleaner.processProducts(rakutenData);
    cleaner.saveCleanResults();
    cleaner.printSummary();

    console.log('\n🎉 Rakuten content cleaning completed!');

  } catch (error) {
    console.error(`\n❌ Rakuten content cleaning failed: ${error.message}`);
    console.error(error.stack);
    process.exit(1);
  }
}

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.log('\n⏹️ Rakuten content cleaning interrupted by user');
  process.exit(0);
});

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}