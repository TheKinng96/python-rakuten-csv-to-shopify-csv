#!/usr/bin/env node
/**
 * Fix HTML table issues in Shopify products via GraphQL
 * 
 * This script:
 * 1. Reads html_tables_to_fix.json from shared directory
 * 2. Finds products by handle and fixes HTML table issues
 * 3. Provides progress logging and error handling
 * 4. Saves fixing results for audit
 */

import { readFileSync, writeFileSync, existsSync } from 'fs';
import { join } from 'path';
import { JSDOM } from 'jsdom';
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

class HTMLTableFixer {
  constructor() {
    this.client = null;
    this.fixResults = {
      successful: [],
      failed: [],
      notFound: [],
      noChangesNeeded: []
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

  loadTableDataFromJSON() {
    const jsonPath = join(pathConfig.sharedPath, 'html_tables_to_fix.json');
    
    if (!existsSync(jsonPath)) {
      throw new Error(`HTML tables JSON file not found: ${jsonPath}`);
    }

    console.log(`📂 Loading HTML table data from ${jsonPath}...`);
    
    const jsonData = JSON.parse(readFileSync(jsonPath, 'utf-8'));
    
    if (!jsonData.data || !Array.isArray(jsonData.data)) {
      throw new Error('Invalid JSON structure: expected data array');
    }

    console.log(`✅ Loaded ${jsonData.data.length} products with table issues from JSON`);
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

  fixHTMLTables(htmlContent) {
    if (!htmlContent) return { fixedHtml: '', changesApplied: [] };

    const dom = new JSDOM(htmlContent);
    const document = dom.window.document;
    const changesApplied = [];

    // Fix 1: Close unclosed table tags
    const tables = document.querySelectorAll('table');
    tables.forEach((table, index) => {
      // Ensure table has proper structure
      if (!table.querySelector('tr')) {
        // Table without rows - wrap content in tr/td
        const content = table.innerHTML;
        table.innerHTML = `<tr><td>${content}</td></tr>`;
        changesApplied.push(`Fixed table ${index + 1}: Added missing row structure`);
      }
    });

    // Fix 2: Close unclosed tr tags
    const rows = document.querySelectorAll('tr');
    rows.forEach((row, index) => {
      if (!row.querySelector('td') && !row.querySelector('th')) {
        // Row without cells - wrap content in td
        const content = row.innerHTML;
        row.innerHTML = `<td>${content}</td>`;
        changesApplied.push(`Fixed row ${index + 1}: Added missing cell`);
      }
    });

    // Fix 3: Remove empty cells that are causing layout issues
    const emptyCells = document.querySelectorAll('td:empty, th:empty');
    emptyCells.forEach((cell, index) => {
      cell.remove();
      changesApplied.push(`Removed empty cell ${index + 1}`);
    });

    // Fix 4: Convert layout tables to divs
    const layoutTables = document.querySelectorAll('table[width], table[height], table[cellpadding], table[cellspacing], table[border="0"]');
    layoutTables.forEach((table, index) => {
      if (this.isLayoutTable(table)) {
        const div = document.createElement('div');
        div.innerHTML = this.convertTableToDiv(table);
        table.parentNode.replaceChild(div, table);
        changesApplied.push(`Converted layout table ${index + 1} to div`);
      }
    });

    // Fix 5: Clean up malformed colspan attributes
    const cellsWithColspan = document.querySelectorAll('td[colspan], th[colspan]');
    cellsWithColspan.forEach((cell, index) => {
      const colspan = cell.getAttribute('colspan');
      if (!/^\d+$/.test(colspan)) {
        cell.removeAttribute('colspan');
        changesApplied.push(`Fixed malformed colspan in cell ${index + 1}`);
      }
    });

    // Fix 6: Remove inline styles from table elements
    const elementsWithStyle = document.querySelectorAll('table[style], tr[style], td[style], th[style]');
    elementsWithStyle.forEach((element, index) => {
      element.removeAttribute('style');
      changesApplied.push(`Removed inline styles from ${element.tagName.toLowerCase()} ${index + 1}`);
    });

    const fixedHtml = document.body.innerHTML;
    
    return {
      fixedHtml,
      changesApplied,
      changeCount: changesApplied.length
    };
  }

  isLayoutTable(table) {
    // Heuristics to detect layout tables vs data tables
    const layoutIndicators = [
      table.hasAttribute('width'),
      table.hasAttribute('height'),
      table.hasAttribute('cellpadding'),
      table.hasAttribute('cellspacing'),
      table.getAttribute('border') === '0',
      table.querySelectorAll('tr').length === 1, // Single row
      table.querySelectorAll('td').length === 1  // Single cell
    ];

    return layoutIndicators.filter(Boolean).length >= 2;
  }

  convertTableToDiv(table) {
    // Simple conversion of table to div structure
    const rows = table.querySelectorAll('tr');
    let divContent = '';
    
    rows.forEach(row => {
      const cells = row.querySelectorAll('td, th');
      if (cells.length === 1) {
        // Single cell - just use content
        divContent += cells[0].innerHTML;
      } else {
        // Multiple cells - create flex layout
        divContent += `<div style="display: flex; gap: 10px;">`;
        cells.forEach(cell => {
          divContent += `<div style="flex: 1;">${cell.innerHTML}</div>`;
        });
        divContent += `</div>`;
      }
    });

    return divContent;
  }

  async fixProductTables(productHandle, index, total) {
    try {
      // Find product in Shopify
      const product = await this.findProductByHandle(productHandle);
      
      if (!product) {
        console.log(`   ⚠️  [${index}/${total}] Product not found: ${productHandle}`);
        this.fixResults.notFound.push({
          handle: productHandle,
          reason: 'Product not found in Shopify'
        });
        return;
      }

      const currentHtml = product.bodyHtml || '';
      
      // Fix HTML tables
      const { fixedHtml, changesApplied, changeCount } = this.fixHTMLTables(currentHtml);
      
      if (changeCount === 0) {
        console.log(`   ℹ️  [${index}/${total}] No changes needed: ${productHandle}`);
        this.fixResults.noChangesNeeded.push({
          handle: productHandle,
          title: product.title,
          shopifyId: product.id,
          reason: 'No table issues found or already fixed'
        });
        return;
      }

      // Update product with fixed HTML
      if (shopifyConfig.dryRun) {
        console.log(`   🔍 [DRY RUN] Would fix ${changeCount} table issues in: ${productHandle}`);
        this.fixResults.successful.push({
          handle: productHandle,
          title: product.title,
          status: 'dry_run',
          changesApplied: changeCount,
          shopifyId: product.id,
          changes: changesApplied
        });
        return;
      }

      const result = await this.client.mutate(UPDATE_PRODUCT_MUTATION, {
        input: {
          id: product.id,
          bodyHtml: fixedHtml
        }
      });

      if (result.productUpdate.userErrors.length > 0) {
        const errors = result.productUpdate.userErrors.map(e => `${e.field}: ${e.message}`);
        throw new Error(`Shopify errors: ${errors.join(', ')}`);
      }

      console.log(`   ✅ [${index}/${total}] Fixed ${changeCount} table issues in: ${productHandle}`);
      
      this.fixResults.successful.push({
        handle: productHandle,
        title: product.title,
        status: 'tables_fixed',
        changesApplied: changeCount,
        shopifyId: product.id,
        changes: changesApplied,
        originalHtmlLength: currentHtml.length,
        fixedHtmlLength: fixedHtml.length
      });

    } catch (error) {
      console.error(`   ❌ [${index}/${total}] Failed: ${productHandle} - ${error.message}`);
      
      this.fixResults.failed.push({
        handle: productHandle,
        status: 'failed',
        error: error.message
      });
    }
  }

  async processProducts(tableData) {
    console.log(`\n🚀 Starting HTML table fixing (${tableData.length} products)...`);
    
    const batchSize = shopifyConfig.batchSize;
    const total = tableData.length;
    let processed = 0;

    // Process in batches to respect rate limits
    for (let i = 0; i < tableData.length; i += batchSize) {
      const batch = tableData.slice(i, i + batchSize);
      
      console.log(`\n📦 Processing batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(tableData.length/batchSize)}`);
      
      // Process batch with concurrency limit
      const promises = batch.map((productData, batchIndex) => 
        this.fixProductTables(productData.productHandle, i + batchIndex + 1, total)
      );
      
      await Promise.all(promises);
      processed += batch.length;
      
      // Progress update
      const successRate = (this.fixResults.successful.length / processed * 100).toFixed(1);
      console.log(`📈 Progress: ${processed}/${total} processed (${successRate}% success rate)`);
      
      // Rate limiting delay between batches
      if (i + batchSize < tableData.length) {
        const delay = Math.ceil(1000 / shopifyConfig.maxRequestsPerSecond * batchSize);
        console.log(`⏱️  Waiting ${delay}ms for rate limiting...`);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
  }

  saveFixResults() {
    const resultsPath = join(pathConfig.reportsPath, '02_table_fix_results.json');
    
    const report = {
      timestamp: new Date().toISOString(),
      summary: {
        total: this.fixResults.successful.length + this.fixResults.failed.length + this.fixResults.notFound.length + this.fixResults.noChangesNeeded.length,
        successful: this.fixResults.successful.length,
        failed: this.fixResults.failed.length,
        notFound: this.fixResults.notFound.length,
        noChangesNeeded: this.fixResults.noChangesNeeded.length,
        totalChanges: this.fixResults.successful.reduce((sum, result) => 
          sum + (result.changesApplied || 0), 0
        )
      },
      results: this.fixResults,
      config: {
        dryRun: shopifyConfig.dryRun,
        batchSize: shopifyConfig.batchSize,
        store: 'test'
      }
    };

    writeFileSync(resultsPath, JSON.stringify(report, null, 2));
    console.log(`📄 Fix results saved to: ${resultsPath}`);
  }

  printSummary() {
    const { successful, failed, notFound, noChangesNeeded } = this.fixResults;
    const total = successful.length + failed.length + notFound.length + noChangesNeeded.length;
    const totalChanges = successful.reduce((sum, result) => sum + (result.changesApplied || 0), 0);
    
    console.log('\n' + '='.repeat(70));
    console.log('📊 HTML TABLE FIXING SUMMARY');
    console.log('='.repeat(70));
    console.log(`Total products: ${total}`);
    console.log(`✅ Successfully fixed: ${successful.length}`);
    console.log(`❌ Failed: ${failed.length}`);
    console.log(`🔍 Not found: ${notFound.length}`);
    console.log(`ℹ️  No changes needed: ${noChangesNeeded.length}`);
    console.log(`🔧 Total fixes applied: ${totalChanges}`);
    
    if (total > 0) {
      const successRate = ((successful.length + noChangesNeeded.length) / total * 100).toFixed(1);
      console.log(`📈 Success rate: ${successRate}%`);
    }
    
    if (shopifyConfig.dryRun) {
      console.log('\n🔍 This was a DRY RUN - no actual fixes applied');
      console.log('💡 Set DRY_RUN=false in .env to perform actual fixes');
    }
    
    console.log('\n💡 Next steps:');
    console.log('   1. Review fix results in reports/02_table_fix_results.json');
    console.log('   2. Check fixed products in Shopify admin');
    console.log('   3. Continue with other processing scripts if needed');
  }
}

async function main() {
  console.log('='.repeat(70));
  console.log('🔧 HTML TABLE FIXING');
  console.log('='.repeat(70));

  const fixer = new HTMLTableFixer();

  try {
    await fixer.initialize();
    
    const tableData = fixer.loadTableDataFromJSON();
    
    if (tableData.length === 0) {
      console.log('⚠️ No products with table issues to process');
      return;
    }

    // Confirmation for live fixing
    if (!shopifyConfig.dryRun) {
      console.log(`\n⚠️  LIVE FIXING MODE - This will modify HTML in ${tableData.length} products!`);
      console.log('Press Ctrl+C to cancel or wait 5 seconds to continue...');
      await new Promise(resolve => setTimeout(resolve, 5000));
    }

    await fixer.processProducts(tableData);
    fixer.saveFixResults();
    fixer.printSummary();

    console.log('\n🎉 HTML table fixing completed!');

  } catch (error) {
    console.error(`\n❌ HTML table fixing failed: ${error.message}`);
    console.error(error.stack);
    process.exit(1);
  }
}

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.log('\n⏹️ HTML table fixing interrupted by user');
  process.exit(0);
});

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}