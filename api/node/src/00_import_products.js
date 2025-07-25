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
import { PRODUCT_CREATE_MUTATION, PRODUCT_VARIANTS_BULK_CREATE_MUTATION, LOCATIONS_QUERY } from '../queries/productCreate.js';

class ProductImporter {
  constructor() {
    this.client = null;
    this.shopLocationId = null;
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
    
    // Get shop location ID
    await this.getShopLocationId();
  }

  async getShopLocationId() {
    // Check if location ID is provided via environment variable
    const envLocationId = process.env.SHOPIFY_LOCATION_ID;
    
    if (envLocationId) {
      this.shopLocationId = envLocationId;
      console.log(`✅ Using location ID from environment: ${envLocationId}`);
      return;
    }

    console.log('🔍 Querying for shop locations...');
    
    try {
      const locationsResult = await this.client.query(LOCATIONS_QUERY);
      
      console.log(locationsResult);
      if (!locationsResult.locations?.edges?.length) {
        throw new Error('No locations found in the shop');
      }

      // Look for location with name "Shop location"
      const shopLocation = locationsResult.locations.edges.find(
        edge => edge.node.name === 'Shop location'
      );


      if (!shopLocation) {
        const availableLocations = locationsResult.locations.edges.map(
          edge => `"${edge.node.name}" (${edge.node.id})`
        ).join(', ');
        
        throw new Error(
          `Shop location not found. Available locations: ${availableLocations}. ` +
          `Please set SHOPIFY_LOCATION_ID environment variable or ensure you have a location named "Shop location".`
        );
      }

      this.shopLocationId = shopLocation.node.id;
      console.log(`✅ Found shop location: ${shopLocation.node.name} (${this.shopLocationId})`);
      
    } catch (error) {
      throw new Error(`Failed to get shop location: ${error.message}`);
    }
  }

  loadProductsFromJSON() {
    const jsonPath = join(pathConfig.sharedPath, 'test.json');
    // const jsonPath = join(pathConfig.sharedPath, 'products_for_import.json');
    
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
    // Extract product and media from the new JSON structure
    const product = productData.product;
    const media = productData.media || [];
    const variants = productData.variants || [];
    const options = product.productOptions || [];

    // Include productOptions directly in product creation (valid in 2025-01 API)
    const productInput = {
      handle: product.handle,
      title: product.title,
      descriptionHtml: product.descriptionHtml,
      vendor: product.vendor,
      productType: product.productType,
      tags: product.tags || [],
      status: product.status || 'DRAFT'
    };

    // Add productOptions if they exist
    if (options && options.length > 0) {
      productInput.productOptions = options.map(option => ({
        name: option.name,
        values: option.values.map(value => ({ 
          name: typeof value === 'string' ? value : value.name 
        })),
        position: option.position
      }));
    }

    // Add SEO if present
    if (product.seo && (product.seo.title || product.seo.description)) {
      productInput.seo = product.seo;
    }

    return {
      product: productInput,
      media: media,
      variants: variants
    };
  }

  // Helper function to create media URL to ID mapping from the response
  createMediaMap(mediaResponse) {
    const mediaMap = new Map();
    
    if (mediaResponse && mediaResponse.edges) {
      mediaResponse.edges.forEach(edge => {
        const mediaNode = edge.node;
        if (mediaNode.originalSource && mediaNode.originalSource.url) {
          mediaMap.set(mediaNode.originalSource.url, mediaNode.id);
        }
      });
    }
    
    return mediaMap;
  }

  // Helper function to find media ID by source URL using the map
  findMediaIdBySource(mediaMap, sourceUrl) {
    return mediaMap.get(sourceUrl) || null;
  }

  async importProduct(productData, index, total) {
    try {
      const input = this.convertToShopifyInput(productData);

      const handle = productData.product.handle;
      const title = productData.product.title;
      
      if (shopifyConfig.dryRun) {
        console.log(`   🔍 [DRY RUN] Would import: ${handle}`);
        this.importResults.successful.push({
          handle: handle,
          title: title,
          status: 'dry_run',
          shopifyId: 'dry_run_id'
        });
        return;
      }

      if (!this.client) {
        throw new Error('Shopify client not initialized');
      }

      // Create product with media and options in single step
      const productResult = await this.client.mutate(PRODUCT_CREATE_MUTATION, {
        product: input.product,
        media: input.media
      });
      
      if (productResult.productCreate.userErrors.length > 0) {
        const errors = productResult.productCreate.userErrors.map(e => `${e.field}: ${e.message}`);
        throw new Error(`Product creation errors: ${errors.join(', ')}`);
      }

      const createdProduct = productResult.productCreate.product;
      
      // Create media mapping from the response
      const mediaMap = this.createMediaMap(createdProduct.media);
      
      // Count created options and media
      const optionCount = createdProduct.options ? createdProduct.options.length : 0;
      const mediaCount = input.media.length;
      let variantCount = 0;

      // Create variants if there are any with valid SKUs
      if (input.variants && input.variants.length > 0) {
        const validVariants = input.variants.filter(v => v.sku && v.sku.trim());
        
        if (validVariants.length > 0) {
          const variantInputs = validVariants.map(variant => {
            const variantInput = {
              inventoryItem: {
                sku: variant.sku,
                requiresShipping: variant.requires_shipping !== false
              },
              price: variant.price?.toString() || '0.00',
              inventoryQuantities: [
                {
                  availableQuantity: variant.inventory_quantity || 0,
                  locationId: this.shopLocationId
                }
              ],
              taxable: variant.taxable !== false,
              inventoryPolicy: 'DENY'
            };

            // Add compareAtPrice if it exists
            if (variant.compare_at_price) {
              variantInput.compareAtPrice = variant.compare_at_price.toString();
            }

            // Add optionValues if they exist
            if (variant.optionValues && variant.optionValues.length > 0) {
              variantInput.optionValues = variant.optionValues;
            }

            // Add variant image assignment if it exists
            if (variant.image) {
              const mediaId = this.findMediaIdBySource(mediaMap, variant.image);
              if (mediaId) {
                variantInput.mediaId = mediaId;
              }
            }

            return variantInput;
          });

          const variantResult = await this.client.mutate(PRODUCT_VARIANTS_BULK_CREATE_MUTATION, {
            productId: createdProduct.id,
            variants: variantInputs
          });

          console.log('variants', JSON.stringify({
            productId: createdProduct.id,
            variants: variantInputs
          }));

          if (variantResult.productVariantsBulkCreate.userErrors.length > 0) {
            const errors = variantResult.productVariantsBulkCreate.userErrors.map(e => `${e.field}: ${e.message}`);
            console.warn(`   ⚠️  Variant creation warnings for ${handle}: ${errors.join(', ')}`);
          }

          variantCount = variantResult.productVariantsBulkCreate.productVariants.length;
        }
      }

      this.importResults.successful.push({
        handle: handle,
        title: title,
        status: 'imported',
        shopifyId: createdProduct.id,
        optionCount: optionCount,
        variantCount: variantCount,
        imageCount: mediaCount
      });

      console.log(`   ✅ [${index}/${total}] Imported: ${handle} (ID: ${createdProduct.id}, ${optionCount} options, ${variantCount} variants, ${mediaCount} images)`);

    } catch (error) {
      console.error(`   ❌ [${index}/${total}] Failed: ${productData.product?.handle || 'unknown'} - ${error.message}`);
      
      this.importResults.failed.push({
        handle: productData.product?.handle || 'unknown',
        title: productData.product?.title || 'unknown',
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