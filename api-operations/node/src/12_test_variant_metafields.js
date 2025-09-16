#!/usr/bin/env node
/**
 * Test script to check variant metafields custom.size availability
 * 
 * Usage:
 *   node 12_test_variant_metafields.js                  # Test first 5 products
 *   node 12_test_variant_metafields.js --test-handle <handle> # Test specific product
 */

import { readFileSync } from 'fs';
import { join } from 'path';
import { pathConfig, validateConfig } from './config.js';
import { ShopifyGraphQLClient } from './shopify-client.js';

// GraphQL query to fetch product with variants and their metafields
const GET_PRODUCTS_WITH_VARIANT_METAFIELDS = `
  query getProductsWithVariantMetafields($first: Int!, $after: String) {
    products(first: $first, after: $after) {
      edges {
        node {
          id
          handle
          title
          metafields(namespace: "custom", first: 10) {
            edges {
              node {
                key
                value
              }
            }
          }
          variants(first: 10) {
            edges {
              node {
                id
                displayName
                metafields(namespace: "custom", first: 10) {
                  edges {
                    node {
                      key
                      value
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
      metafields(namespace: "custom", first: 10) {
        edges {
          node {
            key
            value
          }
        }
      }
      variants(first: 10) {
        edges {
          node {
            id
            displayName
            metafields(namespace: "custom", first: 10) {
              edges {
                node {
                  key
                  value
                }
              }
            }
          }
        }
      }
    }
  }
`;

class VariantMetafieldTester {
  constructor() {
    this.client = null;
    this.testResults = {
      productsWithVariantSize: [],
      productsWithProductSearchSize: [],
      productsWithBoth: [],
      productsWithNeither: [],
      sizeValuesFound: new Set()
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

  async testSpecificProduct(handle) {
    console.log(`\n🔍 Testing specific product: ${handle}`);
    
    try {
      const result = await this.client.query(GET_PRODUCT_BY_HANDLE, { handle });
      const product = result.productByHandle;
      
      if (!product) {
        console.log(`❌ Product not found: ${handle}`);
        return;
      }
      
      this.analyzeProduct(product);
      
    } catch (error) {
      console.error(`❌ Failed to fetch product ${handle}: ${error.message}`);
    }
  }

  async testMultipleProducts(limit = 5) {
    console.log(`\n🔍 Testing first ${limit} products for variant metafields...`);
    
    let cursor = null;
    let testedCount = 0;
    
    while (testedCount < limit) {
      const remaining = Math.min(5, limit - testedCount);
      
      try {
        const result = await this.client.query(GET_PRODUCTS_WITH_VARIANT_METAFIELDS, {
          first: remaining,
          after: cursor
        });
        
        const products = result.products.edges.map(edge => edge.node);
        
        for (const product of products) {
          this.analyzeProduct(product);
          testedCount++;
        }
        
        if (!result.products.pageInfo.hasNextPage || testedCount >= limit) {
          break;
        }
        
        cursor = result.products.pageInfo.endCursor;
        
      } catch (error) {
        console.error(`❌ Failed to fetch products: ${error.message}`);
        break;
      }
    }
  }

  analyzeProduct(product) {
    console.log(`\n📦 Product: ${product.title} (${product.handle})`);
    console.log(`   Shopify ID: ${product.id}`);
    
    // Check product metafields for search_size
    const productMetafields = (product.metafields?.edges || []).map(edge => edge.node);
    const hasSearchSize = productMetafields.find(meta => meta.key === 'search_size');
    
    console.log(`   Product metafields: ${productMetafields.length}`);
    if (hasSearchSize) {
      console.log(`   ✅ Has custom.search_size: "${hasSearchSize.value}"`);
    }
    
    // List all product metafields for debugging
    if (productMetafields.length > 0) {
      console.log(`   Product metafield keys: ${productMetafields.map(m => m.key).join(', ')}`);
    }
    
    // Check variant metafields for size
    const variants = product.variants?.edges || [];
    let variantSizeFound = null;
    
    console.log(`   Variants: ${variants.length}`);
    
    variants.forEach((variantEdge, index) => {
      const variant = variantEdge.node;
      const variantMetafields = (variant.metafields?.edges || []).map(edge => edge.node);
      const sizeMetafield = variantMetafields.find(meta => meta.key === 'size');
      
      console.log(`   Variant ${index + 1} (${variant.displayName}): ${variantMetafields.length} metafields`);
      
      if (variantMetafields.length > 0) {
        console.log(`     Variant metafield keys: ${variantMetafields.map(m => m.key).join(', ')}`);
      }
      
      if (sizeMetafield) {
        console.log(`     ✅ Has custom.size: "${sizeMetafield.value}"`);
        if (!variantSizeFound) {
          variantSizeFound = sizeMetafield.value;
        }
        this.testResults.sizeValuesFound.add(sizeMetafield.value);
      }
    });
    
    // Categorize result
    if (variantSizeFound && hasSearchSize) {
      this.testResults.productsWithBoth.push({
        handle: product.handle,
        title: product.title,
        id: product.id,
        variantSize: variantSizeFound,
        searchSize: hasSearchSize.value
      });
      console.log(`   📊 Category: Has both variant size and product search_size`);
    } else if (variantSizeFound) {
      this.testResults.productsWithVariantSize.push({
        handle: product.handle,
        title: product.title,
        id: product.id,
        variantSize: variantSizeFound
      });
      console.log(`   📊 Category: Has variant size only`);
    } else if (hasSearchSize) {
      this.testResults.productsWithProductSearchSize.push({
        handle: product.handle,
        title: product.title,
        id: product.id,
        searchSize: hasSearchSize.value
      });
      console.log(`   📊 Category: Has product search_size only`);
    } else {
      this.testResults.productsWithNeither.push({
        handle: product.handle,
        title: product.title,
        id: product.id
      });
      console.log(`   📊 Category: Has neither size metafield`);
    }
  }

  printSummary() {
    console.log('\n' + '='.repeat(70));
    console.log('📊 VARIANT METAFIELD TEST SUMMARY');
    console.log('='.repeat(70));
    
    const total = this.testResults.productsWithVariantSize.length +
                  this.testResults.productsWithProductSearchSize.length +
                  this.testResults.productsWithBoth.length +
                  this.testResults.productsWithNeither.length;
    
    console.log(`Total products tested: ${total}`);
    console.log(`✅ Products with variant custom.size: ${this.testResults.productsWithVariantSize.length}`);
    console.log(`✅ Products with product custom.search_size: ${this.testResults.productsWithProductSearchSize.length}`);
    console.log(`🔄 Products with both: ${this.testResults.productsWithBoth.length}`);
    console.log(`❌ Products with neither: ${this.testResults.productsWithNeither.length}`);
    
    console.log(`\n📏 Unique size values found: ${this.testResults.sizeValuesFound.size}`);
    if (this.testResults.sizeValuesFound.size > 0) {
      console.log('Size values:');
      [...this.testResults.sizeValuesFound].sort().forEach(value => {
        console.log(`  - "${value}"`);
      });
    }
    
    if (this.testResults.productsWithVariantSize.length > 0) {
      console.log('\n💡 Migration candidates (have variant size, need product search_size):');
      this.testResults.productsWithVariantSize.forEach(product => {
        console.log(`  - ${product.handle}: "${product.variantSize}"`);
      });
    }
    
    if (this.testResults.productsWithBoth.length > 0) {
      console.log('\n⚠️ Products with both (may need comparison):');
      this.testResults.productsWithBoth.forEach(product => {
        const match = product.variantSize === product.searchSize ? '✅' : '❌';
        console.log(`  - ${product.handle}: variant="${product.variantSize}" vs search="${product.searchSize}" ${match}`);
      });
    }
  }
}

async function main() {
  const args = process.argv.slice(2);
  
  // Check for test handle argument
  let testHandle = null;
  const testHandleIndex = args.findIndex(arg => arg === '--test-handle');
  if (testHandleIndex !== -1 && args[testHandleIndex + 1]) {
    testHandle = args[testHandleIndex + 1];
  }
  
  console.log('='.repeat(70));
  console.log('🔍 VARIANT METAFIELD TEST');
  console.log('='.repeat(70));

  const tester = new VariantMetafieldTester();

  try {
    await tester.initialize();
    
    if (testHandle) {
      await tester.testSpecificProduct(testHandle);
    } else {
      await tester.testMultipleProducts(10);
    }
    
    tester.printSummary();
    
    console.log('\n🎉 Variant metafield test completed!');

  } catch (error) {
    console.error(`\n❌ Test failed: ${error.message}`);
    console.error(error.stack);
    process.exit(1);
  }
}

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.log('\n⏹️ Test interrupted by user');
  process.exit(0);
});

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}