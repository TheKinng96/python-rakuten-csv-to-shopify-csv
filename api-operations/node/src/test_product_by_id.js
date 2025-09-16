#!/usr/bin/env node
/**
 * Test script to fetch product by Shopify ID
 */

import { validateConfig } from './config.js';
import { ShopifyGraphQLClient } from './shopify-client.js';

const GET_PRODUCT_BY_ID = `
  query getProductById($id: ID!) {
    product(id: $id) {
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

async function testProductById(productId) {
  if (!validateConfig()) {
    throw new Error('Configuration validation failed');
  }

  const client = new ShopifyGraphQLClient(true);
  await client.testConnection();
  
  // Convert numeric ID to GraphQL ID format
  const gqlId = `gid://shopify/Product/${productId}`;
  console.log(`🔍 Testing product ID: ${gqlId}`);
  
  try {
    const result = await client.query(GET_PRODUCT_BY_ID, { id: gqlId });
    const product = result.product;
    
    if (!product) {
      console.log(`❌ Product not found: ${gqlId}`);
      return;
    }
    
    console.log(`\n📦 Product: ${product.title}`);
    console.log(`   Handle: ${product.handle}`);
    console.log(`   Shopify ID: ${product.id}`);
    
    // Check product metafields
    const productMetafields = (product.metafields?.edges || []).map(edge => edge.node);
    console.log(`\n   Product metafields: ${productMetafields.length}`);
    if (productMetafields.length > 0) {
      console.log(`   Product metafield keys: ${productMetafields.map(m => `${m.key}="${m.value}"`).join(', ')}`);
    }
    
    // Check variant metafields
    const variants = product.variants?.edges || [];
    console.log(`\n   Variants: ${variants.length}`);
    
    variants.forEach((variantEdge, index) => {
      const variant = variantEdge.node;
      const variantMetafields = (variant.metafields?.edges || []).map(edge => edge.node);
      
      console.log(`\n   Variant ${index + 1}: ${variant.displayName}`);
      console.log(`     ID: ${variant.id}`);
      console.log(`     Metafields: ${variantMetafields.length}`);
      
      if (variantMetafields.length > 0) {
        variantMetafields.forEach(meta => {
          console.log(`     - ${meta.key}: "${meta.value}"`);
        });
      }
    });
    
  } catch (error) {
    console.error(`❌ Failed to fetch product: ${error.message}`);
  }
}

const productId = process.argv[2];
if (!productId) {
  console.error('Usage: node test_product_by_id.js <product_id>');
  console.error('Example: node test_product_by_id.js 10152626094396');
  process.exit(1);
}

testProductById(productId);