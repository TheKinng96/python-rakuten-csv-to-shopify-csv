#!/usr/bin/env node
/**
 * Check metafield definitions for custom.size and custom.search_size
 * to understand their types before migration
 */

import { validateConfig } from './config.js';
import { ShopifyGraphQLClient } from './shopify-client.js';

// Query to get product metafield definitions
const GET_PRODUCT_METAFIELD_DEFINITIONS = `
  query getProductMetafieldDefinitions($namespace: String!, $first: Int!) {
    metafieldDefinitions(namespace: $namespace, ownerType: PRODUCT, first: $first) {
      edges {
        node {
          id
          key
          name
          description
          namespace
          type {
            name
            category
          }
          validations {
            name
            value
          }
          ownerType
        }
      }
    }
  }
`;

// Query to get variant metafield definitions
const GET_VARIANT_METAFIELD_DEFINITIONS = `
  query getVariantMetafieldDefinitions($namespace: String!, $first: Int!) {
    metafieldDefinitions(namespace: $namespace, ownerType: PRODUCTVARIANT, first: $first) {
      edges {
        node {
          id
          key
          name
          description
          namespace
          type {
            name
            category
          }
          validations {
            name
            value
          }
          ownerType
        }
      }
    }
  }
`;

// Query to get product with both metafields for comparison
const GET_PRODUCT_METAFIELDS = `
  query getProductMetafields($handle: String!) {
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
            type
            definition {
              id
              name
              type {
                name
                category
              }
            }
          }
        }
      }
      variants(first: 5) {
        edges {
          node {
            id
            displayName
            metafields(namespace: "custom", first: 10) {
              edges {
                node {
                  key
                  value
                  namespace
                  type
                  definition {
                    id
                    name
                    type {
                      name
                      category
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
`;

class MetafieldDefinitionChecker {
  constructor() {
    this.client = null;
  }

  async initialize() {
    console.log('🔧 Initializing Shopify GraphQL client...');
    
    if (!validateConfig()) {
      throw new Error('Configuration validation failed');
    }

    this.client = new ShopifyGraphQLClient(false);
    await this.client.testConnection();
    console.log('✅ Connected to Shopify store');
  }

  async checkMetafieldDefinitions() {
    console.log('\n🔍 Checking metafield definitions...');
    
    try {
      // Get product metafield definitions
      const productResult = await this.client.query(GET_PRODUCT_METAFIELD_DEFINITIONS, {
        namespace: 'custom',
        first: 50
      });
      
      // Get variant metafield definitions
      const variantResult = await this.client.query(GET_VARIANT_METAFIELD_DEFINITIONS, {
        namespace: 'custom',
        first: 50
      });
      
      const productDefinitions = productResult.metafieldDefinitions.edges.map(edge => edge.node);
      const variantDefinitions = variantResult.metafieldDefinitions.edges.map(edge => edge.node);
      
      console.log(`\n📊 Found ${productDefinitions.length} product metafield definitions`);
      console.log(`📊 Found ${variantDefinitions.length} variant metafield definitions`);
      
      const sizeDefinition = variantDefinitions.find(def => def.key === 'size');
      const searchSizeDefinition = productDefinitions.find(def => def.key === 'search_size');
      
      console.log('\n' + '='.repeat(60));
      console.log('📏 SIZE METAFIELD DEFINITIONS');
      console.log('='.repeat(60));
      
      if (sizeDefinition) {
        console.log('\n✅ custom.size definition (VARIANT):');
        console.log(`   ID: ${sizeDefinition.id}`);
        console.log(`   Name: ${sizeDefinition.name}`);
        console.log(`   Type: ${sizeDefinition.type.name}`);
        console.log(`   Category: ${sizeDefinition.type.category}`);
        console.log(`   Owner: ${sizeDefinition.ownerType}`);
        if (sizeDefinition.description) {
          console.log(`   Description: ${sizeDefinition.description}`);
        }
        if (sizeDefinition.validations.length > 0) {
          console.log(`   Validations: ${sizeDefinition.validations.map(v => `${v.name}=${v.value}`).join(', ')}`);
        }
      } else {
        console.log('\n❌ custom.size definition not found (variant)');
      }
      
      if (searchSizeDefinition) {
        console.log('\n✅ custom.search_size definition (PRODUCT):');
        console.log(`   ID: ${searchSizeDefinition.id}`);
        console.log(`   Name: ${searchSizeDefinition.name}`);
        console.log(`   Type: ${searchSizeDefinition.type.name}`);
        console.log(`   Category: ${searchSizeDefinition.type.category}`);
        console.log(`   Owner: ${searchSizeDefinition.ownerType}`);
        if (searchSizeDefinition.description) {
          console.log(`   Description: ${searchSizeDefinition.description}`);
        }
        if (searchSizeDefinition.validations.length > 0) {
          console.log(`   Validations: ${searchSizeDefinition.validations.map(v => `${v.name}=${v.value}`).join(', ')}`);
        }
      } else {
        console.log('\n❌ custom.search_size definition not found (product)');
      }
      
      // Compare types
      if (sizeDefinition && searchSizeDefinition) {
        console.log('\n🔄 Type Comparison:');
        const typesMatch = sizeDefinition.type.name === searchSizeDefinition.type.name;
        console.log(`   size type: ${sizeDefinition.type.name}`);
        console.log(`   search_size type: ${searchSizeDefinition.type.name}`);
        console.log(`   Types match: ${typesMatch ? '✅' : '❌'}`);
        
        if (!typesMatch) {
          console.log('\n⚠️  TYPE MISMATCH DETECTED!');
          console.log('   Migration script needs to use the correct type for search_size');
          console.log(`   Use type: "${searchSizeDefinition.type.name}"`);
        }
      }
      
      return { sizeDefinition, searchSizeDefinition };
      
    } catch (error) {
      console.error(`❌ Failed to get metafield definitions: ${error.message}`);
      throw error;
    }
  }

  async checkProductMetafields(handle) {
    console.log(`\n🔍 Checking actual metafields for product: ${handle}`);
    
    try {
      const result = await this.client.query(GET_PRODUCT_METAFIELDS, { handle });
      const product = result.productByHandle;
      
      if (!product) {
        console.log(`❌ Product not found: ${handle}`);
        return;
      }
      
      console.log(`\n📦 Product: ${product.title} (${product.handle})`);
      
      // Check product metafields
      const productMetafields = (product.metafields?.edges || []).map(edge => edge.node);
      const searchSizeMetafield = productMetafields.find(meta => meta.key === 'search_size');
      
      if (searchSizeMetafield) {
        console.log('\n✅ Product custom.search_size:');
        console.log(`   Value: "${searchSizeMetafield.value}"`);
        console.log(`   Type: ${searchSizeMetafield.type}`);
        if (searchSizeMetafield.definition) {
          console.log(`   Definition Type: ${searchSizeMetafield.definition.type.name}`);
          console.log(`   Definition ID: ${searchSizeMetafield.definition.id}`);
        }
      } else {
        console.log('\n❌ Product does not have custom.search_size');
      }
      
      // Check variant metafields
      const variants = product.variants?.edges || [];
      console.log(`\n🔍 Checking ${variants.length} variants:`);
      
      variants.forEach((variantEdge, index) => {
        const variant = variantEdge.node;
        const variantMetafields = (variant.metafields?.edges || []).map(edge => edge.node);
        const sizeMetafield = variantMetafields.find(meta => meta.key === 'size');
        
        console.log(`\n   Variant ${index + 1}: ${variant.displayName}`);
        if (sizeMetafield) {
          console.log(`   ✅ Has custom.size: "${sizeMetafield.value}"`);
          console.log(`   Type: ${sizeMetafield.type}`);
          if (sizeMetafield.definition) {
            console.log(`   Definition Type: ${sizeMetafield.definition.type.name}`);
            console.log(`   Definition ID: ${sizeMetafield.definition.id}`);
          }
        } else {
          console.log(`   ❌ No custom.size metafield`);
        }
      });
      
    } catch (error) {
      console.error(`❌ Failed to get product metafields: ${error.message}`);
      throw error;
    }
  }
}

async function main() {
  const handle = process.argv[2] || 'the-t-shirt';
  
  console.log('='.repeat(70));
  console.log('🔍 METAFIELD DEFINITION CHECKER');
  console.log('='.repeat(70));

  const checker = new MetafieldDefinitionChecker();

  try {
    await checker.initialize();
    
    // Check definitions first
    const definitions = await checker.checkMetafieldDefinitions();
    
    // Check actual product metafields
    await checker.checkProductMetafields(handle);
    
    // Provide recommendations
    console.log('\n' + '='.repeat(70));
    console.log('💡 RECOMMENDATIONS');
    console.log('='.repeat(70));
    
    if (definitions.searchSizeDefinition) {
      const correctType = definitions.searchSizeDefinition.type.name;
      console.log(`\n✅ For migration, use type: "${correctType}"`);
      console.log(`📝 Update your migration script metafield type to: "${correctType}"`);
    } else {
      console.log(`\n⚠️  custom.search_size definition not found`);
      console.log(`📝 You may need to create the metafield definition first`);
    }
    
    console.log('\n🎉 Metafield definition check completed!');

  } catch (error) {
    console.error(`\n❌ Check failed: ${error.message}`);
    console.error(error.stack);
    process.exit(1);
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}