/**
 * Get product variants from Shopify store
 */
import { ShopifyGraphQLClient } from './shopify-client.js';

const GET_VARIANTS_QUERY = `
  query getProductVariants {
    products(first: 10) {
      edges {
        node {
          id
          title
          variants(first: 10) {
            edges {
              node {
                id
                title
                price
                sku
                inventoryQuantity
              }
            }
          }
        }
      }
    }
  }
`;

async function getVariants() {
  const client = new ShopifyGraphQLClient(true);
  
  try {
    await client.testConnection();
    
    const result = await client.query(GET_VARIANTS_QUERY);
    
    console.log('📦 Available Products and Variants:\n');
    
    let variantCount = 0;
    
    result.products.edges.forEach(({ node: product }) => {
      console.log(`Product: ${product.title} (${product.id})`);
      
      product.variants.edges.forEach(({ node: variant }) => {
        console.log(`  - Variant: ${variant.title || 'Default'}`);
        console.log(`    ID: ${variant.id}`);
        console.log(`    Price: ¥${variant.price}`);
        console.log(`    SKU: ${variant.sku || 'N/A'}`);
        console.log(`    Stock: ${variant.inventoryQuantity || 'N/A'}`);
        console.log('');
        variantCount++;
      });
    });
    
    console.log(`\n📊 Found ${variantCount} variants across ${result.products.edges.length} products`);
    
    // Return first variant for use in orders
    if (result.products.edges.length > 0) {
      const firstProduct = result.products.edges[0].node;
      if (firstProduct.variants.edges.length > 0) {
        const firstVariant = firstProduct.variants.edges[0].node;
        console.log(`\n✅ Using variant for orders: ${firstVariant.id} (¥${firstVariant.price})`);
        return firstVariant;
      }
    }
    
    return null;
    
  } catch (error) {
    console.error('❌ Failed to get variants:', error.message);
    return null;
  }
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
  getVariants().catch(error => {
    console.error('💥 Script failed:', error.message);
    process.exit(1);
  });
}

export { getVariants };