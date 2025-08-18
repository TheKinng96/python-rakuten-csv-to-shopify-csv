/**
 * Test connection to Shopify GraphQL API
 */
import { testConnection } from './shopify-client.js';
import { validateConfig } from './config.js';

async function main() {
  console.log('🧪 Testing Shopify GraphQL Connection');
  console.log('=====================================');
  
  // Validate configuration
  if (!validateConfig()) {
    console.error('❌ Configuration validation failed');
    process.exit(1);
  }
  
  // Test connection to test store
  console.log('\n🔗 Testing connection to test store...');
  const success = await testConnection(true);
  
  if (success) {
    console.log('\n🎉 Connection test successful!');
    console.log('✅ GraphQL client is working');
    console.log('✅ Access token is valid');
    console.log('✅ Store is accessible');
    console.log('\n💡 You can now run the GraphQL operations');
  } else {
    console.log('\n❌ Connection test failed');
    console.log('💡 Please check your .env configuration');
    process.exit(1);
  }
}

main().catch(console.error);