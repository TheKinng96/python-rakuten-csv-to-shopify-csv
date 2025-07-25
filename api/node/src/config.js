/**
 * Configuration management for Shopify GraphQL operations
 * Uses the shared .env file from the parent api directory
 */
import { config } from 'dotenv';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

// Load environment variables from parent api directory
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const envPath = join(__dirname, '../../.env');

config({ path: envPath });

export const shopifyConfig = {
  // Store configurations (reusing existing env vars)
  testStore: {
    url: process.env.SHOPIFY_TEST_STORE_URL || '',
    accessToken: process.env.SHOPIFY_TEST_ACCESS_TOKEN || '',
  },
  prodStore: {
    url: process.env.SHOPIFY_PROD_STORE_URL || '',
    accessToken: process.env.SHOPIFY_PROD_ACCESS_TOKEN || '',
  },
  
  // Processing settings (reusing existing env vars)
  dryRun: process.env.DRY_RUN?.toLowerCase() === 'true',
  batchSize: parseInt(process.env.BATCH_SIZE) || 250,
  chunkSize: parseInt(process.env.CHUNK_SIZE) || 1000,
  maxRequestsPerSecond: parseInt(process.env.MAX_REQUESTS_PER_SECOND) || 40,
  logLevel: process.env.LOG_LEVEL || 'INFO',
  
  // GraphQL specific settings
  apiVersion: '2025-07',
  maxRetries: 3,
  retryDelay: 1000, // milliseconds
};

export const pathConfig = {
  // Use existing data and reports directories
  csvDataPath: join(__dirname, '../../data'),
  reportsPath: join(__dirname, '../../reports'),
  sharedPath: join(__dirname, '../../shared'),
  queriesPath: join(__dirname, '../queries'),
  
  // Python scripts path for integration
  pythonScriptsPath: join(__dirname, '../../scripts'),
};

/**
 * Get store configuration for Shopify GraphQL client
 */
export function getStoreConfig(useTestStore = true) {
  const config = useTestStore ? shopifyConfig.testStore : shopifyConfig.prodStore;
  
  if (!config.url || !config.accessToken) {
    throw new Error(`Missing ${useTestStore ? 'test' : 'prod'} store configuration in .env file`);
  }
  
  return {
    storeDomain: config.url,
    accessToken: config.accessToken,
    apiVersion: shopifyConfig.apiVersion,
  };
}

/**
 * Validate configuration
 */
export function validateConfig() {
  const issues = [];
  
  if (!shopifyConfig.testStore.url) {
    issues.push('SHOPIFY_TEST_STORE_URL not set in .env');
  }
  if (!shopifyConfig.testStore.accessToken) {
    issues.push('SHOPIFY_TEST_ACCESS_TOKEN not set in .env');
  }
  
  if (issues.length > 0) {
    console.error('Configuration issues:');
    issues.forEach(issue => console.error(`  - ${issue}`));
    return false;
  }
  
  return true;
}

export default {
  shopifyConfig,
  pathConfig,
  getStoreConfig,
  validateConfig,
};