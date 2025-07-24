/**
 * Shopify GraphQL client wrapper
 */
import { createAdminApiClient } from '@shopify/admin-api-client';
import { getStoreConfig, shopifyConfig } from './config.js';

/**
 * Create Shopify GraphQL client
 */
export function createShopifyClient(useTestStore = true) {
  const config = getStoreConfig(useTestStore);
  
  const client = createAdminApiClient({
    storeDomain: config.storeDomain,
    accessToken: config.accessToken,
    apiVersion: config.apiVersion,
  });
  
  return client;
}

/**
 * Execute GraphQL query with error handling and retries
 */
export async function executeQuery(client, query, variables = {}, maxRetries = shopifyConfig.maxRetries) {
  let lastError;
  
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const {data, errors, extensions} = await client.request(query, { variables });
      
      console.log('GraphQL response:', data, errors, extensions);
      // Check for GraphQL errors
      if (errors && errors.length > 0) {
        throw new Error(`GraphQL errors: ${JSON.stringify(errors)}`);
      }
      
      return data;
      
    } catch (error) {
      console.error('GraphQL request error:', {
        message: error.message,
        stack: error.stack,
        response: error.response || undefined,
        errors: error.errors || undefined,
        graphQLErrors: error.graphQLErrors || undefined,
        networkError: error.networkError || undefined
      });
      lastError = error;
      
      if (attempt < maxRetries) {
        console.warn(`Query attempt ${attempt} failed, retrying in ${shopifyConfig.retryDelay}ms...`);
        await new Promise(resolve => setTimeout(resolve, shopifyConfig.retryDelay));
      }
    }
  }
  
  throw lastError;
}

/**
 * Execute bulk operation and wait for completion
 */
export async function executeBulkOperation(client, mutation, variables = {}) {
  console.log('Starting bulk operation...');
  
  // Start bulk operation
  const bulkResponse = await executeQuery(client, mutation, variables);
  
  if (!bulkResponse.bulkOperationRunMutation?.bulkOperation?.id) {
    throw new Error('Failed to start bulk operation');
  }
  
  const operationId = bulkResponse.bulkOperationRunMutation.bulkOperation.id;
  console.log(`Bulk operation started: ${operationId}`);
  
  // Poll for completion
  const pollQuery = `
    query GetBulkOperation($id: ID!) {
      node(id: $id) {
        ... on BulkOperation {
          id
          status
          errorCode
          createdAt
          completedAt
          objectCount
          fileSize
          url
        }
      }
    }
  `;
  
  let status = 'RUNNING';
  while (status === 'RUNNING') {
    await new Promise(resolve => setTimeout(resolve, 2000)); // Wait 2 seconds
    
    const pollResponse = await executeQuery(client, pollQuery, { id: operationId });
    const operation = pollResponse.node;
    
    status = operation.status;
    console.log(`Bulk operation status: ${status} (${operation.objectCount || 0} objects processed)`);
    
    if (status === 'COMPLETED') {
      console.log(`✅ Bulk operation completed successfully`);
      return operation;
    } else if (status === 'FAILED' || status === 'CANCELED') {
      throw new Error(`Bulk operation ${status.toLowerCase()}: ${operation.errorCode}`);
    }
  }
}

/**
 * Test connection to Shopify
 */
export async function testConnection(useTestStore = true) {
  try {
    const client = createShopifyClient(useTestStore);
    
    const query = `
      query {
        shop {
          name
          email
        }
      }
    `;
    
    const response = await executeQuery(client, query);

    console.log('RAW GraphQL response:', response);
    
    if (!response || !response.shop) {
      throw new Error('Shop property missing in response. Full response: ' + JSON.stringify(response));
    }
    
    console.log(`✅ Connected to Shopify store: ${response.shop.name}`);
    console.log(`   Domain: ${response.shop.domain}`);
    console.log(`   Email: ${response.shop.email}`);
    
    return true;
    
  } catch (error) {
    console.error(`❌ Failed to connect to Shopify: ${error.message}`);
    return false;
  }
}

export default {
  createShopifyClient,
  executeQuery,
  executeBulkOperation,
  testConnection,
};