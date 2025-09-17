/**
 * Shopify GraphQL client wrapper with rate limiting and error handling
 */
import { createAdminApiClient } from '@shopify/admin-api-client';
import { getStoreConfig, shopifyConfig } from './config.js';

/**
 * Enhanced Shopify GraphQL Client Class
 */
export class ShopifyGraphQLClient {
  constructor(useTestStore = true) {
    const config = getStoreConfig(useTestStore);
    
    this.client = createAdminApiClient({
      storeDomain: config.storeDomain,
      accessToken: config.accessToken,
      apiVersion: config.apiVersion,
    });
    
    this.requestQueue = [];
    this.isProcessingQueue = false;
    this.lastRequestTime = 0;
    this.minRequestInterval = 1000 / shopifyConfig.maxRequestsPerSecond;
  }

  /**
   * Test connection to Shopify
   */
  async testConnection() {
    const query = `
      query {
        shop {
          name
          email
          plan { 
            displayName
          }
        }
      }
    `;

    try {
      const result = await this.query(query);
      console.log(`🏪 Connected to: ${result.shop.name} (${result.shop.domain})`);
      console.log(`📋 Plan: ${result.shop.plan.displayName}`);
      return result;
    } catch (error) {
      throw new Error(`Connection test failed: ${error.message}`);
    }
  }

  /**
   * Execute GraphQL query with rate limiting
   */
  async query(query, variables = {}) {
    return this.makeRequest('query', query, variables);
  }

  /**
   * Execute GraphQL mutation with rate limiting
   */
  async mutate(mutation, variables = {}) {
    return this.makeRequest('mutation', mutation, variables);
  }

  /**
   * Make rate-limited GraphQL request
   */
  async makeRequest(operation, queryString, variables) {
    return new Promise((resolve, reject) => {
      this.requestQueue.push({
        operation,
        queryString,
        variables,
        resolve,
        reject,
        retries: 0
      });

      this.processQueue();
    });
  }

  /**
   * Process the request queue with rate limiting
   */
  async processQueue() {
    if (this.isProcessingQueue || this.requestQueue.length === 0) {
      return;
    }

    this.isProcessingQueue = true;

    while (this.requestQueue.length > 0) {
      const request = this.requestQueue.shift();
      
      try {
        // Rate limiting
        const timeSinceLastRequest = Date.now() - this.lastRequestTime;
        if (timeSinceLastRequest < this.minRequestInterval) {
          await new Promise(resolve => 
            setTimeout(resolve, this.minRequestInterval - timeSinceLastRequest)
          );
        }

        this.lastRequestTime = Date.now();

        // Execute request
        const result = await this.executeRequest(request);
        request.resolve(result);

      } catch (error) {
        // Handle retries for rate limiting and network errors
        if (this.shouldRetry(error, request.retries)) {
          request.retries++;
          console.warn(`⚠️ Retrying request (${request.retries}/3): ${error.message}`);
          
          // Exponential backoff
          const delay = Math.min(1000 * Math.pow(2, request.retries - 1), 10000);
          await new Promise(resolve => setTimeout(resolve, delay));
          
          this.requestQueue.unshift(request); // Add back to front of queue
        } else {
          request.reject(error);
        }
      }
    }

    this.isProcessingQueue = false;
  }

  /**
   * Execute the actual GraphQL request
   */
  async executeRequest(request) {
    const { queryString, variables } = request;

    const response = await this.client.request(queryString, { variables });
    
    // Handle error response structure from Shopify client
    if (response.errors) {
      const graphQLErrors = response.errors.graphQLErrors || [];
      if (graphQLErrors.length > 0) {
        const errorMessages = graphQLErrors.map(e => e.message).join('; ');
        console.error('GraphQL Errors:', graphQLErrors);
        throw new Error(`GraphQL errors: ${errorMessages}`);
      }
      // Generic error
      throw new Error(response.errors.message || 'Unknown GraphQL error');
    }

    // Handle both response.data and direct response structures
    if (response.data) {
      return response.data;
    } else if (response.orderCreate) {
      // Response might be the data directly
      return response;
    } else {
      // No data property - might be an error
      throw new Error('Invalid response structure from Shopify API');
    }
  }

  /**
   * Determine if request should be retried
   */
  shouldRetry(error, retryCount) {
    if (retryCount >= 3) return false;

    // Retry on rate limiting, network errors, or temporary server errors
    const retryableErrors = [
      'rate limit',
      'throttle',
      'network',
      'timeout',
      'connection',
      'ECONNRESET',
      'ETIMEDOUT',
      '503', // Service Unavailable
      '502', // Bad Gateway
      '500', // Internal Server Error
    ];

    const errorMessage = error.message.toLowerCase();
    return retryableErrors.some(retryable => errorMessage.includes(retryable));
  }
}

/**
 * Create Shopify GraphQL client (legacy function)
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