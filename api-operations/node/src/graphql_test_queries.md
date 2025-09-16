# GraphQL Test Queries for Size Metafield Migration

This file contains GraphQL queries and mutations for testing the size metafield migration in Shopify Admin GraphiQL.

## 0. Check Products with custom.size Metafields (Product Level)

```graphql
query getProductsWithSizeMetafield($first: Int!, $after: String) {
  products(first: $first, after: $after) {
    edges {
      node {
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
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
```

**Variables for testing (first 10 products):**
```json
{
  "first": 10,
  "after": null
}
```

**Look for metafields with `"key": "size"` in the results**

## 1. Check ONLY Variant custom.size Metafields (Focused Query)

```graphql
query getVariantSizeMetafields($first: Int!, $after: String) {
  products(first: $first, after: $after) {
    edges {
      node {
        id
        handle
        title
        variants(first: 20) {
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
```

**Variables for testing (first 5 products):**
```json
{
  "first": 5,
  "after": null
}
```

## 1a. Get Products with Variant Metafields (Full Details)

```graphql
query getProductsWithVariantMetafields($first: Int!, $after: String) {
  products(first: $first, after: $after) {
    edges {
      node {
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
                    type
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
```

**Variables for testing (first 5 products):**
```json
{
  "first": 5,
  "after": null
}
```

## 2. Check Specific Product Variant custom.size by Handle

```graphql
query getProductVariantSizeByHandle($handle: String!) {
  productByHandle(handle: $handle) {
    id
    handle
    title
    variants(first: 20) {
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
              }
            }
          }
        }
      }
    }
  }
}
```

**Variables (replace with actual handle):**
```json
{
  "handle": "your-product-handle-here"
}
```

## 2a. Get Product by Handle (Test specific product)

```graphql
query getProductByHandle($handle: String!) {
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
                type
              }
            }
          }
        }
      }
    }
  }
}
```

**Variables (replace with actual handle):**
```json
{
  "handle": "your-product-handle-here"
}
```

## 3. Get All Tags (for filtering/analysis)

```graphql
query getAllTags($first: Int!, $after: String) {
  shop {
    productTags(first: $first, after: $after) {
      edges {
        node {
          id
          name
        }
      }
      pageInfo {
        hasNextPage
        endCursor
      }
    }
  }
}
```

**Variables:**
```json
{
  "first": 250,
  "after": null
}
```

## 4. Update Product Metafield (custom.search_size)

```graphql
mutation updateProductSearchSize($product: ProductUpdateInput!) {
  productUpdate(product: $product) {
    product {
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
          }
        }
      }
    }
    userErrors {
      field
      message
    }
  }
}
```

**Variables (replace with actual product ID and size value):**
```json
{
  "product": {
    "id": "gid://shopify/Product/YOUR_PRODUCT_ID_HERE",
    "metafields": [
      {
        "namespace": "custom",
        "key": "search_size",
        "value": "SIZE_VALUE_FROM_VARIANT",
        "type": "single_line_text_field"
      }
    ]
  }
}
```

## 5. Get Product by ID (Test after migration)

```graphql
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
          type
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
                type
              }
            }
          }
        }
      }
    }
  }
}
```

**Variables:**
```json
{
  "id": "gid://shopify/Product/YOUR_PRODUCT_ID_HERE"
}
```

## 6. Search Products with Custom Metafields

```graphql
query searchProductsWithSizeMetafields($query: String!, $first: Int!) {
  products(query: $query, first: $first) {
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
              namespace
            }
          }
        }
        variants(first: 5) {
          edges {
            node {
              id
              displayName
              metafields(namespace: "custom", first: 5) {
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
  }
}
```

**Variables (search for products with metafields):**
```json
{
  "query": "metafield:custom.size OR metafield:custom.search_size",
  "first": 10
}
```

## 7. Bulk Query - Multiple Products by IDs

```graphql
query getMultipleProductsById($ids: [ID!]!) {
  nodes(ids: $ids) {
    ... on Product {
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
                  namespace
                }
              }
            }
          }
        }
      }
    }
  }
}
```

**Variables (replace with actual product IDs):**
```json
{
  "ids": [
    "gid://shopify/Product/PRODUCT_ID_1",
    "gid://shopify/Product/PRODUCT_ID_2",
    "gid://shopify/Product/PRODUCT_ID_3"
  ]
}
```

## Testing Instructions

### Step 1: Find Products with Variant Size
1. Use query #1 or #2 to find products that have variant `custom.size` metafields
2. Note the product ID and the size value from the first variant

### Step 2: Test Migration
1. Use mutation #4 to update the product with `custom.search_size`
2. Replace `YOUR_PRODUCT_ID_HERE` with the actual product ID
3. Replace `SIZE_VALUE_FROM_VARIANT` with the size value from step 1

### Step 3: Verify Migration
1. Use query #5 to check that the product now has `custom.search_size`
2. Verify that the variant still has `custom.size` (we don't remove it)

### Step 4: Bulk Testing
1. Use query #6 to search for products with size metafields
2. Use query #7 to test multiple products at once

## Notes

- All queries use the Admin API 2025-07 version
- Replace placeholder values in variables with actual data
- Test in Shopify Admin > Settings > Notifications > Webhooks > GraphiQL app
- Or use Shopify Partner Dashboard GraphiQL explorer
- The migration script preserves variant `custom.size` metafields
- Only products without `custom.search_size` should be migrated