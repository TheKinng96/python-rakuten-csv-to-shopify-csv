# GraphQL Test Queries for Size Metafield Migration

Essential GraphQL queries for testing the size metafield migration in Shopify Admin GraphiQL.

## 1. Scan Products and Variants for All Custom Metafields

```graphql
query scanProductsVariants($first: Int!, $after: String) {
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
        variants(first: 20) {
          edges {
            node {
              id
              displayName
              metafields(namespace: "custom", first: 15) {
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

**Variables:**
```json
{
  "first": 10,
  "after": null
}
```

**Look for:**
- Product metafields with `"key": "search_size"`
- Variant metafields with `"key": "size"`

## 2. Test Specific Product by Handle

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
        }
      }
    }
    variants(first: 20) {
      edges {
        node {
          id
          displayName
          metafields(namespace: "custom", first: 15) {
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

## 3. Update Product custom.search_size Metafield

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

**Variables (replace with actual values):**
```json
{
  "product": {
    "id": "gid://shopify/Product/YOUR_PRODUCT_ID_HERE",
    "metafields": [
      {
        "namespace": "custom",
        "key": "search_size",
        "value": "[\"SIZE_VALUE_FROM_VARIANT\"]",
        "type": "list.single_line_text_field"
      }
    ]
  }
}
```

## 4. Verify Product After Migration

```graphql
query verifyProductById($id: ID!) {
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
```

**Variables:**
```json
{
  "id": "gid://shopify/Product/YOUR_PRODUCT_ID_HERE"
}
```

## Testing Workflow

### Step 1: Find Products with Variant Size
1. Use **Query #1** to scan products and look for:
   - Variants with `"key": "size"` (migration candidates)
   - Products with `"key": "search_size"` (already migrated)

### Step 2: Test Single Product
1. Use **Query #2** with a specific product handle
2. Check if variants have `custom.size` and product lacks `custom.search_size`

### Step 3: Test Migration
1. Use **Mutation #3** to migrate one product
2. Replace placeholders with actual product ID and size value

### Step 4: Verify Migration
1. Use **Query #4** to verify the product now has `custom.search_size`
2. Confirm variant `custom.size` is still present (we don't remove it)

## Notes

- **All queries return ALL custom metafields** - filter client-side for specific keys
- **Use the scan script** (`13_scan_variant_size_metafields.js`) for bulk operations
- **GraphQL doesn't support metafield key filtering** - that's why we need comprehensive queries
- **Test in Shopify Admin GraphiQL** or Partner Dashboard GraphiQL explorer