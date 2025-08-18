/**
 * GraphQL queries and mutations for creating products using 2025-07 API format
 */

export const LOCATIONS_QUERY = `
  query GetLocations {
    locations(first: 5) {
      edges {
        node {
          id
          name
        }
      }
    }
  }
`;

export const PRODUCT_CREATE_MUTATION = `
  mutation($product: ProductCreateInput!, $media: [CreateMediaInput!]) {
    productCreate(product: $product, media: $media) {
      product {
        id
        handle
        title
        status
        options {
          id
          name
          position
          optionValues {
            id
            name
          }
        }
        variants(first: 5) {
          edges {
            node {
              id
              price
              inventoryItem {
                id
              }
            }
          }
        }
        media(first: 10) {
          edges {
            node {
              ... on MediaImage {
                id
                originalSource {
                  url
                }
              }
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
`;

export const PRODUCT_OPTIONS_CREATE_MUTATION = `
  mutation($productId: ID!, $options: [OptionCreateInput!]!) {
    productOptionsCreate(productId: $productId, options: $options) {
      productOptions {
        id
        name
        position
        optionValues {
          id
          name
        }
      }
      userErrors {
        field
        message
      }
    }
  }
`;

// need to use this one https://shopify.dev/docs/api/admin-graphql/latest/mutations/productVariantsBulkUpdate#argument-variants
export const PRODUCT_VARIANT_UPDATE_MUTATION = `
  mutation($input: ProductVariantInput!) {
    productVariantUpdate(input: $input) {
      productVariant {
        id
        price
        compareAtPrice
        inventoryItem {
          id
          sku
          requiresShipping
        }
        inventoryQuantity
        selectedOptions {
          name
          value
        }
        image {
          id
          url
        }
      }
      userErrors {
        field
        message
      }
    }
  }
`;

export const PRODUCT_VARIANTS_BULK_CREATE_MUTATION = `
  mutation($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
    productVariantsBulkCreate(productId: $productId, variants: $variants) {
      productVariants {
        id
        price
        compareAtPrice
        inventoryItem {
          id
          sku
          requiresShipping
        }
        inventoryQuantity
        selectedOptions {
          name
          value
        }
        image {
          id
          url
        }
      }
      userErrors {
        field
        message
      }
    }
  }
`;