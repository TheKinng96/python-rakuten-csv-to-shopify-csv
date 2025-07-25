/**
 * GraphQL mutation for creating products using 2025-07 API format
 */

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

export const PRODUCT_VARIANTS_BULK_CREATE_MUTATION = `
  mutation($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
    productVariantsBulkCreate(productId: $productId, variants: $variants) {
      productVariants {
        id
        sku
        price
        compareAtPrice
        inventoryQuantity
        selectedOptions {
          name
          value
        }
        image {
          id
          src
        }
      }
      userErrors {
        field
        message
      }
    }
  }
`;