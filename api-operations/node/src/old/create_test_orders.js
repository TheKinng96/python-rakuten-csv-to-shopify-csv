/**
 * Create test orders with various Japanese payment methods
 * Uses Shopify GraphQL Admin API 2025-07
 */
import { ShopifyGraphQLClient } from './shopify-client.js';

const VARIANT_ID = 'gid://shopify/ProductVariant/51955045400892';

const ORDER_CREATE_MUTATION = `
  mutation orderCreate($order: OrderCreateOrderInput!, $options: OrderCreateOptionsInput) {
    orderCreate(order: $order, options: $options) {
      userErrors {
        field
        message
      }
      order {
        id
        name
        displayFinancialStatus
        sourceName
        sourceIdentifier
        app {
          id
          name
        }
        publication {
          id
        }
        totalPriceSet {
          shopMoney {
            amount
            currencyCode
          }
        }
        customAttributes {
          key
          value
        }
        transactions(first: 10) {
          id
          kind
          status
          amountSet {
            shopMoney {
              amount
              currencyCode
            }
          }
          gateway
        }
        tags
        note
      }
    }
  }
`;

/**
 * Payment method templates
 */
const PAYMENT_METHODS = {
  suica: {
    order: {
      sourceIdentifier: `POS-${Date.now()}-SUICA`,
      lineItems: [
        {
          variantId: VARIANT_ID,
          quantity: 1
        }
      ],
      transactions: [
        {
          kind: "SALE",
          status: "SUCCESS",
          amountSet: {
            shopMoney: {
              amount: "3108.00",
              currencyCode: "JPY"
            }
          },
          gateway: "Suica"
        }
      ],
      customAttributes: [
        {
          key: "order_source",
          value: "point_of_sale"
        },
        {
          key: "payment_method",
          value: "suica"
        },
        {
          key: "ic_card_type", 
          value: "suica"
        },
        {
          key: "card_balance_before",
          value: "5000"
        },
        {
          key: "card_balance_after", 
          value: "4500"
        },
        {
          key: "card_id_masked",
          value: "****1234"
        },
        {
          key: "transaction_timestamp",
          value: new Date().toISOString()
        },
        {
          key: "payment_terminal_id",
          value: "POS_001_SUICA"
        }
      ],
      note: "Suica IC Card Payment - Contactless",
      tags: ["suica", "ic-card", "contactless", "pos", "pos-order"],
      financialStatus: "PAID"
    }
  },

  creditCard: {
    order: {
      sourceIdentifier: `POS-${Date.now()}-CARD`,
      lineItems: [
        {
          variantId: VARIANT_ID,
          quantity: 1
        }
      ],
      transactions: [
        {
          kind: "SALE",
          status: "SUCCESS",
          amountSet: {
            shopMoney: {
              amount: "3108.00",
              currencyCode: "JPY"
            }
          },
          gateway: "Credit Card"
        }
      ],
      customAttributes: [
        {
          key: "order_source",
          value: "point_of_sale"
        },
        {
          key: "payment_method",
          value: "credit_card"
        },
        {
          key: "card_brand",
          value: "visa"
        },
        {
          key: "card_last_four",
          value: "4242"
        },
        {
          key: "authorization_code",
          value: "ABC123"
        },
        {
          key: "transaction_id",
          value: "txn_cc_123456789"
        },
        {
          key: "payment_processor",
          value: "stripe"
        },
        {
          key: "card_type",
          value: "credit"
        }
      ],
      note: "Credit Card Payment - Visa ending in 4242",
      tags: ["credit-card", "visa", "card-payment", "pos", "pos-order"],
      financialStatus: "PAID"
    }
  },

  paypay: {
    order: {
      sourceIdentifier: `POS-${Date.now()}-PAYPAY`,
      lineItems: [
        {
          variantId: VARIANT_ID,
          quantity: 1
        }
      ],
      transactions: [
        {
          kind: "SALE",
          status: "SUCCESS",
          amountSet: {
            shopMoney: {
              amount: "3108.00",
              currencyCode: "JPY"
            }
          },
          gateway: "PayPay"
        }
      ],
      customAttributes: [
        {
          key: "order_source",
          value: "point_of_sale"
        },
        {
          key: "payment_method",
          value: "paypay"
        },
        {
          key: "paypay_transaction_id",
          value: "pp_txn_987654321"
        },
        {
          key: "paypay_order_id",
          value: "pp_order_456789"
        },
        {
          key: "paypay_user_id_masked",
          value: "pp_user_****456"
        },
        {
          key: "qr_code_id",
          value: "qr_123456789"
        },
        {
          key: "payment_method_type",
          value: "qr_code"
        },
        {
          key: "paypay_points_used",
          value: "0"
        },
        {
          key: "paypay_cashback_earned",
          value: "5"
        }
      ],
      note: "PayPay QR Code Payment",
      tags: ["paypay", "qr-payment", "digital-wallet", "pos", "pos-order"],
      financialStatus: "PAID"
    }
  },

  dPayment: {
    order: {
      sourceIdentifier: `POS-${Date.now()}-DPAYMENT`,
      lineItems: [
        {
          variantId: VARIANT_ID,
          quantity: 1
        }
      ],
      transactions: [
        {
          kind: "SALE",
          status: "SUCCESS",
          amountSet: {
            shopMoney: {
              amount: "3108.00",
              currencyCode: "JPY"
            }
          },
          gateway: "d Payment"
        }
      ],
      customAttributes: [
        {
          key: "order_source",
          value: "point_of_sale"
        },
        {
          key: "payment_method",
          value: "d_payment"
        },
        {
          key: "d_payment_transaction_id",
          value: "dp_txn_159753468"
        },
        {
          key: "d_payment_order_id",
          value: "dp_order_852963"
        },
        {
          key: "qr_code_id",
          value: "dp_qr_741258"
        },
        {
          key: "payment_method_type",
          value: "qr_code"
        },
        {
          key: "d_points_used",
          value: "200"
        },
        {
          key: "d_points_earned",
          value: "31"
        }
      ],
      note: "d Payment QR Code Payment - dポイント used: 200",
      tags: ["d-payment", "qr-payment", "digital-wallet", "docomo", "pos", "pos-order"],
      financialStatus: "PAID"
    }
  },

  linePay: {
    order: {
      sourceIdentifier: `POS-${Date.now()}-LINEPAY`,
      lineItems: [
        {
          variantId: VARIANT_ID,
          quantity: 1
        }
      ],
      transactions: [
        {
          kind: "SALE",
          status: "SUCCESS",
          amountSet: {
            shopMoney: {
              amount: "3108.00",
              currencyCode: "JPY"
            }
          },
          gateway: "LINE Pay"
        }
      ],
      customAttributes: [
        {
          key: "order_source",
          value: "point_of_sale"
        },
        {
          key: "payment_method",
          value: "line_pay"
        },
        {
          key: "line_transaction_id",
          value: "line_txn_456789123"
        },
        {
          key: "line_order_id",
          value: "line_order_789456"
        },
        {
          key: "qr_code_id",
          value: "line_qr_321654"
        },
        {
          key: "payment_method_type",
          value: "qr_code"
        },
        {
          key: "line_points_used",
          value: "0"
        },
        {
          key: "line_cashback_earned",
          value: "10"
        }
      ],
      note: "LINE Pay QR Code Payment",
      tags: ["line-pay", "qr-payment", "digital-wallet", "pos", "pos-order"],
      financialStatus: "PAID"
    }
  },
};

/**
 * Create a test order with specified payment method
 */
async function createTestOrder(client, paymentMethod) {
  try {
    console.log(`\n🎫 Creating ${paymentMethod} test order...`);
    
    const variables = PAYMENT_METHODS[paymentMethod];
    if (!variables) {
      throw new Error(`Unknown payment method: ${paymentMethod}`);
    }

    console.log('Debug: Sending variables:', JSON.stringify(variables, null, 2));

    let result;
    try {
      result = await client.mutate(ORDER_CREATE_MUTATION, variables);
      console.log('Debug: Raw result:', JSON.stringify(result, null, 2));
    } catch (mutateError) {
      console.error('Debug: Mutation error:', mutateError.message);
      console.error('Debug: Full error:', mutateError);
      throw mutateError;
    }
    
    if (!result || !result.orderCreate) {
      console.error('❌ Unexpected response structure:', result);
      return null;
    }
    
    if (result.orderCreate.userErrors?.length > 0) {
      console.error('❌ Order creation errors:', result.orderCreate.userErrors);
      return null;
    }
    
    const order = result.orderCreate.order;
    console.log(`✅ Order created: ${order.name} (${order.id})`);
    console.log(`   Status: ${order.displayFinancialStatus}`);
    console.log(`   Total: ¥${order.totalPriceSet.shopMoney.amount}`);
    console.log(`   Payment: ${paymentMethod}`);
    console.log(`   Source: ${order.sourceName || 'Unknown'} (${order.sourceIdentifier || 'N/A'})`);
    console.log(`   App: ${order.app?.name || 'N/A'}`);
    console.log(`   Note: ${order.note}`);
    
    return order;
    
  } catch (error) {
    console.error(`❌ Failed to create ${paymentMethod} order:`, error.message);
    console.error('Stack trace:', error.stack);
    return null;
  }
}

/**
 * Create all test orders
 */
async function createAllTestOrders() {
  console.log('🚀 Creating test orders with Japanese payment methods...\n');
  
  const client = new ShopifyGraphQLClient(true); // Use test store
  
  // Test connection first
  try {
    await client.testConnection();
  } catch (error) {
    console.error('❌ Failed to connect to Shopify:', error.message);
    return;
  }
  
  const results = {
    success: [],
    failed: []
  };
  
  // Create orders for each payment method
  for (const [method, _] of Object.entries(PAYMENT_METHODS)) {
    const order = await createTestOrder(client, method);
    
    if (order) {
      results.success.push({ method, order });
    } else {
      results.failed.push(method);
    }
    
    // Small delay between requests
    await new Promise(resolve => setTimeout(resolve, 500));
  }
  
  // Summary
  console.log('\n📊 Test Order Creation Summary:');
  console.log(`✅ Successful: ${results.success.length}`);
  console.log(`❌ Failed: ${results.failed.length}`);
  
  if (results.success.length > 0) {
    console.log('\n🎉 Successfully created orders:');
    results.success.forEach(({ method, order }) => {
      console.log(`   ${method}: ${order.name} (¥${order.totalPriceSet.shopMoney.amount})`);
    });
  }
  
  if (results.failed.length > 0) {
    console.log('\n❌ Failed to create orders for:', results.failed.join(', '));
  }
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
  createAllTestOrders().catch(error => {
    console.error('💥 Script failed:', error.message);
    process.exit(1);
  });
}

export {
  createTestOrder,
  createAllTestOrders,
  PAYMENT_METHODS
};