"""
Step 02: SKU Processing and Handle Generation

Processes Rakuten SKUs to generate Shopify handles and variant information.
Handles variant suffixes (-3s, -6s, -t) and creates proper grouping.
"""

import logging
import pandas as pd
import re
from typing import Dict, Any

logger = logging.getLogger(__name__)


def execute(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process SKUs and generate Shopify handles

    Args:
        data: Pipeline context containing cleaned_df and config

    Returns:
        Dict containing dataframe with handle and variant information
    """
    logger.info("Processing SKUs and generating handles...")

    df = data['cleaned_df'].copy()
    config = data['config']

    # Merge product and variant data structure
    logger.info("Consolidating product and variant data...")

    # Group by 商品管理番号（商品URL） and combine product/variant info
    def consolidate_group(group):
        """Consolidate product and variant info for each handle"""
        handle = group['商品管理番号（商品URL）'].iloc[0]

        # Get product info (from rows with 商品名)
        product_info = group[group['商品名'].notna() & (group['商品名'] != '')]
        # Get variant info (from rows with SKU管理番号)
        variant_info = group[group['SKU管理番号'].notna() & (group['SKU管理番号'] != '')]

        # Debug logging for problematic products
        if handle in ['maruzen-4set00', 'abshiri-r330-t']:
            logger.info(f"  {handle}: product_rows={len(product_info)}, variant_rows={len(variant_info)}")
            logger.info(f"  {handle}: total_input_rows={len(group)}")
            # Show sample data
            logger.info(f"  {handle}: product_info has 商品名: {list(product_info['商品名'].values) if len(product_info) > 0 else 'None'}")
            logger.info(f"  {handle}: variant_info has SKU管理番号: {list(variant_info['SKU管理番号'].values) if len(variant_info) > 0 else 'None'}")

        if len(product_info) == 0 and len(variant_info) == 0:
            if handle in ['maruzen-4set00', 'abshiri-r330-t']:
                logger.info(f"  {handle}: No valid product or variant data - skipping")
            return

        # If we have both product and variant info, combine them
        if len(product_info) > 0 and len(variant_info) > 0:
            if handle in ['maruzen-4set00', 'abshiri-r330-t']:
                logger.info(f"  {handle}: Using combined logic (should reduce rows from {len(group)} to {len(variant_info)})")

            # Take product info from product row, preserving ALL columns including 商品属性
            base_row = product_info.iloc[0].copy()

            # Efficiently merge attribute data from all rows in the group to preserve 商品属性
            attribute_columns = [col for col in group.columns if '商品属性' in col]
            if attribute_columns:
                # Count rows with actual non-empty attribute values (not just non-null)
                attr_counts = group[attribute_columns].apply(
                    lambda row: row.astype(str).str.strip().ne('').sum(), axis=1
                )

                # Find row with most actual attribute data
                if attr_counts.max() > 0:
                    best_attr_row_idx = attr_counts.idxmax()

                    # Debug logging for problematic handles
                    if handle in ['abshiri-r330-t']:
                        logger.info(f"  {handle}: Attribute debug - best_attr_row has {attr_counts[best_attr_row_idx]} real attrs")
                        sample_attr = group.loc[best_attr_row_idx, '商品属性（項目）1']
                        logger.info(f"  {handle}: Sample attr value: '{sample_attr}'")

                    # Copy attribute data from the row with most real attributes
                    for attr_col in attribute_columns:
                        attr_value = group.loc[best_attr_row_idx, attr_col]
                        if pd.notna(attr_value) and str(attr_value).strip() != '':
                            base_row[attr_col] = attr_value

            # For Shopify format: first row has product info + first variant
            # Subsequent rows have empty product fields + additional variants
            variant_count = 0
            for i, (_, variant_row) in enumerate(variant_info.iterrows()):
                if i == 0:
                    # First variant: keep all product info + add variant data
                    result_row = base_row.copy()
                else:
                    # Additional variants: empty product fields + variant data
                    result_row = base_row.copy()
                    # Clear product-specific fields for additional variants
                    result_row['商品名'] = ''
                    result_row['PC用商品説明文'] = ''
                    result_row['キャッチコピー'] = ''
                    result_row['商品画像パス1'] = ''
                    result_row['商品画像名（ALT）1'] = ''

                # Add variant data to all rows
                result_row['SKU管理番号'] = variant_row['SKU管理番号']
                result_row['通常購入販売価格'] = variant_row['通常購入販売価格']
                result_row['在庫数'] = variant_row['在庫数']
                result_row['カタログID'] = variant_row['カタログID']
                result_row['SKU画像パス'] = variant_row.get('SKU画像パス', '')
                result_row['SKU画像名（ALT）'] = variant_row.get('SKU画像名（ALT）', '')
                variant_count += 1
                yield result_row

            if handle in ['maruzen-4set00', 'abshiri-r330-t']:
                logger.info(f"  {handle}: Combined logic yielded {variant_count} rows")

        # If only product info (no variants), create with product SKU
        elif len(product_info) > 0:
            if handle in ['maruzen-4set00', 'abshiri-r330-t']:
                logger.info(f"  {handle}: Using product-only logic")
            result_row = product_info.iloc[0].copy()

            # Efficiently merge attribute data from all rows for product-only case
            attribute_columns = [col for col in group.columns if '商品属性' in col]
            if attribute_columns:
                # Count actual non-empty values, not just non-null
                attr_counts = group[attribute_columns].apply(
                    lambda row: row.astype(str).str.strip().ne('').sum(), axis=1
                )
                if attr_counts.max() > 0:
                    best_attr_row_idx = attr_counts.idxmax()
                    for attr_col in attribute_columns:
                        attr_value = group.loc[best_attr_row_idx, attr_col]
                        if pd.notna(attr_value) and str(attr_value).strip() != '':
                            result_row[attr_col] = attr_value

            if pd.isna(result_row['SKU管理番号']):
                result_row['SKU管理番号'] = result_row['商品番号']
            yield result_row

        # If only variant info (shouldn't happen but handle it)
        elif len(variant_info) > 0:
            if handle in ['maruzen-4set00', 'abshiri-r330-t']:
                logger.info(f"  {handle}: Using variant-only logic")
            for _, variant_row in variant_info.iterrows():
                yield variant_row

    # Apply consolidation
    consolidated_rows = []
    for handle, group in df.groupby('商品管理番号（商品URL）'):
        group_rows_before = len(group)
        group_rows_after = 0
        for row in consolidate_group(group):
            consolidated_rows.append(row)
            group_rows_after += 1

        # Debug logging for problematic products
        if handle in ['maruzen-4set00', 'abshiri-r330-t']:
            logger.info(f"Debug {handle}: {group_rows_before} → {group_rows_after} rows")

    df = pd.DataFrame(consolidated_rows).reset_index(drop=True)
    logger.info(f"Consolidated data: {len(df)} final rows")

    # Process SKUs and generate handles
    df['Handle'] = df['商品管理番号（商品URL）'].apply(config.derive_handle)
    df['Variant SKU'] = df['SKU管理番号']

    # Extract set count for variants
    df['Set Count'] = df['SKU管理番号'].apply(config.get_set_count)

    # Identify variant types based on SKU suffix
    def identify_variant_type(sku: str) -> str:
        """Identify the type of variant based on SKU suffix"""
        if not isinstance(sku, str):
            return 'main'

        if sku.endswith('-ss'):
            return 'ss_variant'
        elif re.search(r'-\d+s$', sku):
            return 'set_variant'
        elif sku.endswith('-t'):
            return 'trial_variant'
        else:
            return 'main'

    df['Variant Type'] = df['SKU管理番号'].apply(identify_variant_type)

    # Create variant position for sorting
    def get_variant_position(sku: str, variant_type: str) -> int:
        """Get sorting position for variants within a product group"""
        if variant_type == 'main':
            return 0
        elif variant_type == 'trial_variant':
            return 1
        elif variant_type == 'set_variant':
            # Extract set count for ordering
            match = re.search(r'-(\d+)s$', sku)
            return int(match.group(1)) if match else 999
        elif variant_type == 'ss_variant':
            return 1000  # Put SS variants last
        else:
            return 999

    df['Variant Position'] = df.apply(
        lambda row: get_variant_position(row['SKU管理番号'], row['Variant Type']),
        axis=1
    )

    # Filter out SS variants (remove -ss products by both SKU and Handle)
    initial_count = len(df)

    # Remove rows where SKU ends with -ss
    df = df[df['Variant Type'] != 'ss_variant']
    sku_ss_filtered = initial_count - len(df)

    # Also remove rows where Handle contains -ss (商品管理番号 has -ss)
    before_handle_filter = len(df)
    df = df[~df['Handle'].str.contains('-ss', na=False)]
    handle_ss_filtered = before_handle_filter - len(df)

    total_ss_filtered = sku_ss_filtered + handle_ss_filtered
    if total_ss_filtered > 0:
        logger.info(f"Filtered out {total_ss_filtered} SS variants (-ss products): {sku_ss_filtered} by SKU, {handle_ss_filtered} by Handle")

    # Sort by handle and variant position to group products properly
    df = df.sort_values(['Handle', 'Variant Position']).reset_index(drop=True)

    # Generate option1 values based on set count from SKU suffix
    def generate_option1_value(sku: str) -> str:
        """Generate Option1 Value based on SKU suffix"""
        if not isinstance(sku, str):
            return '1'

        # Extract number from -*s suffix (like -3s, -6s)
        match = re.search(r'-(\d+)s$', sku)
        if match:
            return match.group(1)
        else:
            return '1'

    df['Option1 Value'] = df['SKU管理番号'].apply(generate_option1_value)

    # Set Option1 Name for products with variants
    def generate_option1_name(handle_group):
        """Generate Option1 Name based on whether product has variants"""
        # If product has multiple variants or any variant has -*s suffix
        has_set_variants = any('-' in str(sku) and 's' in str(sku) for sku in handle_group['SKU管理番号'])
        return 'セット' if (len(handle_group) > 1 or has_set_variants) else ''

    # Group by handle and apply option name logic
    df['Option1 Name'] = df.groupby('Handle').apply(
        lambda group: pd.Series(['セット'] * len(group) if
                                (len(group) > 1 or any('-' in str(sku) and 's' in str(sku)
                                                       for sku in group['SKU管理番号']))
                                else [''] * len(group),
                                index=group.index)
    ).reset_index(0, drop=True)

    # Create Shopify CSV structure
    logger.info("Converting to Shopify CSV format...")

    shopify_df = pd.DataFrame()

    # First, preserve all 商品属性 columns for later metafield mapping
    attribute_columns = [col for col in df.columns if '商品属性' in col]
    logger.info(f"Preserving {len(attribute_columns)} attribute columns for metafield mapping")

    # Helper function to format numbers without decimals
    def format_number(value):
        """Format numeric values to remove unnecessary decimals"""
        if pd.isna(value):
            return ''
        try:
            # Convert to float first, then check if it's a whole number
            num = float(value)
            if num.is_integer():
                return str(int(num))
            else:
                return str(num)
        except (ValueError, TypeError):
            return str(value) if value is not None else ''

    # Map essential Shopify columns with proper field mappings
    shopify_df['Handle'] = df['Handle']
    shopify_df['Title'] = df['商品名'].fillna('')  # Empty for additional variants
    shopify_df['Body (HTML)'] = df['PC用商品説明文'].fillna('')  # Empty for additional variants
    shopify_df['Vendor'] = 'にっぽん津々浦々｜本店'
    shopify_df['Product Type'] = ''  # Will be filled in later steps
    shopify_df['Tags'] = ''  # Will be filled in later steps
    shopify_df['Published'] = 'TRUE'
    shopify_df['Option1 Name'] = df['Option1 Name']
    shopify_df['Option1 Value'] = df['Option1 Value']
    shopify_df['Option2 Name'] = ''
    shopify_df['Option2 Value'] = ''
    shopify_df['Option3 Name'] = ''
    shopify_df['Option3 Value'] = ''
    shopify_df['Variant SKU'] = df['SKU管理番号']
    shopify_df['Variant Grams'] = ''  # Weight info not available in Rakuten CSV
    shopify_df['Variant Inventory Tracker'] = 'shopify'
    shopify_df['Variant Inventory Qty'] = df['在庫数'].apply(format_number)
    shopify_df['Variant Inventory Policy'] = 'deny'
    shopify_df['Variant Fulfillment Service'] = 'manual'
    shopify_df['Variant Price'] = df['通常購入販売価格'].apply(format_number)
    shopify_df['Variant Compare At Price'] = ''
    shopify_df['Variant Requires Shipping'] = 'TRUE'
    shopify_df['Variant Taxable'] = 'TRUE'
    shopify_df['Variant Barcode'] = df['カタログID'].apply(format_number)
    # Construct complete image URLs based on type
    def construct_image_url(path, image_type):
        if pd.isna(path) or not str(path).strip():
            return ''

        path_str = str(path).strip()
        type_str = str(image_type).strip().upper() if pd.notna(image_type) else ''

        if type_str == 'CABINET':
            return f"https://tshop.r10s.jp/tsutsu-uraura/cabinet{path_str}"
        elif type_str == 'GOLD':
            return f"https://tshop.r10s.jp/gold/tsutsu-uraura{path_str.lstrip('/')}"
        else:
            # Fallback to basic domain + path
            return f"https://tshop.r10s.jp/tsutsu-uraura{path_str.lstrip('/')}"

    shopify_df['Image Src'] = df.apply(lambda row: construct_image_url(row.get('商品画像パス1'), row.get('商品画像タイプ1')), axis=1)
    shopify_df['Image Position'] = ''
    shopify_df['Image Alt Text'] = df['商品画像名（ALT）1'].fillna('')  # Empty for additional variants

    # Create all additional image columns at once to avoid DataFrame fragmentation
    additional_image_data = {}

    # Map additional image columns (商品画像パス2-20 → Image Src 2-20) with complete URLs
    for i in range(2, 21):
        image_path_col = f'商品画像パス{i}'
        image_type_col = f'商品画像タイプ{i}'
        image_alt_col = f'商品画像名（ALT）{i}'
        shopify_src_col = f'Image Src {i}'
        shopify_pos_col = f'Image Position {i}'
        shopify_alt_col = f'Image Alt Text {i}'

        # Construct complete URLs for additional images
        if image_path_col in df.columns and image_type_col in df.columns:
            additional_image_data[shopify_src_col] = df.apply(
                lambda row: construct_image_url(row.get(image_path_col), row.get(image_type_col)), axis=1
            )
        elif image_path_col in df.columns:
            # Fallback if no type column
            additional_image_data[shopify_src_col] = df.apply(
                lambda row: construct_image_url(row.get(image_path_col), ''), axis=1
            )
        else:
            additional_image_data[shopify_src_col] = ''

        additional_image_data[shopify_pos_col] = ''

        if image_alt_col in df.columns:
            additional_image_data[shopify_alt_col] = df[image_alt_col].fillna('')
        else:
            additional_image_data[shopify_alt_col] = ''

    # Add all image columns at once using pd.concat for better performance
    if additional_image_data:
        additional_image_df = pd.DataFrame(additional_image_data, index=shopify_df.index)
        shopify_df = pd.concat([shopify_df, additional_image_df], axis=1)
    shopify_df['Gift Card'] = 'FALSE'
    shopify_df['SEO Title'] = ''
    shopify_df['SEO Description'] = ''
    shopify_df['Google Shopping / Google Product Category'] = ''
    shopify_df['Google Shopping / Gender'] = ''
    shopify_df['Google Shopping / Age Group'] = ''
    shopify_df['Google Shopping / MPN'] = ''
    shopify_df['Google Shopping / AdWords Grouping'] = ''
    shopify_df['Google Shopping / AdWords Labels'] = ''
    shopify_df['Google Shopping / Condition'] = 'new'
    shopify_df['Google Shopping / Custom Product'] = ''
    shopify_df['Google Shopping / Custom Label 0'] = ''
    shopify_df['Google Shopping / Custom Label 1'] = ''
    shopify_df['Google Shopping / Custom Label 2'] = ''
    shopify_df['Google Shopping / Custom Label 3'] = ''
    shopify_df['Google Shopping / Custom Label 4'] = ''
    shopify_df['Variant Image'] = df['SKU画像パス']
    shopify_df['Variant Weight Unit'] = 'kg'
    shopify_df['Variant Tax Code'] = ''
    shopify_df['Cost per item'] = ''
    shopify_df['Status'] = 'active'

    # Preserve attribute columns for metafield mapping in step 05
    # Use pd.concat for better performance when adding many columns
    if attribute_columns:
        attribute_df = df[attribute_columns].copy()
        shopify_df = pd.concat([shopify_df, attribute_df], axis=1)

    # Statistics
    total_products = len(shopify_df['Handle'].unique())
    total_variants = len(shopify_df)
    main_products = len(df[df['Variant Type'] == 'main'])
    set_variants = len(df[df['Variant Type'] == 'set_variant'])
    trial_variants = len(df[df['Variant Type'] == 'trial_variant'])

    # Count products with sets (Option1 Name = 'セット')
    products_with_sets = len(shopify_df[shopify_df['Option1 Name'] == 'セット']['Handle'].unique())

    sku_stats = {
        'total_products': total_products,
        'total_variants': total_variants,
        'main_products': main_products,
        'set_variants': set_variants,
        'trial_variants': trial_variants,
        'ss_variants_filtered': total_ss_filtered,
        'products_with_sets': products_with_sets
    }

    # Log SKU processing results
    logger.info(f"Shopify CSV creation completed: {total_variants} variants across {total_products} products")
    for key, value in sku_stats.items():
        logger.info(f"SKU stats - {key}: {value}")

    return {
        'shopify_df': shopify_df,
        'sku_stats': sku_stats
    }