"""
Step 08: Data Cleanup and Tag Addition

Adds "9/29追加" tag to all products and performs final data cleanup.
"""

import logging
import pandas as pd
from typing import Dict, Any

logger = logging.getLogger(__name__)


def execute(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Add "9/29追加" tag to all products and perform final cleanup

    Args:
        data: Pipeline context containing tax_mapped_df and config

    Returns:
        Dict containing final cleaned dataframe
    """
    logger.info("Adding tags and performing final data cleanup...")

    df = data['tax_mapped_df'].copy()
    config = data['config']

    # Track cleanup statistics
    cleanup_stats = {
        'products_processed': 0,
        'tags_added': 0,
        'rows_processed': len(df)
    }

    # Add "9/29追加" tag to all products
    df_with_tags = add_date_tags(df, cleanup_stats)

    # Log cleanup results
    logger.info(f"Data cleanup completed")
    for key, value in cleanup_stats.items():
        logger.info(f"Cleanup stats - {key}: {value}")

    return {
        'final_df': df_with_tags,
        'cleanup_stats': cleanup_stats
    }


def add_date_tags(df: pd.DataFrame, stats: Dict[str, Any]) -> pd.DataFrame:
    """
    Add "9/29追加" tag to the Tags field for all products

    Args:
        df: Dataframe to process
        stats: Statistics tracking dictionary

    Returns:
        Dataframe with updated tags
    """
    logger.info("Adding '9/29追加' tag to all products...")

    # Track unique handles to count products processed
    processed_handles = set()
    tag_to_add = "9/29追加"

    # Process each row
    for idx, row in df.iterrows():
        handle = row.get('Handle')
        if handle and handle not in processed_handles:
            stats['products_processed'] += 1
            processed_handles.add(handle)

        # Get current tags
        current_tags = row.get('Tags', '')

        # Convert to string and handle empty/NaN values
        if pd.isna(current_tags) or current_tags == '':
            current_tags = ''
        else:
            current_tags = str(current_tags).strip()

        # Add new tag
        if current_tags:
            # If there are existing tags, append with comma separator
            new_tags = f"{current_tags},{tag_to_add}"
        else:
            # If no existing tags, just add the new tag
            new_tags = tag_to_add

        # Update the dataframe
        df.at[idx, 'Tags'] = new_tags
        stats['tags_added'] += 1

    logger.info(f"Added '{tag_to_add}' tag to {stats['tags_added']} rows across {stats['products_processed']} products")

    return df


def create_cleanup_summary(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Create a summary of cleanup results

    Args:
        df: Final processed dataframe

    Returns:
        Summary dictionary
    """
    # Count rows with the new tag
    tag_count = df['Tags'].str.contains('9/29追加', na=False).sum()

    summary = {
        'total_products': df['Handle'].nunique(),
        'total_rows': len(df),
        'rows_with_date_tag': tag_count,
        'unique_handles': df['Handle'].nunique()
    }

    return summary