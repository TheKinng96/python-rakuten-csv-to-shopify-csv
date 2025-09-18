"""
Step 00: Load and Validate Data

Loads the Rakuten CSV file with proper encoding and performs initial validation.
Ensures data integrity and logs statistics about the input data.
"""

import logging
import pandas as pd
from pathlib import Path
from typing import Dict, Any

logger = logging.getLogger(__name__)


def execute(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Load and validate the Rakuten CSV file

    Args:
        data: Pipeline context containing input_file and config

    Returns:
        Dict containing loaded dataframe and validation results
    """
    logger.info("Loading Rakuten CSV file...")

    input_file = data['input_file']
    config = data['config']

    # Validate input file exists
    if not Path(input_file).exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")

    try:
        # Load CSV with proper encoding
        df = pd.read_csv(
            input_file,
            encoding=config.rakuten_encoding,
            dtype=str,  # Load all as strings to preserve data
            na_values=['', 'NULL', 'null', 'NaN'],
            keep_default_na=False
        )

        logger.info(f"Loaded {len(df)} rows from {input_file}")

        # Basic validation
        if df.empty:
            raise ValueError("Input file is empty")

        required_columns = [
            '管理番号', '商品名', '商品コード', 'PC用商品説明文',
            '携帯用商品説明文', 'PC用キャッチコピー', '販売価格'
        ]

        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        # Log column statistics
        logger.info(f"Columns found: {len(df.columns)}")
        logger.info(f"Required columns validated: {len(required_columns)}")

        # Log data statistics
        stats = {
            'total_rows': len(df),
            'unique_skus': len(df['管理番号'].dropna().unique()),
            'columns_count': len(df.columns),
            'memory_usage': f"{df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB"
        }

        for key, value in stats.items():
            logger.info(f"Input stats - {key}: {value}")

        return {
            'raw_df': df,
            'validation_stats': stats,
            'column_names': list(df.columns)
        }

    except UnicodeDecodeError as e:
        logger.error(f"Encoding error: {e}")
        logger.error("Trying alternative encodings...")

        # Try alternative encodings
        for encoding in ['utf-8', 'shift-jis', 'euc-jp']:
            try:
                df = pd.read_csv(input_file, encoding=encoding, dtype=str)
                logger.info(f"Successfully loaded with {encoding} encoding")
                return {'raw_df': df}
            except UnicodeDecodeError:
                continue

        raise ValueError("Could not decode file with any supported encoding")

    except Exception as e:
        logger.error(f"Failed to load input file: {e}")
        raise