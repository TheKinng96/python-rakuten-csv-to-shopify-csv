import pandas as pd
from pathlib import Path
import logging
import sys

def remove_description_columns(
    input_csv: str = "./sample/new.csv",
    output_csv: str = "./sample/dl-normal-item_no_desc.csv"
) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    path = Path(input_csv)
    if not path.exists():
        logging.error(f"{input_csv} が見つかりません。")
        return

    # Try encodings
    for enc in ("utf-8", "cp932", "shift-jis"):
        try:
            df = pd.read_csv(path, encoding=enc, low_memory=False)
            logging.info(f"Successfully loaded with {enc} encoding")
            break
        except UnicodeDecodeError:
            continue
    else:
        logging.error("文字コードを判定できませんでした。")
        return

    # Show original stats
    original_size = path.stat().st_size / (1024*1024)
    logging.info(f"Original: {len(df):,} rows, {len(df.columns)} columns, {original_size:.1f} MB")

    # Remove description columns (only the 2 specified)
    cols_to_remove = ["PC用商品説明文", "スマートフォン用商品説明文"]
    found_cols = [col for col in cols_to_remove if col in df.columns]

    if found_cols:
        df_cleaned = df.drop(columns=found_cols)
        df_cleaned.to_csv(output_csv, index=False, encoding='utf-8')

        # Show results
        new_size = Path(output_csv).stat().st_size / (1024*1024)
        space_saved = original_size - new_size

        logging.info(f"Cleaned:  {len(df_cleaned):,} rows, {len(df_cleaned.columns)} columns, {new_size:.1f} MB")
        logging.info(f"Columns removed: {found_cols}")
        logging.info(f"Space saved: {space_saved:.1f} MB ({space_saved/original_size*100:.1f}%)")
        logging.info(f"Output written to {output_csv}")
    else:
        logging.warning("No target columns found to remove")

if __name__ == "__main__":
    if len(sys.argv) == 3:
        input_file = sys.argv[1]
        output_file = sys.argv[2]
        remove_description_columns(input_file, output_file)
    elif len(sys.argv) == 1:
        # Default behavior
        remove_description_columns()
    else:
        print("Usage: python remove_description_columns.py [input_file] [output_file]")
        print("Example: python remove_description_columns.py new_products_final.csv new_products_no_desc.csv")
        sys.exit(1)
