import pandas as pd
from pathlib import Path
import logging

def remove_description_columns(
    input_csv: str = "./sample/dl-normal-item.csv",
    output_csv: str = "./sample/dl-normal-item_no_desc.csv"
) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    path = Path(input_csv)
    if not path.exists():
        logging.error(f"{input_csv} が見つかりません。")
        return

    # Try encodings
    for enc in ("cp932", "shift-jis", "utf-8"):
        try:
            df = pd.read_csv(path, encoding=enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        logging.error("文字コードを判定できませんでした。")
        return

    cols_to_remove = ["PC用商品説明文", "スマートフォン用商品説明文", "PC用販売説明文"]
    found_cols = [col for col in cols_to_remove if col in df.columns]
    df = df.drop(columns=found_cols)
    df.to_csv(output_csv, index=False)
    logging.info(f"Columns removed: {found_cols}. Output written to {output_csv}")

if __name__ == "__main__":
    remove_description_columns()
