import pandas as pd
import logging
from pathlib import Path

def log_unique_shohin_kanri_bango(csv_path: str = "./sample/dl-normal-item.csv") -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s: %(message)s")

    path = Path(csv_path)
    if not path.exists():
        logging.error(f"{path} が見つかりません。")
        return

    # 試しに shift‑jis → cp932 → utf‑8 の順で試行
    for enc in ("cp932", "shift-jis", "utf-8"):
        try:
            df = pd.read_csv(path, encoding=enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        logging.error("文字コードを判定できませんでした。")
        return

    # 列名を確認（前後空白やBOMの可能性あり）
    df.columns = df.columns.str.strip()

    target_col = "SKU管理番号"
    if target_col not in df.columns:
        logging.warning(f"列 '{target_col}' が見つかりません。実際の列名一覧: {list(df.columns)}")
        return

    # Check for missing SKU管理番号
    missing_mask = df[target_col].isna() | (df[target_col] == '')
    missing_count = missing_mask.sum()
    logging.info(f"SKU管理番号が未入力の行数: {missing_count}")
    if missing_count > 0:
        missing_rows = df[missing_mask]
        missing_csv_path = "missing_sku_kanri_bango_rows.csv"
        missing_rows.to_csv(missing_csv_path, index=False)
        logging.info(f"Missing SKU管理番号 rows written to {missing_csv_path} ({missing_count} rows)")

    # Check for duplicates
    duplicated = df[target_col][df[target_col].duplicated(keep=False) & df[target_col].notna() & (df[target_col] != '')]
    if not duplicated.empty:
        dup_count = duplicated.nunique()
        logging.warning(f"重複しているSKU管理番号が {dup_count} 件あります。重複値: {duplicated.unique().tolist()}")
    else:
        logging.info("SKU管理番号に重複はありません。")

    unique_cnt = df[target_col].nunique(dropna=True)
    logging.info(f"ユニークな商品管理番号は {unique_cnt} 件です。")

def log_rows_missing_shohin_bango(csv_path: str = "./sample/dl-normal-item.csv", output_path: str = "missing_shohin_bango_rows.csv") -> None:
    """
    Logs rows where 商品番号 is missing but 商品管理番号（商品URL） is present,
    excluding the expected second row of a two-row item.
    Writes the problematic rows to output_path as CSV.
    """
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s: %(message)s")

    path = Path(csv_path)
    if not path.exists():
        logging.error(f"{path} が見つかりません。")
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

    df.columns = df.columns.str.strip()
    col_bango = "商品番号"
    col_kanri = "商品管理番号（商品URL）"
    if col_bango not in df.columns or col_kanri not in df.columns:
        logging.warning(f"列 '{col_bango}' または '{col_kanri}' が見つかりません。実際の列名一覧: {list(df.columns)}")
        return

    # Fill NA for 商品番号 for easier comparison
    df[col_bango] = df[col_bango].astype(str).replace('nan', '')
    df[col_kanri] = df[col_kanri].astype(str).replace('nan', '')

    missing_rows = []
    prev_kanri = None
    prev_bango_present = False
    for idx, row in df.iterrows():
        bango = row[col_bango].strip()
        kanri = row[col_kanri].strip()
        if bango == '' and kanri != '':
            # If previous row had the same 商品管理番号（商品URL） and had 商品番号, skip (second row of item)
            if prev_kanri == kanri and prev_bango_present:
                pass  # expected second row, skip
            else:
                missing_rows.append(row)
        prev_kanri = kanri
        prev_bango_present = bango != ''
    if missing_rows:
        pd.DataFrame(missing_rows).to_csv(output_path, index=False, encoding='utf-8-sig')
        logging.info(f"Missing 商品番号 rows written to {output_path} ({len(missing_rows)} rows)")
    else:
        logging.info("No unexpected missing 商品番号 rows found.")

if __name__ == "__main__":
    log_unique_shohin_kanri_bango()
    log_rows_missing_shohin_bango()
