import pandas as pd
import logging
from pathlib import Path

def check_mismatched_items(
    csv_path: str = "./sample/dl-normal-item2.csv",
    output_path: str = "mismatched_items.csv"
) -> None:
    """`商品番号` と `商品管理番号（商品URL）` が食い違う行だけを抽出して保存する"""

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    path = Path(csv_path)
    if not path.exists():
        logging.error(f"{path} が見つかりません。")
        return

    # ── 文字コード判定 ──────────────────────────────
    for enc in ("cp932", "shift-jis", "utf-8"):
        try:
            df = pd.read_csv(path, encoding=enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        logging.error("文字コードを判定できませんでした。")
        return

    # ── 列名と前処理 ────────────────────────────────
    df.columns = df.columns.str.strip()
    col_bango = "商品番号"
    col_kanri = "商品管理番号（商品URL）"

    if {col_bango, col_kanri}.difference(df.columns):
        logging.warning(f"列 '{col_bango}' または '{col_kanri}' が見つかりません。実際の列名一覧: {list(df.columns)}")
        return

    # 空文字・NaN を揃えてから比較
    df[col_bango] = df[col_bango].fillna("").astype(str).str.strip()
    df[col_kanri] = df[col_kanri].fillna("").astype(str).str.strip()

    # ── 条件: ①商品番号が空でない ②両列の値が異なる ──
    mask = (df[col_bango] != "") & (df[col_bango] != df[col_kanri])
    mismatched = df.loc[mask, [col_bango, col_kanri]]

    if mismatched.empty:
        logging.info("不一致の行はありませんでした。")
        return

    mismatched.to_csv(output_path, index=False, encoding="utf-8-sig")
    logging.info(f"{len(mismatched)} 行を書き出しました → {output_path}")

if __name__ == "__main__":
    check_mismatched_items()
