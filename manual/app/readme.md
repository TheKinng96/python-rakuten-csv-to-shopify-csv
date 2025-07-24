1. Overview

A lightweight desktop utility (Python + GUI) for cleaning and grouping large CSVs by Catalog ID, then exporting a summary CSV with handler/variants/count columns.

Key steps:

a. Input: load user-specified CSV and select relevant columns.
b. Processing:
    a. Filter rows: only keep those with a non-empty Catalog ID.
    b. Group rows by Catalog ID.
    c. Within each group, compute a group name (handler) by finding the shortest common segment-prefix of the SKU column.
    d. Build a list of child SKUs (variants) for each handler.
c. Output: write a new CSV with columns:
    a. Handler (group name)
    b. Variant SKUs (comma-joined list of SKUs)
    c. Number of Variants

2. Prerequisites
- Python 3.9+
- GUI toolkit: PySimpleGUI (or swap for Tkinter/PySide6)
- Packaging tool: PyInstaller (for standalone executables)

Dependencies (in requirements.txt):
```
PySimpleGUI>=4.60.0
pandas>=1.4.0   # optional, for CSV parsing if desired
```

3. Project Structure
```
csv_tool/                  ← root folder
├── app.py                 ← main entry point (PySimpleGUI)
├── splitter.py            ← core CSV-splitting helper (from earlier)
├── grouper.py             ← grouping logic module (GROUP_BY_CATALOG)
├── ui.py                  ← GUI definitions and callbacks
├── utils.py               ← file I/O and CLI helpers
├── requirements.txt       ← Python deps
├── README.md              ← high-level usage
└── dist/                  ← generated executable (after PyInstaller)
```

4. Module Details

4.1 splitter.py
- split_csv(path, chunk_size)
    - Reads the input CSV in binary mode by chunks of chunk_size bytes.
    - Writes out file_partN.csv until EOF.
    - Streams data; minimal RAM footprint.

4.2 grouper.py
- group_by_catalog(sku_list: List[str], id_list: List[str]) -> List[Tuple[str,str,str]]
    - Build a map: catalog_id → List[sku].
    - For each catalog_id:
        - If only one SKU, handler = SKU, variants = [SKU].
        - Else, find common segment-prefix:
            - Split each SKU on - into segments.
            - Reduce pairwise to the longest shared prefix segments.
        - If result is empty, pick the shortest SKU in the group as handler.
    - Emit (catalog_id, handler, sku) for each group member.
- current logic for grouping is in groupByCatalog.js

4.3 ui.py
- Defines the PySimpleGUI window:
    - File Browser to select CSV.
    - Inputs: column indices or headers for SKU & Catalog ID.
    - Buttons: “Load”, “Process & Export”.
    - Progress bar and status text.
- Event loop:
    - On Load: preview first N rows.
    - On Process: call utils.read_csv(), then grouper.group_by_catalog(), then utils.write_csv().

4.4 utils.py
- read_csv(path, cols: List[int]) -> Tuple[List[str], List[str]]
    - Use Python’s csv module (or pandas.read_csv) to extract two lists: SKUs, Catalog IDs.
- write_csv(path, header, rows)
    - Write out the final summary CSV with given header and row tuples.

5. CLI Support

(Optional) expose a command-line interface in app.py:
```
python app.py \
  --input /path/to/data.csv \
  --sku-col URL --id-col CatalogID \
  --output summary.csv
```

- Use argparse to parse flags, then bypass the GUI and run directly:
  1. Call utils.read_csv()
  2. Call grouper.group_by_catalog()
  3. Call utils.write_csv()

6. Packaging & Distribution
- Install PyInstaller: pip install pyinstaller
- Build:
```
pyinstaller --onefile --windowed app.py
```
- Result: `dist/app` (single binary) that users can run without Python.

7. Testing & Validation
- Unit tests for grouper.common_segment_prefix:
    - Cases: single SKU, full-match SKUs, no-match SKUs, multi-segment variations.
- Integration test: small CSV with known groups.
- Manual QA: ensure export matches expectations.

8. Future Enhancements
- Support additional grouping rules (e.g. longest common substring).
- Allow custom delimiters other than -.
- Export to Excel with multi-sheet summary.
- Add scheduling for periodic batch runs.
