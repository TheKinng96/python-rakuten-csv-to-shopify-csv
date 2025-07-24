import csv
import pandas as pd
import chardet
import io

def detect_encoding(file_path):
    """Detect the encoding of a file using chardet."""
    with open(file_path, 'rb') as f:
        rawdata = f.read(10000)  # Read first 10KB to guess encoding
        result = chardet.detect(rawdata)
        encoding = result['encoding']
        confidence = result['confidence']
        print(f"Detected encoding: {encoding} (confidence: {confidence:.2%})")
        return encoding if confidence > 0.7 else 'shift_jis'  # Default to shift_jis if confidence is low

def read_csv_robust(path, sku_col, id_col):
    # 1) sniff the file (optional—you can skip detection entirely and go straight to latin1)
    raw = open(path, 'rb').read()
    guessed = chardet.detect(raw[:20000])  # look at first 20KB
    print(f"chardet guessed {guessed['encoding']} @ {guessed['confidence']:.0%} confidence")

    # 2) try your best encodings list
    for enc in [guessed['encoding'], 'utf-8-sig', 'cp932', 'shift_jis', 'euc-jp', 'latin1']:
        if not enc: 
            continue
        try:
            print(f"Decoding with {enc!r} (replace errors)…")
            text = raw.decode(enc, errors='replace')
            # 3) parse from the in-memory text
            df = pd.read_csv(
                io.StringIO(text),
                dtype=str,
                on_bad_lines='warn',
                engine='python',
            )
            print(f"✔️ success with {enc!r}; columns: {df.columns.tolist()}")
            skus = df[sku_col].str.strip().fillna('').tolist()
            ids  = df[id_col].str.strip().fillna('').tolist()
            return skus, ids

        except Exception as e:
            print(f"❌ failed with {enc!r}: {e}")

    raise Exception("All encodings failed. File may not be a CSV or is severely malformed.")

def read_csv(path, sku_col, id_col):
    encodings_to_try = [
        'utf-8-sig',   # sometimes they add a BOM
        'cp932',       # Japanese Windows
        'shift_jis',   # generic
        'euc-jp',      # older JIS variant
        'latin1'       # single-byte “never fail” fallback
    ]
    last_exc = None

    for enc in encodings_to_try:
        try:
            print(f"Trying encoding {enc!r}…")
            df = pd.read_csv(
                path,
                encoding=enc,
                engine='python',      # more permissive parser
                on_bad_lines='warn',  # skip malformed rows
                dtype=str
            )
            print(f"  success with {enc!r}")
            skus = df[sku_col].str.strip().fillna('').tolist()
            ids  = df[id_col].str.strip().fillna('').tolist()
            return skus, ids

        except Exception as e:
            print(f"  failed: {e}")
            last_exc = e

    # if we get here, everything blew up
    raise last_exc

def write_csv(output_path, data):
    """
    Write processed data to CSV with proper encoding and error handling.
    
    Args:
        output_path (str): Path to output CSV file
        data (list): List of tuples (catalog_id, handler, sku)
        
    Raises:
        Exception: If there's an error writing the file
    """
    try:
        print(f"Writing output to {output_path}...")
        
        # Ensure the output directory exists
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        
        # Write with explicit UTF-8 encoding and handle any encoding errors
        with open(output_path, 'w', newline='', encoding='utf-8', errors='replace') as f:
            writer = csv.writer(f)
            
            # Write header
            writer.writerow(['カタログID', '最終SKU', '各商品SKU'])
            
            # Write data rows
            for i, row in enumerate(data, 1):
                try:
                    # Ensure all row items are strings and properly encoded
                    encoded_row = []
                    for item in row:
                        if item is None:
                            encoded_row.append('')
                        elif not isinstance(item, str):
                            encoded_row.append(str(item))
                        else:
                            encoded_row.append(item)
                    writer.writerow(encoded_row)
                except Exception as row_error:
                    print(f"Warning: Error writing row {i}: {str(row_error)}")
                    continue
        
        print(f"Successfully wrote {len(data)} rows to {output_path}")
        
    except IOError as e:
        error_msg = f"I/O error writing to {output_path}: {str(e)}"
        print(error_msg)
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"Error writing to {output_path}: {str(e)}"
        print(error_msg)
        raise Exception(error_msg)
