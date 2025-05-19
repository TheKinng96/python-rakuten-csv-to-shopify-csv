import csv
import pandas as pd

def read_csv(path, sku_col, id_col):
    """
    Read CSV file and extract SKU and Catalog ID columns.
    Tries UTF-8 first, then falls back to Shift-JIS if needed.
    
    Args:
        path (str): Path to CSV file
        sku_col (str): Name or index of SKU column
        id_col (str): Name or index of Catalog ID column
        
    Returns:
        tuple: (list of SKUs, list of Catalog IDs)
        
    Raises:
        Exception: If file cannot be read with either encoding
    """
    def try_read_with_encoding(encoding):
        try:
            # Try with pandas first
            try:
                df = pd.read_csv(path, encoding=encoding)
                return df[sku_col].astype(str).tolist(), df[id_col].astype(str).tolist()
            except (UnicodeDecodeError, LookupError):
                # If pandas fails, try with csv module
                with open(path, 'r', encoding=encoding) as f:
                    reader = csv.DictReader(f)
                    skus = [str(row[sku_col]) for row in reader]
                    f.seek(0)
                    reader = csv.DictReader(f)
                    ids = [str(row[id_col]) for row in reader]
                    return skus, ids
        except Exception as e:
            return None
    
    # Try UTF-8 first
    result = try_read_with_encoding('utf-8')
    
    # If UTF-8 fails, try Shift-JIS
    if result is None:
        result = try_read_with_encoding('shift_jis')
    
    # If both fail, raise an error
    if result is None:
        raise Exception("Could not read CSV file. Tried UTF-8 and Shift-JIS encodings.")
    
    return result

def write_csv(output_path, data):
    """
    Write processed data to CSV.
    
    Args:
        output_path (str): Path to output CSV file
        data (list): List of tuples (catalog_id, handler, sku)
    """
    try:
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['カタログID', '最終SKU', '各商品SKU'])
            writer.writerows(data)
    except Exception as e:
        raise Exception(f"Error writing CSV: {str(e)}")
