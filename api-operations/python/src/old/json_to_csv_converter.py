import json
import csv
import sys
from pathlib import Path

def convert_missing_variant_images_to_csv(json_file_path, output_csv_path=None):
    """
    Convert missing_variant_images from JSON report to CSV format.
    
    Args:
        json_file_path (str): Path to the JSON report file
        output_csv_path (str, optional): Path for output CSV. If None, uses same name with .csv extension
    
    Returns:
        str: Path to the created CSV file
    """
    json_path = Path(json_file_path)
    
    if not json_path.exists():
        raise FileNotFoundError(f"JSON file not found: {json_file_path}")
    
    # Set output path if not provided
    if output_csv_path is None:
        output_csv_path = json_path.parent / f"{json_path.stem}_missing_variants.csv"
    
    # Read JSON data
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    missing_variants = data.get('missing_variant_images', [])
    
    if not missing_variants:
        print("No missing variant images found in the JSON file.")
        return None
    
    # Write to CSV
    with open(output_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['File', 'Variant SKU', 'Title', 'Handle']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for variant in missing_variants:
            writer.writerow(variant)
    
    print(f"Successfully converted {len(missing_variants)} missing variant images to CSV: {output_csv_path}")
    return str(output_csv_path)

def main():
    if len(sys.argv) < 2:
        print("Usage: python json_to_csv_converter.py <json_file_path> [output_csv_path]")
        sys.exit(1)
    
    json_file = sys.argv[1]
    output_csv = sys.argv[2] if len(sys.argv) > 2 else None
    
    try:
        result_path = convert_missing_variant_images_to_csv(json_file, output_csv)
        if result_path:
            print(f"CSV file created at: {result_path}")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()