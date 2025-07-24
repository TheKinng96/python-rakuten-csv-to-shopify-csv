from pathlib import Path
import pandas as pd
from bs4 import BeautifulSoup

def extract_rakuten_image_urls(
    shopify_csv_path: Path = Path("output/shopify_products.csv"), 
    output_path: Path = Path("output/shopify_image_urls.csv"), 
    domain_filter: str = "https://image.rakuten.co.jp/tsutsu-uraura"
) -> None:
    """
    Reads a generated Shopify CSV, parses the 'Body (HTML)' column,
    and extracts all unique image URLs matching a specific domain,
    along with their corresponding product Handle.

    Args:
        shopify_csv_path (Path): Path to the input Shopify CSV file.
        output_path (Path): Path to save the list of handles and URLs.
        domain_filter (str): The domain prefix to filter image URLs by.
    """
    print(f"\n[+] Starting image URL and Handle extraction from '{shopify_csv_path.name}'...")
    
    if not shopify_csv_path.is_file():
        print(f"  - Error: Input file not found at '{shopify_csv_path}'. Cannot extract URLs.")
        return

    # --- MODIFIED: Use a set of tuples to store (handle, url) pairs to ensure uniqueness ---
    found_items = set() 
    try:
        df = pd.read_csv(shopify_csv_path, dtype=str, keep_default_na=False)

        if "Body (HTML)" not in df.columns or "Handle" not in df.columns:
            print("  - Error: 'Handle' and/or 'Body (HTML)' column not found. Aborting.")
            return

        # --- MODIFIED: Iterate over rows to get both Handle and HTML content ---
        for index, row in df.iterrows():
            handle = row["Handle"]
            html_content = row["Body (HTML)"]

            # Only process rows that have a handle (i.e., the main product rows)
            if not handle.strip() or not html_content.strip():
                continue
            
            soup = BeautifulSoup(html_content, 'lxml')
            for img_tag in soup.find_all('img'):
                src = img_tag.get('src')
                if src and src.strip().startswith(domain_filter):
                    # Add the (handle, url) tuple to the set
                    found_items.add((handle.strip(), src.strip()))

        if not found_items:
            print("  - No image URLs matching the specified domain were found.")
            return
        
        # --- MODIFIED: Write the handle and url to a new CSV file ---
        print(f"  - Found {len(found_items)} unique handle-image pairs.")
        print(f"  - Saving list to '{output_path}'...")
        # Convert the set of tuples to a DataFrame with named columns
        url_df = pd.DataFrame(sorted(list(found_items)), columns=['handle', 'url'])
        url_df.to_csv(output_path, index=False, header=True)
        print("  - URL and Handle extraction complete.")

    except Exception as e:
        print(f"  - An unexpected error occurred during URL extraction: {e}")


if __name__ == "__main__":
    extract_rakuten_image_urls()
