import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path

def extract_rakuten_image_urls(
    shopify_csv_path: Path = Path("output/shopify_products.csv"), 
    output_path: Path = Path("output/shopify_image_urls.csv"), 
    domain_filter: str = "https://image.rakuten.co.jp/tsutsu-uraura"
) -> None:
    """
    Reads a generated Shopify CSV, parses the 'Body (HTML)' column,
    and extracts all unique image URLs matching a specific domain.

    Args:
        shopify_csv_path (Path): Path to the input Shopify CSV file.
        output_path (Path): Path to save the list of URLs.
        domain_filter (str): The domain prefix to filter image URLs by.
    """
    print(f"\n[+] Starting image URL extraction from '{shopify_csv_path.name}'...")
    
    if not shopify_csv_path.is_file():
        print(f"  - Error: Input file not found at '{shopify_csv_path}'. Cannot extract URLs.")
        return

    found_urls = set()
    try:
        # Read the generated Shopify CSV file
        df = pd.read_csv(shopify_csv_path, dtype=str, keep_default_na=False)

        if "Body (HTML)" not in df.columns:
            print("  - Error: 'Body (HTML)' column not found in the CSV. Aborting.")
            return

        # Iterate over each product's HTML body
        for html_content in df["Body (HTML)"]:
            if not html_content.strip():
                continue
            
            soup = BeautifulSoup(html_content, 'lxml')
            # Find all <img> tags
            for img_tag in soup.find_all('img'):
                src = img_tag.get('src')
                # If the src exists and starts with the target domain, add it to our set
                if src and src.strip().startswith(domain_filter):
                    found_urls.add(src.strip())

        if not found_urls:
            print("  - No image URLs matching the specified domain were found.")
            return
        
        # Write the unique URLs to a new CSV file
        print(f"  - Found {len(found_urls)} unique image URLs.")
        print(f"  - Saving list to '{output_path}'...")
        url_df = pd.DataFrame(sorted(list(found_urls)), columns=['url'])
        url_df.to_csv(output_path, index=False, header=True)
        print("  - URL extraction complete.")

    except Exception as e:
        print(f"  - An unexpected error occurred during URL extraction: {e}")

if __name__ == "__main__":
    extract_rakuten_image_urls()