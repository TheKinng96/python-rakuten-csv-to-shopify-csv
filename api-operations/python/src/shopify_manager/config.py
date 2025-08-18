"""
Configuration management for Shopify API operations
"""
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# Load environment variables
load_dotenv()


@dataclass
class ShopifyConfig:
    """Shopify API configuration"""
    
    test_store_url: str
    test_access_token: str
    prod_store_url: str
    prod_access_token: str
    max_requests_per_second: int = 40
    batch_size: int = 250
    chunk_size: int = 1000
    dry_run: bool = True
    enable_logging: bool = True
    log_level: str = "INFO"
    
    @classmethod
    def from_env(cls) -> "ShopifyConfig":
        """Create configuration from environment variables"""
        return cls(
            test_store_url=os.getenv("SHOPIFY_TEST_STORE_URL", ""),
            test_access_token=os.getenv("SHOPIFY_TEST_ACCESS_TOKEN", ""),
            prod_store_url=os.getenv("SHOPIFY_PROD_STORE_URL", ""),
            prod_access_token=os.getenv("SHOPIFY_PROD_ACCESS_TOKEN", ""),
            max_requests_per_second=int(os.getenv("MAX_REQUESTS_PER_SECOND", "40")),
            batch_size=int(os.getenv("BATCH_SIZE", "250")),
            chunk_size=int(os.getenv("CHUNK_SIZE", "1000")),
            dry_run=os.getenv("DRY_RUN", "true").lower() == "true",
            enable_logging=os.getenv("ENABLE_LOGGING", "true").lower() == "true",
            log_level=os.getenv("LOG_LEVEL", "INFO"),
        )
    
    def get_store_credentials(self, use_test: bool = True) -> tuple[str, str]:
        """Get store URL and access token based on environment"""
        if use_test:
            return self.test_store_url, self.test_access_token
        return self.prod_store_url, self.prod_access_token


@dataclass
class PathConfig:
    """File path configuration"""
    
    api_root: Path = Path(__file__).parent.parent.parent
    data_dir: Path = api_root / "data"
    reports_dir: Path = api_root / "reports"
    scripts_dir: Path = api_root / "scripts"
    
    def __post_init__(self) -> None:
        """Ensure directories exist"""
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        self.data_dir.mkdir(parents=True, exist_ok=True)
    
    def get_csv_files(self) -> list[Path]:
        """Get all product CSV files from data directory"""
        return list(self.data_dir.glob("products_export_*.csv"))
    
    def get_report_path(self, report_name: str) -> Path:
        """Get path for a specific report file"""
        return self.reports_dir / report_name


# Global configuration instances
shopify_config = ShopifyConfig.from_env()
path_config = PathConfig()