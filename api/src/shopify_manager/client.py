"""
Shopify API client with rate limiting and error handling
"""
import asyncio
import logging
import time
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urljoin

import aiohttp
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from .config import ShopifyConfig

logger = logging.getLogger(__name__)


class ShopifyAPIError(Exception):
    """Custom exception for Shopify API errors"""
    pass


class ShopifyClient:
    """
    Shopify API client with rate limiting and error handling
    """
    
    def __init__(self, config: ShopifyConfig, use_test_store: bool = True):
        self.config = config
        self.use_test_store = use_test_store
        self.store_url, self.access_token = config.get_store_credentials(use_test_store)
        
        # Rate limiting
        self.request_times: List[float] = []
        self.max_requests_per_second = config.max_requests_per_second
        
        # Base headers
        self.headers = {
            "X-Shopify-Access-Token": self.access_token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        
        # API base URL
        self.base_url = f"https://{self.store_url}/admin/api/2025-07/"
        
        logger.info(f"Initialized Shopify client for {'test' if use_test_store else 'prod'} store")
    
    def _wait_for_rate_limit(self) -> None:
        """Enforce rate limiting"""
        now = time.time()
        
        # Remove requests older than 1 second
        self.request_times = [t for t in self.request_times if now - t < 1.0]
        
        # If we're at the limit, wait
        if len(self.request_times) >= self.max_requests_per_second:
            sleep_time = 1.0 - (now - self.request_times[0])
            if sleep_time > 0:
                time.sleep(sleep_time)
                # Clean up again after waiting
                now = time.time()
                self.request_times = [t for t in self.request_times if now - t < 1.0]
        
        # Record this request
        self.request_times.append(now)
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def _make_request(
        self, 
        method: str, 
        endpoint: str, 
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Make rate-limited request to Shopify API"""
        self._wait_for_rate_limit()
        
        url = urljoin(self.base_url, endpoint)
        
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=self.headers,
                json=data,
                params=params,
                timeout=30
            )
            
            # Handle rate limiting from Shopify
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 2))
                logger.warning(f"Rate limited by Shopify, waiting {retry_after} seconds")
                time.sleep(retry_after)
                raise ShopifyAPIError("Rate limited")
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise ShopifyAPIError(f"Request failed: {e}")
    
    def get_products(
        self, 
        limit: int = 250, 
        since_id: Optional[int] = None,
        fields: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get products from Shopify"""
        params = {"limit": limit}
        if since_id:
            params["since_id"] = since_id
        if fields:
            params["fields"] = fields
            
        return self._make_request("GET", "products.json", params=params)
    
    def get_product(self, product_id: int) -> Dict[str, Any]:
        """Get single product by ID"""
        return self._make_request("GET", f"products/{product_id}.json")
    
    def update_product(self, product_id: int, product_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update product"""
        if self.config.dry_run:
            logger.info(f"DRY RUN: Would update product {product_id}")
            return {"product": product_data}
        
        return self._make_request("PUT", f"products/{product_id}.json", data={"product": product_data})
    
    def create_product(self, product_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create new product"""
        if self.config.dry_run:
            logger.info("DRY RUN: Would create product")
            return {"product": {**product_data, "id": 999999}}
        
        return self._make_request("POST", "products.json", data={"product": product_data})
    
    def delete_product_image(self, product_id: int, image_id: int) -> None:
        """Delete product image"""
        if self.config.dry_run:
            logger.info(f"DRY RUN: Would delete image {image_id} from product {product_id}")
            return
        
        self._make_request("DELETE", f"products/{product_id}/images/{image_id}.json")
    
    def get_all_products(self, fields: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get all products using pagination"""
        all_products = []
        since_id = None
        
        while True:
            response = self.get_products(since_id=since_id, fields=fields)
            products = response.get("products", [])
            
            if not products:
                break
                
            all_products.extend(products)
            since_id = products[-1]["id"]
            
            logger.info(f"Retrieved {len(all_products)} products so far...")
            
            # Break if we got fewer than the limit (last page)
            if len(products) < self.config.batch_size:
                break
        
        logger.info(f"Retrieved total of {len(all_products)} products")
        return all_products