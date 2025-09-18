"""
Rakuten to Shopify CSV Converter

A complete pipeline that transforms Rakuten product data to Shopify-ready CSV format
with all production fixes applied in a single processing run.

Features:
- 15-step modular pipeline
- Complete HTML processing with responsive tables
- Japanese tax classification (8% vs 10%)
- Comprehensive metafield mapping
- Image URL fixes and processing
- Quality validation and reporting

Usage:
    from rakuten_to_shopify import PipelineRunner, PipelineConfig

    config = PipelineConfig()
    runner = PipelineRunner(config)
    success = runner.run_pipeline('input.csv', 'output/')
"""

from .pipeline import PipelineConfig, PipelineRunner

__version__ = "2.0.0"
__all__ = ["PipelineConfig", "PipelineRunner"]