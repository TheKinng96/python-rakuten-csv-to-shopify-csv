"""
Rakuten to Shopify Conversion Pipeline

A modular, step-by-step pipeline that transforms Rakuten product data
to Shopify-ready CSV format with all production fixes applied.

Main Components:
- PipelineConfig: Centralized configuration management
- PipelineRunner: Main orchestrator for all pipeline steps
- Steps 00-14: Individual processing modules

Usage:
    from pipeline import PipelineRunner, PipelineConfig

    config = PipelineConfig()
    runner = PipelineRunner(config)
    success = runner.run_pipeline('input.csv', 'output/')
"""

from .pipeline_config import PipelineConfig
from .pipeline_runner import PipelineRunner

__version__ = "2.0.0"
__all__ = ['PipelineConfig', 'PipelineRunner']