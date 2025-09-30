"""
Pipeline Runner

Orchestrates the complete Rakuten to Shopify conversion pipeline.
Executes all 15 steps in sequence with proper error handling and logging.
"""

import logging
import sys
import time
from pathlib import Path
from typing import Dict, Any, Optional
import pandas as pd

from .pipeline_config import PipelineConfig
from ..transformation_summary import TransformationSummary, QuietProgressIndicator, create_step_summary
from .steps import (
    step_00_load_and_validate,
    step_01_initial_cleaning,
    step_02_sku_processing,
    step_03_html_processing,
    step_04_image_processing,
    step_05_metafield_mapping,
    step_06_image_restructuring,
    step_07_tax_mapping,
    step_08_data_cleanup,
    step_09_variant_image_separation
)


class PipelineRunner:
    """Main pipeline orchestrator"""

    def __init__(self, config: Optional[PipelineConfig] = None, quiet_mode: bool = False):
        self.config = config or PipelineConfig()
        self.quiet_mode = quiet_mode
        self.setup_logging()
        self.data: Dict[str, Any] = {}
        self.summary = TransformationSummary(quiet_mode)
        self.progress = QuietProgressIndicator() if quiet_mode else None

    def setup_logging(self):
        """Configure logging for pipeline execution"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('pipeline.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)

    def run_pipeline(self, input_file: str, output_dir: str) -> bool:
        """
        Execute the complete pipeline

        Args:
            input_file: Path to rakuten_item.csv
            output_dir: Directory for output files

        Returns:
            bool: True if pipeline completed successfully
        """
        start_time = time.time()

        if not self.quiet_mode:
            self.logger.info("Starting Rakuten to Shopify conversion pipeline")

        # Show clean header
        self.summary.print_header(input_file, output_dir)

        try:
            # Initialize pipeline context
            self.data = {
                'input_file': input_file,
                'output_dir': Path(output_dir),
                'config': self.config,
                'stats': {'start_time': start_time}
            }

            # Execute all pipeline steps
            steps = [
                ("00", "Load and Validate", step_00_load_and_validate),
                ("01", "Initial Cleaning", step_01_initial_cleaning),
                ("02", "SKU Processing", step_02_sku_processing),
                ("03", "HTML Processing", step_03_html_processing),
                ("04", "Image Processing", step_04_image_processing),
                ("05", "Metafield Mapping", step_05_metafield_mapping),
                ("06", "Image Restructuring", step_06_image_restructuring),
                ("07", "Tax Mapping", step_07_tax_mapping),
                ("08", "Data Cleanup", step_08_data_cleanup),
                ("09", "Variant Image Separation", step_09_variant_image_separation)
            ]

            for step_num, step_name, step_func in steps:
                self._run_step(step_num, step_name, step_func)

            # Pipeline completed successfully
            duration = time.time() - start_time
            self.logger.info(f"Pipeline completed successfully in {duration:.2f} seconds")
            self._log_final_stats()

            return True

        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
            return False

    def _run_step(self, step_num: str, step_name: str, step_func):
        """Execute a single pipeline step with error handling"""
        step_start = time.time()
        self.logger.info(f"Step {step_num}: {step_name}")

        try:
            # Execute step function
            result = step_func.execute(self.data)

            # Update pipeline data with step results
            if result:
                self.data.update(result)

            step_duration = time.time() - step_start
            self.logger.info(f"Step {step_num} completed in {step_duration:.2f} seconds")

        except Exception as e:
            self.logger.error(f"Step {step_num} failed: {str(e)}", exc_info=True)
            raise

    def _log_final_stats(self):
        """Log final pipeline statistics"""
        if 'final_df' in self.data:
            df = self.data['final_df']
            products = len(df['Handle'].dropna().unique())
            variants = len(df)

            self.logger.info(f"Final output: {products} products, {variants} variants")

        if 'stats' in self.data:
            stats = self.data['stats']
            for key, value in stats.items():
                if key != 'start_time':
                    self.logger.info(f"Stats - {key}: {value}")


def main():
    """CLI entry point for pipeline execution"""
    import argparse

    parser = argparse.ArgumentParser(description='Rakuten to Shopify CSV Converter')
    parser.add_argument('input_file', help='Path to rakuten_item.csv')
    parser.add_argument('--output-dir', default='./output', help='Output directory')
    parser.add_argument('--config', help='Path to custom config file')

    args = parser.parse_args()

    # Load configuration
    config = PipelineConfig(args.config) if args.config else PipelineConfig()

    # Create and run pipeline
    runner = PipelineRunner(config)
    success = runner.run_pipeline(args.input_file, args.output_dir)

    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()