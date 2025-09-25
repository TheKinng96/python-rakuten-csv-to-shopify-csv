#!/usr/bin/env python3

import sys
import pandas as pd
import pickle
import logging
from pathlib import Path

# Setup logging to capture debug output
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('debug_step05.log', mode='w')
    ]
)

# Add the src directory to the path
sys.path.append('src')

from rakuten_to_shopify.pipeline.pipeline_config import PipelineConfig
from rakuten_to_shopify.pipeline.steps.step_05_metafield_mapping import execute

# Load configuration
config = PipelineConfig()

# Load the step 04 output
logger = logging.getLogger(__name__)
logger.info("Loading step 04 output...")
with open('step_output/step_04_output.pkl', 'rb') as f:
    df = pickle.load(f)

logger.info(f"Loaded DataFrame with {len(df)} rows")

# Execute step 05
logger.info("Executing step 05 metafield mapping...")
result = execute({
    'image_processed_df': df,
    'config': config,
    'output_dir': Path('step_output')
})

# Save the output
logger.info("Saving step 05 output...")
result_df = result['df']
with open('step_output/step_05_output_debug.pkl', 'wb') as f:
    pickle.dump(result_df, f)

logger.info("Step 05 completed successfully")
print("Check debug_step05.log for detailed logs")