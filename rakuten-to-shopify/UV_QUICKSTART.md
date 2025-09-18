# Quick Start with uv

This guide shows how to get up and running with the Rakuten to Shopify converter using `uv` for fast, modern Python dependency management.

## Prerequisites

- Python 3.9+ (recommended: 3.11+)
- `uv` package manager

## Installation

### 1. Install uv (if not already installed)

```bash
# On macOS and Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# On Windows
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"

# Via pip (alternative)
pip install uv
```

### 2. Clone and Setup Project

```bash
# Navigate to the project directory
cd rakuten-to-shopify

# Install dependencies and create virtual environment
uv sync

# Verify installation
uv run python -c "from rakuten_to_shopify import PipelineConfig; print('✅ Installation successful!')"
```

## Usage

### Basic Conversion

```bash
# Convert a Rakuten CSV file
uv run python convert.py data/rakuten_item.csv

# With custom output directory
uv run python convert.py data/rakuten_item.csv --output-dir output/my_conversion/

# With debug logging
uv run python convert.py data/rakuten_item.csv --log-level DEBUG
```

### Development Workflow

```bash
# Complete development setup
make setup

# Run tests
make test

# Check code quality
make lint

# Format code
make format

# Run converter with make
make run INPUT_FILE=data/rakuten_item.csv
```

## Key Benefits of uv

✅ **10-100x faster** dependency resolution than pip
✅ **Reproducible builds** with uv.lock file
✅ **Better conflict resolution** and dependency management
✅ **Cross-platform compatibility**
✅ **Modern Python tooling** standards

## Common Commands

```bash
# Install dependencies
uv sync

# Install with development dependencies
uv sync --extra dev --extra test

# Add a new dependency
uv add requests

# Add a development dependency
uv add --dev pytest

# Update dependencies
uv sync --upgrade

# Run scripts in the virtual environment
uv run python script.py
uv run pytest
uv run black .

# Activate virtual environment (optional)
source .venv/bin/activate  # Linux/Mac
# or
.venv\Scripts\activate     # Windows
```

## Output Files

The converter generates several files:

- `shopify_products_YYYYMMDD_HHMMSS.csv` - Main Shopify import file
- `products_summary_YYYYMMDD_HHMMSS.csv` - Product summary
- `pipeline_execution_report_YYYYMMDD_HHMMSS.json` - Detailed processing report
- `quality_validation_report_YYYYMMDD_HHMMSS.json` - Quality metrics
- Various analysis files in the output directory

## Troubleshooting

### Common Issues

1. **Import errors**: Make sure you're using `uv run` to execute Python commands
2. **Missing dependencies**: Run `uv sync` to install all required packages
3. **Permission errors**: Ensure you have write access to the output directory

### Getting Help

```bash
# Show CLI help
uv run python convert.py --help

# Show available make commands
make help

# Check installation
uv run python -c "import rakuten_to_shopify; print('OK')"
```

## Next Steps

1. Place your `rakuten_item.csv` file in the `data/` directory
2. Run the conversion: `uv run python convert.py data/rakuten_item.csv`
3. Review the output files in the `output/` directory
4. Import the main CSV file into Shopify

For detailed configuration options, see `REQUIREMENTS.md`.