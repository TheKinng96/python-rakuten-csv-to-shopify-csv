# Changelog

All notable changes to the Rakuten to Shopify CSV Converter will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0] - 2025-09-18

### Added
- Complete rewrite with modular 15-step pipeline architecture
- Modern Python tooling with uv package manager
- Comprehensive pyproject.toml configuration
- Pre-commit hooks for code quality
- Makefile for common development tasks
- Type hints throughout the codebase
- Extensive logging and error handling
- Quality validation with detailed reporting
- Export generation with multiple output formats

### Changed
- **BREAKING**: New package structure under `src/rakuten_to_shopify/`
- **BREAKING**: New CLI interface with improved argument handling
- Switched from requirements.txt to pyproject.toml
- Updated dependencies to latest versions
- Improved HTML processing with BeautifulSoup4
- Enhanced error handling and validation

### Fixed
- All production issues from manual/final → API fixes → tax compliance
- Image URL pattern fixes (gold pattern correction)
- Japanese tax classification (8% vs 10% rates)
- Comprehensive metafield mapping (28 custom + 17 Shopify)
- Proper variant grouping and handle derivation
- HTML responsive table processing
- EC-UP block removal and marketing content filtering

### Technical Details
- 15 modular pipeline steps (00-14)
- Complete 86-column Shopify export format
- Encoding support (Shift-JIS input → UTF-8 output)
- Memory-efficient processing for large files
- Comprehensive test coverage preparation
- Documentation improvements

## [1.0.0] - Previous versions

Legacy implementation with various scripts and manual processes.
See git history for detailed changes in previous iterations.