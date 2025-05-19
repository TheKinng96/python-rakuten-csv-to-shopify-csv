# MVP TODO List

## Core Implementation
- [ ] Create project structure with required files:
  - [ ] app.py (main entry point)
  - [ ] splitter.py (CSV splitting logic)
  - [ ] grouper.py (grouping logic)
  - [ ] ui.py (GUI implementation)
  - [ ] utils.py (file I/O helpers)
  - [ ] requirements.txt

## Core Features
- [ ] Implement CSV splitting functionality
  - [ ] Read CSV in binary mode by chunks
  - [ ] Write out file_partN.csv until EOF
  - [ ] Maintain minimal RAM footprint

- [ ] Implement grouping logic
  - [ ] Build catalog_id → List[sku] mapping
  - [ ] Implement common segment-prefix finding
  - [ ] Handle single SKU case
  - [ ] Handle empty prefix case

- [ ] Create GUI interface
  - [ ] File Browser for CSV selection
  - [ ] Column selection inputs
  - [ ] Load and Process buttons
  - [ ] Progress bar
  - [ ] Status text display

## File Operations
- [ ] Implement read_csv functionality
  - [ ] Extract SKU and Catalog ID columns
  - [ ] Support both column indices and headers

- [ ] Implement write_csv functionality
  - [ ] Write summary CSV with handler, variants, count
  - [ ] Handle proper CSV formatting

## CLI Support
- [ ] Add command line interface
  - [ ] Parse input arguments
  - [ ] Support --input, --sku-col, --id-col, --output flags
  - [ ] Bypass GUI for direct processing

## Packaging
- [ ] Set up PyInstaller configuration
- [ ] Create single binary executable
- [ ] Test distribution package

## Basic Testing
- [ ] Create basic unit tests
  - [ ] Test CSV reading
  - [ ] Test grouping logic
  - [ ] Test CSV writing

## Documentation
- [ ] Update README with usage instructions
  - [ ] Installation steps
  - [ ] GUI usage guide
  - [ ] CLI usage guide
  - [ ] Troubleshooting
