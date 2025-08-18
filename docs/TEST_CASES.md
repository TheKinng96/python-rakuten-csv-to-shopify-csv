# Test Cases for Shopify Processing Scripts

## Script 00: CSV to JSON Converter (`00_import_to_test.py`)

### Happy Path Test Cases

#### TC-00-001: Standard CSV Processing
- **Input**: 5 CSV files with valid product data (~1000 products each)
- **Expected Output**: 
  - `shared/products_for_import.json` created
  - JSON contains metadata + data array
  - Each product has handle, title, variants, images
  - Progress messages: "xxx/xxx products processed"
- **Validation**: JSON structure matches GraphQL requirements

#### TC-00-002: Large File Processing  
- **Input**: CSV files totaling 500MB+ with 100k+ products
- **Expected Output**:
  - Memory usage stays under 2GB during processing
  - Processing completes without timeout
  - All products converted successfully
- **Validation**: Performance benchmarks met

#### TC-00-003: Multiple Variants Per Product
- **Input**: Products with 5+ variants (different SKUs, options)
- **Expected Output**:
  - Variants properly grouped by Handle
  - Options extracted correctly (Option1 Name/Value)
  - Inventory and pricing data preserved
- **Validation**: Variant count matches input data

### Edge Cases

#### TC-00-E001: Empty CSV Files
- **Input**: Empty CSV files or files with headers only
- **Expected Output**: 
  - Script completes without error
  - Empty JSON file with metadata created
  - Console message: "No products found in CSV files"

#### TC-00-E002: Malformed CSV Data
- **Input**: CSV with missing required columns (Handle, Title)
- **Expected Output**:
  - Products with missing handles skipped with warning
  - Valid products still processed
  - Error count reported in summary

#### TC-00-E003: Special Characters in Data
- **Input**: Product data with emojis, HTML entities, Unicode
- **Expected Output**:
  - Characters preserved in JSON (UTF-8 encoding)
  - No encoding errors or data corruption
  - Special characters displayable in JSON

#### TC-00-E004: Duplicate Product Handles
- **Input**: Multiple CSV rows with same Handle
- **Expected Output**:
  - Rows grouped by Handle correctly
  - All variants and images preserved
  - No data loss during grouping

---

## Script 01: SS Images Analysis (`01_analyze_ss_images.py`)

### Happy Path Test Cases

#### TC-01-001: Standard SS Image Detection
- **Input**: Products with `-12ss.jpg`, `-3ss.jpg` image URLs
- **Expected Output**:
  - `shared/ss_images_to_remove.json` created
  - Images with SS pattern detected correctly
  - Pattern matching includes SS number extraction
- **Validation**: All `-XXss.jpg` patterns found

#### TC-01-002: Mixed Image Types
- **Input**: Products with both normal and SS images
- **Expected Output**:
  - Only SS images flagged for removal
  - Normal images (`-1.jpg`, `-main.jpg`) ignored
  - Position and alt text preserved
- **Validation**: No false positives

### Edge Cases

#### TC-01-E001: No SS Images Found
- **Input**: CSV files with no `-XXss.jpg` patterns
- **Expected Output**:
  - Empty JSON file created with metadata
  - Console message: "No products with -XXss images found!"
  - Script completes successfully

#### TC-01-E002: Malformed Image URLs
- **Input**: Invalid URLs, missing extensions, broken patterns
- **Expected Output**:
  - Malformed URLs skipped without error
  - Valid SS images still processed
  - Error handling graceful

#### TC-01-E003: Case Sensitivity
- **Input**: SS patterns in different cases (`-3SS.JPG`, `-12ss.jpg`)
- **Expected Output**:
  - All case variations detected (regex case-insensitive)
  - Consistent output format regardless of input case

---

## Script 02: HTML Tables Analysis (`02_analyze_html_tables.py`)

### Happy Path Test Cases

#### TC-02-001: Standard Table Issues
- **Input**: HTML with unclosed `<table>`, `<tr>`, `<td>` tags
- **Expected Output**:
  - `shared/html_tables_to_fix.json` created
  - Issues categorized by severity (high/medium/low)
  - Specific issue types identified correctly
- **Validation**: All structural issues detected

#### TC-02-002: Nested Table Detection
- **Input**: Tables within tables (layout issues)
- **Expected Output**:
  - Nested tables flagged as medium severity
  - Nesting depth calculated correctly
  - Layout table heuristics applied

### Edge Cases

#### TC-02-E001: No HTML Tables
- **Input**: Products with plain text or non-table HTML
- **Expected Output**:
  - Empty JSON file created
  - Console message: "No HTML table issues found!"
  - Non-table HTML ignored safely

#### TC-02-E002: Malformed HTML
- **Input**: Severely broken HTML that can't be parsed
- **Expected Output**:
  - Parsing errors caught and reported
  - Other products continue processing
  - Error details included in JSON

#### TC-02-E003: Valid Table Structures
- **Input**: Properly formed HTML tables
- **Expected Output**:
  - Valid tables ignored (no issues reported)
  - Only problematic tables included in output
  - False positive rate near zero

---

## Script 03: Rakuten Content Analysis (`03_analyze_rakuten.py`)

### Happy Path Test Cases

#### TC-03-001: Standard EC-UP Patterns
- **Input**: HTML with `<!--EC-UP_Rakuichi_123_START-->...<!--EC-UP_Rakuichi_123_END-->`
- **Expected Output**:
  - `shared/rakuten_content_to_clean.json` created
  - Pattern names and types extracted correctly
  - Content analysis (images, links, styling) accurate
- **Validation**: All EC-UP blocks found and categorized

#### TC-03-002: Multiple Pattern Types
- **Input**: Various EC-UP patterns (Favorite, Similar, SameTime, etc.)
- **Expected Output**:
  - All pattern types detected
  - Unknown patterns categorized as 'unknown'
  - Content size estimates accurate

### Edge Cases

#### TC-03-E001: No EC-UP Content
- **Input**: Clean HTML without any EC-UP patterns
- **Expected Output**:
  - Empty JSON file created
  - Console message: "No EC-UP patterns found!"
  - Processing completes normally

#### TC-03-E002: Malformed EC-UP Patterns
- **Input**: Incomplete patterns (missing START/END tags)
- **Expected Output**:
  - Incomplete patterns ignored safely
  - Valid patterns still processed
  - No processing errors

#### TC-03-E003: Nested or Complex Content
- **Input**: EC-UP blocks with complex HTML, CSS, JavaScript
- **Expected Output**:
  - Content analysis handles complexity
  - Styling information extracted correctly
  - Content preview truncated appropriately

---

## Script 04: Missing Images Audit (`04_audit_images.py`)

### Happy Path Test Cases

#### TC-04-001: Standard Missing Images
- **Input**: Products with zero images, variants without specific images
- **Expected Output**:
  - `shared/missing_images_audit.json` created
  - Priority levels assigned correctly (high/medium/low)
  - Product and variant data complete
- **Validation**: All missing image cases identified

#### TC-04-002: Priority Classification
- **Input**: Products with priority keywords (限定, 特別, etc.)
- **Expected Output**:
  - High priority assigned to premium products
  - Variant count influences priority
  - Keywords detected in titles and tags

### Edge Cases

#### TC-04-E001: All Products Have Images
- **Input**: Complete product catalog with adequate images
- **Expected Output**:
  - Empty JSON file created
  - Console message: "All products have adequate images!"
  - No false positives

#### TC-04-E002: CSV vs Shopify Mismatch
- **Input**: Products in CSV that don't exist in Shopify
- **Expected Output**:
  - Missing products logged separately
  - Available products still audited
  - Clear reporting of discrepancies

---

## Integration Test Cases

### TC-INT-001: Full Workflow
- **Input**: Complete production dataset
- **Expected Output**:
  - All 5 scripts run successfully in sequence
  - JSON files created for each analysis type
  - No data corruption between steps
- **Validation**: End-to-end workflow completion

### TC-INT-002: Concurrent Execution
- **Input**: Multiple scripts running simultaneously
- **Expected Output**:
  - No file locking conflicts
  - Independent processing without interference
  - Separate output files maintained

### TC-INT-003: Memory and Performance
- **Input**: Maximum realistic dataset size
- **Expected Output**:
  - Memory usage under system limits
  - Processing time within acceptable bounds
  - No memory leaks or resource exhaustion

---

## Error Recovery Test Cases

### TC-ERR-001: Interrupted Processing
- **Input**: Script interrupted mid-processing (Ctrl+C)
- **Expected Output**:
  - Graceful shutdown with progress reported
  - Partial results saved where possible
  - No corrupted output files

### TC-ERR-002: Disk Space Issues
- **Input**: Insufficient disk space for output files
- **Expected Output**:
  - Clear error message about disk space
  - No partial/corrupted files left behind
  - Graceful failure handling

### TC-ERR-003: Permission Issues
- **Input**: Read-only directories or permission restrictions
- **Expected Output**:
  - Clear permission error messages
  - Alternative suggestions provided
  - No silent failures

---

## Data Validation Test Cases

### TC-VAL-001: JSON Schema Validation
- **Input**: Generated JSON files from all scripts
- **Expected Output**:
  - All JSON files validate against expected schema
  - Required fields present in all records
  - Data types consistent and correct

### TC-VAL-002: GraphQL Compatibility
- **Input**: JSON data consumed by Node.js GraphQL operations
- **Expected Output**:
  - No parsing errors in Node.js
  - All required fields accessible
  - Data format matches GraphQL expectations

### TC-VAL-003: Character Encoding
- **Input**: International characters, emojis, special symbols
- **Expected Output**:
  - UTF-8 encoding preserved throughout
  - No character corruption or mojibake
  - JSON parseable by standard tools