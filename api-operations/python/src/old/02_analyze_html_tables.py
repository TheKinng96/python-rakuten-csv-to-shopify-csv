#!/usr/bin/env python3
"""
Script to analyze and identify HTML table structure issues for GraphQL fixes

This script:
1. Scans all CSV files to identify products with problematic HTML table structures
2. Analyzes table structures for common issues (unclosed tags, layout problems)
3. Generates JSON data for Node.js GraphQL operations
4. Outputs: shared/html_tables_to_fix.json
"""
import re
import sys
from pathlib import Path
from typing import List, Dict, Any, Set

import pandas as pd
from bs4 import BeautifulSoup, Tag
from tqdm import tqdm

# Add utils to path
sys.path.append(str(Path(__file__).parent))
from utils.json_output import (
    save_json_report,
    create_html_table_fix_record,
    log_processing_summary,
    validate_json_structure
)


class HTMLTableAnalyzer:
    """Analyzes HTML table structures for common issues"""
    
    def __init__(self):
        # Common table issues to detect
        self.table_issues_patterns = {
            'unclosed_table': re.compile(r'<table[^>]*>(?!.*</table>)', re.IGNORECASE | re.DOTALL),
            'unclosed_tr': re.compile(r'<tr[^>]*>(?!.*</tr>)', re.IGNORECASE | re.DOTALL),
            'unclosed_td': re.compile(r'<td[^>]*>(?!.*</td>)', re.IGNORECASE | re.DOTALL),
            'nested_tables': re.compile(r'<table[^>]*>.*?<table[^>]*>', re.IGNORECASE | re.DOTALL),
            'empty_cells': re.compile(r'<td[^>]*>\s*</td>', re.IGNORECASE),
            'malformed_colspan': re.compile(r'colspan\s*=\s*["\']?\d+["\']?\s*>', re.IGNORECASE),
            'inline_styles': re.compile(r'<(?:table|tr|td|th)[^>]*style\s*=\s*["\'][^"\']*["\'][^>]*>', re.IGNORECASE)
        }
    
    def analyze_html_content(self, html_content: str) -> Dict[str, Any]:
        """
        Analyze HTML content for table structure issues
        Returns analysis results with issues found
        """
        if not html_content or pd.isna(html_content):
            return {'issues_found': [], 'has_tables': False, 'table_count': 0}
        
        html_str = str(html_content)
        issues_found = []
        
        # Check if content has tables at all
        has_tables = bool(re.search(r'<table[^>]*>', html_str, re.IGNORECASE))
        if not has_tables:
            return {'issues_found': [], 'has_tables': False, 'table_count': 0}
        
        # Count tables
        table_matches = re.findall(r'<table[^>]*>', html_str, re.IGNORECASE)
        table_count = len(table_matches)
        
        # Check for pattern-based issues
        for issue_type, pattern in self.table_issues_patterns.items():
            matches = pattern.findall(html_str)
            if matches:
                issues_found.append({
                    'type': issue_type,
                    'count': len(matches),
                    'severity': self._get_issue_severity(issue_type),
                    'description': self._get_issue_description(issue_type)
                })
        
        # Use BeautifulSoup for structural analysis
        try:
            soup = BeautifulSoup(html_str, 'lxml')
            structural_issues = self._analyze_table_structure(soup)
            issues_found.extend(structural_issues)
        except Exception as e:
            issues_found.append({
                'type': 'parsing_error',
                'count': 1,
                'severity': 'medium',
                'description': f'HTML parsing failed: {str(e)[:100]}'
            })
        
        return {
            'issues_found': issues_found,
            'has_tables': True,
            'table_count': table_count,
            'total_issues': len(issues_found)
        }
    
    def _analyze_table_structure(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Analyze table structure using BeautifulSoup"""
        structural_issues = []
        
        tables = soup.find_all('table')
        
        for i, table in enumerate(tables):
            # Check for tables without proper rows
            rows = table.find_all('tr')
            if not rows:
                structural_issues.append({
                    'type': 'table_without_rows',
                    'count': 1,
                    'severity': 'high',
                    'description': f'Table {i+1} has no rows'
                })
            
            # Check for rows without cells
            for j, row in enumerate(rows):
                cells = row.find_all(['td', 'th'])
                if not cells:
                    structural_issues.append({
                        'type': 'row_without_cells',
                        'count': 1,
                        'severity': 'high',
                        'description': f'Table {i+1}, row {j+1} has no cells'
                    })
            
            # Check for excessive nesting
            nested_depth = self._get_nesting_depth(table)
            if nested_depth > 3:
                structural_issues.append({
                    'type': 'excessive_nesting',
                    'count': 1,
                    'severity': 'medium',
                    'description': f'Table {i+1} has nesting depth of {nested_depth}'
                })
            
            # Check for layout table indicators
            if self._is_layout_table(table):
                structural_issues.append({
                    'type': 'layout_table',
                    'count': 1,
                    'severity': 'low',
                    'description': f'Table {i+1} appears to be used for layout'
                })
        
        return structural_issues
    
    def _get_nesting_depth(self, element: Tag, depth: int = 0) -> int:
        """Calculate maximum nesting depth of an element"""
        if not hasattr(element, 'find_all'):
            return depth
        
        max_depth = depth
        for child in element.find_all(recursive=False):
            child_depth = self._get_nesting_depth(child, depth + 1)
            max_depth = max(max_depth, child_depth)
        
        return max_depth
    
    def _is_layout_table(self, table: Tag) -> bool:
        """Check if table is being used for layout (not data)"""
        # Heuristics for layout tables
        layout_indicators = 0
        
        # Check for width/height attributes
        if table.get('width') or table.get('height'):
            layout_indicators += 1
        
        # Check for cellpadding/cellspacing
        if table.get('cellpadding') or table.get('cellspacing'):
            layout_indicators += 1
        
        # Check for border=0
        if table.get('border') == '0':
            layout_indicators += 1
        
        # Check for single column/row
        rows = table.find_all('tr')
        if len(rows) == 1:
            layout_indicators += 1
        elif len(rows) > 0:
            cells_per_row = [len(row.find_all(['td', 'th'])) for row in rows]
            if max(cells_per_row) <= 1:
                layout_indicators += 1
        
        return layout_indicators >= 2
    
    def _get_issue_severity(self, issue_type: str) -> str:
        """Get severity level for issue type"""
        severity_map = {
            'unclosed_table': 'high',
            'unclosed_tr': 'high',
            'unclosed_td': 'medium',
            'nested_tables': 'medium',
            'empty_cells': 'low',
            'malformed_colspan': 'medium',
            'inline_styles': 'low',
            'table_without_rows': 'high',
            'row_without_cells': 'high',
            'excessive_nesting': 'medium',
            'layout_table': 'low'
        }
        return severity_map.get(issue_type, 'medium')
    
    def _get_issue_description(self, issue_type: str) -> str:
        """Get human-readable description for issue type"""
        descriptions = {
            'unclosed_table': 'Table tags not properly closed',
            'unclosed_tr': 'Table row tags not properly closed',
            'unclosed_td': 'Table cell tags not properly closed',
            'nested_tables': 'Tables nested inside other tables',
            'empty_cells': 'Empty table cells found',
            'malformed_colspan': 'Malformed colspan attributes',
            'inline_styles': 'Inline styles on table elements'
        }
        return descriptions.get(issue_type, issue_type.replace('_', ' ').title())


def get_csv_files() -> List[Path]:
    """Get all CSV files from data directory"""
    data_dir = Path(__file__).parent.parent / "data"
    return list(data_dir.glob("products_export_*.csv"))


def analyze_csv_files_for_html_tables() -> List[Dict[str, Any]]:
    """
    Analyze CSV files to find products with HTML table issues
    Returns list of records for JSON output
    """
    print("🔍 Analyzing CSV files for HTML table issues...")
    
    analyzer = HTMLTableAnalyzer()
    table_records = []
    csv_files = get_csv_files()
    total_rows = 0
    total_tables = 0
    
    for csv_file in csv_files:
        print(f"   📄 Processing {csv_file.name}...")
        
        try:
            # Read CSV in chunks
            chunk_size = 1000
            chunk_iter = pd.read_csv(
                csv_file,
                chunksize=chunk_size,
                encoding='utf-8',
                low_memory=False
            )
            
            file_records = {}  # Group by handle
            
            for chunk in chunk_iter:
                total_rows += len(chunk)
                
                for _, row in chunk.iterrows():
                    handle = row.get('Handle', '')
                    body_html = row.get('Body (HTML)', '')
                    
                    if pd.isna(body_html) or not body_html or pd.isna(handle) or not handle:
                        continue
                    
                    # Analyze HTML table structure
                    analysis = analyzer.analyze_html_content(str(body_html))
                    
                    if analysis['has_tables'] and analysis['total_issues'] > 0:
                        total_tables += analysis['table_count']
                        
                        if handle not in file_records:
                            # Create issue summary for JSON
                            issues_summary = []
                            for issue in analysis['issues_found']:
                                issues_summary.append(f"{issue['type']}: {issue['description']} (severity: {issue['severity']})")
                            
                            file_records[handle] = {
                                'productHandle': handle,
                                'productId': None,  # Will be filled by Node.js
                                'currentHtml': str(body_html),
                                'htmlLength': len(str(body_html)),
                                'tableCount': analysis['table_count'],
                                'issuesFound': issues_summary,
                                'totalIssues': analysis['total_issues'],
                                'highSeverityIssues': sum(1 for issue in analysis['issues_found'] if issue['severity'] == 'high'),
                                'priority': 'high' if any(issue['severity'] == 'high' for issue in analysis['issues_found']) else 'medium'
                            }
            
            # Add file records to main list
            table_records.extend(file_records.values())
            print(f"      ✅ Found {len(file_records)} products with table issues")
                        
        except Exception as e:
            print(f"      ❌ Error processing {csv_file.name}: {e}")
            continue
    
    print(f"\n📊 Analysis Summary:")
    print(f"   📄 CSV rows processed: {total_rows:,}")
    print(f"   🎯 Products with table issues: {len(table_records)}")
    print(f"   📊 Total tables analyzed: {total_tables}")
    
    # Show priority distribution
    priority_counts = {}
    for record in table_records:
        priority = record['priority']
        priority_counts[priority] = priority_counts.get(priority, 0) + 1
    
    print(f"   📈 Priority distribution:")
    for priority, count in sorted(priority_counts.items()):
        print(f"      - {priority}: {count}")
    
    return table_records


def main():
    """Main execution function"""
    print("=" * 70)
    print("🔍 HTML TABLE STRUCTURE ANALYSIS (JSON OUTPUT)")
    print("=" * 70)
    
    try:
        # Phase 1: Analyze CSV files
        table_records = analyze_csv_files_for_html_tables()
        
        if not table_records:
            print("\n✅ No HTML table issues found!")
            # Save empty JSON file for consistency
            save_json_report([], "html_tables_to_fix.json", "No HTML table issues found in CSV data")
            return 0
        
        # Phase 2: Validate and save JSON
        print(f"\n💾 Saving JSON data for GraphQL processing...")
        
        required_fields = ['productHandle', 'currentHtml', 'issuesFound', 'totalIssues']
        if not validate_json_structure(table_records, required_fields):
            print("❌ JSON validation failed")
            return 1
        
        json_path = save_json_report(
            table_records,
            "html_tables_to_fix.json",
            f"Products with HTML table issues for fixing via GraphQL ({len(table_records)} products)"
        )
        
        # Phase 3: Generate summary list
        print(f"\n📋 Generating summary list...")
        
        summary_list = []
        for record in table_records:
            summary_list.append({
                'handle': record['productHandle'],
                'title': 'Title not available in CSV analysis',  # Title would need to be extracted from CSV
                'table_count': record['tableCount'],
                'total_issues': record['totalIssues'],
                'high_severity_issues': record['highSeverityIssues'],
                'priority': record['priority'],
                'top_issues': record['issuesFound'][:3]  # Show first 3 issues
            })
        
        # Sort by priority and severity
        summary_list.sort(key=lambda x: (0 if x['priority'] == 'high' else 1, -x['high_severity_issues']))
        
        # Print summary list to console  
        print(f"\n📝 Summary List - Products with HTML Table Issues:")
        print(f"{'Handle':<30} {'Tables':<8} {'Issues':<8} {'Priority':<10} {'Top Issues':<40}")
        print("-" * 100)
        
        for item in summary_list[:20]:  # Show first 20
            top_issues_str = '; '.join([issue.split(':')[0] for issue in item['top_issues']])
            if len(top_issues_str) > 37:
                top_issues_str = top_issues_str[:37] + "..."
                
            print(f"{item['handle']:<30} {item['table_count']:<8} {item['total_issues']:<8} "
                  f"{item['priority']:<10} {top_issues_str:<40}")
        
        if len(summary_list) > 20:
            print(f"... and {len(summary_list)-20} more products")
        
        print(f"\n🎉 Analysis completed successfully!")
        print(f"   📄 JSON data saved to: {json_path}")
        print(f"   🚀 Ready for GraphQL processing")
        print(f"\n💡 Next step: cd node && npm run fix-tables")
        
        return 0
        
    except KeyboardInterrupt:
        print("\n⏹️  Analysis interrupted by user")
        return 1
    except Exception as e:
        print(f"\n❌ Analysis failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())