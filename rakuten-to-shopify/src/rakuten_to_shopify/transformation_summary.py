"""
Clean CSV Transformation Summary

Provides a clear, step-by-step view of what changes during the Rakuten to Shopify conversion.
Focus on CSV in → CSV out with essential transformation details.
"""

import sys
from typing import Dict, Any, List
from datetime import datetime


class TransformationSummary:
    """Clean summary of CSV transformation steps"""

    def __init__(self, quiet_mode: bool = False):
        self.quiet_mode = quiet_mode
        self.steps: List[Dict[str, Any]] = []
        self.start_time = datetime.now()

    def print_header(self, input_file: str, output_dir: str):
        """Print clean header"""
        if not self.quiet_mode:
            print("\n" + "=" * 80)
            print("🔄 RAKUTEN → SHOPIFY CSV CONVERSION")
            print("=" * 80)
            print(f"📥 Input:  {input_file}")
            print(f"📤 Output: {output_dir}")
            print(f"⏰ Started: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 80)

    def add_step(self, step_name: str, before_count: int, after_count: int,
                 changes: List[str] = None, duration: float = 0):
        """Add a transformation step"""
        step_data = {
            'name': step_name,
            'before': before_count,
            'after': after_count,
            'changes': changes or [],
            'duration': duration
        }
        self.steps.append(step_data)

        if not self.quiet_mode:
            self._print_step(step_data)

    def _print_step(self, step: Dict[str, Any]):
        """Print a single step cleanly"""
        name = step['name']
        before = step['before']
        after = step['after']
        changes = step['changes']

        # Calculate change
        if before == after:
            change_str = "no change"
        elif after > before:
            change_str = f"+{after - before}"
        else:
            change_str = f"-{before - after}"

        print(f"📋 {name:<30} {before:>6} → {after:<6} ({change_str})")

        # Show key changes
        if changes:
            for change in changes[:3]:  # Show max 3 changes
                print(f"   • {change}")
            if len(changes) > 3:
                print(f"   • ... and {len(changes) - 3} more changes")

    def print_data_transformation_summary(self, initial_data: Dict[str, Any],
                                        final_data: Dict[str, Any]):
        """Print before/after data comparison"""
        if not self.quiet_mode:
            print("\n" + "-" * 80)
            print("📊 DATA TRANSFORMATION SUMMARY")
            print("-" * 80)

            # CSV structure comparison
            print("📁 CSV Structure:")
            print(f"   Rows:    {initial_data.get('rows', 0):>6} → {final_data.get('rows', 0):<6}")
            print(f"   Columns: {initial_data.get('columns', 0):>6} → {final_data.get('columns', 0):<6}")

            # Product/variant breakdown
            if 'products' in final_data and 'variants' in final_data:
                print(f"   Products: {final_data['products']:>5} ({final_data['variants']} variants)")

            # Key transformations
            print("\n🔧 Key Transformations:")
            transformations = [
                "✓ Japanese text encoding (Shift-JIS → UTF-8)",
                "✓ HTML description cleaning & responsive tables",
                "✓ Image URL fixes (gold pattern correction)",
                "✓ Tax classification (8% food vs 10% general)",
                "✓ Variant grouping & handle generation",
                "✓ Metafield mapping (Rakuten → Shopify)",
                "✓ SEO title & description generation",
                "✓ 86-column Shopify CSV format"
            ]

            for transform in transformations:
                print(f"   {transform}")

    def print_output_files(self, output_files: Dict[str, str]):
        """Print generated output files"""
        if not self.quiet_mode:
            print("\n" + "-" * 80)
            print("📁 OUTPUT FILES GENERATED")
            print("-" * 80)

            # Main CSV
            if 'main_csv' in output_files:
                print(f"📋 Main CSV:     {output_files['main_csv']}")

            # Additional exports
            additional = ['products_summary', 'variants_export', 'metafields_export', 'images_export']
            for file_type in additional:
                if file_type in output_files:
                    friendly_name = file_type.replace('_', ' ').title()
                    print(f"📄 {friendly_name:<12} {output_files[file_type]}")

            # Reports
            reports = ['pipeline_report', 'validation_report', 'metafield_report']
            print("\n📊 Reports:")
            for report_type in reports:
                if report_type in output_files:
                    friendly_name = report_type.replace('_', ' ').title()
                    print(f"   {friendly_name:<20} {output_files[report_type]}")

    def print_footer(self, success: bool, total_duration: float):
        """Print clean footer with results"""
        if not self.quiet_mode:
            end_time = datetime.now()
            print("\n" + "=" * 80)

            if success:
                print("✅ CONVERSION COMPLETED SUCCESSFULLY")
                print(f"⏱️  Total time: {total_duration:.2f} seconds")
                print(f"🏁 Finished: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
                print("\n🎯 Ready for Shopify import!")
            else:
                print("❌ CONVERSION FAILED")
                print("📝 Check rakuten_to_shopify.log for detailed error information")

            print("=" * 80)

    def get_summary_data(self) -> Dict[str, Any]:
        """Get summary data for reporting"""
        return {
            'steps': self.steps,
            'start_time': self.start_time,
            'total_steps': len(self.steps),
            'successful_steps': len([s for s in self.steps if s['after'] >= 0])
        }


class QuietProgressIndicator:
    """Simple progress indicator for quiet mode"""

    def __init__(self):
        self.current_step = 0
        self.total_steps = 15

    def update(self, step_name: str = ""):
        """Update progress"""
        self.current_step += 1
        progress = (self.current_step / self.total_steps) * 100

        # Simple progress bar
        bar_length = 40
        filled_length = int(bar_length * self.current_step // self.total_steps)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)

        print(f"\r🔄 Processing... [{bar}] {progress:5.1f}% {step_name[:20]:<20}", end='', flush=True)

        if self.current_step >= self.total_steps:
            print()  # New line when complete


def create_step_summary(step_name: str, stats: Dict[str, Any]) -> List[str]:
    """Create a clean summary of step changes"""
    changes = []

    # Common patterns for different step types
    if 'rows_removed' in stats:
        changes.append(f"Removed {stats['rows_removed']} invalid rows")

    if 'html_descriptions_cleaned' in stats:
        changes.append(f"Cleaned {stats['html_descriptions_cleaned']} HTML descriptions")

    if 'images_processed' in stats:
        changes.append(f"Processed {stats['images_processed']} product images")

    if 'metafields_mapped' in stats:
        changes.append(f"Mapped {stats['metafields_mapped']} metafields")

    if 'tax_classifications' in stats:
        changes.append(f"Classified tax rates: {stats.get('tax_8_percent', 0)} @ 8%, {stats.get('tax_10_percent', 0)} @ 10%")

    if 'variants_grouped' in stats:
        changes.append(f"Grouped {stats['variants_grouped']} variants into products")

    return changes