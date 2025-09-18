"""
Simple CSV Transformation Summary

A clean, focused view of the Rakuten to Shopify conversion process.
Shows what actually matters: CSV in → transformations → CSV out.
"""

def print_conversion_header(input_file: str, output_dir: str):
    """Print simple conversion header"""
    print("\n" + "🔄 " + "=" * 60)
    print("   RAKUTEN → SHOPIFY CSV CONVERSION")
    print("=" * 64)
    print(f"📥 Input:  {input_file}")
    print(f"📤 Output: {output_dir}")
    print("=" * 64)


def print_step_summary(step_num: str, step_name: str, before: int, after: int, changes: list = None):
    """Print a clean step summary"""
    change_str = "no change"
    if after > before:
        change_str = f"+{after - before}"
    elif after < before:
        change_str = f"-{before - after}"

    print(f"📋 Step {step_num}: {step_name:<25} {before:>6} → {after:<6} ({change_str})")

    if changes:
        for change in changes[:2]:  # Show max 2 key changes
            print(f"    • {change}")


def print_final_summary(initial_rows: int, final_rows: int, products: int, variants: int,
                       output_files: list, duration: float):
    """Print final conversion summary"""
    print("\n" + "📊 " + "=" * 60)
    print("   CONVERSION SUMMARY")
    print("=" * 64)
    print(f"📊 Data:     {initial_rows:>6} rows → {final_rows} rows")
    print(f"🛍️  Result:   {products:>6} products ({variants} variants)")
    print(f"⏱️  Time:     {duration:>6.2f} seconds")
    print(f"📁 Files:    {len(output_files):>6} files generated")

    print("\n🎯 Key Transformations Applied:")
    transformations = [
        "✓ Encoding: Shift-JIS → UTF-8",
        "✓ HTML: Cleaned & responsive tables",
        "✓ Images: Fixed gold URL patterns",
        "✓ Tax: 8% food vs 10% general classification",
        "✓ Structure: Rakuten → 86-column Shopify format"
    ]

    for transform in transformations:
        print(f"   {transform}")

    print("\n📁 Main Output Files:")
    for file_path in output_files[:3]:  # Show top 3 files
        file_name = file_path.split('/')[-1] if '/' in file_path else file_path
        print(f"   📄 {file_name}")

    if len(output_files) > 3:
        print(f"   📄 ... and {len(output_files) - 3} more files")

    print("=" * 64)
    print("✅ READY FOR SHOPIFY IMPORT!")
    print("=" * 64)


def create_simple_progress_bar(current: int, total: int, step_name: str = ""):
    """Simple progress bar for quiet mode"""
    progress = (current / total) * 100
    bar_length = 30
    filled_length = int(bar_length * current // total)
    bar = '█' * filled_length + '░' * (bar_length - filled_length)

    print(f"\r🔄 [{bar}] {progress:5.1f}% {step_name[:20]:<20}", end='', flush=True)

    if current >= total:
        print()  # New line when complete