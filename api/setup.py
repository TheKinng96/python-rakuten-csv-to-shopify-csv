#!/usr/bin/env python3
"""
Setup script for Shopify Product Management System

This script helps initialize the environment and verify everything is working correctly.
"""
import sys
from pathlib import Path

def check_uv_installation():
    """Check if UV is installed"""
    try:
        import subprocess
        result = subprocess.run(['uv', '--version'], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ UV is installed: {result.stdout.strip()}")
            return True
        else:
            print("❌ UV is not working properly")
            return False
    except FileNotFoundError:
        print("❌ UV is not installed")
        print("Install UV with: curl -LsSf https://astral.sh/uv/install.sh | sh")
        return False

def check_env_file():
    """Check if .env file exists and has required variables"""
    env_path = Path(".env")
    env_example_path = Path(".env.example")
    
    if not env_path.exists():
        if env_example_path.exists():
            print("⚠️  .env file not found")
            print("Please copy .env.example to .env and configure your credentials:")
            print("cp .env.example .env")
            return False
        else:
            print("❌ Neither .env nor .env.example found")
            return False
    
    # Check for required variables
    required_vars = [
        'SHOPIFY_TEST_STORE_URL',
        'SHOPIFY_TEST_ACCESS_TOKEN'
    ]
    
    with open(env_path, 'r') as f:
        env_content = f.read()
    
    missing_vars = []
    for var in required_vars:
        if f"{var}=" not in env_content or f"{var}=your-" in env_content:
            missing_vars.append(var)
    
    if missing_vars:
        print("⚠️  .env file exists but missing configuration:")
        for var in missing_vars:
            print(f"   - {var}")
        print("Please update your .env file with proper credentials")
        return False
    
    print("✅ .env file configured")
    return True

def check_data_files():
    """Check if CSV data files exist"""
    data_dir = Path("data")
    
    if not data_dir.exists():
        print("❌ data/ directory not found")
        return False
    
    csv_files = list(data_dir.glob("products_export_*.csv"))
    
    if not csv_files:
        print("❌ No products_export_*.csv files found in data/ directory")
        print("Please ensure your CSV files are in the data/ folder")
        return False
    
    print(f"✅ Found {len(csv_files)} CSV data files")
    for csv_file in csv_files:
        file_size = csv_file.stat().st_size / (1024 * 1024)  # MB
        print(f"   - {csv_file.name} ({file_size:.1f} MB)")
    
    return True

def sync_dependencies():
    """Sync dependencies with UV"""
    try:
        import subprocess
        print("🔄 Syncing dependencies with UV...")
        result = subprocess.run(['uv', 'sync'], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Dependencies synced successfully")
            return True
        else:
            print(f"❌ Failed to sync dependencies: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ Error syncing dependencies: {e}")
        return False

def test_imports():
    """Test if all required modules can be imported"""
    try:
        sys.path.append(str(Path("src")))
        
        from shopify_manager.config import shopify_config, path_config
        
        print("✅ All core modules import successfully")
        
        # Test configuration
        print(f"   - Test store URL: {shopify_config.test_store_url[:20]}...")
        print(f"   - Dry run mode: {shopify_config.dry_run}")
        print(f"   - Data directory: {path_config.data_dir}")
        print(f"   - Reports directory: {path_config.reports_dir}")
        
        return True
    except Exception as e:
        print(f"❌ Import error: {e}")
        return False

def create_directories():
    """Create necessary directories"""
    directories = [
        Path("reports"),
        Path("data"),
    ]
    
    for directory in directories:
        directory.mkdir(exist_ok=True)
        print(f"✅ Directory created/verified: {directory}")
    
    return True

def main():
    """Main setup function"""
    print("🚀 Shopify Product Management System Setup")
    print("=" * 50)
    
    checks = [
        ("UV Installation", check_uv_installation),
        ("Environment Configuration", check_env_file),
        ("Data Files", check_data_files),
        ("Directory Structure", create_directories),
        ("Dependencies", sync_dependencies),
        ("Module Imports", test_imports),
    ]
    
    all_passed = True
    
    for check_name, check_func in checks:
        print(f"\n🔍 Checking {check_name}...")
        if not check_func():
            all_passed = False
    
    print("\n" + "=" * 50)
    
    if all_passed:
        print("🎉 Setup completed successfully!")
        print("\nNext steps:")
        print("1. Import data to test store: uv run scripts/00_import_to_test.py")
        print("2. Run image analysis: uv run scripts/01_remove_ss_images.py")
        print("3. Check generated reports in reports/ folder")
        print("4. Set DRY_RUN=false in .env when ready for actual processing")
    else:
        print("❌ Setup incomplete. Please fix the issues above and run setup.py again.")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())