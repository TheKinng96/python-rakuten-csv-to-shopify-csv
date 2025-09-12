#!/usr/bin/env python3
"""
Test script for CSS scoping functionality
"""
import sys
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent))

# Import the CSS scoper using importlib due to numeric filename
import importlib.util
spec = importlib.util.spec_from_file_location("scope_css", str(Path(__file__).parent / "11_scope_css_styles.py"))
scope_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(scope_module)
CSSStyleScoper = scope_module.CSSStyleScoper

# Sample HTML from the user
sample_html = '''<div class="tab__container --A is-active">
<style>
/* Desktop fixes */
hr {
  width: 100% !important;
  max-width: 100% !important;
  margin: 7rem 0; /* Desktop margin */
}

/* Mobile Responsiveness */
@media (max-width: 768px) {
  /* Force responsive images */
  .mobile-responsive-img, img {
    max-width: 100% !important;
    width: auto !important;
    height: auto !important;
    display: block !important;
    margin: 0 auto !important;
  }
  
  /* Force text wrapping */
  .mobile-responsive-text,
  [style*="white-space:nowrap"],
  [style*="white-space: nowrap"] {
    white-space: normal !important;
    font-size: 16px !important;
    word-wrap: break-word !important;
    overflow-wrap: break-word !important;
  }
  
  /* HR responsive styling - Requirement #3 */
  .mobile-responsive-hr, hr {
    width: 100% !important;
    max-width: 100% !important;
    box-sizing: border-box !important;
    margin: 2rem 0 !important; /* Reduced from 7rem to 2rem on mobile */
  }
  
  /* Global viewport control */
  html, body {
    overflow-x: hidden !important;
  }
  
  /* ===== MAIN TABLE STYLING - Requirement #1 ===== */
  .mobile-responsive-table {
    display: block !important;
    width: 100% !important;
    max-width: 100% !important;
    border-collapse: separate !important;
    border-spacing: 0 !important;
    overflow: visible !important;
    margin: 16px 0 !important;
  }
  
  .mobile-responsive-table tbody,
  .mobile-responsive-table thead,
  .mobile-responsive-table tfoot {
    display: block !important;
    width: 100% !important;
  }
  
  /* Table rows - NO MARGINS between them */
  .mobile-responsive-table tr {
    display: block !important;
    width: 100% !important;
    margin: 0 !important; /* No margin between rows */
    padding: 0 !important;
    border-left: 1px solid #e0e0e0 !important;
    border-right: 1px solid #e0e0e0 !important;
    border-top: none !important;
    border-bottom: 1px solid #e0e0e0 !important;
    background: #fff !important;
    border-radius: 0 !important; 
    margin-bottom: 0 !important; 
  }
  
  /* Override problematic inline styles */
  *[width] {
    max-width: 100% !important;
    width: auto !important;
  }
}
</style>
<div style="font-family:-apple-system, BlinkMacSystemFont, 'Helvetica Neue', YuGothic, 'ヒラギノ角ゴ ProN W3', Hiragino Kaku Gothic ProN, Arial, 'メイリオ', Meiryo, sans-serif;  font-size: 16px; line-height:160%;width: 100%;color: #222222;">
<h4 style="font-weight: 900;color:#FF9933; text-align:center;margin: .8em 0;line-height: 160%;font-size:1.6em">国産有機 白玉（しろたま）梅干</h4>
<h5 style="font-weight: 900;margin: .8em 0; color:#FFF;padding: .4em;  background-color:#FF9933; text-align:center;font-size:1.2em">有機JAS認証</h5>
<div class="mobile-responsive-flex" style=" display:flex;flex-wrap:wrap;margin:0 auto;width:auto;"><div style="flex:1 1 400px">
<p style="margin:auto;">
        奈良吉野や紀州の有機梅を伝統海塩「海の精」だけで熟成させました。<br>
        丁寧に天日干しした白梅干です。<br></p>
<p style="margin:.6em 0;">
        食塩は伝統海塩「海の精」を使用しています。</p>
</div></div>
<br>
<table class="mobile-responsive-table">
<tr><td>名称</td><td>有機梅干</td></tr>
<tr><td>原材料名</td><td>有機梅（国産）、漬け原材料（食塩）</td></tr>
</table>
</div>
</div>'''


def test_css_scoping():
    """Test the CSS scoping functionality"""
    print("=" * 70)
    print("🧪 TESTING CSS SCOPING")
    print("=" * 70)
    
    scoper = CSSStyleScoper()
    
    print("\n📝 Original HTML length:", len(sample_html))
    print("\n🔍 Processing HTML with CSS scoper...")
    
    result = scoper.process_html(sample_html, "test-product-handle")
    
    if result.get('processed'):
        print("\n✅ Processing successful!")
        print(f"   - Changes made: {len(result['changes_made'])}")
        print(f"   - Bytes changed: {result['bytes_changed']}")
        print(f"   - Style tags processed: {result.get('style_tags_processed', 0)}")
        print(f"   - Has wrapper: {result.get('has_wrapper', False)}")
        
        print("\n📋 Changes made:")
        for i, change in enumerate(result['changes_made'], 1):
            if change['type'] == 'selector_scoped':
                print(f"   {i}. Scoped: '{change['original']}' → '{change['scoped']}'")
            elif change['type'] == 'media_query_selector_scoped':
                print(f"   {i}. Media query scoped: '{change['original']}' → '{change['scoped']}'")
            else:
                print(f"   {i}. {change['type']}: {change.get('description', '')}")
        
        # Save output for inspection
        output_file = Path(__file__).parent.parent / "test_output" / "scoped_html_sample.html"
        output_file.parent.mkdir(exist_ok=True)
        
        # Create full HTML document for testing
        full_html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>CSS Scoping Test</title>
    <style>
        /* Simulate theme styles that should NOT be affected */
        body {{ font-family: Arial; margin: 20px; }}
        img {{ border: 5px solid red; /* Should NOT affect product images */ }}
        hr {{ border: 5px solid blue; margin: 50px 0; /* Should NOT affect product HRs */ }}
        table {{ background: yellow; /* Should NOT affect product tables */ }}
    </style>
</head>
<body>
    <h1>Theme Header (should not be affected)</h1>
    <img src="https://via.placeholder.com/150" alt="Theme image">
    <hr>
    
    <h2>Product Description Area:</h2>
    {result['modified_html']}
    
    <hr>
    <h2>Theme Footer (should not be affected)</h2>
    <table><tr><td>Theme table</td></tr></table>
</body>
</html>"""
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(full_html)
        
        print(f"\n📄 Test output saved to: {output_file}")
        print("   Open this file in a browser to verify CSS scoping works correctly")
        
        # Show a snippet of the scoped CSS
        print("\n🔍 Sample of scoped CSS:")
        modified_html = result['modified_html']
        
        # Extract the first few scoped rules
        import re
        style_match = re.search(r'<style>(.*?)</style>', modified_html, re.DOTALL)
        if style_match:
            css_content = style_match.group(1).strip()
            lines = css_content.split('\n')[:20]  # Show first 20 lines
            for line in lines:
                print(f"   {line}")
            if len(css_content.split('\n')) > 20:
                print("   ...")
        
    else:
        print(f"\n❌ Processing failed: {result.get('reason', 'Unknown error')}")
        if result.get('error'):
            print(f"   Error: {result['error']}")
    
    print("\n" + "=" * 70)


if __name__ == "__main__":
    test_css_scoping()