#!/bin/bash

echo "=============================================="
echo "  QUICK FIX: Install xlrd untuk Excel .xls"
echo "=============================================="
echo ""

# Install xlrd
echo "📦 Installing xlrd..."
pip install --break-system-packages xlrd==2.0.1

echo ""
echo "📦 Installing dbfread..."
pip install --break-system-packages dbfread==2.0.7

echo ""
echo "📦 Installing chardet..."
pip install --break-system-packages chardet==5.2.0

echo ""
echo "=============================================="
echo "  ✅ INSTALLATION COMPLETE!"
echo "=============================================="
echo ""

# Test
echo "🔍 Testing libraries..."
python3 << 'EOF'
try:
    import xlrd
    print("  ✅ xlrd version:", xlrd.__version__)
except:
    print("  ❌ xlrd FAILED")

try:
    import dbfread
    print("  ✅ dbfread OK")
except:
    print("  ❌ dbfread FAILED")

try:
    import chardet
    print("  ✅ chardet OK")
except:
    print("  ❌ chardet FAILED")
EOF

echo ""
echo "=============================================="
echo "  ⚠️  RESTART APLIKASI:"
echo "=============================================="
echo ""
echo "  1. Tekan CTRL+C untuk stop aplikasi"
echo "  2. Jalankan lagi: python app.py"
echo "  3. Upload file MC.xls"
echo "  4. Data akan muncul!"
echo ""
echo "=============================================="
