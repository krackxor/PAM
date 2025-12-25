"""
Helper Functions Module
Common utilities used across the application
"""

import pandas as pd
from datetime import datetime

# ==========================================
# DATA CLEANING
# ==========================================

def clean_nomen(val):
    """Clean nomen format (remove .0 from Excel)"""
    try:
        if pd.isna(val):
            return None
        return str(int(float(str(val))))
    except:
        return str(val).strip()

def clean_date(val, fmt_in='%d-%m-%Y', fmt_out='%Y-%m-%d'):
    """Standardize date to YYYY-MM-DD"""
    try:
        return datetime.strptime(str(val).strip(), fmt_in).strftime(fmt_out)
    except:
        try:
            return datetime.strptime(str(val).strip(), '%d/%m/%Y').strftime(fmt_out)
        except:
            return str(val)

def parse_zona_novak(zona):
    """
    Parse ZONA_NOVAK: 350960217 -> rayon:35, pc:096, ez:02, block:17
    """
    zona_str = str(zona).strip()
    if len(zona_str) < 9:
        zona_str = zona_str.zfill(9)
    
    return {
        'rayon': zona_str[:2],
        'pc': zona_str[2:5],
        'ez': zona_str[5:7],
        'block': zona_str[7:9],
        'pcez': f"{zona_str[2:5]}/{zona_str[5:7]}"
    }

# ==========================================
# FORMATTING
# ==========================================

def format_rupiah(amount):
    """Format number as Rupiah"""
    try:
        return f"Rp {int(amount):,}".replace(',', '.')
    except:
        return "Rp 0"

def format_number(num):
    """Format number with thousand separator"""
    try:
        return f"{int(num):,}".replace(',', '.')
    except:
        return "0"

def get_periode_label(bulan, tahun):
    """Convert bulan/tahun to readable label"""
    bulan_names = ['', 'Januari', 'Februari', 'Maret', 'April', 'Mei', 'Juni',
                   'Juli', 'Agustus', 'September', 'Oktober', 'November', 'Desember']
    if 1 <= bulan <= 12:
        return f"{bulan_names[bulan]} {tahun}"
    return f"{bulan}/{tahun}"

# ==========================================
# VALIDATION
# ==========================================

def validate_bulan(bulan):
    """Validate bulan is between 1-12"""
    try:
        bulan = int(bulan)
        if 1 <= bulan <= 12:
            return bulan
    except:
        pass
    return None

def validate_tahun(tahun):
    """Validate tahun is reasonable (2020-2030)"""
    try:
        tahun = int(tahun)
        if 2020 <= tahun <= 2030:
            return tahun
    except:
        pass
    return None

# ==========================================
# FLASK TEMPLATE HELPERS
# ==========================================

def register_helpers(app):
    """Register helper functions for Flask templates"""
    
    @app.template_filter('rupiah')
    def rupiah_filter(amount):
        return format_rupiah(amount)
    
    @app.template_filter('number')
    def number_filter(num):
        return format_number(num)
    
    @app.template_filter('periode')
    def periode_filter(bulan, tahun):
        return get_periode_label(bulan, tahun)
    
    print("✅ Template helpers registered")
