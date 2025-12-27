# auto_detect.py - FIXED VERSION
"""
Auto Detect Periode Module - CORRECTED LOGIC

PERIODE RULES (BUSINESS LOGIC):
================================

Files with DATE OFFSET (+1 month):
- MC: TGL_CATAT in month N → Periode N+1
  Example: TGL_CATAT 19/06/2025 → Periode 07/2025 (Juli)
  
- MB: TGL_BAYAR in month N → Periode N+1
  Example: TGL_BAYAR 04/06/2025 → Periode 07/2025 (Juli)

Files with NO OFFSET (same month):
- Collection: PAY_DT in month N → Periode N
  Example: PAY_DT 01-07-2025 → Periode 07/2025 (Juli)
  
- Mainbill: FREEZE_DT in month N → Periode N
  Example: FREEZE_DT 12/07/2025 → Periode 07/2025 (Juli)
  
- SBRS: cmr_rd_date in month N → Periode N
  Example: cmr_rd_date 22072025 → Periode 07/2025 (Juli)
  
- Ardebt: PERIODE_BILL column (direct from data)
  Example: PERIODE_BILL "072025" → Periode 07/2025 (Juli)
"""

import pandas as pd
import re
from datetime import datetime

# ==========================================
# BULAN MAPPING
# ==========================================
BULAN_INDONESIA = {
    'januari': 1, 'jan': 1,
    'februari': 2, 'feb': 2,
    'maret': 3, 'mar': 3,
    'april': 4, 'apr': 4,
    'mei': 5, 'may': 5,
    'juni': 6, 'jun': 6,
    'juli': 7, 'jul': 7,
    'agustus': 8, 'ags': 8, 'aug': 8,
    'september': 9, 'sep': 9,
    'oktober': 10, 'okt': 10, 'oct': 10,
    'november': 11, 'nov': 11,
    'desember': 12, 'des': 12, 'dec': 12
}

# Files that need +1 month offset
FILES_WITH_OFFSET = ['mc', 'mb']

# Files with no offset
FILES_NO_OFFSET = ['collection', 'mainbill', 'sbrs', 'ardebt']


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


def apply_periode_offset(bulan, tahun, file_type):
    """
    Apply periode offset based on file type
    
    MC & MB: +1 month
    Others: no offset
    """
    if file_type in FILES_WITH_OFFSET:
        bulan += 1
        if bulan > 12:
            bulan = 1
            tahun += 1
        print(f"📅 Applied +1 month offset for {file_type.upper()}: {bulan}/{tahun}")
    else:
        print(f"📅 No offset for {file_type.upper()}: {bulan}/{tahun}")
    
    return bulan, tahun


def auto_detect_file_type(df, filename=''):
    """
    Auto-detect file type based on columns and filename
    
    Returns: 'mc', 'collection', 'sbrs', 'mb', 'mainbill', 'ardebt', or None
    """
    cols = [c.upper().strip() for c in df.columns]
    filename_upper = filename.upper()
    
    # PRIORITY 1: Check filename with word boundaries
    if re.search(r'\bMB\b|^MB_|_MB_|_MB\.|^MB\.', filename_upper):
        return 'mb'
    
    if re.search(r'\bMC\b|^MC_|_MC_|_MC\.|^MC\.', filename_upper) and 'MB' not in filename_upper:
        return 'mc'
    
    if 'MASTER' in filename_upper:
        return 'mc'
    
    if 'SBRS' in filename_upper or 'SBR' in filename_upper:
        return 'sbrs'
    
    if 'COLLECTION' in filename_upper or 'COLL' in filename_upper:
        return 'collection'
    
    if 'MAINBILL' in filename_upper or re.search(r'\bBILL\b', filename_upper):
        return 'mainbill'
    
    if 'ARDEBT' in filename_upper or 'DEBT' in filename_upper:
        return 'ardebt'
    
    # PRIORITY 2: Check column structure
    if any(x in cols for x in ['ZONA_NOVAK', 'ZONA NOVAK']):
        return 'mc'
    
    if 'AMT_COLLECT' in cols or ('PAY_DT' in cols and 'NOMEN' in cols):
        return 'collection'
    
    if 'CMR_ACCOUNT' in cols or 'SB_STAND' in cols or 'READ_METHOD' in cols:
        return 'sbrs'
    
    if 'TGL_BAYAR' in cols and 'JUMLAH' in cols and 'AMT_COLLECT' not in cols:
        return 'mb'
    
    if 'TOTAL_TAGIHAN' in cols or 'BILL_CYCLE' in cols:
        return 'mainbill'
    
    if any(x in cols for x in ['SUMOFJUMLAH', 'SALDO_TUNGGAKAN', 'SALDO', 'PERIODE_BILL']):
        return 'ardebt'
    
    return None


def detect_periode_from_content(df, file_type):
    """
    Detect periode from file content
    
    Returns: (bulan, tahun, method) atau (None, None, error_method)
    """
    try:
        cols = [c.upper().strip() for c in df.columns]
        
        # ===== MC (Master Customer) =====
        if file_type == 'mc':
            date_col = None
            for col_candidate in ['TGL_CATAT', 'TGL CATAT', 'TANGGAL_CATAT', 'TANGGAL']:
                if col_candidate in cols:
                    date_col = df.columns[cols.index(col_candidate)]
                    break
            
            if date_col and len(df) > 0:
                first_date = df[date_col].iloc[0]
                parsed = parse_date(str(first_date))
                if parsed:
                    bulan, tahun = parsed
                    # OFFSET: +1 bulan untuk MC
                    bulan, tahun = apply_periode_offset(bulan, tahun, file_type)
                    return (bulan, tahun, 'from_content_mc')
        
        # ===== MB (Manual Bayar) =====
        elif file_type == 'mb':
            date_col = None
            for col_candidate in ['TGL_BAYAR', 'TGL BAYAR', 'TANGGAL_BAYAR', 'TANGGAL']:
                if col_candidate in cols:
                    date_col = df.columns[cols.index(col_candidate)]
                    break
            
            if date_col and len(df) > 0:
                first_date = df[date_col].iloc[0]
                parsed = parse_date(str(first_date))
                if parsed:
                    bulan, tahun = parsed
                    # OFFSET: +1 bulan untuk MB
                    bulan, tahun = apply_periode_offset(bulan, tahun, file_type)
                    return (bulan, tahun, 'from_content_mb')
        
        # ===== COLLECTION =====
        elif file_type == 'collection':
            date_col = None
            for col_candidate in ['PAY_DT', 'TGL_BAYAR', 'TGL BAYAR']:
                if col_candidate in cols:
                    date_col = df.columns[cols.index(col_candidate)]
                    break
            
            if date_col and len(df) > 0:
                first_date = df[date_col].iloc[0]
                parsed = parse_date(str(first_date))
                if parsed:
                    # NO OFFSET
                    return (parsed[0], parsed[1], 'from_content_collection')
        
        # ===== MAINBILL =====
        elif file_type == 'mainbill':
            date_col = None
            for col_candidate in ['FREEZE_DT', 'TGL_FREEZE', 'BILL_PERIOD', 'PERIODE']:
                if col_candidate in cols:
                    date_col = df.columns[cols.index(col_candidate)]
                    break
            
            if date_col and len(df) > 0:
                first_value = str(df[date_col].iloc[0]).strip()
                
                # Try parse as date
                parsed = parse_date(first_value)
                if parsed:
                    # NO OFFSET
                    return (parsed[0], parsed[1], 'from_content_mainbill')
                
                # Fallback: Format MMM/YYYY
                if '/' in first_value:
                    parts = first_value.split('/')
                    if len(parts) == 2:
                        month_str = parts[0].lower().strip()
                        year_str = parts[1].strip()
                        
                        bulan = BULAN_INDONESIA.get(month_str)
                        tahun = validate_tahun(year_str)
                        if bulan and tahun:
                            return (bulan, tahun, 'from_content_mainbill_text')
        
        # ===== SBRS =====
        elif file_type == 'sbrs':
            # Priority 1: cmr_rd_date (format: DDMMYYYY)
            if 'CMR_RD_DATE' in cols:
                date_col = df.columns[cols.index('CMR_RD_DATE')]
                first_date = str(df[date_col].iloc[0]).strip()
                
                # Format: DDMMYYYY (22072025)
                if len(first_date) == 8 and first_date.isdigit():
                    day = int(first_date[0:2])
                    month = int(first_date[2:4])
                    year = int(first_date[4:8])
                    
                    month = validate_bulan(month)
                    year = validate_tahun(year)
                    
                    if month and year:
                        # NO OFFSET
                        return (month, year, 'from_content_sbrs_cmr_rd_date')
            
            # Priority 2: READ_DATE atau TGL_BACA
            for col_name in ['READ_DATE', 'TGL_BACA', 'CMR_RD_DATE']:
                if col_name in cols:
                    date_col = df.columns[cols.index(col_name)]
                    first_date = df[date_col].iloc[0]
                    parsed = parse_date(str(first_date))
                    if parsed:
                        # NO OFFSET
                        return (parsed[0], parsed[1], 'from_content_sbrs_date')
            
            # Priority 3: BILL_PERIOD (format: YYYYMM)
            if 'BILL_PERIOD' in cols:
                bill_col = df.columns[cols.index('BILL_PERIOD')]
                first_period = str(df[bill_col].iloc[0]).strip()
                
                # Format: YYYYMM (202507)
                if len(first_period) == 6 and first_period.isdigit():
                    tahun = validate_tahun(first_period[:4])
                    bulan = validate_bulan(first_period[4:6])
                    if tahun and bulan:
                        # NO OFFSET
                        return (bulan, tahun, 'from_content_sbrs_bill_period')
        
        # ===== ARDEBT =====
        elif file_type == 'ardebt':
            # Priority: PERIODE_BILL column
            periode_col = None
            for col_candidate in ['PERIODE_BILL', 'PERIODE BILL', 'PERIODE', 'BILL_PERIOD']:
                if col_candidate in cols:
                    periode_col = df.columns[cols.index(col_candidate)]
                    break
            
            if periode_col and len(df) > 0:
                # Get first non-null value
                for idx, value in df[periode_col].items():
                    if pd.notna(value) and str(value).strip():
                        periode_val = str(value).strip()
                        
                        # Try parse: 072025 (MMYYYY)
                        match = re.search(r'^(\d{2})(\d{4})$', periode_val)
                        if match:
                            bulan = validate_bulan(match.group(1))
                            tahun = validate_tahun(match.group(2))
                            if bulan and tahun:
                                # NO OFFSET
                                return (bulan, tahun, 'from_content_ardebt_mmyyyy')
                        
                        # Try parse: 07/2025 (MM/YYYY)
                        match = re.search(r'^(\d{1,2})/(\d{4})$', periode_val)
                        if match:
                            bulan = validate_bulan(match.group(1))
                            tahun = validate_tahun(match.group(2))
                            if bulan and tahun:
                                # NO OFFSET
                                return (bulan, tahun, 'from_content_ardebt_slash')
                        
                        # Try parse: 202507 (YYYYMM)
                        match = re.search(r'^(\d{4})(\d{2})$', periode_val)
                        if match:
                            tahun = validate_tahun(match.group(1))
                            bulan = validate_bulan(match.group(2))
                            if bulan and tahun:
                                # NO OFFSET
                                return (bulan, tahun, 'from_content_ardebt_yyyymm')
                        
                        # Try parse: JUL2025, JULI2025 (Month name + Year)
                        periode_val_upper = periode_val.upper()
                        for month_name, month_num in BULAN_INDONESIA.items():
                            if periode_val_upper.startswith(month_name.upper()):
                                year_str = periode_val_upper[len(month_name):].strip()
                                if year_str.isdigit():
                                    tahun = validate_tahun(year_str)
                                    if tahun:
                                        # NO OFFSET
                                        return (month_num, tahun, 'from_content_ardebt_monthname')
                        
                        # If we found a value but couldn't parse it, try next row
                        continue
    
    except Exception as e:
        print(f"Error detect periode from content: {e}")
        import traceback
        traceback.print_exc()
    
    return (None, None, 'detection_failed')


def detect_periode_from_filename(filename, file_type=None):
    """
    Detect periode from filename
    
    NOTE: Will apply offset if file_type is provided
    """
    filename_upper = filename.upper()
    
    # Pattern 1: YYYYMM (202507)
    match = re.search(r'(\d{4})(\d{2})', filename)
    if match:
        tahun = validate_tahun(match.group(1))
        bulan = validate_bulan(match.group(2))
        if tahun and bulan:
            # Apply offset if file_type provided
            if file_type:
                bulan, tahun = apply_periode_offset(bulan, tahun, file_type)
            return (bulan, tahun, 'filename_yyyymm')
    
    # Pattern 2: MMYY (0625 = Juni 2025)
    match = re.search(r'[_\-](\d{2})(\d{2})[_\.\-]', filename)
    if match:
        bulan_str = match.group(1)
        tahun_str = match.group(2)
        
        bulan = validate_bulan(bulan_str)
        if bulan:
            yy = int(tahun_str)
            if yy >= 20 and yy <= 30:
                tahun = 2000 + yy
            else:
                tahun = None
            
            if tahun and validate_tahun(tahun):
                # Apply offset if file_type provided
                if file_type:
                    bulan, tahun = apply_periode_offset(bulan, tahun, file_type)
                return (bulan, tahun, 'filename_mmyy')
    
    # Pattern 3: Nama bulan (JUN_2025, JUNI_2025)
    for month_name, month_num in BULAN_INDONESIA.items():
        pattern = rf'{month_name.upper()}[_\s-]*(\d{{4}})'
        match = re.search(pattern, filename_upper)
        if match:
            tahun = validate_tahun(match.group(1))
            if tahun:
                bulan = month_num
                # Apply offset if file_type provided
                if file_type:
                    bulan, tahun = apply_periode_offset(bulan, tahun, file_type)
                return (bulan, tahun, 'filename_monthname')
    
    return (None, None, 'filename_detection_failed')


def parse_date(date_str):
    """
    Parse various date formats
    
    Returns: (bulan, tahun) atau None
    """
    if not date_str or date_str == 'nan' or date_str == 'None':
        return None
    
    date_str = str(date_str).strip()
    
    # Format: DDMMYYYY (22072025)
    if len(date_str) == 8 and date_str.isdigit():
        try:
            day = int(date_str[0:2])
            month = int(date_str[2:4])
            year = int(date_str[4:8])
            
            month = validate_bulan(month)
            year = validate_tahun(year)
            
            if month and year and 1 <= day <= 31:
                return (month, year)
        except:
            pass
    
    # Format: DD-MM-YYYY or DD/MM/YYYY or DD.MM.YYYY
    for sep in ['-', '/', '.']:
        try:
            if sep in date_str:
                parts = date_str.split(sep)
                if len(parts) == 3:
                    day, month, year = parts
                    bulan = validate_bulan(month)
                    tahun = validate_tahun(year)
                    
                    # Fix year if 2 digit
                    if tahun is None and len(year) == 2:
                        tahun = validate_tahun(2000 + int(year))
                    
                    if bulan and tahun:
                        return (bulan, tahun)
        except:
            continue
    
    # Format: YYYY-MM-DD (ISO)
    try:
        if '-' in date_str and len(date_str) >= 10:
            parts = date_str.split('-')
            if len(parts) >= 3 and len(parts[0]) == 4:
                year = validate_tahun(parts[0])
                month = validate_bulan(parts[1])
                if year and month:
                    return (month, year)
    except:
        pass
    
    # Fallback: pandas parser
    try:
        dt = pd.to_datetime(date_str, errors='coerce')
        if pd.notna(dt):
            bulan = validate_bulan(dt.month)
            tahun = validate_tahun(dt.year)
            if bulan and tahun:
                return (bulan, tahun)
    except:
        pass
    
    return None


def auto_detect_periode(filepath, filename='', file_type=None):
    """
    AUTO-DETECT PERIODE - MAIN FUNCTION
    
    Detection Priority:
    1. From file content (PRIMARY)
    2. From filename (FALLBACK)
    3. From current date (LAST RESORT)
    
    Args:
        filepath: Path to file
        filename: Filename (optional)
        file_type: File type (optional, will auto-detect if None)
    
    Returns:
        dict: {
            'file_type': str,
            'periode_bulan': int,
            'periode_tahun': int,
            'periode_label': str,
            'method': str
        }
    """
    if not filename:
        filename = filepath.split('/')[-1]
    
    print(f"\n{'='*70}")
    print(f"AUTO-DETECT PERIODE")
    print(f"{'='*70}")
    print(f"File: {filename}")
    
    # Read file
    try:
        if filepath.endswith('.csv'):
            df = pd.read_csv(filepath, nrows=10)
        elif filepath.endswith(('.xls', '.xlsx')):
            df = pd.read_excel(filepath, nrows=10)
        elif filepath.endswith('.txt'):
            df = pd.read_csv(filepath, sep='|', nrows=10)
        else:
            print(f"⚠️ Unsupported file format")
            return None
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return None
    
    # Auto-detect file type if not provided
    if not file_type:
        file_type = auto_detect_file_type(df, filename)
    
    if not file_type:
        print(f"⚠️ Cannot detect file type")
        return None
    
    print(f"📂 File Type: {file_type.upper()}")
    
    # PRIORITY 1: Detect from content
    bulan, tahun, method = detect_periode_from_content(df, file_type)
    
    if bulan and tahun:
        print(f"✅ Detected from content: {bulan:02d}/{tahun} (method: {method})")
    else:
        # PRIORITY 2: Detect from filename
        print("⚠️ Cannot detect from content, trying filename...")
        bulan, tahun, method = detect_periode_from_filename(filename, file_type)
        
        if bulan and tahun:
            print(f"✅ Detected from filename: {bulan:02d}/{tahun} (method: {method})")
        else:
            # PRIORITY 3: Use current date
            now = datetime.now()
            bulan = now.month
            tahun = now.year
            method = 'fallback_current'
            print(f"⚠️ Using current date: {bulan:02d}/{tahun}")
    
    # FINAL VALIDATION
    bulan = validate_bulan(bulan)
    tahun = validate_tahun(tahun)
    
    if not bulan or not tahun:
        print(f"❌ Invalid bulan/tahun")
        now = datetime.now()
        bulan = now.month
        tahun = now.year
        method = 'fallback'
    
    # Create label
    bulan_names = ['', 'Januari', 'Februari', 'Maret', 'April', 'Mei', 'Juni',
                   'Juli', 'Agustus', 'September', 'Oktober', 'November', 'Desember']
    
    if 1 <= bulan <= 12:
        periode_label = f"{bulan_names[bulan]} {tahun}"
    else:
        periode_label = f"{bulan}/{tahun}"
    
    print(f"🎯 FINAL PERIODE: {periode_label}")
    print(f"   Method: {method}")
    print(f"   File Type: {file_type.upper()}")
    if file_type in FILES_WITH_OFFSET:
        print(f"   ⚠️ Note: {file_type.upper()} uses +1 month offset")
    print(f"{'='*70}\n")
    
    return {
        'file_type': file_type,
        'periode_bulan': bulan,
        'periode_tahun': tahun,
        'periode_label': periode_label,
        'method': method
    }


# Export
__all__ = ['auto_detect_periode', 'auto_detect_file_type', 'apply_periode_offset']
