# auto_detect_periode.py
"""
AUTO-DETECT PERIODE FROM FILE - FIXED VERSION
Deteksi periode dari isi file atau nama file dengan proper error handling
"""

import re
import pandas as pd
from datetime import datetime

# ==========================================
# MAPPING BULAN
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

BULAN_ENGLISH = {
    'jan': 1, 'january': 1,
    'feb': 2, 'february': 2,
    'mar': 3, 'march': 3,
    'apr': 4, 'april': 4,
    'may': 5,
    'jun': 6, 'june': 6,
    'jul': 7, 'july': 7,
    'aug': 8, 'august': 8,
    'sep': 9, 'september': 9,
    'oct': 10, 'october': 10,
    'nov': 11, 'november': 11,
    'dec': 12, 'december': 12
}


# ==========================================
# HELPER: VALIDATE BULAN
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
# AUTO-DETECT TIPE FILE
# ==========================================
def auto_detect_file_type(df, filename=''):
    """
    Auto-detect tipe file berdasarkan struktur kolom
    
    Returns: 'MC', 'COLLECTION', 'SBRS', 'MB', 'MAINBILL', 'ARDEBT', atau None
    """
    cols = [c.upper().strip() for c in df.columns]
    filename_upper = filename.upper()
    
    # MC: Ada kolom ZONA_NOVAK, NOMEN, NAMA_PEL
    if any(x in cols for x in ['ZONA_NOVAK', 'ZONA NOVAK']):
        return 'MC'
    
    # Collection: Ada AMT_COLLECT, PAY_DT
    if 'AMT_COLLECT' in cols or ('PAY_DT' in cols and 'NOMEN' in cols):
        return 'COLLECTION'
    
    # SBRS: Ada CMR_ACCOUNT, SB_STAND, READ_METHOD
    if 'CMR_ACCOUNT' in cols or 'SB_STAND' in cols or 'READ_METHOD' in cols:
        return 'SBRS'
    
    # MB: Ada TGL_BAYAR dan struktur mirip collection tapi simple
    if 'TGL_BAYAR' in cols and 'JUMLAH' in cols and 'AMT_COLLECT' not in cols:
        return 'MB'
    
    # MainBill: Ada TOTAL_TAGIHAN, BILL_CYCLE
    if 'TOTAL_TAGIHAN' in cols or 'BILL_CYCLE' in cols:
        return 'MAINBILL'
    
    # Ardebt: Ada SUMOF atau SALDO atau struktur tunggakan
    if any(x in cols for x in ['SUMOFJUMLAH', 'SALDO_TUNGGAKAN', 'SALDO']):
        return 'ARDEBT'
    
    # Fallback: dari nama file
    if 'COLLECTION' in filename_upper or 'COLL' in filename_upper:
        return 'COLLECTION'
    if 'SBRS' in filename_upper:
        return 'SBRS'
    if 'MC' in filename_upper or 'MASTER' in filename_upper:
        return 'MC'
    if 'MB' in filename_upper:
        return 'MB'
    if 'MAINBILL' in filename_upper or 'BILL' in filename_upper:
        return 'MAINBILL'
    if 'ARDEBT' in filename_upper or 'DEBT' in filename_upper:
        return 'ARDEBT'
    
    return None


# ==========================================
# AUTO-DETECT PERIODE FROM CONTENT
# ==========================================
def detect_periode_from_content(df, file_type):
    """
    Deteksi periode dari isi file dengan LOGIKA BISNIS PDAM yang benar
    
    CRITICAL RULES:
    - MC, MB, ARDEBT: Data bulan ini untuk PERIODE BULAN DEPAN
      (MC November → Periode Desember)
    - COLLECTION, MAINBILL, SBRS: Data bulan ini untuk PERIODE BULAN INI
      (Collection Desember → Periode Desember)
    
    Returns: (bulan, tahun) atau (None, None)
    """
    try:
        cols = [c.upper().strip() for c in df.columns]
        
        # ===== MC (Master Customer) =====
        # MC November → Periode Desember
        if file_type == 'MC':
            # Cari kolom TGL_CATAT
            date_col = None
            for col_candidate in ['TGL_CATAT', 'TGL CATAT', 'TANGGAL_CATAT', 'TANGGAL']:
                if col_candidate in cols:
                    date_col = df.columns[cols.index(col_candidate)]
                    break
            
            if date_col and len(df) > 0:
                # Ambil tanggal dari baris pertama
                first_date = df[date_col].iloc[0]
                parsed = parse_date(str(first_date))
                if parsed:
                    bulan, tahun = parsed
                    # OFFSET: +1 bulan (MC Nov → Periode Des)
                    bulan += 1
                    if bulan > 12:
                        bulan = 1
                        tahun += 1
                    return (bulan, tahun)
        
        # ===== MB (Manual Bayar) =====
        # MB November → Periode Desember
        elif file_type == 'MB':
            # Cari kolom TGL_BAYAR
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
                    # OFFSET: +1 bulan (MB Nov → Periode Des)
                    bulan += 1
                    if bulan > 12:
                        bulan = 1
                        tahun += 1
                    return (bulan, tahun)
        
        # ===== ARDEBT (AR Debt) =====
        # ARDEBT November → Periode Desember
        elif file_type == 'ARDEBT':
            # Cari kolom TGL_CATAT atau tanggal lain
            date_col = None
            for col_candidate in ['TGL_CATAT', 'TGL CATAT', 'TANGGAL_CATAT', 'TANGGAL', 'TGL_LAPORAN']:
                if col_candidate in cols:
                    date_col = df.columns[cols.index(col_candidate)]
                    break
            
            if date_col and len(df) > 0:
                first_date = df[date_col].iloc[0]
                parsed = parse_date(str(first_date))
                if parsed:
                    bulan, tahun = parsed
                    # OFFSET: +1 bulan (ARDEBT Nov → Periode Des)
                    bulan += 1
                    if bulan > 12:
                        bulan = 1
                        tahun += 1
                    return (bulan, tahun)
        
        # ===== COLLECTION =====
        # Collection Desember → Periode Desember (NO OFFSET)
        elif file_type == 'COLLECTION':
            # Cari kolom PAY_DT atau TGL_BAYAR
            date_col = None
            for col_candidate in ['PAY_DT', 'TGL_BAYAR', 'TGL BAYAR']:
                if col_candidate in cols:
                    date_col = df.columns[cols.index(col_candidate)]
                    break
            
            if date_col and len(df) > 0:
                first_date = df[date_col].iloc[0]
                parsed = parse_date(str(first_date))
                if parsed:
                    return parsed  # NO OFFSET
        
        # ===== MAINBILL =====
        # MainBill Desember → Periode Desember (NO OFFSET)
        elif file_type == 'MAINBILL':
            # Cari kolom FREEZE_DT
            date_col = None
            for col_candidate in ['FREEZE_DT', 'TGL_FREEZE', 'BILL_PERIOD', 'PERIODE']:
                if col_candidate in cols:
                    date_col = df.columns[cols.index(col_candidate)]
                    break
            
            if date_col and len(df) > 0:
                first_value = str(df[date_col].iloc[0]).strip()
                
                # Coba parse sebagai tanggal
                parsed = parse_date(first_value)
                if parsed:
                    return parsed  # NO OFFSET
                
                # Fallback: Format MMM/YYYY
                if '/' in first_value:
                    parts = first_value.split('/')
                    if len(parts) == 2:
                        month_str = parts[0].lower().strip()
                        year_str = parts[1].strip()
                        
                        bulan = BULAN_ENGLISH.get(month_str) or BULAN_INDONESIA.get(month_str)
                        tahun = validate_tahun(year_str)
                        if bulan and tahun:
                            return (bulan, tahun)  # NO OFFSET
        
        # ===== SBRS =====
        # SBRS Desember → Periode Desember (NO OFFSET)
        elif file_type == 'SBRS':
            # Priority 1: Cari kolom cmr_rd_date (format: DDMMYYYY)
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
                        return (month, year)  # NO OFFSET
            
            # Priority 2: READ_DATE atau TGL_BACA
            for col_name in ['READ_DATE', 'TGL_BACA', 'CMR_RD_DATE']:
                if col_name in cols:
                    date_col = df.columns[cols.index(col_name)]
                    first_date = df[date_col].iloc[0]
                    parsed = parse_date(str(first_date))
                    if parsed:
                        return parsed  # NO OFFSET
            
            # Priority 3: BILL_PERIOD (format: YYYYMM)
            if 'BILL_PERIOD' in cols:
                bill_col = df.columns[cols.index('BILL_PERIOD')]
                first_period = str(df[bill_col].iloc[0]).strip()
                
                # Format: YYYYMM (202507)
                if len(first_period) == 6 and first_period.isdigit():
                    tahun = validate_tahun(first_period[:4])
                    bulan = validate_bulan(first_period[4:6])
                    if tahun and bulan:
                        return (bulan, tahun)  # NO OFFSET
        
    except Exception as e:
        print(f"Error detect periode from content: {e}")
        import traceback
        traceback.print_exc()
    
    return (None, None)


# ==========================================
# AUTO-DETECT PERIODE FROM FILENAME
# ==========================================
def detect_periode_from_filename(filename):
    """
    Deteksi periode dari nama file
    
    Contoh:
    - MC_202512.xls → (12, 2025)
    - Collection_DES_2025.txt → (12, 2025)
    - SBRS_Desember_2025.xlsx → (12, 2025)
    
    Returns: (bulan, tahun) atau (None, None)
    """
    filename_upper = filename.upper()
    
    # Pattern 1: YYYYMM (202512)
    match = re.search(r'(\d{4})(\d{2})', filename)
    if match:
        tahun = validate_tahun(match.group(1))
        bulan = validate_bulan(match.group(2))
        if tahun and bulan:
            return (bulan, tahun)
    
    # Pattern 2: YYYY-MM atau YYYY_MM
    match = re.search(r'(\d{4})[-_](\d{1,2})', filename)
    if match:
        tahun = validate_tahun(match.group(1))
        bulan = validate_bulan(match.group(2))
        if tahun and bulan:
            return (bulan, tahun)
    
    # Pattern 3: Nama bulan + tahun (DES_2025, DESEMBER_2025, DEC_2025)
    for month_name, month_num in {**BULAN_INDONESIA, **BULAN_ENGLISH}.items():
        pattern = rf'{month_name.upper()}[_\s-]*(\d{{4}})'
        match = re.search(pattern, filename_upper)
        if match:
            tahun = validate_tahun(match.group(1))
            if tahun:
                return (month_num, tahun)
    
    # Pattern 4: Tahun + nama bulan (2025_DES, 2025_DESEMBER)
    for month_name, month_num in {**BULAN_INDONESIA, **BULAN_ENGLISH}.items():
        pattern = rf'(\d{{4}})[_\s-]*{month_name.upper()}'
        match = re.search(pattern, filename_upper)
        if match:
            tahun = validate_tahun(match.group(1))
            if tahun:
                return (month_num, tahun)
    
    return (None, None)


# ==========================================
# HELPER: PARSE DATE STRING
# ==========================================
def parse_date(date_str):
    """
    Parse berbagai format tanggal
    
    Supports:
    - DD-MM-YYYY (01-07-2025)
    - DD/MM/YYYY (19/06/2025)
    - DD.MM.YYYY (19.06.2025)
    - DDMMYYYY (22072025)
    - YYYY-MM-DD (2025-07-22)
    
    Returns: (bulan, tahun) atau None
    """
    if not date_str or date_str == 'nan' or date_str == 'None':
        return None
    
    date_str = str(date_str).strip()
    
    # Format: DDMMYYYY (22072025) - 8 digits
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
    
    # Format: DD-MM-YYYY atau DD/MM/YYYY atau DD.MM.YYYY
    for sep in ['-', '/', '.']:
        try:
            if sep in date_str:
                parts = date_str.split(sep)
                if len(parts) == 3:
                    day, month, year = parts
                    bulan = validate_bulan(month)
                    tahun = validate_tahun(year)
                    
                    # Fix year jika 2 digit
                    if tahun is None and len(year) == 2:
                        tahun = validate_tahun(2000 + int(year))
                    
                    if bulan and tahun:
                        return (bulan, tahun)
        except:
            continue
    
    # Format: YYYY-MM-DD (ISO format)
    try:
        if '-' in date_str and len(date_str) >= 10:
            parts = date_str.split('-')
            if len(parts) >= 3 and len(parts[0]) == 4:  # Year first
                year = validate_tahun(parts[0])
                month = validate_bulan(parts[1])
                if year and month:
                    return (month, year)
    except:
        pass
    
    # Fallback: pandas datetime parser
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


# ==========================================
# MAIN FUNCTION
# ==========================================
def auto_detect_periode(filepath, filename='', file_type=None):
    """
    AUTO-DETECT PERIODE (Main Function) - FIXED VERSION
    
    PERIODE LOGIC (PDAM BUSINESS RULES):
    
    Periode Desember 2025 terdiri dari:
    - MC November 2025 (offset +1)
    - MB November 2025 (offset +1)
    - ARDEBT November 2025 (offset +1)
    - Collection Desember 2025 (no offset)
    - SBRS Desember 2025 (no offset)
    - MainBill Desember 2025 (no offset)
    
    Periode November 2025 terdiri dari:
    - MC Oktober 2025 (offset +1)
    - MB Oktober 2025 (offset +1)
    - ARDEBT Oktober 2025 (offset +1)
    - Collection November 2025 (no offset)
    - SBRS November 2025 (no offset)
    - MainBill November 2025 (no offset)
    
    Args:
        filepath: Path ke file
        filename: Nama file (opsional)
        file_type: Tipe file (opsional, akan auto-detect jika None)
    
    Returns:
        dict: {
            'file_type': str,
            'periode_bulan': int,
            'periode_tahun': int,
            'periode_label': str (contoh: "Desember 2025"),
            'method': str (contoh: "from_content", "from_filename", "from_upload_date")
        }
        atau None jika gagal
    """
    if not filename:
        filename = filepath.split('/')[-1]
    
    # Baca file
    try:
        if filepath.endswith('.csv'):
            df = pd.read_csv(filepath, nrows=10)  # Baca 10 baris pertama saja
        elif filepath.endswith(('.xls', '.xlsx')):
            df = pd.read_excel(filepath, nrows=10)
        elif filepath.endswith('.txt'):
            df = pd.read_csv(filepath, sep='|', nrows=10)
        else:
            print(f"⚠️ Unsupported file format: {filepath}")
            return None
    except Exception as e:
        print(f"❌ Error reading file {filename}: {e}")
        return None
    
    # Auto-detect tipe file jika belum ada
    if not file_type:
        file_type = auto_detect_file_type(df, filename)
    
    if not file_type:
        print(f"⚠️ Cannot detect file type: {filename}")
        return None
    
    # PRIORITAS 1: Detect dari content
    bulan, tahun = detect_periode_from_content(df, file_type)
    method = 'from_content'
    
    # PRIORITAS 2: Detect dari filename
    if not bulan or not tahun:
        bulan, tahun = detect_periode_from_filename(filename)
        method = 'from_filename'
        
        # Apply offset jika dari filename
        if bulan and tahun:
            if file_type in ['MC', 'MB', 'ARDEBT']:
                print(f"📅 Applying +1 month offset for {file_type}: {bulan}/{tahun}", end=' → ')
                bulan += 1
                if bulan > 12:
                    bulan = 1
                    tahun += 1
                print(f"{bulan}/{tahun}")
    
    # PRIORITAS 3: Gunakan tanggal upload
    if not bulan or not tahun:
        now = datetime.now()
        bulan = now.month
        tahun = now.year
        method = 'from_upload_date'
        print(f"⚠️ Using upload date for {filename}: {bulan}/{tahun}")
    
    # FINAL VALIDATION
    bulan = validate_bulan(bulan)
    tahun = validate_tahun(tahun)
    
    if not bulan or not tahun:
        print(f"❌ Invalid bulan/tahun for {filename}: bulan={bulan}, tahun={tahun}")
        # Fallback to current date
        now = datetime.now()
        bulan = now.month
        tahun = now.year
        method = 'fallback'
    
    # Buat label periode (SAFE - index 0 akan always be empty string)
    bulan_names = ['', 'Januari', 'Februari', 'Maret', 'April', 'Mei', 'Juni',
                   'Juli', 'Agustus', 'September', 'Oktober', 'November', 'Desember']
    
    # SAFE ACCESS - make sure bulan is valid
    if 1 <= bulan <= 12:
        periode_label = f"{bulan_names[bulan]} {tahun}"
    else:
        periode_label = f"{bulan}/{tahun}"
    
    return {
        'file_type': file_type,
        'periode_bulan': bulan,
        'periode_tahun': tahun,
        'periode_label': periode_label,
        'method': method
    }


# ==========================================
# TEST FUNCTION
# ==========================================
if __name__ == '__main__':
    # Test dengan nama file
    test_files = [
        'MC_202512.xls',
        'Collection_DES_2025.txt',
        'SBRS_Desember_2025.xlsx',
        'MB_2025_12.csv',
        'MainBill_JAN_2026.xls',
        'ARDEBT_2025-11.xlsx',
        'SBRS_FINAL_1225.xls'
    ]
    
    print("=" * 60)
    print("TEST AUTO-DETECT PERIODE")
    print("=" * 60)
    
    for filename in test_files:
        bulan, tahun = detect_periode_from_filename(filename)
        print(f"\n📄 {filename}")
        if bulan and tahun:
            print(f"   ✅ Detected: Bulan {bulan}, Tahun {tahun}")
        else:
            print(f"   ❌ Failed to detect")
