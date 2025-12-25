"""
Auto Detect Periode Module
Automatically detects periode from uploaded files
"""

import pandas as pd
import re
from datetime import datetime

def auto_detect_periode(filepath, file_type):
    """
    Auto-detect periode from file
    Returns: {'bulan': int, 'tahun': int, 'method': str}
    """
    
    try:
        # Read file
        if filepath.endswith('.csv'):
            df = pd.read_csv(filepath, dtype=str, nrows=100)
        elif filepath.endswith(('.xls', '.xlsx')):
            df = pd.read_excel(filepath, nrows=100)
        elif filepath.endswith('.txt'):
            try:
                df = pd.read_csv(filepath, sep='|', dtype=str, nrows=100)
            except:
                df = pd.read_csv(filepath, dtype=str, nrows=100)
        else:
            return {'bulan': None, 'tahun': None, 'method': 'unsupported_format'}
        
        df.columns = df.columns.str.upper().str.strip()
        
        # Method 1: Look for PERIODE column (format: YYYYMM or YYYY-MM)
        for col in ['PERIODE', 'PERIOD', 'BLN_REK']:
            if col in df.columns:
                periode_str = str(df[col].iloc[0]).strip()
                
                # Format: YYYYMM (e.g., 202401)
                if len(periode_str) == 6 and periode_str.isdigit():
                    tahun = int(periode_str[:4])
                    bulan = int(periode_str[4:6])
                    
                    # Apply offset for specific file types
                    if file_type in ['mc', 'mb', 'ardebt']:
                        bulan += 1
                        if bulan > 12:
                            bulan = 1
                            tahun += 1
                    
                    return {'bulan': bulan, 'tahun': tahun, 'method': f'{col}_column'}
                
                # Format: YYYY-MM (e.g., 2024-01)
                if '-' in periode_str:
                    parts = periode_str.split('-')
                    if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                        tahun = int(parts[0])
                        bulan = int(parts[1])
                        
                        if file_type in ['mc', 'mb', 'ardebt']:
                            bulan += 1
                            if bulan > 12:
                                bulan = 1
                                tahun += 1
                        
                        return {'bulan': bulan, 'tahun': tahun, 'method': f'{col}_column_dash'}
        
        # Method 2: Extract from filename (e.g., MC_202401.xlsx)
        filename = filepath.split('/')[-1]
        match = re.search(r'(\d{4})(\d{2})', filename)
        if match:
            tahun = int(match.group(1))
            bulan = int(match.group(2))
            
            if file_type in ['mc', 'mb', 'ardebt']:
                bulan += 1
                if bulan > 12:
                    bulan = 1
                    tahun += 1
            
            return {'bulan': bulan, 'tahun': tahun, 'method': 'filename'}
        
        # Method 3: Look for date columns
        for col in ['TGL_BAYAR', 'TANGGAL', 'TGL_TAGIHAN', 'DATE']:
            if col in df.columns:
                try:
                    date_val = pd.to_datetime(df[col].iloc[0], errors='coerce')
                    if pd.notna(date_val):
                        return {
                            'bulan': date_val.month, 
                            'tahun': date_val.year,
                            'method': f'{col}_date'
                        }
                except:
                    pass
        
        # Method 4: Use current month as fallback
        now = datetime.now()
        return {
            'bulan': now.month,
            'tahun': now.year,
            'method': 'fallback_current_month'
        }
        
    except Exception as e:
        print(f"❌ Auto-detect failed: {e}")
        return {'bulan': None, 'tahun': None, 'method': 'error'}

def validate_periode(bulan, tahun):
    """Validate periode values"""
    try:
        bulan = int(bulan)
        tahun = int(tahun)
        
        if not (1 <= bulan <= 12):
            return False
        
        if not (2020 <= tahun <= 2030):
            return False
        
        return True
    except:
        return False
