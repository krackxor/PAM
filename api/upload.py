"""
Upload API - FINAL VERSION with Column Detector

Changes:
1. Integrated robust column detector
2. Fully automatic detection
3. Proper periode logic
4. Multi-file ready
5. FIXED: master_bayar volume_air column error
6. FIXED: collection jumlah_bayar pd.to_numeric error

File: api/upload.py
"""

import os
import pandas as pd
from flask import jsonify, request, current_app
from datetime import datetime
import re

# Import auto-detect functions
from processors.auto_detect import auto_detect_periode


# ========================================
# COLUMN DETECTOR - ROBUST VERSION
# ========================================

def quick_column_fix(df, file_type):
    """
    Robust column detection and mapping based on actual file structure
    
    Supports:
    - MC: NOMEN, NAMA_PEL, TGL_CATAT, ZONA_NOVAK, NOMINAL
    - MB: NOMEN, TGL_BAYAR, NOMINAL, KUBIKBAYAR
    - COLLECTION: NOMEN/NO, TGL_BAYAR, JML_BAYAR
    - ARDEBT: NOMEN, PERIODE_BILL, JUMLAH, PCEZ
    """
    print(f"\n🔍 Detecting columns in {file_type.upper()} file...")
    print(f"Available columns ({len(df.columns)}): {list(df.columns)[:20]}")
    
    # Column mapping per file type
    if file_type == 'mc':
        col_map = {
            # ID
            'NOMEN': 'nomen', 'NO_PLGGN': 'nomen', 'NO_PELANGGAN': 'nomen',
            # Name & Address
            'NAMA_PEL': 'nama', 'NAMA': 'nama',
            'ALM1_PEL': 'alamat', 'ALAMAT': 'alamat',
            # Area
            'ZONA_NOVAK': 'rayon', 'RAYON': 'rayon',
            # Tarif
            'TARIF': 'tarif',
            # Date
            'TGL_CATAT': 'tgl_catat', 'ENTRY_DATE': 'tgl_catat',
            # Volume
            'KUBIK': 'kubikasi', 'VOLUME': 'kubikasi',
            # Amount
            'NOMINAL': 'target_mc', 'TOTAL': 'target_mc',
            # Others
            'NOMET': 'nomet', 'DIAMETER': 'diameter',
            'ZONA_NOREK': 'zona_norek', 'NOTAGIHAN': 'notagihan'
        }
        required = ['nomen']
        
    elif file_type == 'mb':
        col_map = {
            'NOMEN': 'nomen', 'NO_PLGGN': 'nomen', 'NO_PELANGGAN': 'nomen',
            'TGL_BAYAR': 'tgl_bayar', 'PAY_DT': 'tgl_bayar', 'DATE': 'tgl_bayar',
            'NOMINAL': 'jumlah_bayar', 'JML_BAYAR': 'jumlah_bayar', 'AMOUNT': 'jumlah_bayar',
            'JUMLAH': 'jumlah_bayar', 'TOTAL': 'jumlah_bayar', 'RUPIAH': 'jumlah_bayar',
            'KUBIKBAYAR': 'volume_air', 'VOLUME': 'volume_air',
            'BULAN_REK': 'bulan_rek'
        }
        required = ['nomen', 'tgl_bayar']
        
    elif file_type == 'collection':
        col_map = {
            'NOMEN': 'nomen', 'NO': 'nomen', 'NO_PLGGN': 'nomen', 
            'NOPEL': 'nomen', 'NOPEN': 'nomen', 'NO_PELANGGAN': 'nomen',
            'TGL_BAYAR': 'tgl_bayar', 'PAY_DT': 'tgl_bayar', 'DATE': 'tgl_bayar', 
            'TANGGAL': 'tgl_bayar', 'TGL': 'tgl_bayar',
            # Prioritize AMT_COLLECT over NOMINAL for collection files
            'AMT_COLLECT': 'jumlah_bayar',
            'JML_BAYAR': 'jumlah_bayar', 
            'AMOUNT': 'jumlah_bayar', 'NOMINAL': 'jumlah_bayar', 'BAYAR': 'jumlah_bayar', 
            'JUMLAH': 'jumlah_bayar', 'TOTAL': 'jumlah_bayar', 'RUPIAH': 'jumlah_bayar',
            # Prioritize VOL_COLLECT for collection files
            'VOL_COLLECT': 'volume_air',
            'VOLUME_AIR': 'volume_air', 'VOLUME': 'volume_air', 'KUBIK': 'volume_air', 
            'VOL': 'volume_air'
        }
        required = ['nomen', 'tgl_bayar']
        
    elif file_type == 'mainbill':
        col_map = {
            'NOMEN': 'nomen', 'NO_PLGGN': 'nomen',
            'TOTAL_TAGIHAN': 'total_tagihan', 'TOTAL': 'total_tagihan', 'AMOUNT': 'total_tagihan',
            'TARIF': 'tarif',
            'FREEZE_DT': 'freeze_dt', 'DATE': 'freeze_dt'
        }
        required = ['nomen']
        
    elif file_type == 'sbrs':
        col_map = {
            'NOMEN': 'nomen', 'NO_PLGGN': 'nomen',
            'VOLUME': 'volume', 'VOL': 'volume',
            'CMR_RD_DATE': 'cmr_rd_date', 'READ_DATE': 'cmr_rd_date'
        }
        required = ['nomen']
        
    elif file_type == 'ardebt':
        col_map = {
            'NOMEN': 'nomen', 'NO_PLGGN': 'nomen',
            'RAYON': 'rayon', 'DIVISI': 'divisi',
            'PCEZ': 'pcez', 'BOOKWALK': 'bookwalk',
            'PERIODE_BILL': 'periode_bill', 'PERIOD': 'periode_bill',
            'JUMLAH': 'saldo_tunggakan', 'SALDO_TUNGGAKAN': 'saldo_tunggakan', 'SALDO': 'saldo_tunggakan',
            'VOLUME': 'volume',
            'TIPE_BILL': 'tipe_bill', 'BILL_ID': 'bill_id'
        }
        required = ['nomen']
    
    else:
        col_map = {}
        required = ['nomen']
    
    # Step 1: Case-insensitive rename
    upper_cols = {k.upper(): k for k in df.columns}
    rename_dict = {}
    
    for old_name, new_name in col_map.items():
        if old_name.upper() in upper_cols:
            original_col = upper_cols[old_name.upper()]
            if original_col not in rename_dict:  # Avoid duplicate mapping
                rename_dict[original_col] = new_name
    
    if rename_dict:
        df = df.rename(columns=rename_dict)
        print(f"✅ Mapped {len(rename_dict)} columns")
    
    # Step 2: Auto-detect missing required columns
    for req_col in required:
        if req_col not in df.columns:
            # Define search keywords
            if req_col == 'nomen':
                keywords = ['nomen', 'nopel', 'nopen', 'no_plg', 'plggn', 'pelanggan', 'no', 'pel', 'cust']
            elif req_col == 'tgl_bayar':
                keywords = ['tgl', 'tanggal', 'date', 'bayar', 'pay', 'dt']
            elif req_col == 'jumlah_bayar':
                keywords = ['jml', 'jumlah', 'bayar', 'amt', 'amount', 'nominal']
            else:
                keywords = [req_col.lower()]
            
            # Search for matching column
            candidates = []
            for col in df.columns:
                col_lower = str(col).lower()
                if any(kw in col_lower for kw in keywords):
                    candidates.append(col)
            
            if candidates:
                selected = candidates[0]
                df = df.rename(columns={selected: req_col})
                print(f"🔍 Auto-detected '{req_col}': {selected}")
            elif req_col == 'nomen' and len(df.columns) > 0:
                # Last resort: use first column as nomen
                first_col = df.columns[0]
                df = df.rename(columns={first_col: 'nomen'})
                print(f"⚠️  Using first column as 'nomen': {first_col}")
            else:
                # Cannot find required column
                raise ValueError(
                    f"❌ Cannot find required column '{req_col}'!\n"
                    f"Available columns: {list(df.columns)[:30]}\n"
                    f"Expected keywords: {keywords}\n"
                    f"Please check file format."
                )
    
    print(f"✅ Column detection complete")
    return df


# ========================================
# HELPER FUNCTIONS
# ========================================

def clean_nomen(value):
    """Clean nomen value - remove spaces, special chars"""
    if pd.isna(value):
        return ''
    s = str(value).strip()
    s = re.sub(r'[^\d]', '', s)  # Keep only digits
    return s


def clean_date(value):
    """Clean and parse date"""
    if pd.isna(value):
        return None
    
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.strftime('%Y-%m-%d')
    
    s = str(value).strip()
    
    # Try various formats
    formats = [
        '%d-%m-%Y', '%d/%m/%Y', '%Y-%m-%d', '%Y/%m/%d',
        '%d%m%Y', '%Y%m%d', '%d-%b-%Y', '%d %b %Y'
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime('%Y-%m-%d')
        except:
            continue
    
    return None


def find_header_row(filepath):
    """Find header row in Excel file"""
    try:
        df_peek = pd.read_excel(filepath, nrows=10, header=None)
        
        for idx in range(min(5, len(df_peek))):
            row = df_peek.iloc[idx]
            # Check if row contains column names
            if any(str(val).strip().upper() in ['NOMEN', 'NO_PLGGN', 'NAMA', 'TGL_CATAT', 'TGL_BAYAR'] 
                   for val in row if pd.notna(val)):
                return idx
        
        return 0
    except:
        return 0


def validate_mc_exists(db, bulan, tahun):
    """Check if MC exists for the given periode"""
    cursor = db.cursor()
    cursor.execute("""
        SELECT COUNT(*) as cnt 
        FROM master_pelanggan 
        WHERE periode_bulan = ? AND periode_tahun = ?
    """, (bulan, tahun))
    
    result = cursor.fetchone()
    count = result['cnt'] if result else 0
    
    print(f"🔍 MC validation for {bulan:02d}/{tahun}: {count:,} records")
    return count > 0


def get_available_periodes(db):
    """Get all available periodes from master_pelanggan"""
    cursor = db.cursor()
    cursor.execute("""
        SELECT DISTINCT periode_bulan, periode_tahun
        FROM master_pelanggan
        ORDER BY periode_tahun DESC, periode_bulan DESC
    """)
    
    periodes = cursor.fetchall()
    return [f"{p['periode_bulan']:02d}/{p['periode_tahun']}" for p in periodes]


# ========================================
# MAIN UPLOAD ROUTE
# ========================================

def register_upload_routes(app, get_db):
    """Register upload routes - FULLY AUTOMATIC"""
    
    app.config['MAX_CONTENT_LENGTH'] = 10 * 1024 * 1024 * 1024  # 10GB
    
    @app.route('/api/upload', methods=['POST'])
    def upload_file():
        """
        Upload and process file - FULLY AUTOMATIC
        
        Returns:
        - detection: file_type, periode, method
        - processing: rows_inserted, mc_warning
        - statistics: summary stats + sample data
        """
        try:
            print("\n" + "="*70)
            print("UPLOAD FILE REQUEST - AUTO MODE")
            print("="*70)
            
            if 'file' not in request.files:
                return jsonify({'error': 'No file uploaded'}), 400
            
            file = request.files['file']
            if file.filename == '':
                return jsonify({'error': 'No file selected'}), 400
            
            filename = file.filename
            print(f"📄 File: {filename}")
            
            # Save temporarily
            temp_path = os.path.join('/tmp', f"temp_{datetime.now().timestamp()}_{filename}")
            file.save(temp_path)
            
            file_size = os.path.getsize(temp_path)
            print(f"📦 Size: {file_size:,} bytes")
            
            # AUTO-DETECT
            print(f"\n🔍 AUTO-DETECTING...")
            result = auto_detect_periode(temp_path, filename)
            
            if not result:
                os.remove(temp_path)
                return jsonify({
                    'error': 'Cannot detect file type or periode',
                    'filename': filename
                }), 400
            
            file_type = result['file_type']
            bulan = result['periode_bulan']
            tahun = result['periode_tahun']
            
            print(f"\n✅ DETECTION SUCCESS")
            print(f"   Type: {file_type.upper()}")
            print(f"   Periode: {bulan:02d}/{tahun}")
            print(f"   Method: {result['method']}")
            
            # Validate MC (except for MC itself)
            db = get_db()
            mc_warning = None
            
            if file_type != 'mc':
                if not validate_mc_exists(db, bulan, tahun):
                    mc_warning = f"MC belum ada untuk periode {bulan:02d}/{tahun}"
                    print(f"\n⚠️  WARNING: {mc_warning}")
                else:
                    print(f"✅ MC validation passed")
            
            # Save to permanent location
            upload_folder = current_app.config.get('UPLOAD_FOLDER', 'uploads')
            os.makedirs(upload_folder, exist_ok=True)
            
            filepath = os.path.join(upload_folder, filename)
            import shutil
            shutil.copy(temp_path, filepath)
            os.remove(temp_path)
            
            print(f"💾 Saved to: {filepath}")
            
            # Find header and read data
            header_row = find_header_row(filepath)
            print(f"📋 Header row: {header_row}")
            
            df = pd.read_excel(filepath, header=header_row)
            total_rows = len(df)
            print(f"📊 Total rows: {total_rows:,}")
            
            # Process based on type
            print(f"\n🔄 Processing {file_type.upper()}...")
            
            if file_type == 'mc':
                rows = process_mc(df, bulan, tahun, db)
                stats = get_mc_stats(db, bulan, tahun)
            elif file_type == 'mb':
                rows = process_mb(df, bulan, tahun, db)
                stats = get_mb_stats(db, bulan, tahun)
            elif file_type == 'collection':
                rows = process_collection(df, bulan, tahun, db)
                stats = get_collection_stats(db, bulan, tahun)
            elif file_type == 'mainbill':
                rows = process_mainbill(df, bulan, tahun, db)
                stats = get_mainbill_stats(db, bulan, tahun)
            elif file_type == 'sbrs':
                rows = process_sbrs(df, bulan, tahun, db)
                stats = get_sbrs_stats(db, bulan, tahun)
            elif file_type == 'ardebt':
                rows = process_ardebt(df, bulan, tahun, db)
                stats = get_ardebt_stats(db, bulan, tahun)
            else:
                return jsonify({'error': f'Unknown file type: {file_type}'}), 400
            
            db.commit()
            
            print(f"\n✅ UPLOAD COMPLETE: {rows:,} rows processed")
            print(f"{'='*70}\n")
            
            # Response
            return jsonify({
                'success': True,
                'filename': filename,
                'file_size': file_size,
                'detection': {
                    'file_type': file_type,
                    'periode_bulan': bulan,
                    'periode_tahun': tahun,
                    'periode_label': f"{bulan:02d}/{tahun}",
                    'method': result['method']
                },
                'processing': {
                    'total_rows_in_file': total_rows,
                    'rows_inserted': rows,
                    'mc_warning': mc_warning
                },
                'statistics': stats,
                'available_periodes': get_available_periodes(db)
            })
            
        except Exception as e:
            import traceback
            print("\n❌ UPLOAD ERROR:")
            print(traceback.format_exc())
            print("="*70 + "\n")
            
            return jsonify({
                'error': str(e),
                'traceback': traceback.format_exc()
            }), 500
    
    print("✅ Upload routes registered (FULLY AUTO MODE with Column Detector)")


# ========================================
# PROCESS FUNCTIONS
# ========================================

def process_mc(df, month, year, db):
    """
    Process MC (Master Cetak) file
    """
    cursor = db.cursor()
    
    # COLUMN DETECTION
    df = quick_column_fix(df, 'mc')
    
    # Clean nomen
    df['nomen'] = df['nomen'].apply(clean_nomen)
    df = df.dropna(subset=['nomen'])
    df = df[df['nomen'] != '']
    
    # Reset index to avoid duplicate index errors
    df = df.reset_index(drop=True)
    
    # Clean other fields
    if 'nama' in df.columns:
        df['nama'] = df['nama'].fillna('').astype(str)
    else:
        df['nama'] = ''
    
    if 'alamat' in df.columns:
        df['alamat'] = df['alamat'].fillna('').astype(str)
    else:
        df['alamat'] = ''
    
    if 'rayon' in df.columns:
        df['rayon'] = df['rayon'].fillna('').astype(str)
    else:
        df['rayon'] = ''
    
    if 'tarif' in df.columns:
        df['tarif'] = df['tarif'].fillna('').astype(str)
    else:
        df['tarif'] = ''
    
    if 'target_mc' in df.columns:
        df['target_mc'] = pd.to_numeric(df['target_mc'], errors='coerce').fillna(0)
    else:
        df['target_mc'] = 0
    
    if 'kubikasi' in df.columns:
        df['kubikasi'] = pd.to_numeric(df['kubikasi'], errors='coerce').fillna(0)
    else:
        df['kubikasi'] = 0
    
    df['periode_bulan'] = month
    df['periode_tahun'] = year
    
    print(f"\n{'='*70}")
    print(f"PROCESSING MC - PERIODE {month:02d}/{year}")
    print(f"{'='*70}")
    print(f"📊 Total records: {len(df):,}")
    
    # Delete existing data for this periode
    cursor.execute("""
        DELETE FROM master_pelanggan 
        WHERE periode_bulan = ? AND periode_tahun = ?
    """, (month, year))
    deleted = cursor.rowcount
    print(f"🗑️  Deleted {deleted:,} existing records")
    
    # Insert new data
    inserted = 0
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO master_pelanggan 
            (nomen, nama, alamat, rayon, tarif, target_mc, kubikasi, periode_bulan, periode_tahun)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row['nomen'], row['nama'], row['alamat'], row['rayon'], 
            row['tarif'], row['target_mc'], row['kubikasi'],
            month, year
        ))
        inserted += 1
    
    print(f"✅ Inserted: {inserted:,} records")
    return inserted


def process_mb(df, month, year, db):
    """
    Process MB (Manual Bayar) file
    FIXED: Removed volume_air column that doesn't exist in master_bayar table
    """
    cursor = db.cursor()
    
    # COLUMN DETECTION
    df = quick_column_fix(df, 'mb')
    
    # Clean nomen
    df['nomen'] = df['nomen'].apply(clean_nomen)
    df = df.dropna(subset=['nomen'])
    df = df[df['nomen'] != '']
    
    # Reset index to avoid duplicate index errors
    df = df.reset_index(drop=True)
    
    # Clean date
    df['tgl_bayar'] = df['tgl_bayar'].apply(clean_date)
    
    # Clean amount
    if 'jumlah_bayar' not in df.columns:
        df['jumlah_bayar'] = 0
    else:
        df['jumlah_bayar'] = pd.to_numeric(df['jumlah_bayar'], errors='coerce').fillna(0)
    
    df['periode_bulan'] = month
    df['periode_tahun'] = year
    
    print(f"\n{'='*70}")
    print(f"PROCESSING MB - PERIODE {month:02d}/{year}")
    print(f"{'='*70}")
    print(f"📊 Total records: {len(df):,}")
    
    # Validate MC
    cursor.execute("""
        SELECT COUNT(DISTINCT nomen) as mc_count
        FROM master_pelanggan
        WHERE periode_bulan = ? AND periode_tahun = ?
    """, (month, year))
    mc_result = cursor.fetchone()
    mc_count = mc_result['mc_count'] if mc_result else 0
    
    if mc_count > 0:
        print(f"🔗 MC available: {mc_count:,} nomens")
    else:
        print(f"⚠️  WARNING: No MC data for {month:02d}/{year}")
    
    # Delete existing MB data for this periode
    cursor.execute("""
        DELETE FROM master_bayar
        WHERE periode_bulan = ? AND periode_tahun = ?
    """, (month, year))
    deleted = cursor.rowcount
    print(f"🗑️  Deleted {deleted:,} existing records")
    
    # Insert new data - FIXED: removed volume_air column
    inserted = 0
    linked = 0
    unlinked = 0
    
    for _, row in df.iterrows():
        # Check if nomen exists in MC
        cursor.execute("""
            SELECT 1 FROM master_pelanggan
            WHERE nomen = ? AND periode_bulan = ? AND periode_tahun = ?
        """, (row['nomen'], month, year))
        
        if cursor.fetchone():
            linked += 1
        else:
            unlinked += 1
        
        cursor.execute("""
            INSERT INTO master_bayar
            (nomen, tgl_bayar, jumlah_bayar, periode_bulan, periode_tahun)
            VALUES (?, ?, ?, ?, ?)
        """, (
            row['nomen'], row['tgl_bayar'], row['jumlah_bayar'],
            month, year
        ))
        inserted += 1
    
    print(f"✅ Inserted: {inserted:,} records")
    print(f"🔗 Linked to MC: {linked:,}")
    if unlinked > 0:
        print(f"⚠️  Unlinked: {unlinked:,}")
    
    return inserted


def process_collection(df, month, year, db):
    """
    Process Collection (Bayar Harian) file
    FIXED: Better handling of jumlah_bayar column detection and conversion
    FIXED: Reset index to avoid duplicate index errors
    FIXED: Simpler tipe_bayar classification
    """
    cursor = db.cursor()
    
    # COLUMN DETECTION
    df = quick_column_fix(df, 'collection')
    
    # Clean nomen
    df['nomen'] = df['nomen'].apply(clean_nomen)
    df = df.dropna(subset=['nomen'])
    df = df[df['nomen'] != '']
    
    # CRITICAL: Reset index COMPLETELY - drop old index and create fresh sequential one
    df = df.reset_index(drop=True)
    
    # Double check for duplicate indices
    if df.index.duplicated().any():
        print("⚠️  Warning: Duplicate indices detected, forcing unique index")
        df.index = range(len(df))
    
    # Clean date
    df['tgl_bayar'] = df['tgl_bayar'].apply(clean_date)
    
    # Clean amount - FIXED: Better column existence check
    if 'jumlah_bayar' not in df.columns:
        df['jumlah_bayar'] = 0
    else:
        # Check if column is valid Series before conversion
        if isinstance(df['jumlah_bayar'], pd.Series):
            df['jumlah_bayar'] = pd.to_numeric(df['jumlah_bayar'], errors='coerce').fillna(0)
        else:
            df['jumlah_bayar'] = 0
    
    # Clean volume
    if 'volume_air' not in df.columns:
        df['volume_air'] = 0
    else:
        if isinstance(df['volume_air'], pd.Series):
            df['volume_air'] = pd.to_numeric(df['volume_air'], errors='coerce').fillna(0)
        else:
            df['volume_air'] = 0
    
    # Classify payment type - SIMPLIFIED using numpy
    import numpy as np
    df['tipe_bayar'] = np.where(
        (df['jumlah_bayar'].values > 0) & (df['volume_air'].values == 0),
        'tunggakan',
        'current'
    )
    
    df['periode_bulan'] = month
    df['periode_tahun'] = year
    
    print(f"\n{'='*70}")
    print(f"PROCESSING COLLECTION - PERIODE {month:02d}/{year}")
    print(f"{'='*70}")
    print(f"📊 Total records: {len(df):,}")
    
    # Validate MC
    cursor.execute("""
        SELECT COUNT(DISTINCT nomen) as mc_count
        FROM master_pelanggan
        WHERE periode_bulan = ? AND periode_tahun = ?
    """, (month, year))
    mc_result = cursor.fetchone()
    mc_count = mc_result['mc_count'] if mc_result else 0
    
    if mc_count > 0:
        print(f"🔗 MC available: {mc_count:,} nomens")
    else:
        print(f"⚠️  WARNING: No MC data for {month:02d}/{year}")
    
    # Delete existing collection for this periode
    cursor.execute("""
        DELETE FROM collection_harian
        WHERE periode_bulan = ? AND periode_tahun = ?
    """, (month, year))
    deleted = cursor.rowcount
    print(f"🗑️  Deleted {deleted:,} existing records")
    
    # Insert new data
    inserted = 0
    linked = 0
    unlinked = 0
    
    for idx, row in df.iterrows():
        # Check if nomen exists in MC
        cursor.execute("""
            SELECT 1 FROM master_pelanggan
            WHERE nomen = ? AND periode_bulan = ? AND periode_tahun = ?
        """, (row['nomen'], month, year))
        
        if cursor.fetchone():
            linked += 1
        else:
            unlinked += 1
        
        cursor.execute("""
            INSERT OR REPLACE INTO collection_harian
            (nomen, tgl_bayar, jumlah_bayar, volume_air, tipe_bayar, periode_bulan, periode_tahun)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            row['nomen'], row['tgl_bayar'], row['jumlah_bayar'],
            row['volume_air'], row['tipe_bayar'], month, year
        ))
        inserted += 1
    
    print(f"✅ Inserted: {inserted:,} records")
    print(f"🔗 Linked to MC: {linked:,}")
    if unlinked > 0:
        print(f"⚠️  Unlinked: {unlinked:,}")
    
    return inserted


def process_mainbill(df, month, year, db):
    """
    Process Mainbill file
    """
    cursor = db.cursor()
    
    # COLUMN DETECTION
    df = quick_column_fix(df, 'mainbill')
    
    # Clean nomen
    df['nomen'] = df['nomen'].apply(clean_nomen)
    df = df.dropna(subset=['nomen'])
    df = df[df['nomen'] != '']
    
    # Clean amount
    if 'total_tagihan' not in df.columns:
        df['total_tagihan'] = 0
    else:
        df['total_tagihan'] = pd.to_numeric(df['total_tagihan'], errors='coerce').fillna(0)
    
    if 'tarif' in df.columns:
        df['tarif'] = df['tarif'].fillna('').astype(str)
    else:
        df['tarif'] = ''
    
    df['periode_bulan'] = month
    df['periode_tahun'] = year
    
    print(f"\n{'='*70}")
    print(f"PROCESSING MAINBILL - PERIODE {month:02d}/{year}")
    print(f"{'='*70}")
    print(f"📊 Total records: {len(df):,}")
    
    # Delete existing
    cursor.execute("""
        DELETE FROM mainbill
        WHERE periode_bulan = ? AND periode_tahun = ?
    """, (month, year))
    deleted = cursor.rowcount
    print(f"🗑️  Deleted {deleted:,} existing records")
    
    # Insert
    inserted = 0
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO mainbill
            (nomen, total_tagihan, tarif, periode_bulan, periode_tahun)
            VALUES (?, ?, ?, ?, ?)
        """, (row['nomen'], row['total_tagihan'], row['tarif'], month, year))
        inserted += 1
    
    print(f"✅ Inserted: {inserted:,} records")
    return inserted


def process_sbrs(df, month, year, db):
    """
    Process SBRS file
    """
    cursor = db.cursor()
    
    # COLUMN DETECTION
    df = quick_column_fix(df, 'sbrs')
    
    # Clean nomen
    df['nomen'] = df['nomen'].apply(clean_nomen)
    df = df.dropna(subset=['nomen'])
    df = df[df['nomen'] != '']
    
    # Clean volume
    if 'volume' not in df.columns:
        df['volume'] = 0
    else:
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0)
    
    df['periode_bulan'] = month
    df['periode_tahun'] = year
    
    print(f"\n{'='*70}")
    print(f"PROCESSING SBRS - PERIODE {month:02d}/{year}")
    print(f"{'='*70}")
    print(f"📊 Total records: {len(df):,}")
    
    # Delete existing
    cursor.execute("""
        DELETE FROM sbrs_data
        WHERE periode_bulan = ? AND periode_tahun = ?
    """, (month, year))
    deleted = cursor.rowcount
    print(f"🗑️  Deleted {deleted:,} existing records")
    
    # Insert
    inserted = 0
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO sbrs_data
            (nomen, volume, periode_bulan, periode_tahun)
            VALUES (?, ?, ?, ?)
        """, (row['nomen'], row['volume'], month, year))
        inserted += 1
    
    print(f"✅ Inserted: {inserted:,} records")
    return inserted


def process_ardebt(df, month, year, db):
    """
    Process ARDEBT (Tunggakan) file
    
    Special handling:
    - PCEZ split: "151/10" → pc=151, ez=10
    - Multiple periodes per customer
    - NO DELETE (keep historical data)
    """
    cursor = db.cursor()
    
    # COLUMN DETECTION
    df = quick_column_fix(df, 'ardebt')
    
    # Clean nomen
    df['nomen'] = df['nomen'].apply(clean_nomen)
    df = df.dropna(subset=['nomen'])
    df = df[df['nomen'] != '']
    
    # Clean amount
    if 'saldo_tunggakan' not in df.columns:
        df['saldo_tunggakan'] = 0
    else:
        df['saldo_tunggakan'] = pd.to_numeric(df['saldo_tunggakan'], errors='coerce').fillna(0)
    
    # Parse PCEZ if exists
    if 'pcez' in df.columns:
        df[['pc', 'ez']] = df['pcez'].apply(
            lambda x: pd.Series(str(x).split('/') if pd.notna(x) else [None, None])
        )
    else:
        df['pc'] = None
        df['ez'] = None
    
    # Parse PERIODE_BILL if exists
    if 'periode_bill' in df.columns:
        def parse_periode(val):
            if pd.isna(val):
                return month, year
            try:
                if isinstance(val, (datetime, pd.Timestamp)):
                    return val.month, val.year
                # Try parsing as string
                dt = pd.to_datetime(val)
                return dt.month, dt.year
            except:
                return month, year
        
        df[['bill_month', 'bill_year']] = df['periode_bill'].apply(
            lambda x: pd.Series(parse_periode(x))
        )
    else:
        df['bill_month'] = month
        df['bill_year'] = year
    
    # Calculate umur piutang (age)
    from datetime import datetime
    current_date = datetime(year, month, 1)
    
    def calc_umur(row):
        try:
            bill_date = datetime(int(row['bill_year']), int(row['bill_month']), 1)
            diff = (current_date.year - bill_date.year) * 12 + (current_date.month - bill_date.month)
            return max(0, diff)
        except:
            return 0
    
    df['umur_piutang'] = df.apply(calc_umur, axis=1)
    
    print(f"\n{'='*70}")
    print(f"PROCESSING ARDEBT - PERIODE {month:02d}/{year}")
    print(f"{'='*70}")
    print(f"📊 Total records: {len(df):,}")
    
    # DELETE only for same periode_bill
    cursor.execute("""
        DELETE FROM ardebt
        WHERE periode_bulan = ? AND periode_tahun = ?
    """, (month, year))
    deleted = cursor.rowcount
    print(f"🗑️  Deleted {deleted:,} existing records for this periode")
    
    # Insert
    inserted = 0
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO ardebt
            (nomen, saldo_tunggakan, pc, ez, umur_piutang, 
             periode_bulan, periode_tahun)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            row['nomen'], row['saldo_tunggakan'],
            row.get('pc'), row.get('ez'), row['umur_piutang'],
            int(row['bill_month']), int(row['bill_year'])
        ))
        inserted += 1
    
    print(f"✅ Inserted: {inserted:,} records")
    return inserted


# ========================================
# STATISTICS FUNCTIONS
# ========================================

def get_mc_stats(db, bulan, tahun):
    """Get MC statistics for periode"""
    cursor = db.cursor()
    
    # Total & summary
    cursor.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(target_mc) as total_target,
            COUNT(DISTINCT rayon) as total_rayon
        FROM master_pelanggan 
        WHERE periode_bulan = ? AND periode_tahun = ?
    """, (bulan, tahun))
    result = cursor.fetchone()
    
    # By rayon
    cursor.execute("""
        SELECT rayon, COUNT(*) as cnt, SUM(target_mc) as target
        FROM master_pelanggan
        WHERE periode_bulan = ? AND periode_tahun = ?
        GROUP BY rayon
        ORDER BY cnt DESC
        LIMIT 5
    """, (bulan, tahun))
    by_rayon = [dict(row) for row in cursor.fetchall()]
    
    # Sample data
    cursor.execute("""
        SELECT nomen, nama, alamat, rayon, tarif, target_mc, kubikasi
        FROM master_pelanggan
        WHERE periode_bulan = ? AND periode_tahun = ?
        ORDER BY target_mc DESC
        LIMIT 10
    """, (bulan, tahun))
    sample = [dict(row) for row in cursor.fetchall()]
    
    return {
        'total_records': result['total'],
        'total_target': float(result['total_target'] or 0),
        'total_rayon': result['total_rayon'],
        'by_rayon': by_rayon,
        'sample_data': sample
    }


def get_mb_stats(db, bulan, tahun):
    """Get MB statistics for periode"""
    cursor = db.cursor()
    
    # Total
    cursor.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(jumlah_bayar) as total_bayar,
            AVG(jumlah_bayar) as avg_bayar
        FROM master_bayar 
        WHERE periode_bulan = ? AND periode_tahun = ?
    """, (bulan, tahun))
    result = cursor.fetchone()
    
    # Linking
    cursor.execute("""
        SELECT 
            COUNT(CASE WHEN m.nomen IS NOT NULL THEN 1 END) as linked,
            COUNT(CASE WHEN m.nomen IS NULL THEN 1 END) as unlinked
        FROM master_bayar mb
        LEFT JOIN master_pelanggan m 
            ON mb.nomen = m.nomen 
            AND mb.periode_bulan = m.periode_bulan 
            AND mb.periode_tahun = m.periode_tahun
        WHERE mb.periode_bulan = ? AND mb.periode_tahun = ?
    """, (bulan, tahun))
    linking = cursor.fetchone()
    
    # Sample
    cursor.execute("""
        SELECT mb.nomen, mb.tgl_bayar, mb.jumlah_bayar, m.nama
        FROM master_bayar mb
        LEFT JOIN master_pelanggan m 
            ON mb.nomen = m.nomen 
            AND mb.periode_bulan = m.periode_bulan 
            AND mb.periode_tahun = m.periode_tahun
        WHERE mb.periode_bulan = ? AND mb.periode_tahun = ?
        ORDER BY mb.jumlah_bayar DESC
        LIMIT 10
    """, (bulan, tahun))
    sample = [dict(row) for row in cursor.fetchall()]
    
    return {
        'total_records': result['total'],
        'total_bayar': float(result['total_bayar'] or 0),
        'avg_bayar': float(result['avg_bayar'] or 0),
        'linked_to_mc': linking['linked'],
        'unlinked': linking['unlinked'],
        'sample_data': sample
    }


def get_collection_stats(db, bulan, tahun):
    """Get Collection statistics for periode"""
    cursor = db.cursor()
    
    # Total
    cursor.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(jumlah_bayar) as total_bayar,
            SUM(volume_air) as total_volume,
            COUNT(CASE WHEN tipe_bayar = 'current' THEN 1 END) as current_cnt,
            COUNT(CASE WHEN tipe_bayar = 'tunggakan' THEN 1 END) as tunggakan_cnt
        FROM collection_harian 
        WHERE periode_bulan = ? AND periode_tahun = ?
    """, (bulan, tahun))
    result = cursor.fetchone()
    
    # Linking
    cursor.execute("""
        SELECT 
            COUNT(CASE WHEN m.nomen IS NOT NULL THEN 1 END) as linked,
            COUNT(CASE WHEN m.nomen IS NULL THEN 1 END) as unlinked
        FROM collection_harian c
        LEFT JOIN master_pelanggan m 
            ON c.nomen = m.nomen 
            AND c.periode_bulan = m.periode_bulan 
            AND c.periode_tahun = m.periode_tahun
        WHERE c.periode_bulan = ? AND c.periode_tahun = ?
    """, (bulan, tahun))
    linking = cursor.fetchone()
    
    # Sample
    cursor.execute("""
        SELECT c.nomen, c.tgl_bayar, c.jumlah_bayar, c.volume_air, 
               c.tipe_bayar, m.nama, m.rayon
        FROM collection_harian c
        LEFT JOIN master_pelanggan m 
            ON c.nomen = m.nomen 
            AND c.periode_bulan = m.periode_bulan 
            AND c.periode_tahun = m.periode_tahun
        WHERE c.periode_bulan = ? AND c.periode_tahun = ?
        ORDER BY c.jumlah_bayar DESC
        LIMIT 10
    """, (bulan, tahun))
    sample = [dict(row) for row in cursor.fetchall()]
    
    return {
        'total_records': result['total'],
        'total_bayar': float(result['total_bayar'] or 0),
        'total_volume': float(result['total_volume'] or 0),
        'current_payments': result['current_cnt'],
        'tunggakan_payments': result['tunggakan_cnt'],
        'linked_to_mc': linking['linked'],
        'unlinked': linking['unlinked'],
        'sample_data': sample
    }


def get_mainbill_stats(db, bulan, tahun):
    """Get Mainbill statistics for periode"""
    cursor = db.cursor()
    
    cursor.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(total_tagihan) as total_tagihan,
            AVG(total_tagihan) as avg_tagihan
        FROM mainbill 
        WHERE periode_bulan = ? AND periode_tahun = ?
    """, (bulan, tahun))
    result = cursor.fetchone()
    
    # Linking
    cursor.execute("""
        SELECT 
            COUNT(CASE WHEN m.nomen IS NOT NULL THEN 1 END) as linked,
            COUNT(CASE WHEN m.nomen IS NULL THEN 1 END) as unlinked
        FROM mainbill mb
        LEFT JOIN master_pelanggan m 
            ON mb.nomen = m.nomen 
            AND mb.periode_bulan = m.periode_bulan 
            AND mb.periode_tahun = m.periode_tahun
        WHERE mb.periode_bulan = ? AND mb.periode_tahun = ?
    """, (bulan, tahun))
    linking = cursor.fetchone()
    
    # Sample
    cursor.execute("""
        SELECT mb.nomen, mb.total_tagihan, mb.tarif, m.nama
        FROM mainbill mb
        LEFT JOIN master_pelanggan m 
            ON mb.nomen = m.nomen 
            AND mb.periode_bulan = m.periode_bulan 
            AND mb.periode_tahun = m.periode_tahun
        WHERE mb.periode_bulan = ? AND mb.periode_tahun = ?
        ORDER BY mb.total_tagihan DESC
        LIMIT 10
    """, (bulan, tahun))
    sample = [dict(row) for row in cursor.fetchall()]
    
    return {
        'total_records': result['total'],
        'total_tagihan': float(result['total_tagihan'] or 0),
        'avg_tagihan': float(result['avg_tagihan'] or 0),
        'linked_to_mc': linking['linked'],
        'unlinked': linking['unlinked'],
        'sample_data': sample
    }


def get_sbrs_stats(db, bulan, tahun):
    """Get SBRS statistics for periode"""
    cursor = db.cursor()
    
    cursor.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(volume) as total_volume,
            AVG(volume) as avg_volume
        FROM sbrs_data 
        WHERE periode_bulan = ? AND periode_tahun = ?
    """, (bulan, tahun))
    result = cursor.fetchone()
    
    # Linking
    cursor.execute("""
        SELECT 
            COUNT(CASE WHEN m.nomen IS NOT NULL THEN 1 END) as linked,
            COUNT(CASE WHEN m.nomen IS NULL THEN 1 END) as unlinked
        FROM sbrs_data s
        LEFT JOIN master_pelanggan m 
            ON s.nomen = m.nomen 
            AND s.periode_bulan = m.periode_bulan 
            AND s.periode_tahun = m.periode_tahun
        WHERE s.periode_bulan = ? AND s.periode_tahun = ?
    """, (bulan, tahun))
    linking = cursor.fetchone()
    
    # Sample
    cursor.execute("""
        SELECT s.nomen, s.volume, m.nama, m.rayon
        FROM sbrs_data s
        LEFT JOIN master_pelanggan m 
            ON s.nomen = m.nomen 
            AND s.periode_bulan = m.periode_bulan 
            AND s.periode_tahun = m.periode_tahun
        WHERE s.periode_bulan = ? AND s.periode_tahun = ?
        ORDER BY s.volume DESC
        LIMIT 10
    """, (bulan, tahun))
    sample = [dict(row) for row in cursor.fetchall()]
    
    return {
        'total_records': result['total'],
        'total_volume': float(result['total_volume'] or 0),
        'avg_volume': float(result['avg_volume'] or 0),
        'linked_to_mc': linking['linked'],
        'unlinked': linking['unlinked'],
        'sample_data': sample
    }


def get_ardebt_stats(db, bulan, tahun):
    """Get Ardebt statistics for periode"""
    cursor = db.cursor()
    
    cursor.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(saldo_tunggakan) as total_piutang,
            AVG(saldo_tunggakan) as avg_piutang,
            AVG(umur_piutang) as avg_umur
        FROM ardebt 
        WHERE periode_bulan = ? AND periode_tahun = ?
    """, (bulan, tahun))
    result = cursor.fetchone()
    
    # Linking
    cursor.execute("""
        SELECT 
            COUNT(CASE WHEN m.nomen IS NOT NULL THEN 1 END) as linked,
            COUNT(CASE WHEN m.nomen IS NULL THEN 1 END) as unlinked
        FROM ardebt a
        LEFT JOIN master_pelanggan m 
            ON a.nomen = m.nomen 
            AND a.periode_bulan = m.periode_bulan 
            AND a.periode_tahun = m.periode_tahun
        WHERE a.periode_bulan = ? AND a.periode_tahun = ?
    """, (bulan, tahun))
    linking = cursor.fetchone()
    
    # All periodes
    cursor.execute("""
        SELECT periode_bulan, periode_tahun, COUNT(*) as cnt
        FROM ardebt
        GROUP BY periode_bulan, periode_tahun
        ORDER BY periode_tahun DESC, periode_bulan DESC
    """)
    all_periodes = [dict(row) for row in cursor.fetchall()]
    
    # Sample
    cursor.execute("""
        SELECT a.nomen, a.saldo_tunggakan, a.umur_piutang, a.pc, a.ez,
               m.nama, m.rayon
        FROM ardebt a
        LEFT JOIN master_pelanggan m 
            ON a.nomen = m.nomen 
            AND a.periode_bulan = m.periode_bulan 
            AND a.periode_tahun = m.periode_tahun
        WHERE a.periode_bulan = ? AND a.periode_tahun = ?
        ORDER BY a.saldo_tunggakan DESC
        LIMIT 10
    """, (bulan, tahun))
    sample = [dict(row) for row in cursor.fetchall()]
    
    return {
        'total_records': result['total'],
        'total_piutang': float(result['total_piutang'] or 0),
        'avg_piutang': float(result['avg_piutang'] or 0),
        'avg_umur': float(result['avg_umur'] or 0),
        'linked_to_mc': linking['linked'],
        'unlinked': linking['unlinked'],
        'all_periodes': all_periodes,
        'sample_data': sample
    }
