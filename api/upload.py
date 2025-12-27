"""
Upload API - CORRECTED VERSION with Proper Periode Logic

PERIODE RULES (BUSINESS LOGIC):
================================

MC & MB Files (DATE OFFSET +1):
- MC: TGL_CATAT 19/06/2025 → Periode 07/2025 (Juli)
- MB: TGL_BAYAR 04/06/2025 → Periode 07/2025 (Juli)

Other Files (NO OFFSET):
- Collection: PAY_DT 01-07-2025 → Periode 07/2025 (Juli)
- Mainbill: FREEZE_DT 12/07/2025 → Periode 07/2025 (Juli)  
- SBRS: cmr_rd_date 22072025 → Periode 07/2025 (Juli)
- Ardebt: PERIODE_BILL (from column) → Use column value

MC VALIDATION:
==============
- MC is the MASTER (induk)
- Must upload MC FIRST before other files
- All other files must reference existing MC nomen
- Validation: Check if MC exists for periode before allowing other uploads
"""

import os
import pandas as pd
from flask import jsonify, request, current_app
from datetime import datetime

# Import auto-detect functions
from processors.auto_detect import auto_detect_periode, auto_detect_file_type


def register_upload_routes(app, get_db):
    """Register upload routes - FULLY AUTOMATIC (no manual mode)"""
    
    # Max 10GB
    app.config['MAX_CONTENT_LENGTH'] = 10 * 1024 * 1024 * 1024
    
    @app.route('/api/upload', methods=['POST'])
    def upload_file():
        """
        Upload and process file - FULLY AUTOMATIC
        
        Auto-detect file type and periode, then process immediately
        
        Required params:
        - file: File to upload
        
        Returns:
        - Detection info (file_type, periode)
        - Processing result (rows inserted, linking stats)
        - Data summary (total records, sample data)
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
            
            # Save temporarily for detection
            temp_path = os.path.join('/tmp', f"temp_{datetime.now().timestamp()}_{filename}")
            file.save(temp_path)
            
            file_size = os.path.getsize(temp_path)
            print(f"📦 Size: {file_size:,} bytes")
            
            # STEP 1: AUTO-DETECT file type and periode
            print(f"\n🔍 AUTO-DETECTING file type and periode...")
            result = auto_detect_periode(temp_path, filename)
            
            if not result:
                os.remove(temp_path)
                return jsonify({
                    'error': 'Cannot detect file type or periode. Please check file format.',
                    'filename': filename
                }), 400
            
            file_type = result['file_type']
            bulan = result['periode_bulan']
            tahun = result['periode_tahun']
            
            print(f"\n✅ DETECTION SUCCESS")
            print(f"   Type: {file_type.upper()}")
            print(f"   Periode: {bulan:02d}/{tahun}")
            print(f"   Method: {result['method']}")
            if result.get('date_column'):
                print(f"   Date Column: {result['date_column']}")
            
            # STEP 2: Validate MC exists (except for MC upload itself)
            db = get_db()
            mc_warning = None
            
            if file_type != 'mc':
                mc_exists = validate_mc_exists(db, bulan, tahun)
                if not mc_exists:
                    mc_warning = f"MC belum ada untuk periode {bulan:02d}/{tahun}"
                    print(f"\n⚠️  WARNING: {mc_warning}")
                else:
                    print(f"✅ MC validation passed")
            
            # STEP 3: Find header row and read data
            upload_folder = current_app.config.get('UPLOAD_FOLDER', 'uploads')
            os.makedirs(upload_folder, exist_ok=True)
            
            filepath = os.path.join(upload_folder, filename)
            
            # Copy from temp to permanent location
            import shutil
            shutil.copy(temp_path, filepath)
            os.remove(temp_path)
            
            print(f"💾 Saved to: {filepath}")
            
            header_row = find_header_row(filepath)
            print(f"📋 Header row: {header_row}")
            
            df = pd.read_excel(filepath, header=header_row)
            total_rows = len(df)
            print(f"📊 Total rows: {total_rows:,}")
            
            # STEP 4: Process based on type
            print(f"\n🔄 Processing {file_type.upper()}...")
            
            process_stats = {}
            
            if file_type == 'mc':
                rows = process_mc(df, bulan, tahun, db)
                process_stats = get_mc_stats(db, bulan, tahun)
            elif file_type == 'mb':
                rows = process_mb(df, bulan, tahun, db)
                process_stats = get_mb_stats(db, bulan, tahun)
            elif file_type == 'collection':
                rows = process_collection(df, bulan, tahun, db)
                process_stats = get_collection_stats(db, bulan, tahun)
            elif file_type == 'mainbill':
                rows = process_mainbill(df, bulan, tahun, db)
                process_stats = get_mainbill_stats(db, bulan, tahun)
            elif file_type == 'sbrs':
                rows = process_sbrs(df, bulan, tahun, db)
                process_stats = get_sbrs_stats(db, bulan, tahun)
            elif file_type == 'ardebt':
                rows = process_ardebt(df, bulan, tahun, db)
                process_stats = get_ardebt_stats(db, bulan, tahun)
            else:
                return jsonify({'error': f'Unknown file type: {file_type}'}), 400
            
            db.commit()
            
            print(f"\n✅ UPLOAD COMPLETE")
            print(f"   Processed: {rows:,} rows")
            print(f"{'='*70}\n")
            
            # Get available periodes
            available_periodes = get_available_periodes(db)
            
            # Build response
            response_data = {
                'success': True,
                'filename': filename,
                'file_size': file_size,
                'detection': {
                    'file_type': file_type,
                    'periode_bulan': bulan,
                    'periode_tahun': tahun,
                    'periode_label': f"{bulan:02d}/{tahun}",
                    'method': result['method'],
                    'date_column': result.get('date_column')
                },
                'processing': {
                    'total_rows_in_file': total_rows,
                    'rows_inserted': rows,
                    'mc_warning': mc_warning
                },
                'statistics': process_stats,
                'available_periodes': available_periodes
            }
            
            return jsonify(response_data)
            
        except Exception as e:
            import traceback
            print("\n❌ UPLOAD ERROR:")
            print(traceback.format_exc())
            print("="*70 + "\n")
            
            return jsonify({
                'error': str(e),
                'traceback': traceback.format_exc()
            }), 500
    
    print("✅ Upload routes registered (FULLY AUTO MODE)")


# Helper functions

def validate_mc_exists(db, bulan, tahun):
    """
    Validate that MC exists for the given periode
    
    Returns: True if MC exists, False otherwise
    """
    cursor = db.cursor()
    cursor.execute("""
        SELECT COUNT(*) as cnt 
        FROM master_pelanggan 
        WHERE periode_bulan = ? AND periode_tahun = ?
    """, (bulan, tahun))
    
    result = cursor.fetchone()
    return result['cnt'] > 0


def get_available_periodes(db, direction='all'):
    """Get available periodes from database"""
    cursor = db.cursor()
    cursor.execute("""
        SELECT DISTINCT periode_bulan, periode_tahun
        FROM master_pelanggan
        ORDER BY periode_tahun DESC, periode_bulan DESC
    """)
    
    periodes = cursor.fetchall()
    return [f"{p['periode_bulan']:02d}/{p['periode_tahun']}" for p in periodes]


# Statistics functions - return data summary after upload

def get_mc_stats(db, bulan, tahun):
    """Get MC statistics for periode"""
    cursor = db.cursor()
    
    # Total records
    cursor.execute("""
        SELECT COUNT(*) as total FROM master_pelanggan 
        WHERE periode_bulan = ? AND periode_tahun = ?
    """, (bulan, tahun))
    total = cursor.fetchone()['total']
    
    # By rayon
    cursor.execute("""
        SELECT rayon, COUNT(*) as cnt 
        FROM master_pelanggan 
        WHERE periode_bulan = ? AND periode_tahun = ?
        GROUP BY rayon
        ORDER BY rayon
    """, (bulan, tahun))
    by_rayon = [{'rayon': r['rayon'], 'count': r['cnt']} for r in cursor.fetchall()]
    
    # Sample data (first 10)
    cursor.execute("""
        SELECT nomen, nama, alamat, rayon, target_mc 
        FROM master_pelanggan 
        WHERE periode_bulan = ? AND periode_tahun = ?
        LIMIT 10
    """, (bulan, tahun))
    sample = [dict(row) for row in cursor.fetchall()]
    
    return {
        'total_records': total,
        'by_rayon': by_rayon,
        'sample_data': sample
    }


def get_mb_stats(db, bulan, tahun):
    """Get MB statistics for periode"""
    cursor = db.cursor()
    
    cursor.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(jumlah_bayar) as total_bayar,
            AVG(jumlah_bayar) as avg_bayar
        FROM master_bayar 
        WHERE periode_bulan = ? AND periode_tahun = ?
    """, (bulan, tahun))
    result = cursor.fetchone()
    
    # Sample data
    cursor.execute("""
        SELECT nomen, tgl_bayar, jumlah_bayar 
        FROM master_bayar 
        WHERE periode_bulan = ? AND periode_tahun = ?
        ORDER BY jumlah_bayar DESC
        LIMIT 10
    """, (bulan, tahun))
    sample = [dict(row) for row in cursor.fetchall()]
    
    return {
        'total_records': result['total'],
        'total_bayar': float(result['total_bayar'] or 0),
        'avg_bayar': float(result['avg_bayar'] or 0),
        'sample_data': sample
    }


def get_collection_stats(db, bulan, tahun):
    """Get Collection statistics for periode"""
    cursor = db.cursor()
    
    cursor.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(DISTINCT nomen) as unique_nomen,
            SUM(jumlah_bayar) as total_bayar,
            SUM(volume_air) as total_volume,
            COUNT(CASE WHEN tipe_bayar = 'current' THEN 1 END) as current_count,
            COUNT(CASE WHEN tipe_bayar = 'tunggakan' THEN 1 END) as tunggakan_count
        FROM collection_harian 
        WHERE periode_bulan = ? AND periode_tahun = ?
    """, (bulan, tahun))
    result = cursor.fetchone()
    
    # Check linking to MC
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
    
    # Sample data
    cursor.execute("""
        SELECT c.nomen, c.tgl_bayar, c.jumlah_bayar, c.volume_air, c.tipe_bayar, m.nama
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
        'unique_customers': result['unique_nomen'],
        'total_bayar': float(result['total_bayar'] or 0),
        'total_volume': float(result['total_volume'] or 0),
        'current_payments': result['current_count'],
        'tunggakan_payments': result['tunggakan_count'],
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
    
    # Check linking
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
    
    # Sample data
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
    
    # Check linking
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
    
    # Sample data
    cursor.execute("""
        SELECT s.nomen, s.volume, s.rayon, m.nama
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
    
    # Check linking
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
    
    # Check other periodes
    cursor.execute("""
        SELECT periode_bulan, periode_tahun, COUNT(*) as cnt
        FROM ardebt
        GROUP BY periode_bulan, periode_tahun
        ORDER BY periode_tahun DESC, periode_bulan DESC
    """)
    all_periodes = [{'periode': f"{r['periode_bulan']:02d}/{r['periode_tahun']}", 'count': r['cnt']} 
                    for r in cursor.fetchall()]
    
    # Sample data
    cursor.execute("""
        SELECT a.nomen, a.saldo_tunggakan, a.umur_piutang, m.nama
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


def find_header_row(filepath, max_rows=20):
    """Find header row in Excel file"""
    df = pd.read_excel(filepath, header=None, nrows=max_rows)
    
    for i in range(len(df)):
        row = df.iloc[i]
        row_str = ' '.join([str(val).lower() for val in row if pd.notna(val)])
        
        if any(kw in row_str for kw in ['nomen', 'nama', 'rayon', 'tgl', 'pay', 'freeze', 'cmr', 'periode']):
            return i
    
    return 0


# Processing functions

def process_mc(df, month, year, db):
    """Process MC - MASTER/INDUK"""
    from core.helpers import clean_nomen, parse_zona_novak
    
    cursor = db.cursor()
    rows = 0
    
    # Column mapping
    rename_dict = {}
    
    if 'NAMA_PEL' in df.columns:
        rename_dict['NAMA_PEL'] = 'nama'
    elif 'NAMA' in df.columns:
        rename_dict['NAMA'] = 'nama'
    
    if 'ALM1_PEL' in df.columns:
        rename_dict['ALM1_PEL'] = 'alamat'
    elif 'ALAMAT' in df.columns:
        rename_dict['ALAMAT'] = 'alamat'
    
    if 'TARIF' in df.columns:
        rename_dict['TARIF'] = 'tarif'
    
    if 'NOMINAL' in df.columns:
        rename_dict['NOMINAL'] = 'target_mc'
    elif 'REK_AIR' in df.columns:
        rename_dict['REK_AIR'] = 'target_mc'
    
    if 'KUBIK' in df.columns:
        rename_dict['KUBIK'] = 'kubikasi'
    elif 'KUBIKASI' in df.columns:
        rename_dict['KUBIKASI'] = 'kubikasi'
    
    df = df.rename(columns=rename_dict)
    
    # Clean nomen
    df['nomen'] = df['NOMEN'].apply(clean_nomen)
    df = df.dropna(subset=['nomen'])
    df = df[df['nomen'] != '']
    
    # Parse ZONA_NOVAK
    df['zona_novak'] = df['ZONA_NOVAK'].astype(str).str.strip()
    zona_parsed = df['zona_novak'].apply(parse_zona_novak)
    
    df['rayon'] = zona_parsed.apply(lambda x: x['rayon'])
    df['pc'] = zona_parsed.apply(lambda x: x['pc'])
    df['ez'] = zona_parsed.apply(lambda x: x['ez'])
    df['block'] = zona_parsed.apply(lambda x: x['block'])
    df['pcez'] = zona_parsed.apply(lambda x: x['pcez'])
    
    # Filter Rayon 34/35
    df = df[df['rayon'].isin(['34', '35'])]
    
    if len(df) == 0:
        raise Exception('No data for Rayon 34/35')
    
    # Fill defaults
    for col in ['nama', 'alamat', 'tarif']:
        if col not in df.columns:
            df[col] = ''
    
    if 'target_mc' not in df.columns:
        df['target_mc'] = 0
    
    if 'kubikasi' not in df.columns:
        df['kubikasi'] = 0
    else:
        df['kubikasi'] = pd.to_numeric(df['kubikasi'], errors='coerce').fillna(0).abs()
    
    # Add metadata
    df['periode_bulan'] = month
    df['periode_tahun'] = year
    
    print(f"\n{'='*70}")
    print(f"PROCESSING MC - PERIODE {month:02d}/{year}")
    print(f"{'='*70}")
    print(f"📊 Total records: {len(df):,}")
    
    # Save to database (INSERT OR REPLACE based on nomen + periode)
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT OR REPLACE INTO master_pelanggan 
            (nomen, nama, alamat, rayon, pc, ez, pcez, block, zona_novak, tarif, target_mc, kubikasi, periode_bulan, periode_tahun)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row['nomen'], row.get('nama', ''), row.get('alamat', ''), row['rayon'],
            row['pc'], row['ez'], row['pcez'], row['block'], row['zona_novak'],
            row.get('tarif', ''), row.get('target_mc', 0), row.get('kubikasi', 0),
            month, year
        ))
        rows += 1
    
    print(f"✅ Inserted/Updated {rows:,} records")
    print(f"{'='*70}\n")
    
    return rows


def process_mb(df, month, year, db):
    """Process MB - Master Bayar"""
    from core.helpers import clean_nomen, clean_date
    
    cursor = db.cursor()
    rows = 0
    
    # Column mapping
    col_map = {
        'NO_PLGGN': 'nomen', 'NOPEL': 'nomen',
        'TGL_BAYAR': 'tgl_bayar', 'TANGGAL': 'tgl_bayar',
        'JML_BAYAR': 'jumlah_bayar', 'JUMLAH': 'jumlah_bayar', 'NOMINAL': 'jumlah_bayar'
    }
    
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
    
    # Clean data
    df['nomen'] = df['nomen'].apply(clean_nomen)
    df = df.dropna(subset=['nomen'])
    df = df[df['nomen'] != '']
    
    df['tgl_bayar'] = df['tgl_bayar'].apply(clean_date)
    
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
    
    # Validate: Check how many nomens exist in MC for this periode
    cursor.execute("""
        SELECT COUNT(DISTINCT m.nomen) as mc_count
        FROM master_pelanggan m
        WHERE m.periode_bulan = ? AND m.periode_tahun = ?
    """, (month, year))
    mc_result = cursor.fetchone()
    mc_count = mc_result['mc_count'] if mc_result else 0
    
    if mc_count > 0:
        print(f"🔗 MC available for periode {month:02d}/{year}: {mc_count:,} nomens")
    else:
        print(f"⚠️  WARNING: No MC data for periode {month:02d}/{year}")
    
    # Save to database (INSERT OR REPLACE based on nomen + periode)
    linked = 0
    unlinked = 0
    
    for _, row in df.iterrows():
        # Check if nomen exists in MC for THIS periode
        cursor.execute("""
            SELECT COUNT(*) as cnt FROM master_pelanggan 
            WHERE nomen = ? AND periode_bulan = ? AND periode_tahun = ?
        """, (row['nomen'], month, year))
        
        exists_in_mc = cursor.fetchone()['cnt'] > 0
        
        cursor.execute("""
            INSERT OR REPLACE INTO master_bayar 
            (nomen, tgl_bayar, jumlah_bayar, periode_bulan, periode_tahun, sumber_file)
            VALUES (?, ?, ?, ?, ?, 'mb')
        """, (row['nomen'], row['tgl_bayar'], row['jumlah_bayar'], month, year))
        rows += 1
        
        if exists_in_mc:
            linked += 1
        else:
            unlinked += 1
    
    print(f"✅ Inserted/Updated {rows:,} records")
    print(f"🔗 Linked to MC: {linked:,} records")
    if unlinked > 0:
        print(f"⚠️  Unlinked (no MC): {unlinked:,} records")
    print(f"{'='*70}\n")
    
    return rows


def process_collection(df, month, year, db):
    """Process Collection"""
    from core.helpers import clean_nomen, clean_date
    
    cursor = db.cursor()
    rows = 0
    
    # Column mapping
    col_map = {
        'NO_PLGGN': 'nomen', 'NOPEL': 'nomen',
        'TGL_BAYAR': 'tgl_bayar', 'PAY_DT': 'tgl_bayar',
        'JML_BAYAR': 'jumlah_bayar', 'AMT_COLLECT': 'jumlah_bayar',
        'VOLUME_AIR': 'volume_air', 'VOLUME': 'volume_air',
        'BILL_PERIOD': 'bill_period'
    }
    
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
    
    # Clean data
    df['nomen'] = df['nomen'].apply(clean_nomen)
    df = df.dropna(subset=['nomen'])
    df = df[df['nomen'] != '']
    
    df['tgl_bayar'] = df['tgl_bayar'].apply(clean_date)
    
    if 'jumlah_bayar' not in df.columns:
        df['jumlah_bayar'] = 0
    else:
        df['jumlah_bayar'] = pd.to_numeric(df['jumlah_bayar'], errors='coerce').fillna(0)
    
    if 'volume_air' not in df.columns:
        df['volume_air'] = 0
    else:
        df['volume_air'] = pd.to_numeric(df['volume_air'], errors='coerce').fillna(0)
    
    # Classify payment type
    df['tipe_bayar'] = df.apply(
        lambda row: 'tunggakan' if row['jumlah_bayar'] > 0 and row['volume_air'] == 0 else 'current',
        axis=1
    )
    
    df['periode_bulan'] = month
    df['periode_tahun'] = year
    
    print(f"\n{'='*70}")
    print(f"PROCESSING COLLECTION - PERIODE {month:02d}/{year}")
    print(f"{'='*70}")
    print(f"📊 Total records: {len(df):,}")
    
    # Validate: Check MC availability
    cursor.execute("""
        SELECT COUNT(DISTINCT m.nomen) as mc_count
        FROM master_pelanggan m
        WHERE m.periode_bulan = ? AND m.periode_tahun = ?
    """, (month, year))
    mc_result = cursor.fetchone()
    mc_count = mc_result['mc_count'] if mc_result else 0
    
    if mc_count > 0:
        print(f"🔗 MC available for periode {month:02d}/{year}: {mc_count:,} nomens")
    else:
        print(f"⚠️  WARNING: No MC data for periode {month:02d}/{year}")
    
    # Save to database (INSERT OR REPLACE based on nomen + tgl_bayar + periode)
    linked = 0
    unlinked = 0
    
    for _, row in df.iterrows():
        # Check if nomen exists in MC for THIS periode
        cursor.execute("""
            SELECT COUNT(*) as cnt FROM master_pelanggan 
            WHERE nomen = ? AND periode_bulan = ? AND periode_tahun = ?
        """, (row['nomen'], month, year))
        
        exists_in_mc = cursor.fetchone()['cnt'] > 0
        
        cursor.execute("""
            INSERT OR REPLACE INTO collection_harian 
            (nomen, tgl_bayar, jumlah_bayar, volume_air, tipe_bayar, bill_period, periode_bulan, periode_tahun, sumber_file)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'collection')
        """, (
            row['nomen'], row['tgl_bayar'], row['jumlah_bayar'], row['volume_air'],
            row['tipe_bayar'], row.get('bill_period', ''), month, year
        ))
        rows += 1
        
        if exists_in_mc:
            linked += 1
        else:
            unlinked += 1
    
    print(f"✅ Inserted/Updated {rows:,} records")
    print(f"🔗 Linked to MC: {linked:,} records")
    if unlinked > 0:
        print(f"⚠️  Unlinked (no MC): {unlinked:,} records")
    print(f"{'='*70}\n")
    
    return rows


def process_mainbill(df, month, year, db):
    """Process Mainbill"""
    from core.helpers import clean_nomen, clean_date
    
    cursor = db.cursor()
    rows = 0
    
    # Column mapping
    col_map = {
        'NO_PLGGN': 'nomen', 'NOPEL': 'nomen',
        'TGL_TAGIHAN': 'tgl_tagihan', 'FREEZE_DT': 'tgl_tagihan',
        'TOTAL_TAGIHAN': 'total_tagihan', 'NOMINAL': 'total_tagihan',
        'PCEZBK': 'pcezbk', 'TARIF': 'tarif', 'KODETARIF': 'tarif'
    }
    
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
    
    # Clean data
    df['nomen'] = df['nomen'].apply(clean_nomen)
    df = df.dropna(subset=['nomen'])
    df = df[df['nomen'] != '']
    
    if 'tgl_tagihan' in df.columns:
        df['tgl_tagihan'] = df['tgl_tagihan'].apply(clean_date)
    else:
        df['tgl_tagihan'] = ''
    
    if 'total_tagihan' not in df.columns:
        df['total_tagihan'] = 0
    else:
        df['total_tagihan'] = pd.to_numeric(df['total_tagihan'], errors='coerce').fillna(0)
    
    for col in ['pcezbk', 'tarif']:
        if col not in df.columns:
            df[col] = ''
    
    df['periode_bulan'] = month
    df['periode_tahun'] = year
    
    print(f"\n{'='*70}")
    print(f"PROCESSING MAINBILL - PERIODE {month:02d}/{year}")
    print(f"{'='*70}")
    print(f"📊 Total records: {len(df):,}")
    
    # Validate: Check MC availability
    cursor.execute("""
        SELECT COUNT(DISTINCT m.nomen) as mc_count
        FROM master_pelanggan m
        WHERE m.periode_bulan = ? AND m.periode_tahun = ?
    """, (month, year))
    mc_result = cursor.fetchone()
    mc_count = mc_result['mc_count'] if mc_result else 0
    
    if mc_count > 0:
        print(f"🔗 MC available for periode {month:02d}/{year}: {mc_count:,} nomens")
    else:
        print(f"⚠️  WARNING: No MC data for periode {month:02d}/{year}")
    
    # Save to database (INSERT OR REPLACE based on nomen + periode)
    linked = 0
    unlinked = 0
    
    for _, row in df.iterrows():
        # Check if nomen exists in MC for THIS periode
        cursor.execute("""
            SELECT COUNT(*) as cnt FROM master_pelanggan 
            WHERE nomen = ? AND periode_bulan = ? AND periode_tahun = ?
        """, (row['nomen'], month, year))
        
        exists_in_mc = cursor.fetchone()['cnt'] > 0
        
        cursor.execute("""
            INSERT OR REPLACE INTO mainbill 
            (nomen, tgl_tagihan, total_tagihan, pcezbk, tarif, periode_bulan, periode_tahun)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            row['nomen'], row['tgl_tagihan'], row['total_tagihan'],
            row.get('pcezbk', ''), row.get('tarif', ''), month, year
        ))
        rows += 1
        
        if exists_in_mc:
            linked += 1
        else:
            unlinked += 1
    
    print(f"✅ Inserted/Updated {rows:,} records")
    print(f"🔗 Linked to MC: {linked:,} records")
    if unlinked > 0:
        print(f"⚠️  Unlinked (no MC): {unlinked:,} records")
    print(f"{'='*70}\n")
    
    return rows


def process_sbrs(df, month, year, db):
    """Process SBRS"""
    from core.helpers import clean_nomen
    
    cursor = db.cursor()
    rows = 0
    
    # Column mapping
    col_map = {}
    
    for col in ['CMR_ACCOUNT', 'NOMEN', 'NO_PELANGGAN']:
        if col in df.columns:
            col_map[col] = 'nomen'
            break
    
    for col in ['SB_STAND', 'VOLUME', 'PAKAI']:
        if col in df.columns:
            col_map[col] = 'volume'
            break
    
    for col in ['CMR_NAME', 'NAMA']:
        if col in df.columns:
            col_map[col] = 'nama'
            break
    
    for col in ['CMR_ROUTE', 'RAYON']:
        if col in df.columns:
            col_map[col] = 'rayon'
            break
    
    df = df.rename(columns=col_map)
    
    # Clean data
    df['nomen'] = df['nomen'].apply(clean_nomen)
    df = df.dropna(subset=['nomen'])
    df = df[df['nomen'] != '']
    
    df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0)
    
    # Fill optional columns
    for col in ['nama', 'alamat', 'rayon', 'readmethod', 'skip_status', 'trouble_status']:
        if col not in df.columns:
            df[col] = ''
    
    for col in ['stand_awal', 'stand_akhir']:
        if col not in df.columns:
            df[col] = 0
        else:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    df['periode_bulan'] = month
    df['periode_tahun'] = year
    
    print(f"\n{'='*70}")
    print(f"PROCESSING SBRS - PERIODE {month:02d}/{year}")
    print(f"{'='*70}")
    print(f"📊 Total records: {len(df):,}")
    
    # Validate: Check MC availability
    cursor.execute("""
        SELECT COUNT(DISTINCT m.nomen) as mc_count
        FROM master_pelanggan m
        WHERE m.periode_bulan = ? AND m.periode_tahun = ?
    """, (month, year))
    mc_result = cursor.fetchone()
    mc_count = mc_result['mc_count'] if mc_result else 0
    
    if mc_count > 0:
        print(f"🔗 MC available for periode {month:02d}/{year}: {mc_count:,} nomens")
    else:
        print(f"⚠️  WARNING: No MC data for periode {month:02d}/{year}")
    
    # Save to database (INSERT OR REPLACE based on nomen + periode)
    linked = 0
    unlinked = 0
    
    for _, row in df.iterrows():
        # Check if nomen exists in MC for THIS periode
        cursor.execute("""
            SELECT COUNT(*) as cnt FROM master_pelanggan 
            WHERE nomen = ? AND periode_bulan = ? AND periode_tahun = ?
        """, (row['nomen'], month, year))
        
        exists_in_mc = cursor.fetchone()['cnt'] > 0
        
        cursor.execute("""
            INSERT OR REPLACE INTO sbrs_data 
            (nomen, nama, alamat, rayon, readmethod, skip_status, trouble_status, 
             stand_awal, stand_akhir, volume, periode_bulan, periode_tahun)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row['nomen'], row.get('nama', ''), row.get('alamat', ''), row.get('rayon', ''),
            row.get('readmethod', ''), row.get('skip_status', ''), row.get('trouble_status', ''),
            row.get('stand_awal', 0), row.get('stand_akhir', 0), row['volume'],
            month, year
        ))
        rows += 1
        
        if exists_in_mc:
            linked += 1
        else:
            unlinked += 1
    
    print(f"✅ Inserted/Updated {rows:,} records")
    print(f"🔗 Linked to MC: {linked:,} records")
    if unlinked > 0:
        print(f"⚠️  Unlinked (no MC): {unlinked:,} records")
    print(f"{'='*70}\n")
    
    return rows


def process_ardebt(df, bulan, tahun, db):
    """
    Process Ardebt - PER PERIODE (Replace only same periode)
    
    Logic:
    1. DELETE Ardebt HANYA untuk periode ini (bulan, tahun)
    2. INSERT data baru untuk periode ini
    3. Data periode lain TIDAK terpengaruh (tetap ada)
    
    Example:
    - Existing: Ardebt 06/2025 (1000 records), Ardebt 07/2025 (1200 records)
    - Upload new Ardebt 07/2025 (1500 records)
    - Result: Ardebt 06/2025 tetap 1000 records, Ardebt 07/2025 replaced jadi 1500 records
    """
    from core.helpers import clean_nomen
    
    cursor = db.cursor()
    
    print(f"\n{'='*70}")
    print(f"PROCESSING ARDEBT - PERIODE {bulan:02d}/{tahun}")
    print(f"{'='*70}")
    
    # Step 1: Check existing data for this periode
    cursor.execute("""
        SELECT COUNT(*) as cnt 
        FROM ardebt 
        WHERE periode_bulan = ? AND periode_tahun = ?
    """, (bulan, tahun))
    
    existing_row = cursor.fetchone()
    existing = existing_row['cnt'] if existing_row else 0
    print(f"📊 Existing Ardebt for {bulan:02d}/{tahun}: {existing:,} records")
    
    # Step 2: DELETE only data for this periode
    if existing > 0:
        print(f"🗑️  Deleting old Ardebt data for periode {bulan:02d}/{tahun}...")
        cursor.execute("""
            DELETE FROM ardebt 
            WHERE periode_bulan = ? AND periode_tahun = ?
        """, (bulan, tahun))
        deleted = cursor.rowcount
        print(f"✅ Deleted {deleted:,} records")
    else:
        print(f"ℹ️  No existing data for periode {bulan:02d}/{tahun}")
    
    # Step 3: Check if other periodes exist (should NOT be affected)
    cursor.execute("""
        SELECT periode_bulan, periode_tahun, COUNT(*) as cnt
        FROM ardebt
        GROUP BY periode_bulan, periode_tahun
        ORDER BY periode_tahun DESC, periode_bulan DESC
    """)
    other_periodes = cursor.fetchall()
    
    if other_periodes:
        print(f"\n📂 Other Ardebt periodes (will remain unchanged):")
        for row in other_periodes:
            print(f"   - Periode {row['periode_bulan']:02d}/{row['periode_tahun']}: {row['cnt']:,} records")
    
    # Step 4: Prepare new data
    print(f"\n📥 Preparing new data for periode {bulan:02d}/{tahun}...")
    
    # Column mapping
    col_map = {
        'NO_PLGGN': 'nomen', 
        'NOPEL': 'nomen',
        'NOMEN': 'nomen',
        'SALDO_TUNGGAKAN': 'saldo_tunggakan', 
        'SALDO': 'saldo_tunggakan', 
        'TUNGGAKAN': 'saldo_tunggakan',
        'TOTAL_PIUTANG': 'saldo_tunggakan',
        'PERIODE': 'periode_bill', 
        'PERIODE_BILL': 'periode_bill',
        'UMUR_PIUTANG': 'umur_piutang',
        'UMUR': 'umur_piutang'
    }
    
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
    
    # Validate required columns
    if 'nomen' not in df.columns:
        raise Exception("Ardebt: Column 'nomen' is required")
    
    # Clean nomen
    df['nomen'] = df['nomen'].apply(clean_nomen)
    df = df.dropna(subset=['nomen'])
    df = df[df['nomen'] != '']
    
    # Fill defaults
    if 'saldo_tunggakan' not in df.columns:
        df['saldo_tunggakan'] = 0
    else:
        df['saldo_tunggakan'] = pd.to_numeric(df['saldo_tunggakan'], errors='coerce').fillna(0)
    
    if 'umur_piutang' not in df.columns:
        df['umur_piutang'] = 0
    else:
        df['umur_piutang'] = pd.to_numeric(df['umur_piutang'], errors='coerce').fillna(0)
    
    # Keep original PERIODE_BILL value for reference
    if 'periode_bill' not in df.columns:
        df['periode_bill'] = f"{bulan:02d}/{tahun}"
    
    print(f"📋 Total records to insert: {len(df):,}")
    
    # Validate: Check MC availability
    cursor.execute("""
        SELECT COUNT(DISTINCT m.nomen) as mc_count
        FROM master_pelanggan m
        WHERE m.periode_bulan = ? AND m.periode_tahun = ?
    """, (bulan, tahun))
    mc_result = cursor.fetchone()
    mc_count = mc_result['mc_count'] if mc_result else 0
    
    if mc_count > 0:
        print(f"🔗 MC available for periode {bulan:02d}/{tahun}: {mc_count:,} nomens")
    else:
        print(f"⚠️  WARNING: No MC data for periode {bulan:02d}/{tahun}")
    
    # Step 5: INSERT new data with linking validation
    rows = 0
    errors = 0
    linked = 0
    unlinked = 0
    
    for idx, row in df.iterrows():
        try:
            # Check if nomen exists in MC for THIS periode
            cursor.execute("""
                SELECT COUNT(*) as cnt FROM master_pelanggan 
                WHERE nomen = ? AND periode_bulan = ? AND periode_tahun = ?
            """, (row['nomen'], bulan, tahun))
            
            exists_in_mc = cursor.fetchone()['cnt'] > 0
            
            cursor.execute("""
                INSERT INTO ardebt 
                (nomen, saldo_tunggakan, periode_bulan, periode_tahun, periode_bill, umur_piutang)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                row['nomen'],
                row['saldo_tunggakan'],
                bulan,
                tahun,
                row['periode_bill'],
                row.get('umur_piutang', 0)
            ))
            rows += 1
            
            if exists_in_mc:
                linked += 1
            else:
                unlinked += 1
                
        except Exception as e:
            errors += 1
            if errors <= 5:  # Show first 5 errors only
                print(f"⚠️  Error inserting row {idx}: {e}")
    
    print(f"\n✅ Inserted {rows:,} new records")
    print(f"🔗 Linked to MC: {linked:,} records")
    if unlinked > 0:
        print(f"⚠️  Unlinked (no MC): {unlinked:,} records")
    if errors > 0:
        print(f"⚠️  {errors} errors occurred")
    
    # Step 6: Verify final state
    cursor.execute("""
        SELECT periode_bulan, periode_tahun, COUNT(*) as cnt
        FROM ardebt
        GROUP BY periode_bulan, periode_tahun
        ORDER BY periode_tahun DESC, periode_bulan DESC
    """)
    final_periodes = cursor.fetchall()
    
    print(f"\n📊 Final Ardebt state (all periodes):")
    for row in final_periodes:
        marker = " 👈 NEW" if row['periode_bulan'] == bulan and row['periode_tahun'] == tahun else ""
        print(f"   - Periode {row['periode_bulan']:02d}/{row['periode_tahun']}: {row['cnt']:,} records{marker}")
    
    print(f"{'='*70}\n")
    
    return rows
    for row in final_periodes:
        marker = " 👈 NEW" if row['periode_bulan'] == bulan and row['periode_tahun'] == tahun else ""
        print(f"   - Periode {row['periode_bulan']:02d}/{row['periode_tahun']}: {row['cnt']:,} records{marker}")
    
    print(f"{'='*70}\n")
    
    return rows
