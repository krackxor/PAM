"""
Upload API - FINAL VERSION dengan Periode Logic yang Benar

PERIODE RULES:
1. MC       → TGL_CATAT (19/06/2025) = Periode 06/2025 (data Juni, file keluar Juli)
2. MB       → TGL_BAYAR (04/06/2025) = Periode 06/2025 (data Juni, file keluar Juli)  
3. Collection → PAY_DT (01-07-2025) = Periode 07/2025 (data Juli)
4. Mainbill → FREEZE_DT (12/07/2025) = Periode 07/2025 (data Juli)
5. SBRS     → cmr_rd_date (22072025) = Periode 07/2025 (data Juli)
6. Ardebt   → PERIODE_BILL (kolom periode) = Periode dari kolom

LINKING DATA:
- MC adalah INDUK (master)
- Semua data linked by: NOMEN + PERIODE_BULAN + PERIODE_TAHUN
- Harus ada MC dulu sebelum upload data lain
"""

import os
import pandas as pd
from flask import jsonify, request, current_app
from datetime import datetime
import re

# Column patterns
COLUMN_PATTERNS = {
    'mc': {
        'date_column': ['TGL_CATAT', 'tgl_catat'],
        'periode_from': 'date',  # Periode dari tanggal
        'keywords': ['mc', 'master', 'catat'],
        'required': ['NOMEN', 'NAMA']
    },
    'mb': {
        'date_column': ['TGL_BAYAR', 'tgl_bayar'],
        'periode_from': 'date',
        'keywords': ['mb', 'belum', 'bayar'],
        'required': ['NOMEN', 'TGL_BAYAR']
    },
    'collection': {
        'date_column': ['PAY_DT', 'pay_dt'],
        'periode_from': 'date',
        'keywords': ['collection', 'coll', 'pay'],
        'required': ['NOMEN', 'PAY_DT']
    },
    'mainbill': {
        'date_column': ['FREEZE_DT', 'freeze_dt'],
        'periode_from': 'date',
        'keywords': ['mainbill', 'bill', 'freeze'],
        'required': ['NOMEN', 'FREEZE_DT']
    },
    'sbrs': {
        'date_column': ['cmr_rd_date', 'CMR_RD_DATE'],
        'periode_from': 'date',
        'keywords': ['sbrs', 'sbr', 'cmr'],
        'required': ['RAYON']
    },
    'ardebt': {
        'date_column': ['PERIODE_BILL', 'periode_bill'],  # Hanya untuk info
        'periode_from': 'universal',  # TIDAK ADA PERIODE - Universal
        'keywords': ['ardebt', 'debt', 'piutang', 'ar'],
        'required': ['NOMEN']
    }
}


def detect_file_type(filename, columns):
    """Detect file type - case insensitive"""
    filename_lower = filename.lower()
    columns_lower = [str(col).lower().strip() for col in columns]
    
    print(f"DEBUG: Detecting file type")
    print(f"  Filename: {filename_lower}")
    print(f"  Columns: {columns_lower[:10]}")
    
    for file_type, patterns in COLUMN_PATTERNS.items():
        has_keyword = any(kw in filename_lower for kw in patterns['keywords'])
        
        if has_keyword:
            required_lower = [req.lower() for req in patterns['required']]
            has_columns = all(
                any(req_lower in col_lower for col_lower in columns_lower)
                for req_lower in required_lower
            )
            
            if has_columns:
                print(f"  ✓ Matched: {file_type}")
                return file_type
    
    # Fallback: check columns only
    for file_type, patterns in COLUMN_PATTERNS.items():
        required_lower = [req.lower() for req in patterns['required']]
        has_columns = all(
            any(req_lower in col_lower for col_lower in columns_lower)
            for req_lower in required_lower
        )
        
        if has_columns:
            print(f"  ✓ Matched by columns: {file_type}")
            return file_type
    
    return None


def extract_periode_from_data(df, file_type):
    """
    Extract periode sesuai business rules
    
    Returns: (month, year, method)
    
    SPECIAL: Ardebt tidak punya periode (universal)
    """
    if file_type not in COLUMN_PATTERNS:
        return None, None, 'unknown'
    
    patterns = COLUMN_PATTERNS[file_type]
    
    # ARDEBT: Universal (no periode) - return current month/year for display only
    if patterns['periode_from'] == 'universal':
        now = datetime.now()
        return now.month, now.year, 'universal_no_periode'
    
    # For Ardebt: periode from column (OLD - not used anymore)
    if patterns['periode_from'] == 'column':
        periode_col = None
        for col in patterns['date_column']:
            if col in df.columns:
                periode_col = col
                break
        
        if periode_col:
            try:
                # PERIODE_BILL format: "062025" atau "06/2025"
                periode_val = str(df[periode_col].iloc[0])
                
                # Try parse: 062025
                match = re.search(r'(\d{2})(\d{4})', periode_val)
                if match:
                    month = int(match.group(1))
                    year = int(match.group(2))
                    return month, year, 'periode_column'
                
                # Try parse: 06/2025
                match = re.search(r'(\d{2})/(\d{4})', periode_val)
                if match:
                    month = int(match.group(1))
                    year = int(match.group(2))
                    return month, year, 'periode_column'
                    
            except Exception as e:
                print(f"Error parsing periode column: {e}")
    
    # For others: periode from date
    date_columns = patterns['date_column']
    date_col = None
    for col in date_columns:
        if col in df.columns:
            date_col = col
            break
    
    if not date_col:
        return None, None, 'no_date_column'
    
    try:
        # Parse dates
        dates = pd.to_datetime(df[date_col], errors='coerce', infer_datetime_format=True)
        dates = dates.dropna()
        
        if len(dates) == 0:
            return None, None, 'no_valid_dates'
        
        # Get first date
        first_date = dates.iloc[0]
        month = first_date.month
        year = first_date.year
        
        return month, year, 'from_date'
        
    except Exception as e:
        print(f"Error parsing date: {e}")
        return None, None, 'parse_error'


def validate_mc_exists(db, periode_bulan, periode_tahun):
    """
    Validate bahwa MC (master) sudah ada untuk periode ini
    Harus upload MC dulu sebelum upload data lain
    """
    cursor = db.cursor()
    cursor.execute("""
        SELECT COUNT(*) as cnt 
        FROM master_pelanggan 
        WHERE periode_bulan = ? AND periode_tahun = ?
    """, (periode_bulan, periode_tahun))
    
    result = cursor.fetchone()
    return result['cnt'] > 0


def get_available_periodes(db, direction='all'):
    """
    Get available periode di database
    direction: 'previous', 'next', 'all'
    """
    cursor = db.cursor()
    cursor.execute("""
        SELECT DISTINCT periode_bulan, periode_tahun
        FROM master_pelanggan
        ORDER BY periode_tahun DESC, periode_bulan DESC
    """)
    
    periodes = cursor.fetchall()
    return [f"{p['periode_bulan']:02d}/{p['periode_tahun']}" for p in periodes]


def find_header_row(filepath, max_rows=20):
    """Find header row"""
    df = pd.read_excel(filepath, header=None, nrows=max_rows)
    
    for i in range(len(df)):
        row = df.iloc[i]
        row_str = ' '.join([str(val).lower() for val in row if pd.notna(val)])
        
        if any(kw in row_str for kw in ['nomen', 'nama', 'rayon', 'tgl', 'pay', 'freeze', 'cmr', 'periode']):
            return i
    
    return 0


def register_upload_routes(app, get_db):
    """Register upload routes"""
    
    # Max 10GB
    app.config['MAX_CONTENT_LENGTH'] = 10 * 1024 * 1024 * 1024
    
    @app.route('/api/upload/analyze', methods=['POST'])
    def analyze_file():
        """Analyze file for auto-detection"""
        try:
            print("="*70)
            print("ANALYZE FILE")
            print("="*70)
            
            if 'file' not in request.files:
                return jsonify({'error': 'No file uploaded'}), 400
            
            file = request.files['file']
            if file.filename == '':
                return jsonify({'error': 'No file selected'}), 400
            
            filename = file.filename
            temp_path = os.path.join('/tmp', f"analyze_{datetime.now().timestamp()}_{filename}")
            file.save(temp_path)
            
            print(f"File: {filename}")
            print(f"Size: {os.path.getsize(temp_path)} bytes")
            
            # Find header
            header_row = find_header_row(temp_path)
            print(f"Header row: {header_row}")
            
            # Read Excel
            df = pd.read_excel(temp_path, header=header_row)
            columns = [str(col).strip() for col in df.columns]
            print(f"Columns: {columns[:10]}")
            
            # Detect type
            file_type = detect_file_type(filename, columns)
            if not file_type:
                return jsonify({
                    'error': 'Cannot detect file type',
                    'debug': {'columns': columns}
                }), 400
            
            print(f"Type: {file_type}")
            
            # Extract periode
            month, year, method = extract_periode_from_data(df, file_type)
            if not month or not year:
                return jsonify({
                    'error': f'Cannot extract periode (method: {method})',
                    'debug': {'expected_columns': COLUMN_PATTERNS[file_type]['date_column']}
                }), 400
            
            print(f"Periode: {month:02d}/{year} (method: {method})")
            
            # Find date column
            date_col = None
            for col in COLUMN_PATTERNS[file_type]['date_column']:
                if col in columns:
                    date_col = col
                    break
            
            # Validate MC exists (kecuali upload MC sendiri atau Ardebt)
            db = get_db()
            warning = None
            
            if file_type not in ['mc', 'ardebt']:
                mc_exists = validate_mc_exists(db, month, year)
                if not mc_exists:
                    warning = f"⚠️ MC belum ada untuk periode {month:02d}/{year}. Upload MC dulu!"
            
            # Special message for Ardebt
            if file_type == 'ardebt':
                warning = "ℹ️ Ardebt adalah data universal (tanpa periode). Upload baru akan replace semua data lama."
            
            # Get available periodes
            available_periodes = get_available_periodes(db)
            
            # Clean up
            if os.path.exists(temp_path):
                os.remove(temp_path)
            
            print("✓ Analysis complete")
            print("="*70)
            
            return jsonify({
                'success': True,
                'detected': {
                    'file_type': file_type,
                    'month': month,
                    'year': year,
                    'date_column': date_col,
                    'columns': columns,
                    'total_rows': len(df),
                    'warning': warning,
                    'available_periodes': available_periodes,
                    'detection_method': method
                }
            })
            
        except Exception as e:
            import traceback
            print("ERROR:", traceback.format_exc())
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/upload', methods=['POST'])
    def upload_file():
        """Upload and process file"""
        try:
            if 'file' not in request.files:
                return jsonify({'error': 'No file'}), 400
            
            file = request.files['file']
            if file.filename == '':
                return jsonify({'error': 'No file selected'}), 400
            
            file_type = request.form.get('file_type')
            bulan = request.form.get('bulan', type=int)
            tahun = request.form.get('tahun', type=int)
            
            if not all([file_type, bulan, tahun]):
                return jsonify({'error': 'Missing parameters'}), 400
            
            # Save file
            filename = file.filename
            upload_folder = current_app.config.get('UPLOAD_FOLDER', 'uploads')
            os.makedirs(upload_folder, exist_ok=True)
            
            filepath = os.path.join(upload_folder, filename)
            file.save(filepath)
            
            print(f"\n{'='*70}")
            print(f"UPLOAD: {filename}")
            print(f"Type: {file_type}")
            print(f"Periode: {bulan:02d}/{tahun}")
            print(f"{'='*70}")
            
            # Validate MC exists (kecuali upload MC atau Ardebt)
            db = get_db()
            
            if file_type not in ['mc', 'ardebt']:
                if not validate_mc_exists(db, bulan, tahun):
                    return jsonify({
                        'error': f'MC belum ada untuk periode {bulan:02d}/{tahun}. Upload MC dulu!'
                    }), 400
            
            # Find header & read
            header_row = find_header_row(filepath)
            df = pd.read_excel(filepath, header=header_row)
            
            # Process
            if file_type == 'mc':
                rows = process_mc(df, bulan, tahun, db)
            elif file_type == 'mb':
                rows = process_mb(df, bulan, tahun, db)
            elif file_type == 'collection':
                rows = process_collection(df, bulan, tahun, db)
            elif file_type == 'mainbill':
                rows = process_mainbill(df, bulan, tahun, db)
            elif file_type == 'sbrs':
                rows = process_sbrs(df, bulan, tahun, db)
            elif file_type == 'ardebt':
                # Ardebt: DELETE ALL old data first, then insert new
                rows = process_ardebt(df, bulan, tahun, db)  # bulan/tahun just for logging
            else:
                return jsonify({'error': f'Unknown type: {file_type}'}), 400
            
            db.commit()
            
            print(f"✓ Processed: {rows} rows")
            print("="*70)
            
            return jsonify({
                'success': True,
                'filename': filename,
                'file_type': file_type,
                'periode': f"{bulan:02d}/{tahun}",
                'rows_processed': rows
            })
            
        except Exception as e:
            import traceback
            print("ERROR:", traceback.format_exc())
            return jsonify({'error': str(e)}), 500
    
    print("✅ Upload routes registered (FINAL VERSION)")


# Processing functions
def process_mc(df, month, year, db):
    """Process MC - MASTER/INDUK"""
    cursor = db.cursor()
    rows = 0
    
    for _, row in df.iterrows():
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO master_pelanggan 
                (nomen, nama, alamat, rayon, target_mc, pcez, periode_bulan, periode_tahun)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row.get('NOMEN'),
                row.get('NAMA'),
                row.get('ALAMAT'),
                row.get('RAYON'),
                row.get('TARGET_MC', 0),
                row.get('PCEZ'),
                month,
                year
            ))
            rows += 1
        except Exception as e:
            print(f"Error row {rows}: {e}")
    
    return rows


def process_mb(df, month, year, db):
    cursor = db.cursor()
    rows = 0
    
    for _, row in df.iterrows():
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO belum_bayar 
                (nomen, nama, total_tagihan, periode_bulan, periode_tahun)
                VALUES (?, ?, ?, ?, ?)
            """, (
                row.get('NOMEN'),
                row.get('NAMA'),
                row.get('TOTAL_TAGIHAN', 0),
                month,
                year
            ))
            rows += 1
        except:
            pass
    
    return rows


def process_collection(df, month, year, db):
    cursor = db.cursor()
    rows = 0
    
    for _, row in df.iterrows():
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO collection_harian 
                (nomen, pay_dt, volume, current, tunggakan, total, periode_bulan, periode_tahun)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row.get('NOMEN'),
                row.get('PAY_DT'),
                row.get('VOLUME', 0),
                row.get('CURRENT', 0),
                row.get('TUNGGAKAN', 0),
                row.get('TOTAL', 0),
                month,
                year
            ))
            rows += 1
        except:
            pass
    
    return rows


def process_mainbill(df, month, year, db):
    cursor = db.cursor()
    rows = 0
    
    for _, row in df.iterrows():
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO mainbill 
                (nomen, freeze_dt, tagihan, periode_bulan, periode_tahun)
                VALUES (?, ?, ?, ?, ?)
            """, (
                row.get('NOMEN'),
                row.get('FREEZE_DT'),
                row.get('TAGIHAN', 0),
                month,
                year
            ))
            rows += 1
        except:
            pass
    
    return rows


def process_sbrs(df, month, year, db):
    cursor = db.cursor()
    rows = 0
    
    for _, row in df.iterrows():
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO sbrs 
                (rayon, total_pelanggan, sudah_bayar, belum_bayar, periode_bulan, periode_tahun)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                row.get('RAYON'),
                row.get('TOTAL_PELANGGAN', 0),
                row.get('SUDAH_BAYAR', 0),
                row.get('BELUM_BAYAR', 0),
                month,
                year
            ))
            rows += 1
        except:
            pass
    
    return rows


def process_ardebt(df, month, year, db):
    """
    Process Ardebt - UNIVERSAL (no periode)
    DELETE ALL old data, then INSERT new data
    """
    cursor = db.cursor()
    
    # DELETE ALL old data first
    print("Ardebt: Deleting all old data...")
    cursor.execute("DELETE FROM ardebt")
    deleted = cursor.rowcount
    print(f"Ardebt: Deleted {deleted} old rows")
    
    # INSERT new data
    rows = 0
    for _, row in df.iterrows():
        try:
            # Ardebt has PERIODE_BILL column for reference
            periode_bill = row.get('PERIODE_BILL', '')
            
            cursor.execute("""
                INSERT INTO ardebt 
                (nomen, nama, total_piutang, periode_bill, umur_piutang)
                VALUES (?, ?, ?, ?, ?)
            """, (
                row.get('NOMEN'),
                row.get('NAMA'),
                row.get('TOTAL_PIUTANG', 0),
                periode_bill,
                row.get('UMUR_PIUTANG', 0)
            ))
            rows += 1
        except Exception as e:
            print(f"Error inserting Ardebt row: {e}")
    
    print(f"Ardebt: Inserted {rows} new rows")
    return rows
