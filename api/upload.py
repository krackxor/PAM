"""
Upload API - UPDATED dengan business rules yang benar
UPDATE file yang sudah ada: api/upload.py

Aturan Periode:
- MC: TGL_CATAT → periode dari tanggal (data bulan N keluar bulan N+1)
- MB: TGL_BAYAR → periode dari tanggal (data bulan N keluar bulan N+1)
- Collection: PAY_DT → periode dari tanggal
- Mainbill: FREEZE_DT → periode dari tanggal
- SBRS: cmr_rd_date → periode dari tanggal
- Ardebt: TGL_CATAT → periode dari tanggal (data bulan N keluar bulan N+1)
"""

import os
import pandas as pd
from flask import jsonify, request, current_app
from werkzeug.utils import secure_filename
from datetime import datetime
import re

# Column patterns untuk detection
COLUMN_PATTERNS = {
    'mc': {
        'date_column': ['TGL_CATAT', 'tgl_catat', 'TANGGAL_CATAT'],
        'keywords': ['mc', 'master', 'catat'],
        'required': ['NOMEN', 'NAMA']
    },
    'mb': {
        'date_column': ['TGL_BAYAR', 'tgl_bayar', 'TANGGAL_BAYAR'],
        'keywords': ['mb', 'belum', 'bayar'],
        'required': ['NOMEN', 'TGL_BAYAR']
    },
    'collection': {
        'date_column': ['PAY_DT', 'pay_dt', 'TANGGAL_BAYAR'],
        'keywords': ['collection', 'coll', 'pay'],
        'required': ['NOMEN', 'PAY_DT']
    },
    'mainbill': {
        'date_column': ['FREEZE_DT', 'freeze_dt', 'TANGGAL_FREEZE'],
        'keywords': ['mainbill', 'bill', 'freeze'],
        'required': ['NOMEN', 'FREEZE_DT']
    },
    'sbrs': {
        'date_column': ['cmr_rd_date', 'CMR_RD_DATE', 'READ_DATE'],
        'keywords': ['sbrs', 'sbr', 'cmr'],
        'required': ['RAYON']
    },
    'ardebt': {
        'date_column': ['TGL_CATAT', 'tgl_catat'],
        'keywords': ['ardebt', 'debt', 'piutang'],
        'required': ['NOMEN', 'TOTAL_PIUTANG']
    }
}


def detect_file_type(filename, columns):
    """Detect file type dari filename dan columns"""
    filename_lower = filename.lower()
    
    for file_type, patterns in COLUMN_PATTERNS.items():
        # Check filename
        if any(kw in filename_lower for kw in patterns['keywords']):
            # Verify columns
            if all(any(col in columns for col in [req]) for req in patterns['required']):
                return file_type
    
    return None


def extract_periode_from_data(df, file_type):
    """
    Extract periode dari date column
    PENTING: Periode adalah dari tanggal di data, bukan dari filename!
    """
    if file_type not in COLUMN_PATTERNS:
        return None, None
    
    date_columns = COLUMN_PATTERNS[file_type]['date_column']
    
    # Find date column
    date_col = None
    for col in date_columns:
        if col in df.columns:
            date_col = col
            break
    
    if not date_col:
        return None, None
    
    try:
        # Parse dates - support multiple formats
        dates = pd.to_datetime(df[date_col], errors='coerce', infer_datetime_format=True)
        dates = dates.dropna()
        
        if len(dates) == 0:
            return None, None
        
        # Get first date as reference
        first_date = dates.iloc[0]
        return first_date.month, first_date.year
        
    except Exception as e:
        print(f"Error parsing date: {e}")
        return None, None


def find_header_row(filepath, max_rows=20):
    """Find actual header row in Excel"""
    df = pd.read_excel(filepath, header=None, nrows=max_rows)
    
    for i in range(len(df)):
        row = df.iloc[i]
        row_str = ' '.join([str(val).lower() for val in row if pd.notna(val)])
        
        if any(keyword in row_str for keyword in ['nomen', 'nama', 'rayon', 'tgl', 'pay', 'freeze', 'cmr']):
            return i
    
    return 0


def register_upload_routes(app, get_db):
    """Register upload routes - UPDATED VERSION"""
    
    @app.route('/api/upload/analyze', methods=['POST'])
    def analyze_file():
        """
        Analyze file untuk auto-detection
        Return detection result without processing
        """
        try:
            if 'file' not in request.files:
                return jsonify({'error': 'No file uploaded'}), 400
            
            file = request.files['file']
            if file.filename == '':
                return jsonify({'error': 'No file selected'}), 400
            
            # Save temp
            filename = secure_filename(file.filename)
            temp_path = os.path.join('/tmp', filename)
            file.save(temp_path)
            
            # Find header
            header_row = find_header_row(temp_path)
            
            # Read Excel
            df = pd.read_excel(temp_path, header=header_row)
            columns = list(df.columns)
            
            # Detect file type
            file_type = detect_file_type(filename, columns)
            
            if not file_type:
                return jsonify({'error': 'Cannot detect file type'}), 400
            
            # Extract periode
            month, year = extract_periode_from_data(df, file_type)
            
            if not month or not year:
                return jsonify({'error': 'Cannot extract periode from date column'}), 400
            
            # Find date column
            date_col = None
            for col in COLUMN_PATTERNS[file_type]['date_column']:
                if col in columns:
                    date_col = col
                    break
            
            # Validate periode
            current = datetime.now()
            warning = None
            
            if file_type in ['mc', 'mb', 'ardebt']:
                # Expected: data bulan lalu
                expected_month = current.month - 1
                if expected_month == 0:
                    expected_month = 12
                
                if month != expected_month:
                    warning = f"⚠️ {file_type.upper()}: Expected periode {expected_month:02d}, got {month:02d}"
            
            # Clean up temp file
            if os.path.exists(temp_path):
                os.remove(temp_path)
            
            return jsonify({
                'success': True,
                'detected': {
                    'file_type': file_type,
                    'month': month,
                    'year': year,
                    'date_column': date_col,
                    'columns': columns,
                    'total_rows': len(df),
                    'warning': warning
                }
            })
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/upload', methods=['POST'])
    def upload_file():
        """
        Upload and process file
        UPDATED: Gunakan detection yang benar
        """
        try:
            if 'file' not in request.files:
                return jsonify({'error': 'No file part'}), 400
            
            file = request.files['file']
            if file.filename == '':
                return jsonify({'error': 'No selected file'}), 400
            
            # Get parameters
            file_type = request.form.get('file_type')
            bulan = request.form.get('bulan', type=int)
            tahun = request.form.get('tahun', type=int)
            
            # Save file
            filename = secure_filename(file.filename)
            upload_folder = current_app.config.get('UPLOAD_FOLDER', 'uploads')
            
            if not os.path.exists(upload_folder):
                os.makedirs(upload_folder)
            
            filepath = os.path.join(upload_folder, filename)
            file.save(filepath)
            
            print(f"\n{'='*70}")
            print(f"📁 Processing: {filename}")
            print(f"📊 Type: {file_type}")
            print(f"📅 Periode: {bulan}/{tahun}")
            print(f"{'='*70}")
            
            # Find header
            header_row = find_header_row(filepath)
            
            # Read Excel
            df = pd.read_excel(filepath, header=header_row)
            
            # Get database
            db = get_db()
            
            # Process based on file type
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
                rows = process_ardebt(df, bulan, tahun, db)
            else:
                return jsonify({'error': f'Unknown file type: {file_type}'}), 400
            
            db.commit()
            
            print(f"✅ Success: {rows} rows processed")
            
            return jsonify({
                'success': True,
                'filename': filename,
                'file_type': file_type,
                'periode': f"{bulan}/{tahun}",
                'rows_processed': rows
            })
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            return jsonify({'error': str(e)}), 500
    
    print("✅ Upload routes registered (UPDATED)")


# Processing functions
def process_mc(df, month, year, db):
    """Process MC"""
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
    """Process MB"""
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
        except Exception as e:
            print(f"Error row {rows}: {e}")
    
    return rows


def process_collection(df, month, year, db):
    """Process Collection"""
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
        except Exception as e:
            print(f"Error row {rows}: {e}")
    
    return rows


def process_mainbill(df, month, year, db):
    """Process Mainbill"""
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
        except Exception as e:
            print(f"Error row {rows}: {e}")
    
    return rows


def process_sbrs(df, month, year, db):
    """Process SBRS"""
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
        except Exception as e:
            print(f"Error row {rows}: {e}")
    
    return rows


def process_ardebt(df, month, year, db):
    """Process Ardebt"""
    cursor = db.cursor()
    rows = 0
    
    for _, row in df.iterrows():
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO ardebt 
                (nomen, nama, total_piutang, periode_bulan, periode_tahun)
                VALUES (?, ?, ?, ?, ?)
            """, (
                row.get('NOMEN'),
                row.get('NAMA'),
                row.get('TOTAL_PIUTANG', 0),
                month,
                year
            ))
            rows += 1
        except Exception as e:
            print(f"Error row {rows}: {e}")
    
    return rows
