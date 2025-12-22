import os
import sqlite3
import pandas as pd
from flask import Flask, render_template, g, request, redirect, url_for, flash, jsonify
from werkzeug.utils import secure_filename
from datetime import datetime
import traceback

# ==========================================
# KONFIGURASI & DATABASE INIT
# ==========================================
app = Flask(__name__)
app.config['SECRET_KEY'] = 'SUNTER-ADMIN-MODE'
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024 

DB_FOLDER = os.path.join(os.getcwd(), 'database')
DB_PATH = os.path.join(DB_FOLDER, 'sunter.db')

for folder in [app.config['UPLOAD_FOLDER'], DB_FOLDER]:
    if not os.path.exists(folder): 
        os.makedirs(folder)

# Import auto-detect periode
try:
    from auto_detect_periode import auto_detect_periode, detect_periode_from_content
    AUTO_DETECT_AVAILABLE = True
except ImportError:
    AUTO_DETECT_AVAILABLE = False

def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row 
    return db

@app.teardown_appcontext
def close_db(exception):
    db = getattr(g, '_database', None)
    if db is not None: 
        db.close()

# ==========================================
# HELPER FUNCTIONS
# ==========================================
def clean_nomen(val):
    """Membersihkan format Nomen"""
    try:
        if pd.isna(val): return None
        return str(int(float(str(val))))
    except:
        return str(val).strip()

def parse_zona_novak(zona):
    """Parse ZONA_NOVAK: 350960217 -> rayon:35, pc:096, ez:02, block:17"""
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
# CORE PROCESS FUNCTIONS (ETL LOGIC)
# ==========================================

def process_mc_file(filepath, upload_id, periode_bulan, periode_tahun, db):
    try:
        df = pd.read_excel(filepath) if filepath.endswith(('.xls', '.xlsx')) else pd.read_csv(filepath)
        df.columns = df.columns.str.upper().str.strip()
        
        # Validasi Kolom Minimal
        if 'ZONA_NOVAK' not in df.columns or 'NOMEN' not in df.columns:
            raise Exception('MC: Need ZONA_NOVAK and NOMEN columns!')

        # Pemetaan Kolom
        rename_dict = {'NOMEN': 'nomen', 'NAMA_PEL': 'nama', 'ALM1_PEL': 'alamat', 'TARIF': 'tarif'}
        df = df.rename(columns={k: v for k, v in rename_dict.items() if k in df.columns})
        
        # Cleaning
        df['nomen'] = df['nomen'].apply(clean_nomen)
        df = df.dropna(subset=['nomen'])
        
        # Parsing Zona
        zona_parsed = df['ZONA_NOVAK'].apply(parse_zona_novak)
        df['rayon'] = zona_parsed.apply(lambda x: x['rayon'])
        df['pc'] = zona_parsed.apply(lambda x: x['pc'])
        df['ez'] = zona_parsed.apply(lambda x: x['ez'])
        df['pcez'] = zona_parsed.apply(lambda x: x['pcez'])
        df['block'] = zona_parsed.apply(lambda x: x['block'])
        df['zona_novak'] = df['ZONA_NOVAK']

        # Metadata
        df['periode_bulan'] = periode_bulan
        df['periode_tahun'] = periode_tahun
        df['upload_id'] = upload_id
        df['periode'] = f"{periode_tahun}-{str(periode_bulan).zfill(2)}"
        
        # Simpan
        db.execute('DELETE FROM master_pelanggan WHERE periode_bulan = ? AND periode_tahun = ?', (periode_bulan, periode_tahun))
        cols_db = ['nomen', 'nama', 'alamat', 'rayon', 'pc', 'ez', 'pcez', 'block', 'zona_novak', 'tarif', 'periode_bulan', 'periode_tahun', 'upload_id', 'periode']
        df[cols_db].to_sql('master_pelanggan', db, if_exists='append', index=False)
        return len(df)
    except Exception as e:
        traceback.print_exc()
        raise e

def process_sbrs_file(filepath, upload_id, periode_bulan, periode_tahun, db):
    try:
        df = pd.read_excel(filepath) if filepath.endswith(('.xls', '.xlsx')) else pd.read_csv(filepath)
        df.columns = df.columns.str.upper().str.strip()
        
        # Mapping kolom SBRS (Account -> Nomen)
        mapping = {'ACCOUNT': 'nomen', 'CMR_ACCOUNT': 'nomen', 'SB_STAND': 'volume', 'STAND_INI': 'stand_akhir'}
        df = df.rename(columns={k: v for k, v in mapping.items() if k in df.columns})
        
        df['nomen'] = df['nomen'].apply(clean_nomen)
        df['periode_bulan'] = periode_bulan
        df['periode_tahun'] = periode_tahun
        df['upload_id'] = upload_id
        
        db.execute('DELETE FROM sbrs_data WHERE periode_bulan = ? AND periode_tahun = ?', (periode_bulan, periode_tahun))
        df.to_sql('sbrs_data', db, if_exists='append', index=False)
        return len(df)
    except Exception as e:
        raise e

def process_collection_file(filepath, upload_id, periode_bulan, periode_tahun, db):
    try:
        df = pd.read_excel(filepath) if filepath.endswith(('.xls', '.xlsx')) else pd.read_csv(filepath)
        df.columns = df.columns.str.upper().str.strip()
        
        mapping = {'NOMEN': 'nomen', 'TGL_BAYAR': 'tgl_bayar', 'AMT_COLLECT': 'jumlah_bayar'}
        df = df.rename(columns={k: v for k, v in mapping.items() if k in df.columns})
        
        df['nomen'] = df['nomen'].apply(clean_nomen)
        df['periode_bulan'] = periode_bulan
        df['periode_tahun'] = periode_tahun
        df['upload_id'] = upload_id
        df['sumber_file'] = 'COLLECTION'
        
        db.execute("DELETE FROM collection_harian WHERE periode_bulan = ? AND periode_tahun = ? AND sumber_file = 'COLLECTION'", (periode_bulan, periode_tahun))
        df.to_sql('collection_harian', db, if_exists='append', index=False)
        return len(df)
    except Exception as e:
        raise e

# ==========================================
# ROUTING
# ==========================================

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload_multi', methods=['POST'])
def upload_multi():
    if not AUTO_DETECT_AVAILABLE:
        return jsonify({'error': 'Auto-detect module not available'}), 500
    
    files = request.files.getlist('files[]')
    results = []
    db = get_db()
    
    for file in files:
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        
        try:
            detection = auto_detect_periode(filepath, filename)
            if not detection: continue
            
            file_type = detection['file_type']
            bulan, tahun = detection['periode_bulan'], detection['periode_tahun']
            
            # Simpan Metadata
            cursor = db.execute('''
                INSERT INTO upload_metadata (file_type, file_name, periode_bulan, periode_tahun, row_count)
                VALUES (?, ?, ?, ?, 0)
            ''', (file_type, filename, bulan, tahun))
            upload_id = cursor.lastrowid
            
            # Proses berdasarkan tipe
            row_count = 0
            if file_type == 'MC':
                row_count = process_mc_file(filepath, upload_id, bulan, tahun, db)
            elif file_type == 'SBRS':
                row_count = process_sbrs_file(filepath, upload_id, bulan, tahun, db)
            elif file_type == 'COLLECTION':
                row_count = process_collection_file(filepath, upload_id, bulan, tahun, db)
            
            db.execute('UPDATE upload_metadata SET row_count = ? WHERE id = ?', (row_count, upload_id))
            db.commit()
            
            results.append({'filename': filename, 'status': 'success', 'row_count': row_count})
        except Exception as e:
            db.rollback()
            results.append({'filename': filename, 'status': 'error', 'message': str(e)})
            
    return jsonify({'results': results})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
