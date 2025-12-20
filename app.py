import os
import sqlite3
import pandas as pd
from flask import Flask, render_template, g, request, redirect, url_for, flash, jsonify
from werkzeug.utils import secure_filename

# --- KONFIGURASI ---
app = Flask(__name__)
app.config['SECRET_KEY'] = 'SUNTER-ADMIN-MODE'
app.config['UPLOAD_FOLDER'] = 'uploads'
DB_FOLDER = os.path.join(os.getcwd(), 'database')
DB_PATH = os.path.join(DB_FOLDER, 'sunter.db')

# Buat folder otomatis
for folder in [app.config['UPLOAD_FOLDER'], DB_FOLDER]:
    if not os.path.exists(folder):
        os.makedirs(folder)

# --- DATABASE ENGINE ---
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

def init_db():
    with app.app_context():
        db = get_db()
        cursor = db.cursor()
        
        # 1. Master Pelanggan
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS master_pelanggan (
                nomen TEXT PRIMARY KEY, nama TEXT, rayon TEXT, tarif TEXT, target_mc REAL DEFAULT 0
            )
        ''')
        
        # 2. Collection Harian
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS collection_harian (
                id INTEGER PRIMARY KEY AUTOINCREMENT, nomen TEXT, tgl_bayar TEXT, 
                jumlah_bayar REAL, sumber_file TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # 3. Analisa Manual
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS analisa_manual (
                id INTEGER PRIMARY KEY AUTOINCREMENT, nomen TEXT, jenis_anomali TEXT, 
                analisa_tim TEXT, kesimpulan TEXT, rekomendasi TEXT, status TEXT DEFAULT 'Open',
                user_editor TEXT, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        db.commit()
        print("✅ Database Terinisialisasi.")

# --- ROUTING UTAMA ---
@app.route('/')
def index():
    db = get_db()
    kpi = {}
    try:
        kpi['cust'] = db.execute('SELECT COUNT(*) as t FROM master_pelanggan').fetchone()['t']
        kpi['coll'] = db.execute('SELECT SUM(jumlah_bayar) as t FROM collection_harian').fetchone()['t'] or 0
        kpi['anomali'] = db.execute('SELECT COUNT(*) as t FROM analisa_manual WHERE status != "Closed"').fetchone()['t']
    except:
        kpi = {'cust': 0, 'coll': 0, 'anomali': 0}
    
    return render_template('index.html', kpi=kpi)

@app.route('/api/collection_data')
def api_collection():
    db = get_db()
    query = '''
        SELECT c.tgl_bayar, m.rayon, m.nomen, m.nama, m.target_mc, c.jumlah_bayar 
        FROM collection_harian c
        LEFT JOIN master_pelanggan m ON c.nomen = m.nomen
        ORDER BY c.tgl_bayar DESC LIMIT 1000
    '''
    rows = db.execute(query).fetchall()
    data = [dict(row) for row in rows]
    return jsonify(data)

@app.route('/simpan_analisa', methods=['POST'])
def simpan_analisa():
    try:
        db = get_db()
        db.execute('''
            INSERT INTO analisa_manual (nomen, jenis_anomali, analisa_tim, kesimpulan, rekomendasi, status, user_editor)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            request.form['nomen'], request.form['jenis_anomali'], request.form['analisa_tim'],
            request.form['kesimpulan'], request.form['rekomendasi'], request.form['status'], "Admin System"
        ))
        db.commit()
        return jsonify({'status': 'success', 'msg': 'Data Analisa Tersimpan!'})
    except Exception as e:
        return jsonify({'status': 'error', 'msg': str(e)})

# --- FITUR UPLOAD (YANG DIPERBAIKI) ---
@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files: return redirect(url_for('index'))
    file = request.files['file']
    tipe = request.form.get('tipe_upload')
    
    if file:
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(file.filename))
        file.save(filepath)
        
        try:
            conn = get_db()
            
            # 1. DETEKSI FORMAT (CSV atau EXCEL)
            if filepath.endswith('.csv'):
                # Coba baca CSV (separator koma atau titik koma)
                try:
                    df = pd.read_csv(filepath, sep=',')
                    # Cek kalau header ngumpul jadi 1 kolom (tanda sep salah), coba pakai ;
                    if len(df.columns) < 2:
                        df = pd.read_csv(filepath, sep=';')
                except:
                    df = pd.read_csv(filepath, sep=';')
            else:
                # Baca Excel biasa
                df = pd.read_excel(filepath)

            # 2. BERSIHKAN HEADER (Jadikan Huruf Besar Semua & Hapus Spasi)
            df.columns = df.columns.str.upper().str.strip()
            print("Kolom ditemukan:", df.columns.tolist()) # Debugging

            if tipe == 'master':
                # MAPPING KHUSUS FILE MC...csv
                # File kamu: NOMEN, NAMA_PEL, ZONA_NOVAK (Rayon), TARIF, REK_AIR (Target)
                
                # Kita buat kamus (dictionary) penyesuaian nama kolom
                # Kiri: Nama di Database, Kanan: Kemungkinan nama di File
                mapping = {
                    'nomen': ['NOMEN'],
                    'nama': ['NAMA_PEL', 'NAMA'],
                    'rayon': ['ZONA_NOVAK', 'RAYON'],
                    'tarif': ['TARIF', 'KODETARIF'],
                    'target_mc': ['REK_AIR', 'TARGET', 'TAGIHAN']
                }

                # Fungsi pencari kolom otomatis
                def cari_kolom(target_db):
                    for calon in mapping[target_db]:
                        if calon in df.columns:
                            return calon
                    return None

                # Rename kolom sesuai database
                rename_dict = {}
                for col_db in mapping:
                    found = cari_kolom(col_db)
                    if found:
                        rename_dict[found] = col_db
                
                # Cek kelengkapan
                if 'nomen' not in rename_dict.values(): # Minimal Nomen wajib ada
                    flash(f'Kolom NOMEN tidak ditemukan! Header file: {df.columns.tolist()}', 'danger')
                    return redirect(url_for('index'))

                df = df.rename(columns=rename_dict)
                
                # LOGIKA KHUSUS RAYON: Ambil 2 digit pertama dari ZONA_NOVAK (misal 35096.. jadi 35)
                if 'rayon' in df.columns:
                    df['rayon'] = df['rayon'].astype(str).str[:2]

                # Pastikan kolom yang tidak ada diisi default
                required_cols = ['nomen', 'nama', 'rayon', 'tarif', 'target_mc']
                for col in required_cols:
                    if col not in df.columns:
                        df[col] = '' if col != 'target_mc' else 0

                # Pilih & Simpan
                df = df[required_cols]
                df.to_sql('master_pelanggan', conn, if_exists='replace', index=False)
                flash(f'Sukses Upload {len(df)} Data Master Pelanggan!', 'success')
                
            elif tipe == 'collection':
                # Mapping Collection
                # Standar: NOMEN, TGL_BAYAR, JUMLAH
                # Jika file collection formatnya lain, tambahkan di sini
                rename_dict = {}
                if 'NOMEN' in df.columns: rename_dict['NOMEN'] = 'nomen'
                if 'TGL_BAYAR' in df.columns: rename_dict['TGL_BAYAR'] = 'tgl_bayar'
                if 'JUMLAH' in df.columns: rename_dict['JUMLAH'] = 'jumlah_bayar'
                
                if not rename_dict:
                    flash('Format Collection Salah! Wajib: NOMEN, TGL_BAYAR, JUMLAH', 'danger')
                    return redirect(url_for('index'))
                
                df = df.rename(columns=rename_dict)
                df['sumber_file'] = file.filename
                
                # Filter kolom yang ada saja
                save_cols = [c for c in ['nomen', 'tgl_bayar', 'jumlah_bayar', 'sumber_file'] if c in df.columns]
                df[save_cols].to_sql('collection_harian', conn, if_exists='append', index=False)
                flash(f'Sukses Upload {len(df)} Transaksi!', 'success')
                
        except Exception as e:
            print(f"Error Detail: {e}")
            flash(f'Gagal Upload: {e}', 'danger')
            
    return redirect(url_for('index'))

@app.route('/login')
@app.route('/logout')
def auth_bypass(): return redirect(url_for('index'))

if __name__ == '__main__':
    if not os.path.exists(DB_PATH): init_db()
    print("🚀 APLIKASI SIAP (CSV SUPPORTED)!")
    app.run(host='0.0.0.0', port=5000, debug=True)
