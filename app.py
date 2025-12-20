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

# Pastikan folder ada
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
        
        # 1. TABEL MASTER (Struktur Baru dengan Pecahan Zona)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS master_pelanggan (
                nomen TEXT PRIMARY KEY,
                nama TEXT,
                rayon TEXT,   -- 2 Digit Awal
                pc TEXT,      -- 3 Digit (Digit 3-5)
                ez TEXT,      -- 2 Digit (Digit 6-7)
                pcez TEXT,    -- Gabungan PC/EZ
                block TEXT,   -- 2 Digit Akhir (Digit 8-9)
                tarif TEXT,
                target_mc REAL DEFAULT 0 -- Dari REK_AIR
            )
        ''')
        
        # 2. TABEL COLLECTION (Harian)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS collection_harian (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nomen TEXT,
                tgl_bayar TEXT,
                jumlah_bayar REAL,
                sumber_file TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # 3. TABEL ANALISA
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS analisa_manual (
                id INTEGER PRIMARY KEY AUTOINCREMENT, nomen TEXT, jenis_anomali TEXT, 
                analisa_tim TEXT, kesimpulan TEXT, rekomendasi TEXT, status TEXT DEFAULT 'Open',
                user_editor TEXT, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        db.commit()
        print("✅ Database Siap (Support Split ZONA_NOVAK).")

# --- ROUTING UTAMA ---
@app.route('/')
def index():
    db = get_db()
    kpi = {}
    try:
        # Hitung KPI
        kpi['cust'] = db.execute('SELECT COUNT(*) as t FROM master_pelanggan').fetchone()['t']
        kpi['coll'] = db.execute('SELECT SUM(jumlah_bayar) as t FROM collection_harian').fetchone()['t'] or 0
        kpi['anomali'] = db.execute('SELECT COUNT(*) as t FROM analisa_manual WHERE status != "Closed"').fetchone()['t']
        
        # Hitung Persentase Collection
        target_total = db.execute('SELECT SUM(target_mc) as t FROM master_pelanggan').fetchone()['t'] or 0
        if target_total > 0:
            kpi['persen'] = round((kpi['coll'] / target_total * 100), 2)
        else:
            kpi['persen'] = 0
            
    except:
        kpi = {'cust': 0, 'coll': 0, 'anomali': 0, 'persen': 0}
    
    return render_template('index.html', kpi=kpi)

# API Data untuk Tabel
@app.route('/api/collection_data')
def api_collection():
    db = get_db()
    # Kita join untuk menampilkan PC/PCEZ di tabel collection
    query = '''
        SELECT c.tgl_bayar, m.rayon, m.pcez, m.nomen, m.nama, m.target_mc, c.jumlah_bayar 
        FROM collection_harian c
        LEFT JOIN master_pelanggan m ON c.nomen = m.nomen
        ORDER BY c.tgl_bayar DESC LIMIT 1000
    '''
    rows = db.execute(query).fetchall()
    return jsonify([dict(row) for row in rows])

@app.route('/simpan_analisa', methods=['POST'])
def simpan_analisa():
    try:
        db = get_db()
        db.execute('INSERT INTO analisa_manual (nomen, jenis_anomali, analisa_tim, kesimpulan, rekomendasi, status, user_editor) VALUES (?, ?, ?, ?, ?, ?, ?)',
                   (request.form['nomen'], request.form['jenis_anomali'], request.form['analisa_tim'], request.form['kesimpulan'], request.form['rekomendasi'], request.form['status'], "Admin"))
        db.commit()
        return jsonify({'status': 'success', 'msg': 'Tersimpan!'})
    except Exception as e:
        return jsonify({'status': 'error', 'msg': str(e)})

# --- LOGIKA UPLOAD PENTING ---
@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files: return redirect(url_for('index'))
    file = request.files['file']
    tipe = request.form.get('tipe_upload') # 'master' atau 'collection'
    
    if file:
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(file.filename))
        file.save(filepath)
        
        try:
            conn = get_db()
            
            # 1. BACA FILE (Support CSV & Excel)
            if filepath.endswith('.csv'):
                try:
                    # Coba baca koma dulu
                    df = pd.read_csv(filepath, sep=',')
                    # Kalau kolomnya nyatu, coba titik koma
                    if len(df.columns) < 2: df = pd.read_csv(filepath, sep=';')
                except:
                    df = pd.read_csv(filepath, sep=';')
            else:
                df = pd.read_excel(filepath)

            # 2. BERSIHKAN HEADER (Huruf Besar & Trim Spasi)
            df.columns = df.columns.str.upper().str.strip()

            # =========================================================
            # KONDISI 1: UPLOAD MASTER PELANGGAN (MB/MC) -> REPLACE
            # =========================================================
            if tipe == 'master':
                # Pastikan ada kolom kunci
                if 'ZONA_NOVAK' not in df.columns:
                    flash('Gagal: File Master wajib punya kolom ZONA_NOVAK', 'danger')
                    return redirect(url_for('index'))

                # A. LOGIKA PECAH ZONA_NOVAK
                # Ubah ke string dulu, hilangkan .0 jika ada (efek Excel)
                df['ZONA_NOVAK'] = df['ZONA_NOVAK'].astype(str).str.replace(r'\.0$', '', regex=True)
                
                # Slicing String (Ingat: Python mulai dari index 0)
                # Contoh: 350960217
                
                # Rayon: 2 digit pertama (0-2) -> '35'
                df['rayon'] = df['ZONA_NOVAK'].str[:2]
                
                # PC: Digit ke 3,4,5 (2-5) -> '096'
                df['pc'] = df['ZONA_NOVAK'].str[2:5]
                
                # EZ: Digit ke 6,7 (5-7) -> '02'
                df['ez'] = df['ZONA_NOVAK'].str[5:7]
                
                # Block: Digit ke 8,9 (7-9) -> '17'
                df['block'] = df['ZONA_NOVAK'].str[7:9]
                
                # PCEZ: Gabungan
                df['pcez'] = df['pc'] + '/' + df['ez']

                # B. MAPPING KOLOM LAIN
                rename_dict = {}
                # Nomen
                if 'NOMEN' in df.columns: rename_dict['NOMEN'] = 'nomen'
                
                # Nama (MB kadang gak ada nama, MC ada)
                if 'NAMA_PEL' in df.columns: rename_dict['NAMA_PEL'] = 'nama'
                elif 'NAMA' in df.columns: rename_dict['NAMA'] = 'nama'
                
                # Tarif
                if 'TARIF' in df.columns: rename_dict['TARIF'] = 'tarif'
                elif 'KODETARIF' in df.columns: rename_dict['KODETARIF'] = 'tarif'
                
                # Target (REK_AIR adalah Target di file MB)
                if 'REK_AIR' in df.columns: rename_dict['REK_AIR'] = 'target_mc'
                elif 'TAGIHAN' in df.columns: rename_dict['TAGIHAN'] = 'target_mc'
                elif 'TARGET' in df.columns: rename_dict['TARGET'] = 'target_mc'

                df = df.rename(columns=rename_dict)
                
                # C. SIMPAN KE DB
                # Pastikan kolom wajib ada (kalau gak ada diisi default)
                cols_db = ['nomen', 'nama', 'rayon', 'pc', 'ez', 'pcez', 'block', 'tarif', 'target_mc']
                for c in cols_db:
                    if c not in df.columns:
                        df[c] = '' if c != 'target_mc' else 0
                
                # Replace data lama
                df[cols_db].to_sql('master_pelanggan', conn, if_exists='replace', index=False)
                flash(f'Sukses! Data Master (MB) diupdate. {len(df)} pelanggan.', 'success')

            # =========================================================
            # KONDISI 2: UPLOAD DAILY COLLECTION (HARIAN) -> APPEND
            # =========================================================
            elif tipe == 'collection':
                rename_dict = {}
                
                # Cari Nomen
                if 'NOMEN' in df.columns: rename_dict['NOMEN'] = 'nomen'
                elif 'NO_SAMBUNGAN' in df.columns: rename_dict['NO_SAMBUNGAN'] = 'nomen'
                
                # Cari Tanggal
                for c in ['TGL_BAYAR', 'TGL_LUNAS', 'TANGGAL']:
                    if c in df.columns: rename_dict[c] = 'tgl_bayar'; break
                
                # Cari Jumlah
                for c in ['JUMLAH', 'BAYAR', 'TOTAL', 'TAGIHAN']:
                    if c in df.columns: rename_dict[c] = 'jumlah_bayar'; break

                if 'nomen' not in rename_dict.values():
                    flash('Format Collection Salah! Butuh kolom NOMEN & JUMLAH/BAYAR', 'danger')
                    return redirect(url_for('index'))

                df = df.rename(columns=rename_dict)
                df['sumber_file'] = file.filename
                
                # Default Tanggal Hari Ini jika tidak ada
                if 'tgl_bayar' not in df.columns:
                    from datetime import date
                    df['tgl_bayar'] = str(date.today())
                else:
                    df['tgl_bayar'] = df['tgl_bayar'].astype(str)

                # Simpan (Append)
                cols = [c for c in ['nomen', 'tgl_bayar', 'jumlah_bayar', 'sumber_file'] if c in df.columns]
                df[cols].to_sql('collection_harian', conn, if_exists='append', index=False)
                flash(f'Sukses! {len(df)} Transaksi harian ditambahkan.', 'success')

        except Exception as e:
            flash(f'Gagal Upload: {e}', 'danger')
            print(f"Error Upload: {e}")
            
    return redirect(url_for('index'))

@app.route('/login')
@app.route('/logout')
def auth_bypass(): return redirect(url_for('index'))

if __name__ == '__main__':
    # HAPUS DB LAMA AGAR KOLOM BARU TERBUAT
    if not os.path.exists(DB_PATH):
        init_db()
    else:
        # Cek kolom manual (opsional) atau user suruh hapus db manual
        pass 
        
    print("🚀 APLIKASI SIAP! BUKA: http://localhost:5000")
    app.run(host='0.0.0.0', port=5000, debug=True)
