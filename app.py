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
        
        # 1. Master Pelanggan (STRUKTUR BARU: Tambah PC, PCEZ, EZ, BLOCK)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS master_pelanggan (
                nomen TEXT PRIMARY KEY,
                nama TEXT,
                rayon TEXT,
                pc TEXT,
                ez TEXT,
                pcez TEXT,
                block TEXT,
                tarif TEXT,
                target_mc REAL DEFAULT 0
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
        print("✅ Database Terinisialisasi dengan Struktur Baru (ZONA_NOVAK Split).")

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
    # Update query untuk mengambil data PC/PCEZ jika perlu ditampilkan nanti
    query = '''
        SELECT c.tgl_bayar, m.rayon, m.pcez, m.nomen, m.nama, m.target_mc, c.jumlah_bayar 
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

# --- FITUR UPLOAD (LOGIKA PECAH ZONA_NOVAK) ---
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
            
            # 1. BACA FILE (CSV / EXCEL)
            if filepath.endswith('.csv'):
                try:
                    df = pd.read_csv(filepath, sep=',')
                    if len(df.columns) < 2: df = pd.read_csv(filepath, sep=';')
                except:
                    df = pd.read_csv(filepath, sep=';')
            else:
                df = pd.read_excel(filepath)

            # 2. BERSIHKAN HEADER
            df.columns = df.columns.str.upper().str.strip()

            if tipe == 'master':
                # --- LOGIKA BARU: EXTRAKSI ZONA_NOVAK ---
                
                # Cek ketersediaan kolom ZONA_NOVAK
                if 'ZONA_NOVAK' not in df.columns:
                    flash('Gagal: Kolom ZONA_NOVAK tidak ditemukan di file!', 'danger')
                    return redirect(url_for('index'))

                # Pastikan format string agar bisa di-slice (potong)
                df['ZONA_NOVAK'] = df['ZONA_NOVAK'].astype(str).str.replace(r'\.0$', '', regex=True) # Hapus .0 jika ada

                # LAKUKAN PEMISAHAN DATA (SLICING)
                # Contoh: 350960217
                
                # 1. RAYON (Digit 1-2) -> '35'
                df['rayon'] = df['ZONA_NOVAK'].str[:2]
                
                # 2. PC (Digit 3-5) -> '096' (Index 2 sampai 5)
                df['pc'] = df['ZONA_NOVAK'].str[2:5]
                
                # 3. EZ (Digit 6-7) -> '02' (Index 5 sampai 7)
                df['ez'] = df['ZONA_NOVAK'].str[5:7]
                
                # 4. BLOCK (Digit 8-9) -> '17' (Index 7 sampai 9)
                df['block'] = df['ZONA_NOVAK'].str[7:9]
                
                # 5. PCEZ (Gabungan PC/EZ) -> '096/02'
                df['pcez'] = df['pc'] + '/' + df['ez']

                # --- MAPPING SISA KOLOM ---
                rename_dict = {}
                if 'NOMEN' in df.columns: rename_dict['NOMEN'] = 'nomen'
                
                # Cari Nama (Bisa NAMA_PEL atau NAMA)
                if 'NAMA_PEL' in df.columns: rename_dict['NAMA_PEL'] = 'nama'
                elif 'NAMA' in df.columns: rename_dict['NAMA'] = 'nama'
                
                # Cari Tarif
                if 'TARIF' in df.columns: rename_dict['TARIF'] = 'tarif'
                elif 'KODETARIF' in df.columns: rename_dict['KODETARIF'] = 'tarif'
                
                # Cari Target (Rekening Air)
                if 'REK_AIR' in df.columns: rename_dict['REK_AIR'] = 'target_mc'
                elif 'TARGET' in df.columns: rename_dict['TARGET'] = 'target_mc'
                
                df = df.rename(columns=rename_dict)

                # Pastikan kolom wajib ada
                wajib = ['nomen', 'nama', 'rayon', 'pc', 'ez', 'pcez', 'block', 'tarif', 'target_mc']
                
                # Isi default jika ada kolom yang kosong (selain hasil ektraksi tadi)
                for col in wajib:
                    if col not in df.columns:
                        df[col] = '' if col != 'target_mc' else 0

                # SIMPAN KE DATABASE
                df = df[wajib] # Hanya ambil kolom yang sesuai tabel
                df.to_sql('master_pelanggan', conn, if_exists='replace', index=False)
                
                flash(f'Sukses! Data Master dipecah: Rayon, PC, EZ, Block berhasil disimpan. ({len(df)} data)', 'success')
                
            elif tipe == 'collection':
                # Logika Collection Tetap Sama
                rename_dict = {}
                if 'NOMEN' in df.columns: rename_dict['NOMEN'] = 'nomen'
                if 'TGL_BAYAR' in df.columns: rename_dict['TGL_BAYAR'] = 'tgl_bayar'
                if 'JUMLAH' in df.columns: rename_dict['JUMLAH'] = 'jumlah_bayar'
                
                if not rename_dict:
                    flash('Format Collection Salah!', 'danger')
                    return redirect(url_for('index'))
                
                df = df.rename(columns=rename_dict)
                df['sumber_file'] = file.filename
                cols = [c for c in ['nomen', 'tgl_bayar', 'jumlah_bayar', 'sumber_file'] if c in df.columns]
                df[cols].to_sql('collection_harian', conn, if_exists='append', index=False)
                flash(f'Sukses Upload {len(df)} Transaksi!', 'success')
                
        except Exception as e:
            print(f"Error: {e}")
            flash(f'Gagal Upload: {e}', 'danger')
            
    return redirect(url_for('index'))

@app.route('/login')
@app.route('/logout')
def auth_bypass(): return redirect(url_for('index'))

if __name__ == '__main__':
    if not os.path.exists(DB_PATH): init_db()
    print("🚀 APLIKASI SIAP (ZONA_NOVAK SPLITTER AKTIF)!")
    app.run(host='0.0.0.0', port=5000, debug=True)
