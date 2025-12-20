import os
import sqlite3
import pandas as pd
from flask import Flask, render_template, g, request, redirect, url_for, flash, jsonify
from werkzeug.utils import secure_filename
from datetime import datetime

# --- KONFIGURASI ---
app = Flask(__name__)
app.config['SECRET_KEY'] = 'SUNTER-ADMIN-MODE'
app.config['UPLOAD_FOLDER'] = 'uploads'
DB_FOLDER = os.path.join(os.getcwd(), 'database')
DB_PATH = os.path.join(DB_FOLDER, 'sunter.db')

for folder in [app.config['UPLOAD_FOLDER'], DB_FOLDER]:
    if not os.path.exists(folder): os.makedirs(folder)

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
    if db is not None: db.close()

def init_db():
    with app.app_context():
        db = get_db()
        cursor = db.cursor()
        
        # 1. Master Pelanggan (Target & Nama)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS master_pelanggan (
                nomen TEXT PRIMARY KEY,
                nama TEXT,
                rayon TEXT, pc TEXT, ez TEXT, pcez TEXT, block TEXT,
                tarif TEXT,
                target_mc REAL DEFAULT 0
            )
        ''')
        
        # 2. Collection Harian
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

        # 3. Analisa Manual
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS analisa_manual (
                id INTEGER PRIMARY KEY AUTOINCREMENT, nomen TEXT, jenis_anomali TEXT, 
                analisa_tim TEXT, kesimpulan TEXT, rekomendasi TEXT, status TEXT DEFAULT 'Open',
                user_editor TEXT, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        db.commit()
        print("✅ Database Siap.")

# --- ROUTING UTAMA ---
@app.route('/')
def index():
    return render_template('index.html') # Data KPI dihitung via JS/API biar ringan

# API: DATA COLLECTION (SEMUA DATA BULAN TERBARU)
@app.route('/api/collection_data')
def api_collection():
    db = get_db()
    
    # 1. Cari Tahu Bulan Terbaru yang ada di Database
    # (Supaya kalau datanya bulan lalu, tetap muncul, gak kosong)
    cek_tgl = db.execute("SELECT MAX(tgl_bayar) as last_date FROM collection_harian").fetchone()
    
    if cek_tgl['last_date']:
        # Ambil YYYY-MM dari tanggal terakhir data
        last_month_str = cek_tgl['last_date'][:7] # Contoh: '2025-11'
    else:
        # Kalau kosong, pakai bulan hari ini
        last_month_str = datetime.now().strftime('%Y-%m')

    # 2. Query Tanpa Limit (Tapi difilter Bulan Terbaru)
    query = f'''
        SELECT 
            c.tgl_bayar, 
            m.rayon, 
            m.pcez, 
            c.nomen, 
            COALESCE(m.nama, 'Belum Ada Nama') as nama, 
            m.target_mc, 
            c.jumlah_bayar 
        FROM collection_harian c
        LEFT JOIN master_pelanggan m ON c.nomen = m.nomen
        WHERE strftime('%Y-%m', c.tgl_bayar) = ? 
        ORDER BY c.tgl_bayar DESC, c.nomen ASC
    '''
    
    # Eksekusi Query
    rows = db.execute(query, (last_month_str,)).fetchall()
    
    # Ubah ke JSON
    data = [dict(row) for row in rows]
    
    # Tambahkan Info Bulan di response header/metadata (opsional, kita kirim data aja)
    return jsonify(data)

# API: KPI Summary (Biar Dashboard gak berat)
@app.route('/api/kpi_data')
def api_kpi():
    db = get_db()
    kpi = {}
    try:
        # Cari Bulan Terbaru
        cek_tgl = db.execute("SELECT MAX(tgl_bayar) as last_date FROM collection_harian").fetchone()
        last_month = cek_tgl['last_date'][:7] if cek_tgl['last_date'] else datetime.now().strftime('%Y-%m')
        
        kpi['cust'] = db.execute('SELECT COUNT(*) as t FROM master_pelanggan').fetchone()['t']
        
        # Collection Bulan Ini Saja
        kpi['coll'] = db.execute(f"SELECT SUM(jumlah_bayar) as t FROM collection_harian WHERE strftime('%Y-%m', tgl_bayar) = ?", (last_month,)).fetchone()['t'] or 0
        
        kpi['anomali'] = db.execute('SELECT COUNT(*) as t FROM analisa_manual WHERE status != "Closed"').fetchone()['t']
        
        target_total = db.execute('SELECT SUM(target_mc) as t FROM master_pelanggan').fetchone()['t'] or 0
        kpi['persen'] = round((kpi['coll'] / target_total * 100), 2) if target_total > 0 else 0
        kpi['periode'] = last_month
    except:
        kpi = {'cust': 0, 'coll': 0, 'anomali': 0, 'persen': 0, 'periode': '-'}
    return jsonify(kpi)

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

# --- UPLOAD FILE ---
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
            # Baca CSV/Excel
            if filepath.endswith('.csv'):
                try:
                    df = pd.read_csv(filepath, sep=',')
                    if len(df.columns) < 2: df = pd.read_csv(filepath, sep=';')
                except: df = pd.read_csv(filepath, sep=';')
            else:
                df = pd.read_excel(filepath)

            df.columns = df.columns.str.upper().str.strip()

            # --- UPLOAD MASTER (MC/MB) ---
            if tipe == 'master':
                if 'ZONA_NOVAK' not in df.columns:
                    flash('Format Salah: File Master wajib punya kolom ZONA_NOVAK', 'danger')
                    return redirect(url_for('index'))

                df['ZONA_NOVAK'] = df['ZONA_NOVAK'].astype(str).str.replace(r'\.0$', '', regex=True)
                df['rayon'] = df['ZONA_NOVAK'].str[:2]
                df['pc'] = df['ZONA_NOVAK'].str[2:5]
                df['ez'] = df['ZONA_NOVAK'].str[5:7]
                df['block'] = df['ZONA_NOVAK'].str[7:9]
                df['pcez'] = df['pc'] + '/' + df['ez']

                rename_dict = {}
                if 'NOMEN' in df.columns: rename_dict['NOMEN'] = 'nomen'
                
                # PRIORITAS NAMA (PENTING BIAR GAK KOSONG)
                if 'NAMA_PEL' in df.columns: rename_dict['NAMA_PEL'] = 'nama'
                elif 'NAMA' in df.columns: rename_dict['NAMA'] = 'nama'
                
                if 'TARIF' in df.columns: rename_dict['TARIF'] = 'tarif'
                elif 'KODETARIF' in df.columns: rename_dict['KODETARIF'] = 'tarif'
                
                if 'REK_AIR' in df.columns: rename_dict['REK_AIR'] = 'target_mc'
                elif 'TARGET' in df.columns: rename_dict['TARGET'] = 'target_mc'
                elif 'TAGIHAN' in df.columns: rename_dict['TAGIHAN'] = 'target_mc'

                df = df.rename(columns=rename_dict)
                
                cols_db = ['nomen', 'nama', 'rayon', 'pc', 'ez', 'pcez', 'block', 'tarif', 'target_mc']
                for c in cols_db:
                    if c not in df.columns: df[c] = '' if c != 'target_mc' else 0
                
                # REPLACE DATA MASTER
                df[cols_db].to_sql('master_pelanggan', conn, if_exists='replace', index=False)
                flash(f'Sukses! Master Pelanggan Diperbarui ({len(df)} data). Pastikan upload file MC agar nama muncul.', 'success')

            # --- UPLOAD COLLECTION (DAILY) ---
            elif tipe == 'collection':
                rename_dict = {}
                if 'NOMEN' in df.columns: rename_dict['NOMEN'] = 'nomen'
                elif 'NO_SAMBUNGAN' in df.columns: rename_dict['NO_SAMBUNGAN'] = 'nomen'
                
                for c in ['TGL_BAYAR', 'TGL_LUNAS', 'TANGGAL']:
                    if c in df.columns: rename_dict[c] = 'tgl_bayar'; break
                
                for c in ['JUMLAH', 'BAYAR', 'TOTAL', 'TAGIHAN']:
                    if c in df.columns: rename_dict[c] = 'jumlah_bayar'; break

                if 'nomen' not in rename_dict.values():
                    flash('Format Collection Salah! Butuh kolom NOMEN', 'danger')
                    return redirect(url_for('index'))

                df = df.rename(columns=rename_dict)
                df['sumber_file'] = file.filename
                
                if 'tgl_bayar' not in df.columns:
                    from datetime import date
                    df['tgl_bayar'] = str(date.today())
                else:
                    # Pastikan format tanggal aman
                    try:
                        df['tgl_bayar'] = pd.to_datetime(df['tgl_bayar']).dt.strftime('%Y-%m-%d')
                    except:
                        df['tgl_bayar'] = df['tgl_bayar'].astype(str)

                cols = [c for c in ['nomen', 'tgl_bayar', 'jumlah_bayar', 'sumber_file'] if c in df.columns]
                df[cols].to_sql('collection_harian', conn, if_exists='append', index=False)
                flash(f'Sukses! {len(df)} Transaksi ditambahkan.', 'success')

        except Exception as e:
            flash(f'Gagal Upload: {e}', 'danger')
            print(f"Error Upload: {e}")
            
    return redirect(url_for('index'))

@app.route('/login')
@app.route('/logout')
def auth_bypass(): return redirect(url_for('index'))

if __name__ == '__main__':
    if not os.path.exists(DB_PATH): init_db()
    print("🚀 APLIKASI SIAP (UNLIMITED ROWS + NAMA FIX)!")
    app.run(host='0.0.0.0', port=5000, debug=True)
