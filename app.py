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
    # Bypass Login (Langsung Dashboard)
    db = get_db()
    kpi = {}
    try:
        kpi['cust'] = db.execute('SELECT COUNT(*) as t FROM master_pelanggan').fetchone()['t']
        kpi['coll'] = db.execute('SELECT SUM(jumlah_bayar) as t FROM collection_harian').fetchone()['t'] or 0
        kpi['anomali'] = db.execute('SELECT COUNT(*) as t FROM analisa_manual WHERE status != "Closed"').fetchone()['t']
    except:
        kpi = {'cust': 0, 'coll': 0, 'anomali': 0}
    
    return render_template('index.html', kpi=kpi)

# API: Ambil Data untuk Tabel
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
    # Convert ke list of dict
    data = [dict(row) for row in rows]
    return jsonify(data)

# API: Simpan Analisa Manual
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

# FITUR: Upload Excel
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
            df = pd.read_excel(filepath)
            
            if tipe == 'master':
                # Harap Header Excel: NOMEN, NAMA, RAYON, TARIF, TARGET
                df = df.rename(columns={'NOMEN':'nomen', 'NAMA':'nama', 'RAYON':'rayon', 'TARIF':'tarif', 'TARGET':'target_mc'})
                df[['nomen','nama','rayon','tarif','target_mc']].to_sql('master_pelanggan', conn, if_exists='replace', index=False)
                flash('Master Pelanggan Berhasil Diupload!', 'success')
                
            elif tipe == 'collection':
                # Harap Header Excel: NOMEN, TGL_BAYAR, JUMLAH
                df = df.rename(columns={'NOMEN':'nomen', 'TGL_BAYAR':'tgl_bayar', 'JUMLAH':'jumlah_bayar'})
                df['sumber_file'] = file.filename
                df[['nomen','tgl_bayar','jumlah_bayar','sumber_file']].to_sql('collection_harian', conn, if_exists='append', index=False)
                flash('Data Transaksi Berhasil Diupload!', 'success')
                
        except Exception as e:
            flash(f'Gagal Upload: {e}', 'danger')
            
    return redirect(url_for('index'))

# Route Login/Logout dibuang/redirect ke index
@app.route('/login')
@app.route('/logout')
def auth_bypass(): return redirect(url_for('index'))

if __name__ == '__main__':
    if not os.path.exists(DB_PATH): init_db()
    print("🚀 APLIKASI SIAP! BUKA BROWSER DI: http://localhost:5000")
    app.run(host='0.0.0.0', port=5000, debug=True)
