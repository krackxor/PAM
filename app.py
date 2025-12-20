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
        
        # 1. Master Pelanggan (dari MC - Data Induk)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS master_pelanggan (
                nomen TEXT PRIMARY KEY,
                nama TEXT,
                alamat TEXT,
                rayon TEXT,
                pc TEXT,
                ez TEXT,
                pcez TEXT,
                block TEXT,
                zona_novak TEXT,
                tarif TEXT,
                target_mc REAL DEFAULT 0,
                periode TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 2. Collection Harian (dari DAILY)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS collection_harian (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nomen TEXT,
                tgl_bayar TEXT,
                jumlah_bayar REAL,
                sumber_file TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(nomen) REFERENCES master_pelanggan(nomen)
            )
        ''')

        # 3. MB (Master Bayar - Pembayaran Bulan Sebelumnya)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS master_bayar (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nomen TEXT,
                tgl_bayar TEXT,
                jumlah_bayar REAL DEFAULT 0,
                periode TEXT,
                sumber_file TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(nomen) REFERENCES master_pelanggan(nomen)
            )
        ''')
        
        # 4. MainBill (Tagihan Bulan Depan - Bill Baru)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS mainbill (
                nomen TEXT PRIMARY KEY,
                tgl_tagihan TEXT,
                total_tagihan REAL DEFAULT 0,
                pcezbk TEXT,
                tarif TEXT,
                periode TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(nomen) REFERENCES master_pelanggan(nomen)
            )
        ''')
        
        # 5. Ardebt (Tunggakan)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ardebt (
                nomen TEXT PRIMARY KEY,
                saldo_tunggakan REAL DEFAULT 0,
                periode TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(nomen) REFERENCES master_pelanggan(nomen)
            )
        ''')

        # 6. Analisa Manual
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS analisa_manual (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nomen TEXT,
                jenis_anomali TEXT,
                analisa_tim TEXT,
                kesimpulan TEXT,
                rekomendasi TEXT,
                status TEXT DEFAULT 'Open',
                user_editor TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(nomen) REFERENCES master_pelanggan(nomen)
            )
        ''')
        
        # Index untuk performa
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_coll_tgl ON collection_harian(tgl_bayar)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_coll_nomen ON collection_harian(nomen)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_master_rayon ON master_pelanggan(rayon)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_mb_nomen ON master_bayar(nomen)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_mb_periode ON master_bayar(periode)')
        
        db.commit()
        print("✅ Database Siap.")

# --- HELPER FUNCTIONS ---
def clean_nomen(val):
    """Membersihkan format Nomen"""
    try:
        if pd.isna(val): return None
        # Hilangkan .0 dari Excel
        return str(int(float(str(val))))
    except:
        return str(val).strip()

def parse_zona_novak(zona):
    """Parse ZONA_NOVAK menjadi komponen (Rayon, PC, EZ, PCEZ, Block)
    Contoh: 350960217 -> Rayon:35, PC:096, EZ:02, Block:17
    """
    zona_str = str(zona).strip()
    if len(zona_str) < 9:
        zona_str = zona_str.zfill(9)  # Padding dengan 0 di depan jika kurang
    
    return {
        'rayon': zona_str[:2],
        'pc': zona_str[2:5],
        'ez': zona_str[5:7],
        'block': zona_str[7:9],
        'pcez': f"{zona_str[2:5]}/{zona_str[5:7]}"
    }

def clean_date(val, fmt_in='%d-%m-%Y', fmt_out='%Y-%m-%d'):
    """Standardisasi format tanggal ke YYYY-MM-DD"""
    try:
        return datetime.strptime(str(val).strip(), fmt_in).strftime(fmt_out)
    except:
        try:
            # Coba format lain (DD/MM/YYYY)
            return datetime.strptime(str(val).strip(), '%d/%m/%Y').strftime(fmt_out)
        except:
            return str(val)

# --- ROUTING UTAMA ---
@app.route('/')
def index():
    return render_template('index.html')

# API: KPI Summary
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
        kpi['coll'] = db.execute(
            "SELECT SUM(jumlah_bayar) as t FROM collection_harian WHERE strftime('%Y-%m', tgl_bayar) = ?",
            (last_month,)
        ).fetchone()['t'] or 0
        
        kpi['anomali'] = db.execute('SELECT COUNT(*) as t FROM analisa_manual WHERE status != "Closed"').fetchone()['t']
        
        target_total = db.execute('SELECT SUM(target_mc) as t FROM master_pelanggan').fetchone()['t'] or 0
        kpi['persen'] = round((kpi['coll'] / target_total * 100), 2) if target_total > 0 else 0
        kpi['periode'] = last_month
    except Exception as e:
        print(f"Error KPI: {e}")
        kpi = {'cust': 0, 'coll': 0, 'anomali': 0, 'persen': 0, 'periode': '-'}
    return jsonify(kpi)

# API: DATA COLLECTION (SEMUA DATA BULAN TERBARU)
@app.route('/api/collection_data')
def api_collection():
    db = get_db()
    
    # Filter parameter (optional)
    rayon_filter = request.args.get('rayon', 'SUNTER')  # SUNTER, 34, atau 35
    
    # Cari Bulan Terbaru
    cek_tgl = db.execute("SELECT MAX(tgl_bayar) as last_date FROM collection_harian").fetchone()
    
    if cek_tgl['last_date']:
        last_month_str = cek_tgl['last_date'][:7]
    else:
        last_month_str = datetime.now().strftime('%Y-%m')

    # Query dengan filter rayon
    if rayon_filter == 'SUNTER':
        rayon_condition = "AND (m.rayon = '34' OR m.rayon = '35')"
    elif rayon_filter in ['34', '35']:
        rayon_condition = f"AND m.rayon = '{rayon_filter}'"
    else:
        rayon_condition = ""
    
    query = f'''
        SELECT 
            c.tgl_bayar, 
            m.rayon, 
            m.pcez, 
            m.pc,
            m.ez,
            c.nomen, 
            COALESCE(m.nama, 'Belum Ada Nama') as nama,
            m.zona_novak,
            m.tarif,
            m.target_mc, 
            c.jumlah_bayar 
        FROM collection_harian c
        LEFT JOIN master_pelanggan m ON c.nomen = m.nomen
        WHERE strftime('%Y-%m', c.tgl_bayar) = ? {rayon_condition}
        ORDER BY c.tgl_bayar DESC, c.nomen ASC
    '''
    
    rows = db.execute(query, (last_month_str,)).fetchall()
    data = [dict(row) for row in rows]
    
    return jsonify(data)

# API: Breakdown per Rayon (untuk Chart)
@app.route('/api/breakdown_rayon')
def api_breakdown_rayon():
    db = get_db()
    
    # Ambil bulan terbaru
    cek_tgl = db.execute("SELECT MAX(tgl_bayar) as last_date FROM collection_harian").fetchone()
    last_month = cek_tgl['last_date'][:7] if cek_tgl['last_date'] else datetime.now().strftime('%Y-%m')
    
    query = '''
        SELECT 
            m.rayon,
            SUM(c.jumlah_bayar) as total_collection,
            SUM(m.target_mc) as total_target,
            COUNT(DISTINCT c.nomen) as jumlah_pelanggan
        FROM collection_harian c
        LEFT JOIN master_pelanggan m ON c.nomen = m.nomen
        WHERE strftime('%Y-%m', c.tgl_bayar) = ?
        AND (m.rayon = '34' OR m.rayon = '35')
        GROUP BY m.rayon
    '''
    
    rows = db.execute(query, (last_month,)).fetchall()
    data = [dict(row) for row in rows]
    
    return jsonify(data)

@app.route('/simpan_analisa', methods=['POST'])
def simpan_analisa():
    try:
        db = get_db()
        db.execute('''
            INSERT INTO analisa_manual 
            (nomen, jenis_anomali, analisa_tim, kesimpulan, rekomendasi, status, user_editor) 
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            request.form['nomen'],
            request.form['jenis_anomali'],
            request.form['analisa_tim'],
            request.form['kesimpulan'],
            request.form['rekomendasi'],
            request.form['status'],
            "Admin"
        ))
        db.commit()
        return jsonify({'status': 'success', 'msg': 'Analisa tersimpan!'})
    except Exception as e:
        return jsonify({'status': 'error', 'msg': str(e)})

# --- UPLOAD FILE ---
@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        flash('Tidak ada file yang dipilih', 'danger')
        return redirect(url_for('index'))
    
    file = request.files['file']
    tipe = request.form.get('tipe_upload')
    
    if file.filename == '':
        flash('Nama file kosong', 'danger')
        return redirect(url_for('index'))
    
    if file:
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(file.filename))
        file.save(filepath)
        
        try:
            conn = get_db()
            
            # Baca File
            if filepath.endswith('.csv'):
                try:
                    df = pd.read_csv(filepath, sep=',', encoding='utf-8')
                    if len(df.columns) < 2:
                        df = pd.read_csv(filepath, sep=';', encoding='utf-8')
                except:
                    df = pd.read_csv(filepath, sep=';', encoding='latin-1')
            elif filepath.endswith('.txt'):
                # Untuk file DAILY (Collection) yang pakai delimiter |
                df = pd.read_csv(filepath, sep='|', encoding='utf-8')
            else:
                df = pd.read_excel(filepath)

            # Normalize column names
            df.columns = df.columns.str.upper().str.strip()

            # --- UPLOAD MASTER (MC) ---
            if tipe == 'master':
                # Validasi kolom wajib
                if 'ZONA_NOVAK' not in df.columns:
                    flash('❌ Format MC salah! Wajib ada kolom ZONA_NOVAK', 'danger')
                    return redirect(url_for('index'))
                
                if 'NOTAGIHAN' not in df.columns:
                    flash('❌ Format MC salah! Wajib ada kolom NOTAGIHAN (ini KEY UTAMA)', 'danger')
                    return redirect(url_for('index'))

                # Mapping field MC
                rename_dict = {}
                
                # Field NOMEN dari NOTAGIHAN (MC) - INI DATA INDUK!
                rename_dict['NOTAGIHAN'] = 'nomen'
                
                # Field NAMA
                if 'NAMA_PEL' in df.columns:
                    rename_dict['NAMA_PEL'] = 'nama'
                elif 'NAMA' in df.columns:
                    rename_dict['NAMA'] = 'nama'
                
                # Field ALAMAT
                if 'ALM1_PEL' in df.columns:
                    rename_dict['ALM1_PEL'] = 'alamat'
                elif 'ALAMAT' in df.columns:
                    rename_dict['ALAMAT'] = 'alamat'
                
                # Field TARIF
                if 'TARIF' in df.columns:
                    rename_dict['TARIF'] = 'tarif'
                elif 'KODETARIF' in df.columns:
                    rename_dict['KODETARIF'] = 'tarif'
                
                # Field TARGET (REK_AIR)
                if 'REK_AIR' in df.columns:
                    rename_dict['REK_AIR'] = 'target_mc'
                elif 'TARGET' in df.columns:
                    rename_dict['TARGET'] = 'target_mc'
                elif 'TAGIHAN' in df.columns:
                    rename_dict['TAGIHAN'] = 'target_mc'
                
                df = df.rename(columns=rename_dict)
                
                # Clean nomen
                df['nomen'] = df['nomen'].apply(clean_nomen)
                df = df.dropna(subset=['nomen'])
                
                # Parse ZONA_NOVAK
                df['zona_novak'] = df['ZONA_NOVAK'].astype(str).str.strip()
                zona_parsed = df['zona_novak'].apply(parse_zona_novak)
                
                df['rayon'] = zona_parsed.apply(lambda x: x['rayon'])
                df['pc'] = zona_parsed.apply(lambda x: x['pc'])
                df['ez'] = zona_parsed.apply(lambda x: x['ez'])
                df['block'] = zona_parsed.apply(lambda x: x['block'])
                df['pcez'] = zona_parsed.apply(lambda x: x['pcez'])
                
                # Filter hanya Rayon 34 dan 35
                df = df[df['rayon'].isin(['34', '35'])]
                
                if len(df) == 0:
                    flash('Tidak ada data Rayon 34/35 dalam file MC', 'warning')
                    return redirect(url_for('index'))
                
                # Set default values
                for col in ['nama', 'alamat', 'tarif']:
                    if col not in df.columns:
                        df[col] = ''
                
                if 'target_mc' not in df.columns:
                    df['target_mc'] = 0
                
                df['periode'] = datetime.now().strftime('%Y-%m')
                
                # Insert/Update Master
                cols_db = ['nomen', 'nama', 'alamat', 'rayon', 'pc', 'ez', 'pcez', 'block', 'zona_novak', 'tarif', 'target_mc', 'periode']
                df[cols_db].to_sql('master_pelanggan', conn, if_exists='replace', index=False)
                
                flash(f'✅ Master Pelanggan berhasil diupdate ({len(df)} data Rayon 34/35)', 'success')

            # --- UPLOAD COLLECTION DAILY ---
            elif tipe == 'collection':
                # Field mapping untuk DAILY
                rename_dict = {}
                
                # Field NOMEN dari NOTAG (DAILY) - PENTING!
                # NOTAG di DAILY = NOTAGIHAN di MC
                if 'NOTAG' in df.columns:
                    rename_dict['NOTAG'] = 'nomen'
                elif 'NO_SAMBUNGAN' in df.columns:
                    rename_dict['NO_SAMBUNGAN'] = 'nomen'
                else:
                    flash('❌ Format Collection salah! Butuh kolom NOTAG', 'danger')
                    return redirect(url_for('index'))
                
                # Field TANGGAL dari PAY_DT (DAILY)
                if 'PAY_DT' in df.columns:
                    rename_dict['PAY_DT'] = 'tgl_bayar'
                elif 'TGL_BAYAR' in df.columns:
                    rename_dict['TGL_BAYAR'] = 'tgl_bayar'
                
                # Field JUMLAH dari AMT_COLLECT
                if 'AMT_COLLECT' in df.columns:
                    rename_dict['AMT_COLLECT'] = 'jumlah_bayar'
                elif 'JUMLAH' in df.columns:
                    rename_dict['JUMLAH'] = 'jumlah_bayar'
                
                # Field RAYON untuk validasi
                if 'RAYON' in df.columns:
                    rename_dict['RAYON'] = 'rayon_check'
                
                df = df.rename(columns=rename_dict)
                
                # Clean data
                df['nomen'] = df['nomen'].apply(clean_nomen)
                df = df.dropna(subset=['nomen'])
                
                # Clean tanggal
                if 'tgl_bayar' in df.columns:
                    df['tgl_bayar'] = df['tgl_bayar'].apply(lambda x: clean_date(x))
                else:
                    df['tgl_bayar'] = datetime.now().strftime('%Y-%m-%d')
                
                # Clean jumlah (hilangkan minus dari SAP)
                if 'jumlah_bayar' in df.columns:
                    df['jumlah_bayar'] = df['jumlah_bayar'].apply(lambda x: abs(float(x)) if pd.notna(x) else 0)
                else:
                    df['jumlah_bayar'] = 0
                
                df['sumber_file'] = file.filename
                
                # Pastikan nomen ada di master (create dummy jika belum ada)
                for nomen in df['nomen'].unique():
                    conn.execute(
                        "INSERT OR IGNORE INTO master_pelanggan (nomen) VALUES (?)",
                        (nomen,)
                    )
                conn.commit()
                
                # Insert collection
                cols = ['nomen', 'tgl_bayar', 'jumlah_bayar', 'sumber_file']
                df[cols].to_sql('collection_harian', conn, if_exists='append', index=False)
                
                flash(f'✅ Collection berhasil ditambahkan ({len(df)} transaksi)', 'success')
            
            # --- UPLOAD MB (MASTER BAYAR - Pembayaran Bulan Lalu) ---
            elif tipe == 'mb':
                # Field mapping untuk MB
                rename_dict = {}
                
                # Field NOMEN dari NOTAGIHAN (MB)
                if 'NOTAGIHAN' in df.columns:
                    rename_dict['NOTAGIHAN'] = 'nomen'
                elif 'NOMEN' in df.columns:
                    rename_dict['NOMEN'] = 'nomen'
                else:
                    flash('❌ Format MB salah! Butuh kolom NOTAGIHAN atau NOMEN', 'danger')
                    return redirect(url_for('index'))
                
                # Field TANGGAL dari TGL_BAYAR (MB)
                if 'TGL_BAYAR' in df.columns:
                    rename_dict['TGL_BAYAR'] = 'tgl_bayar'
                elif 'TANGGAL' in df.columns:
                    rename_dict['TANGGAL'] = 'tgl_bayar'
                
                # Field JUMLAH BAYAR
                if 'BAYAR' in df.columns:
                    rename_dict['BAYAR'] = 'jumlah_bayar'
                elif 'JUMLAH_BAYAR' in df.columns:
                    rename_dict['JUMLAH_BAYAR'] = 'jumlah_bayar'
                elif 'JUMLAH' in df.columns:
                    rename_dict['JUMLAH'] = 'jumlah_bayar'
                
                df = df.rename(columns=rename_dict)
                
                # Clean data
                df['nomen'] = df['nomen'].apply(clean_nomen)
                df = df.dropna(subset=['nomen'])
                
                if 'tgl_bayar' in df.columns:
                    df['tgl_bayar'] = df['tgl_bayar'].apply(lambda x: clean_date(x))
                else:
                    df['tgl_bayar'] = ''
                
                if 'jumlah_bayar' not in df.columns:
                    df['jumlah_bayar'] = 0
                else:
                    # Pastikan positif
                    df['jumlah_bayar'] = df['jumlah_bayar'].apply(lambda x: abs(float(x)) if pd.notna(x) else 0)
                
                df['periode'] = datetime.now().strftime('%Y-%m')
                df['sumber_file'] = file.filename
                
                # Pastikan nomen ada di master
                for nomen in df['nomen'].unique():
                    conn.execute(
                        "INSERT OR IGNORE INTO master_pelanggan (nomen) VALUES (?)",
                        (nomen,)
                    )
                conn.commit()
                
                # Insert MB (append, bisa banyak record per nomen)
                cols = ['nomen', 'tgl_bayar', 'jumlah_bayar', 'periode', 'sumber_file']
                df[cols].to_sql('master_bayar', conn, if_exists='append', index=False)
                
                flash(f'✅ Master Bayar (MB) berhasil ditambahkan ({len(df)} transaksi)', 'success')
            
            # --- UPLOAD MAINBILL (Tagihan Bulan Depan) ---
            elif tipe == 'mainbill':
                # Field mapping untuk MB
                rename_dict = {}
                
                # Field NOMEN dari NOMEN (MB) - langsung
                if 'NOMEN' in df.columns:
                    rename_dict['NOMEN'] = 'nomen'
                else:
                    flash('❌ Format MainBill salah! Butuh kolom NOMEN', 'danger')
                    return redirect(url_for('index'))
                
                # Field TANGGAL dari FREEZE_DT (MB)
                if 'FREEZE_DT' in df.columns:
                    rename_dict['FREEZE_DT'] = 'tgl_bayar'
                elif 'TGL_BAYAR' in df.columns:
                    rename_dict['TGL_BAYAR'] = 'tgl_bayar'
                
                # Field TAGIHAN dari TOTAL_TAGIHAN
                if 'TOTAL_TAGIHAN' in df.columns:
                    rename_dict['TOTAL_TAGIHAN'] = 'tagihan'
                elif 'TAGIHAN' in df.columns:
                    rename_dict['TAGIHAN'] = 'tagihan'
                
                # Field PCEZBK untuk informasi tambahan
                if 'PCEZBK' in df.columns:
                    rename_dict['PCEZBK'] = 'pcezbk'
                
                # Field CC (Rayon) untuk validasi
                if 'CC' in df.columns:
                    rename_dict['CC'] = 'rayon_check'
                
                df = df.rename(columns=rename_dict)
                
                # Clean data
                df['nomen'] = df['nomen'].apply(clean_nomen)
                df = df.dropna(subset=['nomen'])
                
                if 'tgl_bayar' in df.columns:
                    df['tgl_bayar'] = df['tgl_bayar'].apply(lambda x: clean_date(x))
                else:
                    df['tgl_bayar'] = ''
                
                if 'tagihan' not in df.columns:
                    df['tagihan'] = 0
                
            # --- UPLOAD MAINBILL (Tagihan Bulan Depan) ---
            elif tipe == 'mainbill':
                # Field mapping untuk MainBill
                rename_dict = {}
                
                # Field NOMEN dari NOMEN (MainBill) - langsung
                if 'NOMEN' in df.columns:
                    rename_dict['NOMEN'] = 'nomen'
                else:
                    flash('❌ Format MainBill salah! Butuh kolom NOMEN', 'danger')
                    return redirect(url_for('index'))
                
                # Field TANGGAL dari FREEZE_DT (MainBill)
                if 'FREEZE_DT' in df.columns:
                    rename_dict['FREEZE_DT'] = 'tgl_tagihan'
                elif 'TGL_TAGIHAN' in df.columns:
                    rename_dict['TGL_TAGIHAN'] = 'tgl_tagihan'
                
                # Field TAGIHAN dari TOTAL_TAGIHAN
                if 'TOTAL_TAGIHAN' in df.columns:
                    rename_dict['TOTAL_TAGIHAN'] = 'total_tagihan'
                elif 'TAGIHAN' in df.columns:
                    rename_dict['TAGIHAN'] = 'total_tagihan'
                
                # Field PCEZBK untuk informasi tambahan
                if 'PCEZBK' in df.columns:
                    rename_dict['PCEZBK'] = 'pcezbk'
                
                # Field TARIF
                if 'TARIF' in df.columns:
                    rename_dict['TARIF'] = 'tarif'
                
                df = df.rename(columns=rename_dict)
                
                # Clean data
                df['nomen'] = df['nomen'].apply(clean_nomen)
                df = df.dropna(subset=['nomen'])
                
                if 'tgl_tagihan' in df.columns:
                    df['tgl_tagihan'] = df['tgl_tagihan'].apply(lambda x: clean_date(x))
                else:
                    df['tgl_tagihan'] = ''
                
                if 'total_tagihan' not in df.columns:
                    df['total_tagihan'] = 0
                else:
                    df['total_tagihan'] = df['total_tagihan'].apply(lambda x: abs(float(x)) if pd.notna(x) else 0)
                
                df['periode'] = datetime.now().strftime('%Y-%m')
                
                # Pastikan nomen ada di master
                for nomen in df['nomen'].unique():
                    conn.execute(
                        "INSERT OR IGNORE INTO master_pelanggan (nomen) VALUES (?)",
                        (nomen,)
                    )
                conn.commit()
                
                # Insert/Update MainBill (replace karena 1 nomen = 1 tagihan aktif)
                cols = ['nomen', 'tgl_tagihan', 'total_tagihan', 'pcezbk', 'tarif', 'periode']
                available_cols = [c for c in cols if c in df.columns]
                df[available_cols].to_sql('mainbill', conn, if_exists='replace', index=False)
                
                flash(f'✅ MainBill (Tagihan) berhasil diupdate ({len(df)} data)', 'success')
            
            # --- UPLOAD ARDEBT (Tunggakan) ---
            elif tipe == 'ardebt':
                # Field mapping untuk Ardebt
                rename_dict = {}
                
                # Field NOMEN
                if 'NOMEN' in df.columns:
                    rename_dict['NOMEN'] = 'nomen'
                elif 'NOTAGIHAN' in df.columns:
                    rename_dict['NOTAGIHAN'] = 'nomen'
                else:
                    flash('❌ Format Ardebt salah! Butuh kolom NOMEN', 'danger')
                    return redirect(url_for('index'))
                
                # Field TUNGGAKAN
                if 'SumOfJUMLAH' in df.columns:
                    rename_dict['SumOfJUMLAH'] = 'saldo_tunggakan'
                elif 'TUNGGAKAN' in df.columns:
                    rename_dict['TUNGGAKAN'] = 'saldo_tunggakan'
                elif 'SALDO' in df.columns:
                    rename_dict['SALDO'] = 'saldo_tunggakan'
                
                df = df.rename(columns=rename_dict)
                
                # Clean data
                df['nomen'] = df['nomen'].apply(clean_nomen)
                df = df.dropna(subset=['nomen'])
                
                if 'saldo_tunggakan' not in df.columns:
                    df['saldo_tunggakan'] = 0
                else:
                    df['saldo_tunggakan'] = df['saldo_tunggakan'].apply(lambda x: abs(float(x)) if pd.notna(x) else 0)
                
                df['periode'] = datetime.now().strftime('%Y-%m')
                
                # Pastikan nomen ada di master
                for nomen in df['nomen'].unique():
                    conn.execute(
                        "INSERT OR IGNORE INTO master_pelanggan (nomen) VALUES (?)",
                        (nomen,)
                    )
                conn.commit()
                
                # Insert/Update Ardebt (replace)
                cols = ['nomen', 'saldo_tunggakan', 'periode']
                df[cols].to_sql('ardebt', conn, if_exists='replace', index=False)
                
                flash(f'✅ Ardebt (Tunggakan) berhasil diupdate ({len(df)} data)', 'success')

        except Exception as e:
            flash(f'❌ Gagal Upload: {e}', 'danger')
            print(f"Error Upload: {e}")
            import traceback
            traceback.print_exc()
            
    return redirect(url_for('index'))

@app.route('/login')
@app.route('/logout')
def auth_bypass():
    return redirect(url_for('index'))

if __name__ == '__main__':
    if not os.path.exists(DB_PATH):
        init_db()
    print("🚀 APLIKASI SUNTER SIAP! (FIELD MAPPING LENGKAP)")
    print("="*60)
    print("📌 MC         : NOTAGIHAN → nomen (DATA INDUK)")
    print("📌 COLLECTION : NOTAG → nomen, PAY_DT → tgl_bayar")
    print("📌 MB         : NOTAGIHAN → nomen, TGL_BAYAR → tgl_bayar")
    print("📌 MAINBILL   : NOMEN → nomen, FREEZE_DT → tgl_tagihan")
    print("📌 ARDEBT     : NOMEN → nomen, SumOfJUMLAH → saldo_tunggakan")
    print("="*60)
    print("🎯 URUTAN UPLOAD:")
    print("   1. MC (Master Customer) - WAJIB PERTAMA")
    print("   2. Collection (Transaksi Current+Undue)")
    print("   3. MB (Master Bayar bulan lalu)")
    print("   4. MainBill (Tagihan bulan depan)")
    print("   5. Ardebt (Tunggakan)")
    print("="*60)
    app.run(host='0.0.0.0', port=5000, debug=True)
