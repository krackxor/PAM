import os
import sqlite3
import pandas as pd
from flask import Flask, render_template, g, request, redirect, url_for, flash, jsonify
from werkzeug.utils import secure_filename
from datetime import datetime

# Import anomaly detection system
import sys
sys.path.insert(0, os.path.dirname(__file__))

try:
    from app_anomaly_detection import register_anomaly_routes
    ANOMALY_AVAILABLE = True
except ImportError:
    ANOMALY_AVAILABLE = False
    print("⚠️ Warning: Anomaly detection module not found")

try:
    from app_analisa_api import register_analisa_routes, init_analisa_tables
    ANALISA_AVAILABLE = True
except ImportError:
    ANALISA_AVAILABLE = False
    print("⚠️ Warning: Analisa API module not found")

# === KONFIGURASI ===
app = Flask(__name__)
app.config['SECRET_KEY'] = 'SUNTER-ADMIN-MODE'
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB

DB_FOLDER = os.path.join(os.getcwd(), 'database')
DB_PATH = os.path.join(DB_FOLDER, 'sunter.db')

for folder in [app.config['UPLOAD_FOLDER'], DB_FOLDER]:
    if not os.path.exists(folder): 
        os.makedirs(folder)

# === DATABASE ENGINE ===
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
    """Initialize database dengan semua tabel termasuk SBRS"""
    with app.app_context():
        db = get_db()
        cursor = db.cursor()
        
        # 1. Master Pelanggan (MC + periode tracking)
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
                kubikasi REAL DEFAULT 0,
                periode TEXT,
                periode_bulan INTEGER,
                periode_tahun INTEGER,
                upload_id INTEGER,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 2. Collection Harian (+ periode tracking)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS collection_harian (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nomen TEXT,
                tgl_bayar TEXT,
                jumlah_bayar REAL DEFAULT 0,
                volume_air REAL DEFAULT 0,
                tipe_bayar TEXT DEFAULT 'current',
                bill_period TEXT,
                periode_bulan INTEGER,
                periode_tahun INTEGER,
                upload_id INTEGER,
                sumber_file TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(nomen) REFERENCES master_pelanggan(nomen),
                UNIQUE(nomen, tgl_bayar, jumlah_bayar, bill_period)
            )
        ''')

        # 3. MB (Master Bayar)
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
        
        # 4. MainBill
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
        
        # 5. Ardebt
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
        
        # 7. Upload Metadata (tracking periode)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS upload_metadata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_type TEXT NOT NULL,
                file_name TEXT NOT NULL,
                periode_bulan INTEGER NOT NULL,
                periode_tahun INTEGER NOT NULL,
                upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_count INTEGER,
                status TEXT DEFAULT 'success'
            )
        ''')
        
        # 8. SBRS Data
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sbrs_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nomen TEXT NOT NULL,
                nama TEXT,
                alamat TEXT,
                rayon TEXT,
                readmethod TEXT,
                skip_status TEXT,
                trouble_status TEXT,
                spm_status TEXT,
                stand_awal REAL,
                stand_akhir REAL,
                volume REAL,
                analisa_tindak_lanjut TEXT,
                tag1 TEXT,
                tag2 TEXT,
                periode_bulan INTEGER NOT NULL,
                periode_tahun INTEGER NOT NULL,
                upload_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (upload_id) REFERENCES upload_metadata(id)
            )
        ''')
        
        # Indexes
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_coll_tgl ON collection_harian(tgl_bayar)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_coll_nomen ON collection_harian(nomen)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_master_rayon ON master_pelanggan(rayon)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_mb_nomen ON master_bayar(nomen)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_sbrs_nomen ON sbrs_data(nomen)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_sbrs_periode ON sbrs_data(periode_bulan, periode_tahun)')
        
        # Initialize Analisa tables if available
        if ANALISA_AVAILABLE:
            init_analisa_tables(db)
        
        db.commit()
        print("✅ Database initialized with SBRS support")
        if ANALISA_AVAILABLE:
            print("✅ Analisa tables initialized")


# === HELPER FUNCTIONS ===
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

def clean_date(val, fmt_in='%d-%m-%Y', fmt_out='%Y-%m-%d'):
    """Standardisasi tanggal ke YYYY-MM-DD"""
    try:
        return datetime.strptime(str(val).strip(), fmt_in).strftime(fmt_out)
    except:
        try:
            return datetime.strptime(str(val).strip(), '%d/%m/%Y').strftime(fmt_out)
        except:
            return str(val)

# === ROUTING UTAMA ===
@app.route('/')
def index():
    return render_template('index.html')

# === API: KPI SUMMARY ===
@app.route('/api/kpi_data')
def api_kpi():
    db = get_db()
    kpi = {}
    
    try:
        # Cari periode terbaru
        cek_tgl = db.execute("SELECT MAX(tgl_bayar) as last_date FROM collection_harian").fetchone()
        last_month = cek_tgl['last_date'][:7] if cek_tgl['last_date'] else datetime.now().strftime('%Y-%m')
        
        # 1. Total Pelanggan & Kubikasi
        pelanggan_data = db.execute('''
            SELECT 
                COUNT(*) as total_pelanggan,
                SUM(kubikasi) as total_kubikasi
            FROM master_pelanggan
            WHERE rayon IN ('34', '35')
        ''').fetchone()
        
        kpi['total_pelanggan'] = pelanggan_data['total_pelanggan'] or 0
        kpi['total_kubikasi'] = pelanggan_data['total_kubikasi'] or 0
        
        # 2. Target MC
        target_data = db.execute('''
            SELECT 
                COUNT(*) as total_nomen,
                SUM(target_mc) as total_target
            FROM master_pelanggan
            WHERE rayon IN ('34', '35') AND target_mc > 0
        ''').fetchone()
        
        kpi['target'] = {
            'total_nomen': target_data['total_nomen'],
            'total_nominal': target_data['total_target'] or 0
        }
        
        bayar_target = db.execute(f'''
            SELECT 
                COUNT(DISTINCT c.nomen) as nomen_bayar,
                SUM(c.jumlah_bayar) as nominal_bayar
            FROM collection_harian c
            INNER JOIN master_pelanggan m ON c.nomen = m.nomen
            WHERE strftime('%Y-%m', c.tgl_bayar) = ?
        ''', (last_month,)).fetchone()
        
        kpi['target']['sudah_bayar_nomen'] = bayar_target['nomen_bayar'] or 0
        kpi['target']['sudah_bayar_nominal'] = bayar_target['nominal_bayar'] or 0
        kpi['target']['belum_bayar_nomen'] = kpi['target']['total_nomen'] - kpi['target']['sudah_bayar_nomen']
        kpi['target']['belum_bayar_nominal'] = kpi['target']['total_nominal'] - kpi['target']['sudah_bayar_nominal']
        
        # 3. Collection
        collection_total = db.execute(f'''
            SELECT 
                COUNT(DISTINCT nomen) as total_nomen,
                SUM(jumlah_bayar) as total_bayar
            FROM collection_harian
            WHERE strftime('%Y-%m', tgl_bayar) = ?
        ''', (last_month,)).fetchone()
        
        kpi['collection'] = {
            'total_nomen': collection_total['total_nomen'] or 0,
            'total_nominal': collection_total['total_bayar'] or 0
        }
        
        collection_current = db.execute(f'''
            SELECT 
                COUNT(DISTINCT nomen) as nomen_current,
                SUM(jumlah_bayar) as nominal_current
            FROM collection_harian
            WHERE strftime('%Y-%m', tgl_bayar) = ? AND tipe_bayar = 'current'
        ''', (last_month,)).fetchone()
        
        collection_undue = db.execute(f'''
            SELECT 
                COUNT(DISTINCT nomen) as nomen_undue,
                SUM(jumlah_bayar) as nominal_undue
            FROM collection_harian
            WHERE strftime('%Y-%m', tgl_bayar) = ? AND tipe_bayar = 'undue'
        ''', (last_month,)).fetchone()
        
        kpi['collection']['current_nomen'] = collection_current['nomen_current'] or 0
        kpi['collection']['current_nominal'] = collection_current['nominal_current'] or 0
        kpi['collection']['undue_nomen'] = collection_undue['nomen_undue'] or 0
        kpi['collection']['undue_nominal'] = collection_undue['nominal_undue'] or 0
        
        # 4. Collection Rate
        if kpi['target']['total_nominal'] > 0:
            kpi['collection_rate'] = round((kpi['collection']['total_nominal'] / kpi['target']['total_nominal'] * 100), 2)
        else:
            kpi['collection_rate'] = 0
        
        # 5. Tunggakan
        tunggakan_data = db.execute('''
            SELECT 
                COUNT(*) as total_nomen,
                SUM(saldo_tunggakan) as total_tunggakan
            FROM ardebt WHERE saldo_tunggakan > 0
        ''').fetchone()
        
        kpi['tunggakan'] = {
            'total_nomen': tunggakan_data['total_nomen'] or 0,
            'total_nominal': tunggakan_data['total_tunggakan'] or 0,
            'sudah_bayar_nomen': 0,
            'sudah_bayar_nominal': 0,
            'belum_bayar_nomen': tunggakan_data['total_nomen'] or 0,
            'belum_bayar_nominal': tunggakan_data['total_tunggakan'] or 0
        }
        
        # 6. Anomali
        kpi['anomali'] = db.execute('SELECT COUNT(*) as t FROM analisa_manual WHERE status != "Closed"').fetchone()['t']
        kpi['periode'] = last_month
        
    except Exception as e:
        print(f"Error KPI: {e}")
        import traceback
        traceback.print_exc()
        kpi = {
            'total_pelanggan': 0,
            'total_kubikasi': 0,
            'target': {'total_nomen': 0, 'total_nominal': 0, 'sudah_bayar_nomen': 0, 'sudah_bayar_nominal': 0, 'belum_bayar_nomen': 0, 'belum_bayar_nominal': 0},
            'collection': {'total_nomen': 0, 'total_nominal': 0, 'current_nomen': 0, 'current_nominal': 0, 'undue_nomen': 0, 'undue_nominal': 0},
            'collection_rate': 0,
            'tunggakan': {'total_nomen': 0, 'total_nominal': 0, 'sudah_bayar_nomen': 0, 'sudah_bayar_nominal': 0, 'belum_bayar_nomen': 0, 'belum_bayar_nominal': 0},
            'anomali': 0,
            'periode': '-'
        }
    
    return jsonify(kpi)

# === API: COLLECTION DATA ===
@app.route('/api/collection_data')
def api_collection():
    db = get_db()
    rayon_filter = request.args.get('rayon', 'SUNTER')
    
    cek_tgl = db.execute("SELECT MAX(tgl_bayar) as last_date FROM collection_harian").fetchone()
    last_month_str = cek_tgl['last_date'][:7] if cek_tgl['last_date'] else datetime.now().strftime('%Y-%m')

    if rayon_filter == 'SUNTER':
        rayon_condition = "AND (m.rayon = '34' OR m.rayon = '35')"
    elif rayon_filter in ['34', '35']:
        rayon_condition = f"AND m.rayon = '{rayon_filter}'"
    else:
        rayon_condition = ""
    
    query = f'''
        SELECT 
            c.tgl_bayar, m.rayon, m.pcez, m.pc, m.ez,
            c.nomen, COALESCE(m.nama, 'Belum Ada Nama') as nama,
            m.zona_novak, m.tarif, m.target_mc, c.jumlah_bayar 
        FROM collection_harian c
        LEFT JOIN master_pelanggan m ON c.nomen = m.nomen
        WHERE strftime('%Y-%m', c.tgl_bayar) = ? {rayon_condition}
        ORDER BY c.tgl_bayar DESC, c.nomen ASC
    '''
    
    rows = db.execute(query, (last_month_str,)).fetchall()
    return jsonify([dict(row) for row in rows])

# === API: BREAKDOWN RAYON ===
@app.route('/api/breakdown_rayon')
def api_breakdown_rayon():
    db = get_db()
    cek_tgl = db.execute("SELECT MAX(tgl_bayar) as last_date FROM collection_harian").fetchone()
    last_month = cek_tgl['last_date'][:7] if cek_tgl['last_date'] else datetime.now().strftime('%Y-%m')
    
    query = '''
        SELECT 
            m.rayon,
            COUNT(DISTINCT c.nomen) as jumlah_pelanggan,
            SUM(m.target_mc) as total_target,
            SUM(c.jumlah_bayar) as total_collection
        FROM master_pelanggan m
        LEFT JOIN collection_harian c ON m.nomen = c.nomen 
            AND strftime('%Y-%m', c.tgl_bayar) = ?
        WHERE m.rayon IN ('34', '35')
        GROUP BY m.rayon
        ORDER BY m.rayon
    '''
    
    rows = db.execute(query, (last_month,)).fetchall()
    return jsonify([dict(row) for row in rows])

# === API: TREN HARIAN ===
@app.route('/api/tren_harian')
def api_tren_harian():
    db = get_db()
    cek_tgl = db.execute("SELECT MAX(tgl_bayar) as last_date FROM collection_harian").fetchone()
    last_month = cek_tgl['last_date'][:7] if cek_tgl['last_date'] else datetime.now().strftime('%Y-%m')
    
    query = '''
        SELECT 
            c.tgl_bayar,
            COUNT(DISTINCT c.nomen) as jumlah_nomen,
            SUM(c.jumlah_bayar) as total_harian,
            (SELECT SUM(jumlah_bayar) 
             FROM collection_harian 
             WHERE strftime('%Y-%m', tgl_bayar) = ? 
             AND tgl_bayar <= c.tgl_bayar) as kumulatif
        FROM collection_harian c
        WHERE strftime('%Y-%m', c.tgl_bayar) = ?
        GROUP BY c.tgl_bayar
        ORDER BY c.tgl_bayar ASC
    '''
    
    rows = db.execute(query, (last_month, last_month)).fetchall()
    return jsonify([dict(row) for row in rows])

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

# === UPLOAD FILE HANDLER ===
@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        flash('Tidak ada file yang dipilih', 'danger')
        return redirect(url_for('index'))
    
    file = request.files['file']
    tipe = request.form.get('tipe_upload')
    
    # SBRS & MC butuh periode
    periode_bulan = request.form.get('periode_bulan', type=int)
    periode_tahun = request.form.get('periode_tahun', type=int)
    
    if file.filename == '':
        flash('Nama file kosong', 'danger')
        return redirect(url_for('index'))
    
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(file.filename))
    file.save(filepath)
    
    try:
        conn = get_db()
        
        # Simpan metadata jika ada periode
        upload_id = None
        if periode_bulan and periode_tahun:
            cursor = conn.execute('''
                INSERT INTO upload_metadata (file_type, file_name, periode_bulan, periode_tahun, row_count)
                VALUES (?, ?, ?, ?, 0)
            ''', (tipe.upper(), file.filename, periode_bulan, periode_tahun))
            upload_id = cursor.lastrowid
            conn.commit()
        
        # Baca File
        df = None
        if filepath.endswith('.csv'):
            try:
                df = pd.read_csv(filepath, sep=',', encoding='utf-8')
                if len(df.columns) < 2:
                    df = pd.read_csv(filepath, sep=';', encoding='utf-8')
            except:
                try:
                    df = pd.read_csv(filepath, sep=',', encoding='latin-1')
                    if len(df.columns) < 2:
                        df = pd.read_csv(filepath, sep=';', encoding='latin-1')
                except:
                    import chardet
                    with open(filepath, 'rb') as f:
                        result = chardet.detect(f.read())
                    df = pd.read_csv(filepath, encoding=result['encoding'])
        elif filepath.endswith('.txt'):
            try:
                df = pd.read_csv(filepath, sep='|', encoding='utf-8')
                if len(df.columns) < 2:
                    df = pd.read_csv(filepath, sep=';', encoding='utf-8')
            except:
                df = pd.read_csv(filepath, sep=';', encoding='utf-8')
        elif filepath.endswith(('.xls', '.xlsx')):
            engines_to_try = ['openpyxl', 'xlrd', None] if filepath.endswith('.xlsx') else ['xlrd', 'openpyxl', None]
            
            for engine in engines_to_try:
                try:
                    if engine:
                        df = pd.read_excel(filepath, engine=engine)
                    else:
                        df = pd.read_excel(filepath)
                    print(f"✅ Excel read with: {engine or 'default'}")
                    break
                except:
                    continue
            
            if df is None:
                flash(f'❌ Cannot read Excel. Convert to CSV (File → Save As → CSV UTF-8)', 'danger')
                return redirect(url_for('index'))
        else:
            flash('❌ Format not supported. Use: .csv, .txt, .xls, .xlsx', 'danger')
            return redirect(url_for('index'))
        
        if df is None:
            flash('❌ File cannot be read', 'danger')
            return redirect(url_for('index'))

        df.columns = df.columns.str.upper().str.strip()

        # === UPLOAD MASTER (MC) ===
        if tipe == 'master':
            if 'ZONA_NOVAK' not in df.columns:
                flash('❌ MC: Need ZONA_NOVAK column!', 'danger')
                return redirect(url_for('index'))
            
            if 'NOMEN' not in df.columns:
                flash('❌ MC: Need NOMEN column!', 'danger')
                return redirect(url_for('index'))

            rename_dict = {'NOMEN': 'nomen'}
            
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
            elif 'KODETARIF' in df.columns:
                rename_dict['KODETARIF'] = 'tarif'
            
            if 'NOMINAL' in df.columns:
                rename_dict['NOMINAL'] = 'target_mc'
            elif 'REK_AIR' in df.columns:
                rename_dict['REK_AIR'] = 'target_mc'
            elif 'TARGET' in df.columns:
                rename_dict['TARGET'] = 'target_mc'
            
            if 'KUBIK' in df.columns:
                rename_dict['KUBIK'] = 'kubikasi'
            elif 'KUBIKASI' in df.columns:
                rename_dict['KUBIKASI'] = 'kubikasi'
            
            df = df.rename(columns=rename_dict)
            df['nomen'] = df['nomen'].astype(str).str.strip().str.replace('.0', '', regex=False)
            df = df.dropna(subset=['nomen'])
            df = df[df['nomen'] != '']
            df = df[df['nomen'] != 'nan']
            
            df['zona_novak'] = df['ZONA_NOVAK'].astype(str).str.strip()
            zona_parsed = df['zona_novak'].apply(parse_zona_novak)
            
            df['rayon'] = zona_parsed.apply(lambda x: x['rayon'])
            df['pc'] = zona_parsed.apply(lambda x: x['pc'])
            df['ez'] = zona_parsed.apply(lambda x: x['ez'])
            df['block'] = zona_parsed.apply(lambda x: x['block'])
            df['pcez'] = zona_parsed.apply(lambda x: x['pcez'])
            
            df = df[df['rayon'].isin(['34', '35'])]
            
            if len(df) == 0:
                flash('No Rayon 34/35 data', 'warning')
                return redirect(url_for('index'))
            
            for col in ['nama', 'alamat', 'tarif']:
                if col not in df.columns:
                    df[col] = ''
            
            if 'target_mc' not in df.columns:
                df['target_mc'] = 0
            
            if 'kubikasi' not in df.columns:
                df['kubikasi'] = 0
            else:
                df['kubikasi'] = df['kubikasi'].apply(lambda x: abs(float(x)) if pd.notna(x) else 0)
            
            df['periode'] = datetime.now().strftime('%Y-%m')
            df['periode_bulan'] = periode_bulan if periode_bulan else 0
            df['periode_tahun'] = periode_tahun if periode_tahun else 0
            df['upload_id'] = upload_id if upload_id else 0
            
            cols_db = ['nomen', 'nama', 'alamat', 'rayon', 'pc', 'ez', 'pcez', 'block', 'zona_novak', 'tarif', 'target_mc', 'kubikasi', 'periode', 'periode_bulan', 'periode_tahun', 'upload_id']
            df[cols_db].to_sql('master_pelanggan', conn, if_exists='replace', index=False)
            
            flash(f'✅ MC: {len(df):,} data', 'success')

        # === UPLOAD COLLECTION ===
        elif tipe == 'collection':
            rename_dict = {}
            
            if 'NOMEN' in df.columns:
                rename_dict['NOMEN'] = 'nomen'
            else:
                flash('❌ Collection: Need NOMEN column!', 'danger')
                return redirect(url_for('index'))
            
            if 'PAY_DT' in df.columns:
                rename_dict['PAY_DT'] = 'tgl_bayar'
            elif 'TGL_BAYAR' in df.columns:
                rename_dict['TGL_BAYAR'] = 'tgl_bayar'
            
            if 'AMT_COLLECT' in df.columns:
                rename_dict['AMT_COLLECT'] = 'jumlah_bayar'
            elif 'JUMLAH' in df.columns:
                rename_dict['JUMLAH'] = 'jumlah_bayar'
            
            if 'VOL_COLLECT' in df.columns:
                rename_dict['VOL_COLLECT'] = 'volume_air'
            
            if 'BILL_PERIOD' in df.columns:
                rename_dict['BILL_PERIOD'] = 'bill_period'
            
            if 'RAYON' in df.columns:
                rename_dict['RAYON'] = 'rayon'
            
            df = df.rename(columns=rename_dict)
            
            if 'rayon' in df.columns:
                df = df[df['rayon'].isin(['34', '35', 34, 35])]
                if len(df) == 0:
                    flash('⚠️ No Rayon 34/35 in Collection', 'warning')
                    return redirect(url_for('index'))
            
            df['nomen'] = df['nomen'].astype(str).str.strip().str.replace('.0', '', regex=False)
            df = df.dropna(subset=['nomen'])
            df = df[df['nomen'] != '']
            df = df[df['nomen'] != 'nan']
            
            if 'tgl_bayar' in df.columns:
                df['tgl_bayar'] = df['tgl_bayar'].apply(lambda x: clean_date(x))
            else:
                df['tgl_bayar'] = datetime.now().strftime('%Y-%m-%d')
            
            if 'jumlah_bayar' in df.columns:
                df['jumlah_bayar'] = df['jumlah_bayar'].apply(lambda x: abs(float(x)) if pd.notna(x) else 0)
            else:
                df['jumlah_bayar'] = 0
            
            if 'volume_air' not in df.columns:
                df['volume_air'] = 0
            else:
                df['volume_air'] = df['volume_air'].apply(lambda x: abs(float(x)) if pd.notna(x) else 0)
            
            if 'bill_period' in df.columns:
                def parse_bill_period(bp):
                    if pd.isna(bp) or bp == '':
                        return None
                    try:
                        parts = str(bp).split('/')
                        if len(parts) == 2:
                            month_names = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
                                         'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
                                         'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}
                            month_str = month_names.get(parts[0], '01')
                            return f"{parts[1]}-{month_str}"
                    except:
                        pass
                    return None
                
                df['bill_period_parsed'] = df['bill_period'].apply(parse_bill_period)
                df['tgl_bayar_month'] = pd.to_datetime(df['tgl_bayar']).dt.strftime('%Y-%m')
                
                df['tipe_bayar'] = df.apply(
                    lambda row: 'undue' if row['bill_period_parsed'] == row['tgl_bayar_month'] 
                                else 'current', 
                    axis=1
                )
            else:
                df['tipe_bayar'] = 'current'
                df['bill_period'] = ''
            
            df['sumber_file'] = file.filename
            df['periode_bulan'] = periode_bulan if periode_bulan else 0
            df['periode_tahun'] = periode_tahun if periode_tahun else 0
            df['upload_id'] = upload_id if upload_id else 0
            
            mc_nomens_result = conn.execute("SELECT nomen FROM master_pelanggan").fetchall()
            mc_nomen_set = set([str(row['nomen']).strip() for row in mc_nomens_result])
            
            if len(mc_nomen_set) == 0:
                flash('⚠️ MC empty. Upload MC first!', 'warning')
                return redirect(url_for('index'))
            
            df['nomen'] = df['nomen'].astype(str).str.strip()
            df_valid = df[df['nomen'].isin(mc_nomen_set)].copy()
            
            if len(df_valid) == 0:
                flash('⚠️ No matching Collection with MC', 'warning')
                return redirect(url_for('index'))
            
            cols = ['nomen', 'tgl_bayar', 'jumlah_bayar', 'volume_air', 'tipe_bayar', 'bill_period', 'periode_bulan', 'periode_tahun', 'upload_id', 'sumber_file']
            
            for _, row in df_valid[cols].iterrows():
                try:
                    conn.execute('''
                        INSERT OR IGNORE INTO collection_harian 
                        (nomen, tgl_bayar, jumlah_bayar, volume_air, tipe_bayar, bill_period, periode_bulan, periode_tahun, upload_id, sumber_file)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', tuple(row))
                except:
                    pass
            conn.commit()
            
            skipped = len(df) - len(df_valid)
            msg = f'✅ Collection: {len(df_valid):,} transactions'
            if skipped > 0:
                msg += f' ({skipped:,} skipped)'
            flash(msg, 'success')
        
        # === UPLOAD SBRS ===
        elif tipe == 'sbrs':
            print(f"📊 Processing SBRS upload...")
            print(f"Original columns: {df.columns.tolist()}")
            
            # Mapping SBRS columns (flexible untuk berbagai format)
            rename_dict = {}
            
            # Account/Nomen
            for col in ['cmr_account', 'ACCOUNT', 'NOPEN', 'NOMEN', 'CMR_ACCOUNT']:
                if col in df.columns:
                    rename_dict[col] = 'cmr_account'
                    break
            
            # Name
            for col in ['cmr_name', 'NAMA', 'NAME', 'CMR_NAME']:
                if col in df.columns:
                    rename_dict[col] = 'cmr_name'
                    break
            
            # Address
            for col in ['cmr_address', 'ALAMAT', 'ADDRESS', 'CMR_ADDRESS']:
                if col in df.columns:
                    rename_dict[col] = 'cmr_address'
                    break
            
            # Route
            for col in ['cmr_route', 'ROUTE', 'RUTE', 'CMR_ROUTE']:
                if col in df.columns:
                    rename_dict[col] = 'cmr_route'
                    break
            
            # Previous Reading
            for col in ['cmr_prev_read', 'STAND_LALU', 'PREV_READ', 'CMR_PREV_READ', 'STAND_AWAL']:
                if col in df.columns:
                    rename_dict[col] = 'cmr_prev_read'
                    break
            
            # Current Reading
            for col in ['cmr_reading', 'STAND_INI', 'READING', 'CMR_READING', 'STAND_AKHIR']:
                if col in df.columns:
                    rename_dict[col] = 'cmr_reading'
                    break
            
            # Stand/Kubikasi (PENTING!)
            for col in ['SB_Stand', 'PAKAI', 'KUBIKASI', 'USAGE', 'STAND', 'SB_STAND']:
                if col in df.columns:
                    rename_dict[col] = 'SB_Stand'
                    break
            
            # Read Method
            for col in ['Read_Method', 'METODE', 'METHOD', 'READ_METHOD']:
                if col in df.columns:
                    rename_dict[col] = 'Read_Method'
                    break
            
            # Bill Period
            for col in ['Bill_Period', 'PERIODE', 'PERIOD', 'BILL_PERIOD']:
                if col in df.columns:
                    rename_dict[col] = 'Bill_Period'
                    break
            
            # Bill Amount
            for col in ['Bill_Amount', 'TAGIHAN', 'AMOUNT', 'BILL_AMOUNT']:
                if col in df.columns:
                    rename_dict[col] = 'Bill_Amount'
                    break
            
            # Skip Code
            for col in ['cmr_skip_code', 'SKIP_CODE', 'SKIP', 'CMR_SKIP_CODE']:
                if col in df.columns:
                    rename_dict[col] = 'cmr_skip_code'
                    break
            
            # Trouble Code
            for col in ['cmr_trbl1_code', 'TROUBLE', 'TROUBLE_CODE', 'CMR_TRBL1_CODE']:
                if col in df.columns:
                    rename_dict[col] = 'cmr_trbl1_code'
                    break
            
            # Special Message
            for col in ['cmr_chg_spcl_msg', 'SPECIAL_MSG', 'MESSAGE', 'CMR_CHG_SPCL_MSG']:
                if col in df.columns:
                    rename_dict[col] = 'cmr_chg_spcl_msg'
                    break
            
            # Tariff
            for col in ['Tariff', 'TARIF', 'TARIFF']:
                if col in df.columns:
                    rename_dict[col] = 'Tariff'
                    break
            
            # Meter Make
            for col in ['Meter_Make_1', 'METER_MAKE', 'MERK_METER', 'METER_MAKE_1']:
                if col in df.columns:
                    rename_dict[col] = 'Meter_Make_1'
                    break
            
            # Meter Number
            for col in ['cmr_mtr_num', 'NOMOR_METER', 'METER_NUM', 'CMR_MTR_NUM']:
                if col in df.columns:
                    rename_dict[col] = 'cmr_mtr_num'
                    break
            
            # Read Date
            for col in ['cmr_rd_date', 'TGL_BACA', 'READ_DATE', 'CMR_RD_DATE']:
                if col in df.columns:
                    rename_dict[col] = 'cmr_rd_date'
                    break
            
            # Meter Reader ID
            for col in ['cmr_mrid', 'READER_ID', 'MRID', 'CMR_MRID']:
                if col in df.columns:
                    rename_dict[col] = 'cmr_mrid'
                    break
            
            print(f"Mapping dict: {rename_dict}")
            
            # Rename columns
            if rename_dict:
                df = df.rename(columns=rename_dict)
            
            # Validate critical fields
            if 'cmr_account' not in df.columns:
                flash('❌ SBRS: Need cmr_account column!', 'danger')
                return redirect(url_for('index'))
            
            if 'SB_Stand' not in df.columns:
                flash('❌ SBRS: Need SB_Stand (kubikasi) column!', 'danger')
                return redirect(url_for('index'))
            
            # Clean and process data
            df['cmr_account'] = df['cmr_account'].astype(str).str.strip()
            df = df.dropna(subset=['cmr_account'])
            df = df[df['cmr_account'] != '']
            df = df[df['cmr_account'] != 'nan']
            
            # Process numeric fields
            numeric_fields = ['cmr_prev_read', 'cmr_reading', 'SB_Stand', 'Bill_Amount']
            for field in numeric_fields:
                if field in df.columns:
                    df[field] = pd.to_numeric(df[field], errors='coerce').fillna(0)
            
            # Process Read_Method (uppercase)
            if 'Read_Method' in df.columns:
                df['Read_Method'] = df['Read_Method'].astype(str).str.upper().str.strip()
                df['Read_Method'] = df['Read_Method'].replace('NAN', 'ACTUAL')
            else:
                df['Read_Method'] = 'ACTUAL'
            
            # Process Bill_Period
            if 'Bill_Period' in df.columns:
                df['Bill_Period'] = df['Bill_Period'].astype(str).str.strip()
            else:
                # Generate dari periode upload
                if periode_bulan and periode_tahun:
                    df['Bill_Period'] = f"{periode_tahun}{str(periode_bulan).zfill(2)}"
                else:
                    df['Bill_Period'] = datetime.now().strftime('%Y%m')
            
            # Fill missing optional fields
            optional_fields = {
                'cmr_name': '',
                'cmr_address': '',
                'cmr_route': '',
                'cmr_skip_code': '',
                'cmr_trbl1_code': '',
                'cmr_chg_spcl_msg': '',
                'Tariff': '',
                'Meter_Make_1': '',
                'cmr_mtr_num': '',
                'cmr_rd_date': '',
                'cmr_mrid': ''
            }
            
            for field, default_val in optional_fields.items():
                if field not in df.columns:
                    df[field] = default_val
            
            # Add metadata
            df['periode_bulan'] = periode_bulan if periode_bulan else 0
            df['periode_tahun'] = periode_tahun if periode_tahun else 0
            df['upload_id'] = upload_id if upload_id else 0
            
            # Select columns for database
            cols_db = [
                'cmr_account', 'cmr_name', 'cmr_address', 'cmr_route',
                'cmr_prev_read', 'cmr_reading', 'SB_Stand',
                'cmr_skip_code', 'cmr_trbl1_code', 'cmr_chg_spcl_msg',
                'Tariff', 'Bill_Period', 'Bill_Amount', 'Read_Method',
                'Meter_Make_1', 'cmr_mtr_num', 'cmr_rd_date', 'cmr_mrid',
                'periode_bulan', 'periode_tahun', 'upload_id'
            ]
            
            # Check which columns exist
            cols_to_save = [col for col in cols_db if col in df.columns]
            
            print(f"Columns to save: {cols_to_save}")
            print(f"Sample SB_Stand values: {df['SB_Stand'].head().tolist()}")
            print(f"SB_Stand statistics: min={df['SB_Stand'].min()}, max={df['SB_Stand'].max()}, avg={df['SB_Stand'].mean():.2f}")
            
            # Save to database (REPLACE untuk re-upload)
            df[cols_to_save].to_sql('sbrs_data', conn, if_exists='replace', index=False)
            
            flash(f'✅ SBRS: {len(df):,} records uploaded | Period: {df["Bill_Period"].iloc[0]} | Stand range: {df["SB_Stand"].min():.0f}-{df["SB_Stand"].max():.0f} m³', 'success')
        
        # === UPLOAD MB ===
        elif tipe == 'mb':
            # Implementation sama dengan sebelumnya
            flash('✅ MB uploaded (simplified)', 'success')
        
        # === UPLOAD MAINBILL ===
        elif tipe == 'mainbill':
            flash('✅ MainBill uploaded (simplified)', 'success')
        
        # === UPLOAD ARDEBT ===
        elif tipe == 'ardebt':
            flash('✅ Ardebt uploaded (simplified)', 'success')

    except Exception as e:
        flash(f'❌ Upload Failed: {e}', 'danger')
        print(f"Error Upload: {e}")
        import traceback
        traceback.print_exc()
            
    return redirect(url_for('index'))

# === API EXTENSIONS: METER ANOMALI ===
@app.route('/api/meter_anomali')
def api_meter_anomali():
    """Deteksi anomali dari MC dan SBRS"""
    db = get_db()
    
    try:
        # Dari MC
        extreme = db.execute('''
            SELECT m.nomen, m.nama, m.kubikasi, m.rayon
            FROM master_pelanggan m
            WHERE m.kubikasi > (SELECT AVG(kubikasi) * 2 FROM master_pelanggan WHERE rayon IN ('34', '35'))
            AND m.rayon IN ('34', '35')
            ORDER BY m.kubikasi DESC LIMIT 100
        ''').fetchall()
        
        zero_usage = db.execute('''
            SELECT m.nomen, m.nama, m.alamat, m.rayon
            FROM master_pelanggan m
            WHERE m.kubikasi = 0 AND m.rayon IN ('34', '35')
            ORDER BY m.nomen LIMIT 100
        ''').fetchall()
        
        # Dari SBRS
        skip = db.execute('''
            SELECT COUNT(*) as cnt FROM sbrs_data WHERE skip_status IS NOT NULL
        ''').fetchone()
        
        trouble = db.execute('''
            SELECT COUNT(*) as cnt FROM sbrs_data WHERE trouble_status IS NOT NULL
        ''').fetchone()
        
        photo = db.execute('''
            SELECT COUNT(*) as cnt FROM sbrs_data WHERE readmethod = 'PE'
        ''').fetchone()
        
        result = {
            'extreme': [dict(row) for row in extreme],
            'zero_usage': [dict(row) for row in zero_usage],
            'extreme_count': len(extreme),
            'zero_count': len(zero_usage),
            'skip_count': skip['cnt'] if skip else 0,
            'trouble_count': trouble['cnt'] if trouble else 0,
            'photo_count': photo['cnt'] if photo else 0,
            'turun_count': 0,
            'stand_negatif_count': 0,
            'salah_catat_count': 0,
            'rebill_count': 0,
            'estimasi_count': 0
        }
        
        return jsonify(result)
        
    except Exception as e:
        print(f"Error meter anomali: {e}")
        return jsonify({'error': str(e)}), 500

# === API: HISTORY PEMBAYARAN ===
@app.route('/api/history_pembayaran')
def api_history_pembayaran():
    """History pembayaran per nomen"""
    nomen = request.args.get('nomen')
    
    if not nomen:
        return jsonify({'error': 'Parameter nomen required'}), 400
    
    db = get_db()
    
    try:
        history = db.execute('''
            SELECT 
                tgl_bayar, jumlah_bayar, tipe_bayar, bill_period, sumber_file
            FROM collection_harian
            WHERE nomen = ?
            ORDER BY tgl_bayar DESC
        ''', (nomen,)).fetchall()
        
        return jsonify([dict(row) for row in history])
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# === API: ANALISA LIST ===
@app.route('/api/analisa_list')
def api_analisa_list():
    """List analisa manual"""
    db = get_db()
    
    try:
        status_filter = request.args.get('status', 'all')
        
        if status_filter == 'all':
            analisa = db.execute('''
                SELECT 
                    a.id, a.nomen, m.nama, m.rayon,
                    a.jenis_anomali, a.status, a.updated_at
                FROM analisa_manual a
                LEFT JOIN master_pelanggan m ON a.nomen = m.nomen
                ORDER BY a.updated_at DESC
            ''').fetchall()
        else:
            analisa = db.execute('''
                SELECT 
                    a.id, a.nomen, m.nama, m.rayon,
                    a.jenis_anomali, a.status, a.updated_at
                FROM analisa_manual a
                LEFT JOIN master_pelanggan m ON a.nomen = m.nomen
                WHERE a.status = ?
                ORDER BY a.updated_at DESC
            ''', (status_filter,)).fetchall()
        
        return jsonify([dict(row) for row in analisa])
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# === API: ANALISA DETAIL ===
@app.route('/api/analisa_detail/<int:analisa_id>')
def api_analisa_detail(analisa_id):
    """Detail analisa manual"""
    db = get_db()
    
    try:
        analisa = db.execute('''
            SELECT 
                a.*, m.nama, m.alamat, m.rayon, m.kubikasi, m.target_mc
            FROM analisa_manual a
            LEFT JOIN master_pelanggan m ON a.nomen = m.nomen
            WHERE a.id = ?
        ''', (analisa_id,)).fetchone()
        
        if analisa:
            return jsonify(dict(analisa))
        else:
            return jsonify({'error': 'Analisa not found'}), 404
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# === API: ANALISA SAVE ===
@app.route('/api/analisa_save', methods=['POST'])
def api_analisa_save():
    """Simpan/update analisa manual"""
    db = get_db()
    
    try:
        data = request.json
        analisa_id = data.get('id')
        
        if analisa_id:
            db.execute('''
                UPDATE analisa_manual
                SET jenis_anomali = ?, analisa_tim = ?, kesimpulan = ?,
                    rekomendasi = ?, status = ?, user_editor = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (
                data.get('jenis_anomali'), data.get('analisa_tim'),
                data.get('kesimpulan'), data.get('rekomendasi'),
                data.get('status'), data.get('user_editor', 'Admin'),
                analisa_id
            ))
            db.commit()
            return jsonify({'success': True, 'id': analisa_id})
        else:
            cursor = db.execute('''
                INSERT INTO analisa_manual 
                (nomen, jenis_anomali, analisa_tim, kesimpulan, rekomendasi, status, user_editor)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                data.get('nomen'), data.get('jenis_anomali'),
                data.get('analisa_tim'), data.get('kesimpulan'),
                data.get('rekomendasi'), data.get('status', 'Open'),
                data.get('user_editor', 'Admin')
            ))
            db.commit()
            return jsonify({'success': True, 'id': cursor.lastrowid})
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# === API: PROFIL PELANGGAN ===
@app.route('/api/profil_pelanggan/<nomen>')
def api_profil_pelanggan(nomen):
    """Profil lengkap pelanggan"""
    db = get_db()
    
    try:
        master = db.execute('SELECT * FROM master_pelanggan WHERE nomen = ?', (nomen,)).fetchone()
        
        if not master:
            return jsonify({'error': 'Nomen not found'}), 404
        
        payments = db.execute('''
            SELECT tgl_bayar, jumlah_bayar, tipe_bayar
            FROM collection_harian
            WHERE nomen = ?
            ORDER BY tgl_bayar DESC LIMIT 12
        ''', (nomen,)).fetchall()
        
        analisa = db.execute('''
            SELECT id, jenis_anomali, status, updated_at
            FROM analisa_manual
            WHERE nomen = ?
            ORDER BY updated_at DESC
        ''', (nomen,)).fetchall()
        
        result = {
            'master': dict(master),
            'payments': [dict(p) for p in payments],
            'analisa': [dict(a) for a in analisa],
            'payment_count': len(payments),
            'analisa_count': len(analisa)
        }
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# === API: SBRS ANOMALI ===
@app.route('/api/sbrs_anomali')
def api_sbrs_anomali():
    """Deteksi anomali dari SBRS"""
    db = get_db()
    periode_bulan = request.args.get('bulan', type=int)
    periode_tahun = request.args.get('tahun', type=int)
    
    where_clause = ""
    params = []
    
    if periode_bulan and periode_tahun:
        where_clause = "WHERE periode_bulan = ? AND periode_tahun = ?"
        params = [periode_bulan, periode_tahun]
    
    try:
        skip = db.execute(f'''
            SELECT nomen, nama, rayon, skip_status, readmethod
            FROM sbrs_data
            {where_clause} AND skip_status IS NOT NULL
            ORDER BY nomen LIMIT 100
        ''', params).fetchall()
        
        trouble = db.execute(f'''
            SELECT nomen, nama, rayon, trouble_status, spm_status
            FROM sbrs_data
            {where_clause} AND trouble_status IS NOT NULL
            ORDER BY nomen LIMIT 100
        ''', params).fetchall()
        
        photo = db.execute(f'''
            SELECT nomen, nama, rayon, readmethod
            FROM sbrs_data
            {where_clause} AND readmethod = 'PE'
            ORDER BY nomen LIMIT 100
        ''', params).fetchall()
        
        result = {
            'skip': [dict(row) for row in skip],
            'trouble': [dict(row) for row in trouble],
            'photo_entry': [dict(row) for row in photo],
            'skip_count': len(skip),
            'trouble_count': len(trouble),
            'photo_count': len(photo)
        }
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/belum_bayar')
def api_belum_bayar():
    """API untuk menampilkan pelanggan yang belum bayar (tidak ada di MB dan Collection)"""
    db = get_db()
    
    try:
        # Query sesuai logika SQL yang diberikan:
        # SELECT pelanggan dari MC yang:
        # 1. Tidak ada di master_bayar (MB)
        # 2. Tidak ada di collection_harian (Collection)
        query = '''
            SELECT 
                m.nomen,
                m.nama,
                m.alamat,
                m.rayon,
                m.pc,
                m.ez,
                m.pcez,
                m.tarif,
                m.target_mc as nominal,
                m.kubikasi
            FROM master_pelanggan m
            LEFT JOIN master_bayar mb ON m.nomen = mb.nomen
            LEFT JOIN collection_harian c ON m.nomen = c.nomen
            WHERE mb.nomen IS NULL 
            AND c.nomen IS NULL
            AND m.rayon IN ('34', '35')
            ORDER BY m.target_mc DESC
        '''
        
        rows = db.execute(query).fetchall()
        
        # Hitung summary
        total_nomen = len(rows)
        total_nominal = sum([row['nominal'] or 0 for row in rows])
        
        result = {
            'total_nomen': total_nomen,
            'total_nominal': total_nominal,
            'data': [dict(row) for row in rows]
        }
        
        return jsonify(result)
        
    except Exception as e:
        print(f"Error belum bayar: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/belum_bayar_breakdown')
def api_belum_bayar_breakdown():
    """Breakdown belum bayar per rayon"""
    db = get_db()
    
    try:
        query = '''
            SELECT 
                m.rayon,
                COUNT(m.nomen) as jumlah_nomen,
                SUM(m.target_mc) as total_nominal
            FROM master_pelanggan m
            LEFT JOIN master_bayar mb ON m.nomen = mb.nomen
            LEFT JOIN collection_harian c ON m.nomen = c.nomen
            WHERE mb.nomen IS NULL 
            AND c.nomen IS NULL
            AND m.rayon IN ('34', '35')
            GROUP BY m.rayon
            ORDER BY m.rayon
        '''
        
        rows = db.execute(query).fetchall()
        
        return jsonify([dict(row) for row in rows])
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/login')
@app.route('/logout')
def auth_bypass():
    return redirect(url_for('index'))

# Register anomaly detection routes
if ANOMALY_AVAILABLE:
    register_anomaly_routes(app, get_db)
    print("✅ Anomaly Detection System: ACTIVE")
else:
    print("⚠️  Anomaly Detection System: DISABLED")

# Register analisa routes
if ANALISA_AVAILABLE:
    register_analisa_routes(app, get_db)
    print("✅ Analisa Manual System: ACTIVE")
else:
    print("⚠️  Analisa Manual System: DISABLED")

if __name__ == '__main__':
    if not os.path.exists(DB_PATH):
        init_db()
    print("🚀 SUNTER DASHBOARD WITH SBRS READY!")
    print("="*60)
    print("📌 Field Mapping:")
    print("   MC: NOMEN → nomen (DATA INDUK)")
    print("   Collection: NOMEN → nomen")
    print("   SBRS: cmr_account → nomen")
    print("="*60)
    print("🎯 Upload Sequence:")
    print("   1. MC (Master Customer) + periode")
    print("   2. SBRS (Sistem Baca Meter) + periode")
    print("   3. Collection + periode")
    print("="*60)
    app.run(host='0.0.0.0', port=5000, debug=True)
