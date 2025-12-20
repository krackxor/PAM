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

# Pastikan folder ada
for folder in [app.config['UPLOAD_FOLDER'], DB_FOLDER]:
    if not os.path.exists(folder):
        os.makedirs(folder, exist_ok=True)

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
        val_str = str(val).strip()
        if val_str.endswith('.0'):
            return val_str[:-2]
        return val_str
    except:
        return str(val).strip()

def parse_zona_novak(zona):
    """Parse ZONA_NOVAK menjadi komponen"""
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
    """Standardisasi format tanggal"""
    try:
        val_str = str(val).strip()
        return datetime.strptime(val_str, fmt_in).strftime(fmt_out)
    except:
        try:
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
        cek_tgl = db.execute("SELECT MAX(tgl_bayar) as last_date FROM collection_harian").fetchone()
        last_month = cek_tgl['last_date'][:7] if cek_tgl and cek_tgl['last_date'] else datetime.now().strftime('%Y-%m')
        
        kpi['total_pelanggan'] = db.execute('SELECT COUNT(*) as t FROM master_pelanggan').fetchone()['t']
        
        target_data = db.execute('SELECT COUNT(*) as total_nomen, SUM(target_mc) as total_target FROM master_pelanggan WHERE target_mc > 0').fetchone()
        kpi['target'] = {'total_nomen': target_data['total_nomen'], 'total_nominal': target_data['total_target'] or 0}
        
        bayar_target = db.execute(f'''
            SELECT COUNT(DISTINCT c.nomen) as nomen_bayar, SUM(c.jumlah_bayar) as nominal_bayar
            FROM collection_harian c INNER JOIN master_pelanggan m ON c.nomen = m.nomen
            WHERE strftime('%Y-%m', c.tgl_bayar) = ?
        ''', (last_month,)).fetchone()
        
        kpi['target']['sudah_bayar_nomen'] = bayar_target['nomen_bayar'] or 0
        kpi['target']['sudah_bayar_nominal'] = bayar_target['nominal_bayar'] or 0
        kpi['target']['belum_bayar_nomen'] = kpi['target']['total_nomen'] - kpi['target']['sudah_bayar_nomen']
        kpi['target']['belum_bayar_nominal'] = kpi['target']['total_nominal'] - kpi['target']['sudah_bayar_nominal']
        
        collection_total = db.execute(f'''
            SELECT COUNT(DISTINCT nomen) as total_nomen, SUM(jumlah_bayar) as total_bayar
            FROM collection_harian WHERE strftime('%Y-%m', tgl_bayar) = ?
        ''', (last_month,)).fetchone()
        
        kpi['collection'] = {'total_nomen': collection_total['total_nomen'] or 0, 'total_nominal': collection_total['total_bayar'] or 0}
        
        total_coll = kpi['collection']['total_nominal']
        kpi['collection']['current_nomen'] = collection_total['total_nomen'] or 0
        kpi['collection']['current_nominal'] = int(total_coll * 0.9) 
        kpi['collection']['undue_nomen'] = 0 
        kpi['collection']['undue_nominal'] = int(total_coll * 0.1)
        
        if kpi['target']['total_nominal'] > 0:
            kpi['collection_rate'] = round((kpi['collection']['total_nominal'] / kpi['target']['total_nominal'] * 100), 2)
        else:
            kpi['collection_rate'] = 0
        
        tunggakan_data = db.execute('SELECT COUNT(*) as total_nomen, SUM(saldo_tunggakan) as total_tunggakan FROM ardebt WHERE saldo_tunggakan > 0').fetchone()
        kpi['tunggakan'] = {'total_nomen': tunggakan_data['total_nomen'] or 0, 'total_nominal': tunggakan_data['total_tunggakan'] or 0}
        
        kpi['tunggakan']['sudah_bayar_nomen'] = 0
        kpi['tunggakan']['sudah_bayar_nominal'] = 0
        kpi['tunggakan']['belum_bayar_nomen'] = kpi['tunggakan']['total_nomen']
        kpi['tunggakan']['belum_bayar_nominal'] = kpi['tunggakan']['total_nominal']
        
        kpi['anomali'] = db.execute('SELECT COUNT(*) as t FROM analisa_manual WHERE status != "Closed"').fetchone()['t']
        kpi['periode'] = last_month
        
    except Exception as e:
        print(f"Error KPI: {e}")
        kpi = {'total_pelanggan': 0, 'target': {'total_nomen': 0, 'total_nominal': 0}, 'collection': {'total_nomen': 0, 'total_nominal': 0}, 'collection_rate': 0, 'tunggakan': {'total_nomen': 0, 'total_nominal': 0}, 'anomali': 0, 'periode': '-'}
    
    return jsonify(kpi)

# API: Summary Table untuk Tab Collection (AB Sunter, 34, 35)
@app.route('/api/collection_summary_table')
def api_collection_summary_table():
    db = get_db()
    
    # Tentukan Bulan Berjalan
    cek_tgl = db.execute("SELECT MAX(tgl_bayar) as last_date FROM collection_harian").fetchone()
    last_month_str = cek_tgl['last_date'][:7] if cek_tgl and cek_tgl['last_date'] else datetime.now().strftime('%Y-%m')

    # Query Helper
    def get_stats(rayon_filter=None):
        where_clause = ""
        params = []
        
        if rayon_filter:
            where_clause = "AND m.rayon = ?"
            params.append(rayon_filter)
            
        # 1. Target (MC)
        q_target = f"SELECT COUNT(*) as cnt, SUM(target_mc) as nominal FROM master_pelanggan m WHERE target_mc > 0 {where_clause}"
        target = db.execute(q_target, params).fetchone()
        
        # 2. Realisasi (Collection)
        coll_params = [last_month_str]
        if rayon_filter:
            coll_params.append(rayon_filter)
            
        q_coll = f'''
            SELECT 
                COUNT(DISTINCT c.nomen) as cnt, 
                SUM(c.jumlah_bayar) as nominal 
            FROM collection_harian c
            JOIN master_pelanggan m ON c.nomen = m.nomen
            WHERE strftime('%Y-%m', c.tgl_bayar) = ? {where_clause}
        '''
        coll = db.execute(q_coll, coll_params).fetchone()
        
        return {
            'target_nomen': target['cnt'] or 0,
            'target_nominal': target['nominal'] or 0,
            'realisasi_nomen': coll['cnt'] or 0,
            'realisasi_nominal': coll['nominal'] or 0
        }

    # Hitung Data
    data_34 = get_stats('34')
    data_35 = get_stats('35')
    
    # Data Total (AB Sunter) adalah penjumlahan 34 + 35
    data_total = {
        'target_nomen': data_34['target_nomen'] + data_35['target_nomen'],
        'target_nominal': data_34['target_nominal'] + data_35['target_nominal'],
        'realisasi_nomen': data_34['realisasi_nomen'] + data_35['realisasi_nomen'],
        'realisasi_nominal': data_34['realisasi_nominal'] + data_35['realisasi_nominal']
    }

    # Format Output List
    result = [
        {'kategori': 'AB SUNTER (Total)', 'data': data_total, 'class': 'table-primary fw-bold'},
        {'kategori': 'Rayon 34', 'data': data_34, 'class': ''},
        {'kategori': 'Rayon 35', 'data': data_35, 'class': ''}
    ]
    
    return jsonify(result)

# API: DATA COLLECTION
@app.route('/api/collection_data')
def api_collection():
    db = get_db()
    rayon_filter = request.args.get('rayon', 'SUNTER')
    
    cek_tgl = db.execute("SELECT MAX(tgl_bayar) as last_date FROM collection_harian").fetchone()
    last_month_str = cek_tgl['last_date'][:7] if cek_tgl and cek_tgl['last_date'] else datetime.now().strftime('%Y-%m')

    if rayon_filter == 'SUNTER':
        rayon_condition = "AND (m.rayon = '34' OR m.rayon = '35')"
    elif rayon_filter in ['34', '35']:
        rayon_condition = f"AND m.rayon = '{rayon_filter}'"
    else:
        rayon_condition = ""
    
    query = f'''
        SELECT c.tgl_bayar, m.rayon, m.pcez, m.pc, m.ez, c.nomen, 
               COALESCE(m.nama, 'Belum Ada Nama') as nama, m.zona_novak, m.tarif, m.target_mc, c.jumlah_bayar 
        FROM collection_harian c
        LEFT JOIN master_pelanggan m ON c.nomen = m.nomen
        WHERE strftime('%Y-%m', c.tgl_bayar) = ? {rayon_condition}
        ORDER BY c.tgl_bayar DESC, c.nomen ASC
    '''
    rows = db.execute(query, (last_month_str,)).fetchall()
    return jsonify([dict(row) for row in rows])

# API: Breakdown per Rayon
@app.route('/api/breakdown_rayon')
def api_breakdown_rayon():
    db = get_db()
    cek_tgl = db.execute("SELECT MAX(tgl_bayar) as last_date FROM collection_harian").fetchone()
    last_month = cek_tgl['last_date'][:7] if cek_tgl and cek_tgl['last_date'] else datetime.now().strftime('%Y-%m')
    
    query = '''
        SELECT m.rayon, COUNT(DISTINCT c.nomen) as jumlah_pelanggan, SUM(m.target_mc) as total_target, SUM(c.jumlah_bayar) as total_collection
        FROM master_pelanggan m
        LEFT JOIN collection_harian c ON m.nomen = c.nomen AND strftime('%Y-%m', c.tgl_bayar) = ?
        WHERE m.rayon IN ('34', '35')
        GROUP BY m.rayon ORDER BY m.rayon
    '''
    rows = db.execute(query, (last_month,)).fetchall()
    return jsonify([dict(row) for row in rows])

# API: Tren Harian
@app.route('/api/tren_harian')
def api_tren_harian():
    db = get_db()
    cek_tgl = db.execute("SELECT MAX(tgl_bayar) as last_date FROM collection_harian").fetchone()
    last_month = cek_tgl['last_date'][:7] if cek_tgl and cek_tgl['last_date'] else datetime.now().strftime('%Y-%m')
    
    query = '''
        SELECT c.tgl_bayar, COUNT(DISTINCT c.nomen) as jumlah_nomen, SUM(c.jumlah_bayar) as total_harian,
               (SELECT SUM(jumlah_bayar) FROM collection_harian WHERE strftime('%Y-%m', tgl_bayar) = ? AND tgl_bayar <= c.tgl_bayar) as kumulatif
        FROM collection_harian c
        WHERE strftime('%Y-%m', c.tgl_bayar) = ?
        GROUP BY c.tgl_bayar ORDER BY c.tgl_bayar ASC
    '''
    rows = db.execute(query, (last_month, last_month)).fetchall()
    return jsonify([dict(row) for row in rows])

@app.route('/simpan_analisa', methods=['POST'])
def simpan_analisa():
    try:
        db = get_db()
        db.execute('''INSERT INTO analisa_manual (nomen, jenis_anomali, analisa_tim, kesimpulan, rekomendasi, status, user_editor) VALUES (?, ?, ?, ?, ?, ?, ?)''', 
                   (request.form['nomen'], request.form['jenis_anomali'], request.form['analisa_tim'], request.form['kesimpulan'], request.form['rekomendasi'], request.form['status'], "Admin"))
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
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        
        try:
            conn = get_db()
            try:
                if filepath.endswith('.csv') or filepath.endswith('.txt'):
                    try:
                        df = pd.read_csv(filepath, sep=',', encoding='utf-8', on_bad_lines='skip')
                        if len(df.columns) < 2: df = pd.read_csv(filepath, sep=';', encoding='utf-8', on_bad_lines='skip')
                        if len(df.columns) < 2: df = pd.read_csv(filepath, sep='|', encoding='utf-8', on_bad_lines='skip')
                    except:
                        df = pd.read_csv(filepath, sep=';', encoding='latin-1', on_bad_lines='skip')
                else:
                    df = pd.read_excel(filepath)
            except Exception as e:
                flash(f'Gagal membaca file: {e}', 'danger')
                return redirect(url_for('index'))

            df.columns = df.columns.str.upper().str.strip()

            if tipe == 'master':
                if 'ZONA_NOVAK' not in df.columns or 'NOTAGIHAN' not in df.columns:
                    flash('❌ Format MC salah! Wajib ada ZONA_NOVAK & NOTAGIHAN', 'danger')
                    return redirect(url_for('index'))
                
                rename_dict = {'NOTAGIHAN': 'nomen'}
                if 'NAMA_PEL' in df.columns: rename_dict['NAMA_PEL'] = 'nama'
                elif 'NAMA' in df.columns: rename_dict['NAMA'] = 'nama'
                if 'ALM1_PEL' in df.columns: rename_dict['ALM1_PEL'] = 'alamat'
                elif 'ALAMAT' in df.columns: rename_dict['ALAMAT'] = 'alamat'
                if 'TARIF' in df.columns: rename_dict['TARIF'] = 'tarif'
                elif 'KODETARIF' in df.columns: rename_dict['KODETARIF'] = 'tarif'
                if 'REK_AIR' in df.columns: rename_dict['REK_AIR'] = 'target_mc'
                elif 'TARGET' in df.columns: rename_dict['TARGET'] = 'target_mc'
                
                df = df.rename(columns=rename_dict)
                df['nomen'] = df['nomen'].apply(clean_nomen)
                df = df.dropna(subset=['nomen'])
                
                zona_parsed = df['ZONA_NOVAK'].astype(str).str.strip().apply(parse_zona_novak)
                df['rayon'] = zona_parsed.apply(lambda x: x['rayon'])
                df['pc'] = zona_parsed.apply(lambda x: x['pc'])
                df['ez'] = zona_parsed.apply(lambda x: x['ez'])
                df['block'] = zona_parsed.apply(lambda x: x['block'])
                df['pcez'] = zona_parsed.apply(lambda x: x['pcez'])
                df['zona_novak'] = df['ZONA_NOVAK']
                
                df = df[df['rayon'].isin(['34', '35'])]
                if len(df) == 0:
                    flash('Tidak ada data Rayon 34/35 dalam file MC', 'warning')
                    return redirect(url_for('index'))
                
                for col in ['nama', 'alamat', 'tarif']:
                    if col not in df.columns: df[col] = ''
                if 'target_mc' not in df.columns: df['target_mc'] = 0
                df['periode'] = datetime.now().strftime('%Y-%m')
                
                cols_db = ['nomen', 'nama', 'alamat', 'rayon', 'pc', 'ez', 'pcez', 'block', 'zona_novak', 'tarif', 'target_mc', 'periode']
                df[cols_db].to_sql('master_pelanggan', conn, if_exists='replace', index=False)
                flash(f'✅ MC: {len(df):,} data', 'success')

            elif tipe == 'collection':
                rename_dict = {}
                if 'NOTAG' in df.columns: rename_dict['NOTAG'] = 'nomen'
                elif 'NO_SAMBUNGAN' in df.columns: rename_dict['NO_SAMBUNGAN'] = 'nomen'
                else:
                    flash('❌ Format Collection salah! Butuh NOTAG', 'danger')
                    return redirect(url_for('index'))
                if 'PAY_DT' in df.columns: rename_dict['PAY_DT'] = 'tgl_bayar'
                elif 'TGL_BAYAR' in df.columns: rename_dict['TGL_BAYAR'] = 'tgl_bayar'
                if 'AMT_COLLECT' in df.columns: rename_dict['AMT_COLLECT'] = 'jumlah_bayar'
                elif 'JUMLAH' in df.columns: rename_dict['JUMLAH'] = 'jumlah_bayar'
                
                df = df.rename(columns=rename_dict)
                df['nomen'] = df['nomen'].apply(clean_nomen)
                df = df.dropna(subset=['nomen'])
                df['tgl_bayar'] = df['tgl_bayar'].apply(lambda x: clean_date(x)) if 'tgl_bayar' in df.columns else datetime.now().strftime('%Y-%m-%d')
                df['jumlah_bayar'] = df['jumlah_bayar'].apply(lambda x: abs(float(x)) if pd.notna(x) else 0) if 'jumlah_bayar' in df.columns else 0
                df['sumber_file'] = filename
                
                mc_nomens = set([str(r['nomen']).strip() for r in conn.execute("SELECT nomen FROM master_pelanggan").fetchall()])
                if not mc_nomens:
                    flash('⚠️ MC kosong. Upload MC terlebih dahulu!', 'warning')
                    return redirect(url_for('index'))
                
                df['nomen'] = df['nomen'].astype(str).str.strip()
                df_valid = df[df['nomen'].isin(mc_nomens)].copy()
                
                if len(df_valid) == 0:
                    flash('⚠️ Tidak ada data Collection yang cocok dengan MC.', 'warning')
                    return redirect(url_for('index'))
                
                df_valid[['nomen', 'tgl_bayar', 'jumlah_bayar', 'sumber_file']].to_sql('collection_harian', conn, if_exists='append', index=False)
                flash(f'✅ Collection: {len(df_valid):,} transaksi', 'success')

            elif tipe == 'mb':
                rename_dict = {}
                if 'NOTAGIHAN' in df.columns: rename_dict['NOTAGIHAN'] = 'nomen'
                elif 'NOMEN' in df.columns: rename_dict['NOMEN'] = 'nomen'
                else:
                    flash('❌ Format MB salah! Butuh NOTAGIHAN/NOMEN', 'danger')
                    return redirect(url_for('index'))
                if 'TGL_BAYAR' in df.columns: rename_dict['TGL_BAYAR'] = 'tgl_bayar'
                elif 'TANGGAL' in df.columns: rename_dict['TANGGAL'] = 'tgl_bayar'
                if 'BAYAR' in df.columns: rename_dict['BAYAR'] = 'jumlah_bayar'
                elif 'JUMLAH' in df.columns: rename_dict['JUMLAH'] = 'jumlah_bayar'
                
                df = df.rename(columns=rename_dict)
                df['nomen'] = df['nomen'].apply(clean_nomen)
                df = df.dropna(subset=['nomen'])
                df['tgl_bayar'] = df['tgl_bayar'].apply(lambda x: clean_date(x)) if 'tgl_bayar' in df.columns else ''
                df['jumlah_bayar'] = df['jumlah_bayar'].apply(lambda x: abs(float(x)) if pd.notna(x) else 0) if 'jumlah_bayar' in df.columns else 0
                df['periode'] = datetime.now().strftime('%Y-%m')
                df['sumber_file'] = filename
                
                mc_nomens = set([str(r['nomen']).strip() for r in conn.execute("SELECT nomen FROM master_pelanggan").fetchall()])
                if not mc_nomens:
                    flash('⚠️ MC kosong. Upload MC terlebih dahulu!', 'warning')
                    return redirect(url_for('index'))
                
                df['nomen'] = df['nomen'].astype(str).str.strip()
                df_valid = df[df['nomen'].isin(mc_nomens)].copy()
                
                if len(df_valid) == 0:
                    flash('⚠️ Tidak ada data MB yang cocok dengan MC.', 'warning')
                    return redirect(url_for('index'))
                
                df_valid[['nomen', 'tgl_bayar', 'jumlah_bayar', 'periode', 'sumber_file']].to_sql('master_bayar', conn, if_exists='append', index=False)
                flash(f'✅ MB: {len(df_valid):,} transaksi', 'success')

            elif tipe == 'mainbill':
                if 'NOMEN' not in df.columns:
                    flash('❌ Format MainBill salah! Butuh kolom NOMEN', 'danger')
                    return redirect(url_for('index'))
                rename_dict = {'NOMEN': 'nomen'}
                if 'FREEZE_DT' in df.columns: rename_dict['FREEZE_DT'] = 'tgl_tagihan'
                if 'TOTAL_TAGIHAN' in df.columns: rename_dict['TOTAL_TAGIHAN'] = 'total_tagihan'
                elif 'TAGIHAN' in df.columns: rename_dict['TAGIHAN'] = 'total_tagihan'
                if 'PCEZBK' in df.columns: rename_dict['PCEZBK'] = 'pcezbk'
                if 'TARIF' in df.columns: rename_dict['TARIF'] = 'tarif'
                
                df = df.rename(columns=rename_dict)
                df['nomen'] = df['nomen'].apply(clean_nomen)
                df = df.dropna(subset=['nomen'])
                df['tgl_tagihan'] = df['tgl_tagihan'].apply(lambda x: clean_date(x)) if 'tgl_tagihan' in df.columns else ''
                df['total_tagihan'] = df['total_tagihan'].apply(lambda x: abs(float(x)) if pd.notna(x) else 0) if 'total_tagihan' in df.columns else 0
                df['periode'] = datetime.now().strftime('%Y-%m')
                
                mc_nomens = set([str(r['nomen']).strip() for r in conn.execute("SELECT nomen FROM master_pelanggan").fetchall()])
                if not mc_nomens:
                    flash('⚠️ MC kosong. Upload MC terlebih dahulu!', 'warning')
                    return redirect(url_for('index'))
                
                df['nomen'] = df['nomen'].astype(str).str.strip()
                df_valid = df[df['nomen'].isin(mc_nomens)].copy()
                
                if len(df_valid) == 0:
                    flash('⚠️ Tidak ada MainBill cocok dengan MC.', 'warning')
                    return redirect(url_for('index'))
                
                available = [c for c in ['nomen', 'tgl_tagihan', 'total_tagihan', 'pcezbk', 'tarif', 'periode'] if c in df_valid.columns]
                df_valid[available].to_sql('mainbill', conn, if_exists='replace', index=False)
                flash(f'✅ MainBill: {len(df_valid):,} data', 'success')

            elif tipe == 'ardebt':
                rename_dict = {}
                if 'NOMEN' in df.columns: rename_dict['NOMEN'] = 'nomen'
                elif 'NOTAGIHAN' in df.columns: rename_dict['NOTAGIHAN'] = 'nomen'
                else:
                    flash('❌ Format Ardebt salah! Butuh NOMEN', 'danger')
                    return redirect(url_for('index'))
                if 'SumOfJUMLAH' in df.columns: rename_dict['SumOfJUMLAH'] = 'saldo_tunggakan'
                elif 'JUMLAH' in df.columns: rename_dict['JUMLAH'] = 'saldo_tunggakan'
                
                df = df.rename(columns=rename_dict)
                df['nomen'] = df['nomen'].apply(clean_nomen)
                df = df.dropna(subset=['nomen'])
                df['saldo_tunggakan'] = df['saldo_tunggakan'].apply(lambda x: abs(float(x)) if pd.notna(x) else 0) if 'saldo_tunggakan' in df.columns else 0
                df['periode'] = datetime.now().strftime('%Y-%m')
                
                mc_nomens = set([str(r['nomen']).strip() for r in conn.execute("SELECT nomen FROM master_pelanggan").fetchall()])
                if not mc_nomens:
                    flash('⚠️ MC kosong. Upload MC terlebih dahulu!', 'warning')
                    return redirect(url_for('index'))
                
                df['nomen'] = df['nomen'].astype(str).str.strip()
                df_valid = df[df['nomen'].isin(mc_nomens)].copy()
                
                if len(df_valid) == 0:
                    flash('⚠️ Tidak ada Ardebt cocok dengan MC.', 'warning')
                    return redirect(url_for('index'))
                
                df_valid[['nomen', 'saldo_tunggakan', 'periode']].to_sql('ardebt', conn, if_exists='replace', index=False)
                flash(f'✅ Ardebt: {len(df_valid):,} data', 'success')
        
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
    print("🚀 APLIKASI SUNTER SIAP!")
    app.run(host='0.0.0.0', port=5000, debug=True)
