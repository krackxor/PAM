import os
import sqlite3
import pandas as pd
from flask import Flask, render_template, g, request, redirect, url_for, flash, jsonify
from werkzeug.utils import secure_filename
from datetime import datetime
import traceback

# Import anomaly detection system
import sys
sys.path.insert(0, os.path.dirname(__file__))

# Import auto-detect periode
try:
    from auto_detect_periode import auto_detect_periode, detect_periode_from_content
    AUTO_DETECT_AVAILABLE = True
    print("✅ Auto-detect periode module loaded")
except ImportError:
    AUTO_DETECT_AVAILABLE = False
    print("⚠️ Warning: Auto-detect periode module not found")

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
                periode_bulan INTEGER,
                periode_tahun INTEGER,
                upload_id INTEGER,
                periode TEXT,
                sumber_file TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(nomen) REFERENCES master_pelanggan(nomen)
            )
        ''')
        
        # 4. MainBill
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS mainbill (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nomen TEXT,
                tgl_tagihan TEXT,
                total_tagihan REAL DEFAULT 0,
                pcezbk TEXT,
                tarif TEXT,
                periode_bulan INTEGER,
                periode_tahun INTEGER,
                upload_id INTEGER,
                periode TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(nomen) REFERENCES master_pelanggan(nomen)
            )
        ''')
        
        # 5. Ardebt
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ardebt (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nomen TEXT,
                saldo_tunggakan REAL DEFAULT 0,
                periode_bulan INTEGER,
                periode_tahun INTEGER,
                upload_id INTEGER,
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

def get_periode_label(bulan, tahun):
    """Convert bulan/tahun to readable label"""
    bulan_names = ['', 'Januari', 'Februari', 'Maret', 'April', 'Mei', 'Juni',
                   'Juli', 'Agustus', 'September', 'Oktober', 'November', 'Desember']
    if 1 <= bulan <= 12:
        return f"{bulan_names[bulan]} {tahun}"
    return f"{bulan}/{tahun}"

# === ROUTING UTAMA ===
@app.route('/')
def index():
    return render_template('index.html')

# === API: AVAILABLE PERIODES (NEW!) ===
@app.route('/api/available_periodes')
def api_available_periodes():
    """Get list of available periodes from upload_metadata"""
    db = get_db()
    
    try:
        periodes = db.execute('''
            SELECT DISTINCT 
                periode_bulan, 
                periode_tahun,
                COUNT(*) as file_count
            FROM upload_metadata
            WHERE periode_bulan IS NOT NULL 
            AND periode_tahun IS NOT NULL
            GROUP BY periode_bulan, periode_tahun
            ORDER BY periode_tahun DESC, periode_bulan DESC
        ''').fetchall()
        
        result = []
        is_first = True
        
        for p in periodes:
            bulan = p['periode_bulan']
            tahun = p['periode_tahun']
            
            result.append({
                'bulan': bulan,
                'tahun': tahun,
                'label': get_periode_label(bulan, tahun),
                'value': f"{tahun}-{str(bulan).zfill(2)}",
                'file_count': p['file_count'],
                'is_latest': is_first
            })
            
            is_first = False
        
        return jsonify({
            'success': True,
            'count': len(result),
            'periodes': result
        })
        
    except Exception as e:
        print(f"Error loading periodes: {e}")
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

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

# === API: COLLECTION DATA (FIXED - AMBIL NAMA DARI MC) ===
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
    
    # FIXED: Ambil nama dari master_pelanggan menggunakan LEFT JOIN
    query = f'''
        SELECT 
            c.tgl_bayar, 
            COALESCE(m.rayon, '') as rayon, 
            COALESCE(m.pcez, '') as pcez, 
            COALESCE(m.pc, '') as pc, 
            COALESCE(m.ez, '') as ez,
            c.nomen, 
            COALESCE(m.nama, 'Tanpa Nama') as nama,
            COALESCE(m.zona_novak, '') as zona_novak, 
            COALESCE(m.tarif, '') as tarif, 
            COALESCE(m.target_mc, 0) as target_mc, 
            c.jumlah_bayar 
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


# === NEW: COLLECTION ANALYTICS DASHBOARD APIs ===

@app.route('/api/collection/summary')
def api_collection_summary():
    """Summary collection untuk dashboard"""
    db = get_db()
    
    try:
        # Get latest periode
        periode_query = """
            SELECT periode_bulan, periode_tahun 
            FROM collection_harian 
            WHERE periode_bulan IS NOT NULL AND periode_tahun IS NOT NULL
            ORDER BY periode_tahun DESC, periode_bulan DESC 
            LIMIT 1
        """
        periode_row = db.execute(periode_query).fetchone()
        
        if not periode_row:
            return jsonify({
                'success': False,
                'message': 'No collection data found'
            })
        
        periode_bulan = periode_row[0]
        periode_tahun = periode_row[1]
        
        # Format periode label
        bulan_names = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'Mei', 'Jun', 
                      'Jul', 'Ags', 'Sep', 'Okt', 'Nov', 'Des']
        periode_label = f"{bulan_names[periode_bulan]} {periode_tahun}"
        
        # 1. Total Target vs Collection
        target_query = """
            SELECT 
                COUNT(DISTINCT m.nomen) as total_pelanggan,
                SUM(m.target_mc) as total_target
            FROM master_pelanggan m
            WHERE m.rayon IN ('34', '35')
        """
        target = db.execute(target_query).fetchone()
        
        collection_query = """
            SELECT 
                COUNT(DISTINCT c.nomen) as pelanggan_bayar,
                SUM(c.jumlah_bayar) as total_collection
            FROM collection_harian c
            WHERE c.periode_bulan = ? AND c.periode_tahun = ?
        """
        collection = db.execute(collection_query, (periode_bulan, periode_tahun)).fetchone()
        
        total_target = target[1] or 0
        total_collection = collection[1] or 0
        
        # 2. Performance percentage
        performance_pct = (total_collection / total_target * 100) if total_target > 0 else 0
        
        # 3. Breakdown by rayon
        rayon_query = """
            SELECT 
                m.rayon,
                COUNT(DISTINCT m.nomen) as total_pelanggan,
                SUM(m.target_mc) as target,
                COUNT(DISTINCT c.nomen) as pelanggan_bayar,
                SUM(c.jumlah_bayar) as collection
            FROM master_pelanggan m
            LEFT JOIN collection_harian c ON m.nomen = c.nomen 
                AND c.periode_bulan = ? AND c.periode_tahun = ?
            WHERE m.rayon IN ('34', '35')
            GROUP BY m.rayon
        """
        rayon_data = db.execute(rayon_query, (periode_bulan, periode_tahun)).fetchall()
        
        return jsonify({
            'success': True,
            'periode': periode_label,
            'periode_bulan': periode_bulan,
            'periode_tahun': periode_tahun,
            'summary': {
                'total_pelanggan': target[0] or 0,
                'pelanggan_bayar': collection[0] or 0,
                'pelanggan_belum_bayar': (target[0] or 0) - (collection[0] or 0),
                'total_target': total_target,
                'total_collection': total_collection,
                'selisih': total_target - total_collection,
                'performance_pct': round(performance_pct, 2)
            },
            'by_rayon': [dict(row) for row in rayon_data]
        })
        
    except Exception as e:
        print(f"Error collection summary: {e}")
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/collection/by_pcez')
def api_collection_by_pcez():
    """Collection performance by PCEZ"""
    db = get_db()
    
    try:
        # Get latest periode
        periode_query = """
            SELECT periode_bulan, periode_tahun 
            FROM collection_harian 
            WHERE periode_bulan IS NOT NULL
            ORDER BY periode_tahun DESC, periode_bulan DESC 
            LIMIT 1
        """
        periode_row = db.execute(periode_query).fetchone()
        
        if not periode_row:
            return jsonify({'success': False, 'data': []})
        
        periode_bulan, periode_tahun = periode_row[0], periode_row[1]
        
        # Performance by PCEZ
        query = """
            SELECT 
                m.pcez,
                m.rayon,
                COUNT(DISTINCT m.nomen) as total_pelanggan,
                SUM(m.target_mc) as target,
                COUNT(DISTINCT c.nomen) as pelanggan_bayar,
                SUM(c.jumlah_bayar) as collection,
                ROUND(SUM(c.jumlah_bayar) * 100.0 / SUM(m.target_mc), 2) as performance_pct
            FROM master_pelanggan m
            LEFT JOIN collection_harian c ON m.nomen = c.nomen 
                AND c.periode_bulan = ? AND c.periode_tahun = ?
            WHERE m.rayon IN ('34', '35') AND m.pcez IS NOT NULL AND m.pcez != ''
            GROUP BY m.pcez, m.rayon
            ORDER BY m.rayon, m.pcez
        """
        
        rows = db.execute(query, (periode_bulan, periode_tahun)).fetchall()
        
        return jsonify({
            'success': True,
            'data': [dict(row) for row in rows]
        })
        
    except Exception as e:
        print(f"Error by PCEZ: {e}")
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/collection/top_payers')
def api_collection_top_payers():
    """Top 20 pembayar terbesar"""
    db = get_db()
    
    try:
        # Get latest periode
        periode_query = """
            SELECT periode_bulan, periode_tahun 
            FROM collection_harian 
            WHERE periode_bulan IS NOT NULL
            ORDER BY periode_tahun DESC, periode_bulan DESC 
            LIMIT 1
        """
        periode_row = db.execute(periode_query).fetchone()
        
        if not periode_row:
            return jsonify({'success': False, 'data': []})
        
        periode_bulan, periode_tahun = periode_row[0], periode_row[1]
        
        # Top payers
        query = """
            SELECT 
                c.nomen,
                m.nama,
                m.alamat,
                m.rayon,
                m.pcez,
                SUM(c.jumlah_bayar) as total_bayar,
                COUNT(*) as jumlah_transaksi
            FROM collection_harian c
            LEFT JOIN master_pelanggan m ON c.nomen = m.nomen
            WHERE c.periode_bulan = ? AND c.periode_tahun = ?
            GROUP BY c.nomen, m.nama, m.alamat, m.rayon, m.pcez
            ORDER BY total_bayar DESC
            LIMIT 20
        """
        
        rows = db.execute(query, (periode_bulan, periode_tahun)).fetchall()
        
        return jsonify({
            'success': True,
            'data': [dict(row) for row in rows]
        })
        
    except Exception as e:
        print(f"Error top payers: {e}")
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/collection/daily_trend')
def api_collection_daily_trend():
    """Tren collection harian dengan moving average"""
    db = get_db()
    
    try:
        # Get latest periode
        periode_query = """
            SELECT periode_bulan, periode_tahun 
            FROM collection_harian 
            WHERE periode_bulan IS NOT NULL
            ORDER BY periode_tahun DESC, periode_bulan DESC 
            LIMIT 1
        """
        periode_row = db.execute(periode_query).fetchone()
        
        if not periode_row:
            return jsonify({'success': False, 'data': []})
        
        periode_bulan, periode_tahun = periode_row[0], periode_row[1]
        
        # Daily trend
        query = """
            SELECT 
                strftime('%d', c.tgl_bayar) as tanggal,
                c.tgl_bayar,
                COUNT(DISTINCT c.nomen) as jumlah_pelanggan,
                SUM(c.jumlah_bayar) as total_harian,
                (SELECT SUM(jumlah_bayar) 
                 FROM collection_harian 
                 WHERE periode_bulan = ? AND periode_tahun = ?
                 AND tgl_bayar <= c.tgl_bayar) as kumulatif
            FROM collection_harian c
            WHERE c.periode_bulan = ? AND c.periode_tahun = ?
            GROUP BY c.tgl_bayar
            ORDER BY c.tgl_bayar ASC
        """
        
        rows = db.execute(query, (periode_bulan, periode_tahun, periode_bulan, periode_tahun)).fetchall()
        
        return jsonify({
            'success': True,
            'data': [dict(row) for row in rows]
        })
        
    except Exception as e:
        print(f"Error daily trend: {e}")
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/collection/payment_distribution')
def api_collection_payment_distribution():
    """Distribusi pembayaran by amount ranges"""
    db = get_db()
    
    try:
        # Get latest periode
        periode_query = """
            SELECT periode_bulan, periode_tahun 
            FROM collection_harian 
            WHERE periode_bulan IS NOT NULL
            ORDER BY periode_tahun DESC, periode_bulan DESC 
            LIMIT 1
        """
        periode_row = db.execute(periode_query).fetchone()
        
        if not periode_row:
            return jsonify({'success': False, 'data': []})
        
        periode_bulan, periode_tahun = periode_row[0], periode_row[1]
        
        # Distribution by amount ranges
        query = """
            SELECT 
                CASE 
                    WHEN jumlah_bayar < 50000 THEN '< 50K'
                    WHEN jumlah_bayar < 100000 THEN '50K - 100K'
                    WHEN jumlah_bayar < 200000 THEN '100K - 200K'
                    WHEN jumlah_bayar < 500000 THEN '200K - 500K'
                    WHEN jumlah_bayar < 1000000 THEN '500K - 1M'
                    ELSE '> 1M'
                END as range_bayar,
                COUNT(*) as jumlah_transaksi,
                SUM(jumlah_bayar) as total_nilai
            FROM collection_harian
            WHERE periode_bulan = ? AND periode_tahun = ?
            GROUP BY range_bayar
            ORDER BY MIN(jumlah_bayar)
        """
        
        rows = db.execute(query, (periode_bulan, periode_tahun)).fetchall()
        
        return jsonify({
            'success': True,
            'data': [dict(row) for row in rows]
        })
        
    except Exception as e:
        print(f"Error payment distribution: {e}")
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500

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

# === UPLOAD FILE HANDLER (SINGLE) ===
@app.route('/upload', methods=['POST'])
def upload_file():
    # Keep existing single upload - not modified
    return jsonify({'status': 'Use multi-upload instead'}), 400

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
            SELECT COUNT(*) as cnt FROM sbrs_data WHERE skip_status IS NOT NULL AND skip_status != ''
        ''').fetchone()
        
        trouble = db.execute('''
            SELECT COUNT(*) as cnt FROM sbrs_data WHERE trouble_status IS NOT NULL AND trouble_status != ''
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
            {where_clause} AND skip_status IS NOT NULL AND skip_status != ''
            ORDER BY nomen LIMIT 100
        ''', params).fetchall()
        
        trouble = db.execute(f'''
            SELECT nomen, nama, rayon, trouble_status, spm_status
            FROM sbrs_data
            {where_clause} AND trouble_status IS NOT NULL AND trouble_status != ''
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

# === API: BELUM BAYAR (FIXED - AMBIL NAMA, ALAMAT, TARIF DARI MC) ===
@app.route('/api/belum_bayar')
def api_belum_bayar():
    """API untuk menampilkan pelanggan yang belum bayar"""
    db = get_db()
    
    try:
        # FIXED: Ambil nama, alamat, tarif dari master_pelanggan
        query = '''
            SELECT 
                m.nomen,
                COALESCE(m.nama, 'Tanpa Nama') as nama,
                COALESCE(m.alamat, '-') as alamat,
                m.rayon,
                COALESCE(m.pc, '') as pc,
                COALESCE(m.ez, '') as ez,
                COALESCE(m.pcez, '') as pcez,
                COALESCE(m.tarif, '-') as tarif,
                COALESCE(m.target_mc, 0) as nominal,
                COALESCE(m.kubikasi, 0) as kubikasi
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


@app.route('/collection_dashboard')
def collection_dashboard():
    """Serve Collection Dashboard HTML"""
    import os
    dashboard_path = os.path.join(os.path.dirname(__file__), 'collection_dashboard.html')
    
    if os.path.exists(dashboard_path):
        with open(dashboard_path, 'r', encoding='utf-8') as f:
            return f.read()
    else:
        return '''
        <html>
        <body>
        <h1>Error: Dashboard file not found</h1>
        <p>Please ensure collection_dashboard.html is in the same directory as app.py</p>
        </body>
        </html>
        ''', 404

# ==========================================
# ANOMALY DETECTION API ROUTES (BUILT-IN)
# ==========================================

@app.route('/api/anomaly/summary')
def api_anomaly_summary():
    """Summary count untuk setiap jenis anomali dari SBRS"""
    db = get_db()
    
    try:
        # Ambil periode terakhir dari SBRS
        periode_query = """
            SELECT periode_bulan, periode_tahun 
            FROM sbrs_data 
            WHERE periode_bulan IS NOT NULL AND periode_tahun IS NOT NULL
            ORDER BY periode_tahun DESC, periode_bulan DESC 
            LIMIT 1
        """
        periode_row = db.execute(periode_query).fetchone()
        
        if not periode_row:
            return jsonify({
                'periode': None,
                'anomalies': {
                    'extreme': {'count': 0},
                    'turun': {'count': 0},
                    'zero': {'count': 0},
                    'negatif': {'count': 0},
                    'salah_catat': {'count': 0},
                    'rebill': {'count': 0},
                    'estimasi': {'count': 0}
                }
            })
        
        periode_bulan = periode_row[0]
        periode_tahun = periode_row[1]
        
        # Format periode untuk display
        bulan_names = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'Mei', 'Jun', 
                      'Jul', 'Ags', 'Sep', 'Okt', 'Nov', 'Des']
        periode_label = f"{bulan_names[periode_bulan]} {periode_tahun}"
        
        # Query untuk hitung setiap anomali
        anomalies = {}
        
        # 1. PEMAKAIAN EXTREME (>100 m3)
        extreme_query = """
            SELECT COUNT(DISTINCT nomen) as count
            FROM sbrs_data
            WHERE periode_bulan = ? AND periode_tahun = ?
            AND volume > 100
        """
        extreme = db.execute(extreme_query, (periode_bulan, periode_tahun)).fetchone()
        anomalies['extreme'] = {'count': extreme[0] or 0}
        
        # 2. ZERO USAGE (volume = 0)
        zero_query = """
            SELECT COUNT(DISTINCT nomen) as count
            FROM sbrs_data
            WHERE periode_bulan = ? AND periode_tahun = ?
            AND volume = 0
        """
        zero = db.execute(zero_query, (periode_bulan, periode_tahun)).fetchone()
        anomalies['zero'] = {'count': zero[0] or 0}
        
        # 3. STAND NEGATIF (volume < 0)
        negatif_query = """
            SELECT COUNT(DISTINCT nomen) as count
            FROM sbrs_data
            WHERE periode_bulan = ? AND periode_tahun = ?
            AND volume < 0
        """
        negatif = db.execute(negatif_query, (periode_bulan, periode_tahun)).fetchone()
        anomalies['negatif'] = {'count': negatif[0] or 0}
        
        # 4. SALAH CATAT (stand_akhir < stand_awal)
        salah_query = """
            SELECT COUNT(DISTINCT nomen) as count
            FROM sbrs_data
            WHERE periode_bulan = ? AND periode_tahun = ?
            AND stand_akhir < stand_awal
            AND stand_awal > 0
        """
        salah = db.execute(salah_query, (periode_bulan, periode_tahun)).fetchone()
        anomalies['salah_catat'] = {'count': salah[0] or 0}
        
        # 5. REBILL (ada flag rebill di spm_status)
        rebill_query = """
            SELECT COUNT(DISTINCT nomen) as count
            FROM sbrs_data
            WHERE periode_bulan = ? AND periode_tahun = ?
            AND (spm_status LIKE '%REBILL%' OR spm_status LIKE '%rebill%')
        """
        rebill = db.execute(rebill_query, (periode_bulan, periode_tahun)).fetchone()
        anomalies['rebill'] = {'count': rebill[0] or 0}
        
        # 6. ESTIMASI (readmethod != 'ACTUAL')
        estimasi_query = """
            SELECT COUNT(DISTINCT nomen) as count
            FROM sbrs_data
            WHERE periode_bulan = ? AND periode_tahun = ?
            AND (readmethod != 'ACTUAL' OR (skip_status IS NOT NULL AND skip_status != ''))
        """
        estimasi = db.execute(estimasi_query, (periode_bulan, periode_tahun)).fetchone()
        anomalies['estimasi'] = {'count': estimasi[0] or 0}
        
        # 7. PEMAKAIAN TURUN (placeholder - butuh data periode sebelumnya)
        anomalies['turun'] = {'count': 0}
        
        return jsonify({
            'periode': periode_label,
            'periode_bulan': periode_bulan,
            'periode_tahun': periode_tahun,
            'anomalies': anomalies
        })
        
    except Exception as e:
        print(f"Error anomaly summary: {e}")
        traceback.print_exc()
        return jsonify({
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500


@app.route('/api/anomaly/<anomaly_type>')
def api_anomaly_detail(anomaly_type):
    """Detail data untuk jenis anomali tertentu"""
    db = get_db()
    
    try:
        # Ambil periode terakhir
        periode_query = """
            SELECT periode_bulan, periode_tahun 
            FROM sbrs_data 
            WHERE periode_bulan IS NOT NULL AND periode_tahun IS NOT NULL
            ORDER BY periode_tahun DESC, periode_bulan DESC 
            LIMIT 1
        """
        periode_row = db.execute(periode_query).fetchone()
        
        if not periode_row:
            return jsonify({'data': []})
        
        periode_bulan = periode_row[0]
        periode_tahun = periode_row[1]
        
        # Query berdasarkan tipe anomali
        if anomaly_type == 'extreme':
            query = """
                SELECT s.nomen, s.nama, s.alamat, s.rayon, s.volume,
                       s.stand_awal, s.stand_akhir, s.readmethod
                FROM sbrs_data s
                WHERE s.periode_bulan = ? AND s.periode_tahun = ?
                AND s.volume > 100
                ORDER BY s.volume DESC
                LIMIT 100
            """
            rows = db.execute(query, (periode_bulan, periode_tahun)).fetchall()
            
        elif anomaly_type == 'turun':
            # Placeholder - need previous period data
            return jsonify({'data': []})
            
        elif anomaly_type == 'zero':
            query = """
                SELECT nomen, nama, alamat, rayon, volume,
                       readmethod, skip_status, trouble_status
                FROM sbrs_data
                WHERE periode_bulan = ? AND periode_tahun = ?
                AND volume = 0
                ORDER BY nomen
                LIMIT 100
            """
            rows = db.execute(query, (periode_bulan, periode_tahun)).fetchall()
            
        elif anomaly_type == 'negatif':
            query = """
                SELECT nomen, nama, alamat, rayon, volume,
                       stand_awal, stand_akhir
                FROM sbrs_data
                WHERE periode_bulan = ? AND periode_tahun = ?
                AND volume < 0
                ORDER BY volume ASC
                LIMIT 100
            """
            rows = db.execute(query, (periode_bulan, periode_tahun)).fetchall()
            
        elif anomaly_type == 'salah_catat':
            query = """
                SELECT nomen, nama, alamat, rayon,
                       stand_awal, stand_akhir, volume
                FROM sbrs_data
                WHERE periode_bulan = ? AND periode_tahun = ?
                AND stand_akhir < stand_awal
                AND stand_awal > 0
                ORDER BY (stand_awal - stand_akhir) DESC
                LIMIT 100
            """
            rows = db.execute(query, (periode_bulan, periode_tahun)).fetchall()
            
        elif anomaly_type == 'rebill':
            query = """
                SELECT nomen, nama, alamat, rayon, volume,
                       spm_status
                FROM sbrs_data
                WHERE periode_bulan = ? AND periode_tahun = ?
                AND (spm_status LIKE '%REBILL%' OR spm_status LIKE '%rebill%')
                ORDER BY nomen
                LIMIT 100
            """
            rows = db.execute(query, (periode_bulan, periode_tahun)).fetchall()
            
        elif anomaly_type == 'estimasi':
            query = """
                SELECT nomen, nama, alamat, rayon, volume,
                       readmethod, skip_status
                FROM sbrs_data
                WHERE periode_bulan = ? AND periode_tahun = ?
                AND (readmethod != 'ACTUAL' OR (skip_status IS NOT NULL AND skip_status != ''))
                ORDER BY nomen
                LIMIT 100
            """
            rows = db.execute(query, (periode_bulan, periode_tahun)).fetchall()
        else:
            return jsonify({'error': 'Invalid anomaly type'}), 400
        
        # Convert to dict
        data = []
        for row in rows:
            data.append(dict(row))
        
        return jsonify({'data': data})
        
    except Exception as e:
        print(f"Error anomaly detail: {e}")
        traceback.print_exc()
        return jsonify({
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500


# Register anomaly detection routes (if external module available)
if ANOMALY_AVAILABLE:
    try:
        register_anomaly_routes(app, get_db)
        print("✅ Anomaly Detection System: ACTIVE (External)")
    except:
        print("⚠️  External Anomaly module failed, using built-in")
else:
    print("✅ Anomaly Detection System: ACTIVE (Built-in)")

# Register analisa routes
if ANALISA_AVAILABLE:
    register_analisa_routes(app, get_db)
    print("✅ Analisa Manual System: ACTIVE")
else:
    print("⚠️  Analisa Manual System: DISABLED")


# ==========================================
# PROCESS FUNCTIONS FOR MULTI-UPLOAD
# ==========================================

def process_mc_file(filepath, upload_id, periode_bulan, periode_tahun, db):
    """Process MC file - FIXED VERSION"""
    try:
        # Read file
        if filepath.endswith('.csv'):
            df = pd.read_csv(filepath, dtype=str)
        elif filepath.endswith(('.xls', '.xlsx')):
            df = pd.read_excel(filepath)
        else:
            raise Exception('Unsupported file format')
        
        df.columns = df.columns.str.upper().str.strip()
        
        # Validation
        if 'ZONA_NOVAK' not in df.columns or 'NOMEN' not in df.columns:
            raise Exception('MC: Need ZONA_NOVAK and NOMEN columns')
        
        # Rename columns
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
        
        if 'KUBIK' in df.columns:
            rename_dict['KUBIK'] = 'kubikasi'
        elif 'KUBIKASI' in df.columns:
            rename_dict['KUBIKASI'] = 'kubikasi'
        
        df = df.rename(columns=rename_dict)
        
        # Clean nomen
        df['nomen'] = df['nomen'].astype(str).str.strip().str.replace('.0', '', regex=False)
        df = df.dropna(subset=['nomen'])
        df = df[df['nomen'] != '']
        
        # Parse zona
        df['zona_novak'] = df['ZONA_NOVAK'].astype(str).str.strip()
        zona_parsed = df['zona_novak'].apply(parse_zona_novak)
        
        df['rayon'] = zona_parsed.apply(lambda x: x['rayon'])
        df['pc'] = zona_parsed.apply(lambda x: x['pc'])
        df['ez'] = zona_parsed.apply(lambda x: x['ez'])
        df['block'] = zona_parsed.apply(lambda x: x['block'])
        df['pcez'] = zona_parsed.apply(lambda x: x['pcez'])
        
        # Filter rayon 34/35
        df = df[df['rayon'].isin(['34', '35'])]
        
        if len(df) == 0:
            raise Exception('No Rayon 34/35 data found')
        
        # Fill missing columns
        for col in ['nama', 'alamat', 'tarif']:
            if col not in df.columns:
                df[col] = ''
        
        if 'target_mc' not in df.columns:
            df['target_mc'] = 0
        
        if 'kubikasi' not in df.columns:
            df['kubikasi'] = 0
        else:
            df['kubikasi'] = df['kubikasi'].apply(lambda x: abs(float(x)) if pd.notna(x) else 0)
        
        # Add metadata
        df['periode_bulan'] = periode_bulan
        df['periode_tahun'] = periode_tahun
        df['upload_id'] = upload_id
        
        # Save to database
        cols_db = ['nomen', 'nama', 'alamat', 'rayon', 'pc', 'ez', 'pcez', 'block', 
                   'zona_novak', 'tarif', 'target_mc', 'kubikasi', 
                   'periode_bulan', 'periode_tahun', 'upload_id']
        
        df[cols_db].to_sql('master_pelanggan', db, if_exists='replace', index=False)
        
        print(f"✅ MC processed: {len(df)} rows")
        return len(df)
        
    except Exception as e:
        print(f"❌ MC processing error: {e}")
        raise


def process_collection_file(filepath, upload_id, periode_bulan, periode_tahun, db):
    """Process Collection file - FIXED VERSION"""
    try:
        # Read file
        if filepath.endswith('.txt'):
            df = pd.read_csv(filepath, sep='|', dtype=str)
        elif filepath.endswith('.csv'):
            df = pd.read_csv(filepath, dtype=str)
        elif filepath.endswith(('.xls', '.xlsx')):
            df = pd.read_excel(filepath)
        else:
            raise Exception('Unsupported file format')
        
        df.columns = df.columns.str.upper().str.strip()
        
        # Rename columns
        rename_dict = {}
        if 'NOMEN' in df.columns:
            rename_dict['NOMEN'] = 'nomen'
        else:
            raise Exception('Collection: Need NOMEN column')
        
        if 'PAY_DT' in df.columns:
            rename_dict['PAY_DT'] = 'tgl_bayar'
        elif 'TGL_BAYAR' in df.columns:
            rename_dict['TGL_BAYAR'] = 'tgl_bayar'
        
        if 'AMT_COLLECT' in df.columns:
            rename_dict['AMT_COLLECT'] = 'jumlah_bayar'
        elif 'JUMLAH' in df.columns:
            rename_dict['JUMLAH'] = 'jumlah_bayar'
        
        if 'BILL_PERIOD' in df.columns:
            rename_dict['BILL_PERIOD'] = 'bill_period'
        
        df = df.rename(columns=rename_dict)
        
        # Clean nomen
        df['nomen'] = df['nomen'].astype(str).str.strip().str.replace('.0', '', regex=False)
        df = df.dropna(subset=['nomen'])
        df = df[df['nomen'] != '']
        
        # Clean date
        if 'tgl_bayar' in df.columns:
            df['tgl_bayar'] = df['tgl_bayar'].apply(lambda x: clean_date(x))
        else:
            df['tgl_bayar'] = datetime.now().strftime('%Y-%m-%d')
        
        # Clean amount
        if 'jumlah_bayar' in df.columns:
            df['jumlah_bayar'] = df['jumlah_bayar'].apply(lambda x: abs(float(x)) if pd.notna(x) else 0)
        else:
            df['jumlah_bayar'] = 0
        
        # Determine tipe_bayar
        if 'bill_period' not in df.columns:
            df['tipe_bayar'] = 'current'
            df['bill_period'] = ''
        else:
            df['tipe_bayar'] = 'current'
        
        # Add metadata
        df['periode_bulan'] = periode_bulan
        df['periode_tahun'] = periode_tahun
        df['upload_id'] = upload_id
        df['sumber_file'] = os.path.basename(filepath)
        
        # Save to database
        cols = ['nomen', 'tgl_bayar', 'jumlah_bayar', 'tipe_bayar', 'bill_period', 
                'periode_bulan', 'periode_tahun', 'upload_id', 'sumber_file']
        
        for _, row in df[cols].iterrows():
            try:
                db.execute('''
                    INSERT OR IGNORE INTO collection_harian 
                    (nomen, tgl_bayar, jumlah_bayar, tipe_bayar, bill_period, 
                     periode_bulan, periode_tahun, upload_id, sumber_file)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', tuple(row))
            except:
                pass
        
        db.commit()
        
        print(f"✅ Collection processed: {len(df)} rows")
        return len(df)
        
    except Exception as e:
        print(f"❌ Collection processing error: {e}")
        raise


def process_sbrs_file(filepath, upload_id, periode_bulan, periode_tahun, db):
    """Process SBRS file - ULTRA SAFE VERSION (FIXED list index out of range)"""
    try:
        print(f"\n🔍 Reading SBRS file: {filepath}")
        
        # Read file with multiple attempts
        df = None
        if filepath.endswith(('.xls', '.xlsx')):
            try:
                df = pd.read_excel(filepath, engine='openpyxl')
            except:
                try:
                    df = pd.read_excel(filepath, engine='xlrd')
                except:
                    df = pd.read_excel(filepath)
        elif filepath.endswith('.csv'):
            try:
                df = pd.read_csv(filepath, encoding='utf-8')
            except:
                df = pd.read_csv(filepath, encoding='latin-1')
        else:
            raise Exception('Unsupported file format')
        
        if df is None or len(df) == 0:
            raise Exception('File is empty or cannot be read')
        
        print(f"✅ File read successfully: {len(df)} rows")
        
        # Uppercase columns
        df.columns = df.columns.str.upper().str.strip()
        print(f"📋 Columns found: {df.columns.tolist()}")
        
        # ULTRA FLEXIBLE MAPPING
        rename_dict = {}
        
        # 1. NOMEN/ACCOUNT (WAJIB) - Check if ANY keyword exists in column names
        nomen_found = False
        nomen_keywords = ['CMR_ACCOUNT', 'ACCOUNT', 'NOPEN', 'NOMEN', 'NOPEL', 'NO_PEL', 'CUSTOMER']
        for col in df.columns:
            for keyword in nomen_keywords:
                if keyword in col.upper():
                    rename_dict[col] = 'nomen'
                    print(f"  ✓ Nomen: {col} → nomen")
                    nomen_found = True
                    break
            if nomen_found:
                break
        
        # 2. VOLUME/STAND (WAJIB) - Check if ANY keyword exists
        volume_found = False
        volume_keywords = ['SB_STAND', 'STAND', 'PAKAI', 'KUBIKASI', 'KUBIK', 'USAGE', 'VOLUME', 'VOL']
        for col in df.columns:
            for keyword in volume_keywords:
                if keyword in col.upper():
                    rename_dict[col] = 'volume'
                    print(f"  ✓ Volume: {col} → volume")
                    volume_found = True
                    break
            if volume_found:
                break
        
        # 3. NAMA (Optional)
        for col in df.columns:
            if any(k in col.upper() for k in ['CMR_NAME', 'NAME', 'NAMA', 'CUSTOMER_NAME']):
                rename_dict[col] = 'nama'
                print(f"  ✓ Nama: {col} → nama")
                break
        
        # 4. ALAMAT (Optional)
        for col in df.columns:
            if any(k in col.upper() for k in ['CMR_ADDRESS', 'ADDRESS', 'ALAMAT']):
                rename_dict[col] = 'alamat'
                print(f"  ✓ Alamat: {col} → alamat")
                break
        
        # 5. RAYON/ROUTE (Optional)
        for col in df.columns:
            if any(k in col.upper() for k in ['CMR_ROUTE', 'ROUTE', 'RUTE', 'RAYON']):
                rename_dict[col] = 'rayon'
                print(f"  ✓ Rayon: {col} → rayon")
                break
        
        # 6. READ_METHOD (Optional)
        for col in df.columns:
            if any(k in col.upper() for k in ['READ_METHOD', 'METODE', 'METHOD']):
                rename_dict[col] = 'readmethod'
                print(f"  ✓ Method: {col} → readmethod")
                break
        
        # 7. STAND_AWAL (Optional)
        for col in df.columns:
            if any(k in col.upper() for k in ['CMR_PREV_READ', 'PREV_READ', 'STAND_LALU', 'STAND_AWAL']):
                rename_dict[col] = 'stand_awal'
                print(f"  ✓ Stand Awal: {col} → stand_awal")
                break
        
        # 8. STAND_AKHIR (Optional)
        for col in df.columns:
            if any(k in col.upper() for k in ['CMR_READING', 'READING', 'STAND_INI', 'STAND_AKHIR']):
                rename_dict[col] = 'stand_akhir'
                print(f"  ✓ Stand Akhir: {col} → stand_akhir")
                break
        
        # Validate minimum requirements
        if not nomen_found:
            raise Exception(f'❌ Cannot find NOMEN/ACCOUNT column. Available: {df.columns.tolist()}')
        
        if not volume_found:
            raise Exception(f'❌ Cannot find VOLUME/STAND column. Available: {df.columns.tolist()}')
        
        # Apply renaming
        df = df.rename(columns=rename_dict)
        print(f"📝 After renaming: {df.columns.tolist()}")
        
        # Clean nomen
        df['nomen'] = df['nomen'].astype(str).str.strip().str.replace('.0', '', regex=False)
        df = df.dropna(subset=['nomen'])
        df = df[df['nomen'] != '']
        df = df[df['nomen'].str.lower() != 'nan']
        
        if len(df) == 0:
            raise Exception('No valid data after cleaning nomen')
        
        print(f"✅ After cleaning: {len(df)} rows")
        
        # Clean volume
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0)
        
        # Add metadata
        df['periode_bulan'] = periode_bulan
        df['periode_tahun'] = periode_tahun
        df['upload_id'] = upload_id
        
        # Fill missing optional columns
        optional_cols = {
            'nama': '',
            'alamat': '',
            'rayon': '',
            'readmethod': 'ACTUAL',
            'skip_status': '',
            'trouble_status': '',
            'spm_status': '',
            'stand_awal': 0,
            'stand_akhir': 0,
            'analisa_tindak_lanjut': '',
            'tag1': '',
            'tag2': ''
        }
        
        for col, default_val in optional_cols.items():
            if col not in df.columns:
                df[col] = default_val
        
        # Select columns to save
        cols_to_save = [
            'nomen', 'volume', 'periode_bulan', 'periode_tahun', 'upload_id',
            'nama', 'alamat', 'rayon', 'readmethod', 
            'skip_status', 'trouble_status', 'spm_status',
            'stand_awal', 'stand_akhir',
            'analisa_tindak_lanjut', 'tag1', 'tag2'
        ]
        
        print(f"💾 Saving to database...")
        print(f"   Volume stats: min={df['volume'].min():.0f}, max={df['volume'].max():.0f}, avg={df['volume'].mean():.2f}")
        
        # Save to database
        df[cols_to_save].to_sql('sbrs_data', db, if_exists='replace', index=False)
        
        print(f"✅ SBRS processed: {len(df)} rows")
        return len(df)
        
    except Exception as e:
        print(f"❌ SBRS processing error: {e}")
        traceback.print_exc()
        raise


def process_mb_file(filepath, upload_id, periode_bulan, periode_tahun, db):
    """Process MB (Manual Bayar) file"""
    try:
        # Read file
        if filepath.endswith('.txt'):
            df = pd.read_csv(filepath, sep='|', dtype=str)
        elif filepath.endswith('.csv'):
            df = pd.read_csv(filepath, dtype=str)
        elif filepath.endswith(('.xls', '.xlsx')):
            df = pd.read_excel(filepath)
        else:
            raise Exception('Unsupported file format')
        
        if len(df) == 0:
            raise Exception('File is empty')
        
        # Normalize column names
        df.columns = df.columns.str.upper().str.strip()
        
        print(f"MB Columns found: {list(df.columns)}")
        
        # Find NOMEN column (more flexible)
        nomen_col = None
        for col in df.columns:
            col_upper = col.upper()
            if any(x in col_upper for x in ['NOMEN', 'NOPEL', 'NO_PEL', 'PELANGGAN', 'ACCOUNT']):
                nomen_col = col
                print(f"  ✓ NOMEN column: {col}")
                break
        
        if not nomen_col:
            raise Exception(f'NOMEN column not found. Available columns: {list(df.columns)}')
        
        # Find TGL_BAYAR column (more flexible)
        tgl_col = None
        for col in df.columns:
            col_upper = col.upper()
            if ('TGL' in col_upper or 'TANGGAL' in col_upper or 'DATE' in col_upper) and \
               ('BAYAR' in col_upper or 'PAY' in col_upper or 'PAYMENT' in col_upper):
                tgl_col = col
                print(f"  ✓ TGL_BAYAR column: {col}")
                break
        
        if not tgl_col:
            raise Exception(f'TGL_BAYAR column not found. Available columns: {list(df.columns)}')
        
        # Find JUMLAH column (more flexible but exclude TGL_BAYAR)
        jumlah_col = None
        for col in df.columns:
            col_upper = col.upper()
            # Skip if this is a date column
            if 'TGL' in col_upper or 'TANGGAL' in col_upper or 'DATE' in col_upper:
                continue
            # Now check for amount keywords
            if any(x in col_upper for x in ['JUMLAH', 'NOMINAL', 'AMOUNT', 'TOTAL', 'NILAI']):
                jumlah_col = col
                print(f"  ✓ JUMLAH column: {col}")
                break
        
        if not jumlah_col:
            raise Exception(f'JUMLAH column not found. Available columns: {list(df.columns)}')
        
        # Rename columns
        rename_dict = {
            nomen_col: 'nomen',
            tgl_col: 'tgl_bayar',
            jumlah_col: 'jumlah_bayar'  # Match database schema
        }
        
        df = df.rename(columns=rename_dict)
        
        # Clean data
        df['nomen'] = df['nomen'].astype(str).str.strip().str.replace('.0', '', regex=False)
        df = df[df['nomen'].notna() & (df['nomen'] != '') & (df['nomen'] != 'nan')]
        
        if len(df) == 0:
            raise Exception('No valid NOMEN found after cleaning')
        
        # Parse date
        def parse_tgl_bayar(tgl_str):
            if pd.isna(tgl_str) or tgl_str == '':
                return None
            try:
                tgl_str = str(tgl_str).strip()
                # Try various formats
                for fmt in ['%d/%m/%Y', '%d-%m-%Y', '%Y-%m-%d', '%d.%m.%Y']:
                    try:
                        return pd.to_datetime(tgl_str, format=fmt).strftime('%Y-%m-%d')
                    except:
                        continue
                return pd.to_datetime(tgl_str).strftime('%Y-%m-%d')
            except:
                return None
        
        df['tgl_bayar'] = df['tgl_bayar'].apply(parse_tgl_bayar)
        df = df[df['tgl_bayar'].notna()]
        
        if len(df) == 0:
            raise Exception('No valid TGL_BAYAR found after parsing')
        
        # Clean jumlah_bayar
        df['jumlah_bayar'] = pd.to_numeric(df['jumlah_bayar'], errors='coerce')
        df['jumlah_bayar'] = df['jumlah_bayar'].fillna(0)
        
        # Add metadata
        df['periode_bulan'] = periode_bulan
        df['periode_tahun'] = periode_tahun
        df['upload_id'] = upload_id
        
        # Save to database - match schema columns
        cols_to_save = ['nomen', 'tgl_bayar', 'jumlah_bayar', 'periode_bulan', 'periode_tahun', 'upload_id']
        df[cols_to_save].to_sql('master_bayar', db, if_exists='append', index=False)
        
        print(f"✅ MB processed: {len(df)} rows")
        return len(df)
        
    except Exception as e:
        print(f"❌ MB processing error: {e}")
        traceback.print_exc()
        raise


def process_mainbill_file(filepath, upload_id, periode_bulan, periode_tahun, db):
    """Process MainBill file"""
    try:
        # Read file
        if filepath.endswith('.txt'):
            df = pd.read_csv(filepath, sep='|', dtype=str)
        elif filepath.endswith('.csv'):
            df = pd.read_csv(filepath, dtype=str)
        elif filepath.endswith(('.xls', '.xlsx')):
            df = pd.read_excel(filepath)
        else:
            raise Exception('Unsupported file format')
        
        # Normalize column names
        df.columns = df.columns.str.upper().str.strip()
        
        print(f"MainBill Columns: {list(df.columns)}")
        
        # Find key columns (flexible mapping)
        nomen_col = None
        for col in df.columns:
            if any(x in col for x in ['NOMEN', 'NOPEL', 'ACCOUNT', 'CUSTOMER']):
                nomen_col = col
                break
        
        if not nomen_col:
            raise Exception('NOMEN column not found')
        
        # Find tagihan column
        tagihan_col = None
        for col in df.columns:
            if any(x in col for x in ['TAGIHAN', 'TOTAL', 'BILL', 'AMOUNT']):
                tagihan_col = col
                break
        
        # Rename
        rename_dict = {nomen_col: 'nomen'}
        if tagihan_col:
            rename_dict[tagihan_col] = 'total_tagihan'
        
        df = df.rename(columns=rename_dict)
        
        # Clean nomen
        df['nomen'] = df['nomen'].astype(str).str.strip().str.replace('.0', '', regex=False)
        df = df[df['nomen'].notna() & (df['nomen'] != '') & (df['nomen'] != 'nan')]
        
        # Clean tagihan if exists
        if 'total_tagihan' in df.columns:
            df['total_tagihan'] = pd.to_numeric(df['total_tagihan'], errors='coerce').fillna(0)
        else:
            df['total_tagihan'] = 0
        
        # Add metadata
        df['periode_bulan'] = periode_bulan
        df['periode_tahun'] = periode_tahun
        df['upload_id'] = upload_id
        
        # Save to database
        cols_to_save = ['nomen', 'total_tagihan', 'periode_bulan', 'periode_tahun', 'upload_id']
        df[cols_to_save].to_sql('mainbill', db, if_exists='append', index=False)
        
        print(f"✅ MainBill processed: {len(df)} rows")
        return len(df)
        
    except Exception as e:
        print(f"❌ MainBill processing error: {e}")
        traceback.print_exc()
        raise


def process_ardebt_file(filepath, upload_id, periode_bulan, periode_tahun, db):
    """Process Ardebt (AR Debt) file"""
    try:
        # Read file
        if filepath.endswith('.txt'):
            df = pd.read_csv(filepath, sep='|', dtype=str)
        elif filepath.endswith('.csv'):
            df = pd.read_csv(filepath, dtype=str)
        elif filepath.endswith(('.xls', '.xlsx')):
            df = pd.read_excel(filepath)
        else:
            raise Exception('Unsupported file format')
        
        # Normalize column names
        df.columns = df.columns.str.upper().str.strip()
        
        print(f"Ardebt Columns: {list(df.columns)}")
        
        # Find key columns (flexible mapping)
        nomen_col = None
        for col in df.columns:
            if any(x in col for x in ['NOMEN', 'NOPEL', 'ACCOUNT', 'CUSTOMER']):
                nomen_col = col
                break
        
        if not nomen_col:
            raise Exception('NOMEN column not found')
        
        # Find saldo column
        saldo_col = None
        for col in df.columns:
            if any(x in col for x in ['SALDO', 'JUMLAH', 'TOTAL', 'DEBT', 'TUNGGAKAN']):
                saldo_col = col
                break
        
        # Rename
        rename_dict = {nomen_col: 'nomen'}
        if saldo_col:
            rename_dict[saldo_col] = 'saldo_tunggakan'
        
        df = df.rename(columns=rename_dict)
        
        # Clean nomen
        df['nomen'] = df['nomen'].astype(str).str.strip().str.replace('.0', '', regex=False)
        df = df[df['nomen'].notna() & (df['nomen'] != '') & (df['nomen'] != 'nan')]
        
        # Clean saldo if exists
        if 'saldo_tunggakan' in df.columns:
            df['saldo_tunggakan'] = pd.to_numeric(df['saldo_tunggakan'], errors='coerce').fillna(0)
        else:
            df['saldo_tunggakan'] = 0
        
        # Add metadata
        df['periode_bulan'] = periode_bulan
        df['periode_tahun'] = periode_tahun
        df['upload_id'] = upload_id
        
        # Save to database
        cols_to_save = ['nomen', 'saldo_tunggakan', 'periode_bulan', 'periode_tahun', 'upload_id']
        df[cols_to_save].to_sql('ardebt', db, if_exists='append', index=False)
        
        print(f"✅ Ardebt processed: {len(df)} rows")
        return len(df)
        
    except Exception as e:
        print(f"❌ Ardebt processing error: {e}")
        traceback.print_exc()
        raise


@app.route('/upload_multi', methods=['POST'])
def upload_multi():
    """Upload multiple files dengan auto-detect"""
    if not AUTO_DETECT_AVAILABLE:
        return jsonify({'error': 'Auto-detect module not available'}), 500
    
    if 'files[]' not in request.files:
        return jsonify({'error': 'No files selected'}), 400
    
    files = request.files.getlist('files[]')
    
    if not files or files[0].filename == '':
        return jsonify({'error': 'No files selected'}), 400
    
    # Check manual override
    manual_override = request.form.get('manual_override')
    override_dict = {}
    if manual_override:
        try:
            import json
            override_dict = json.loads(manual_override)
        except Exception as json_error:
            print(f"⚠️ Failed to parse manual override: {json_error}")
    
    results = []
    db = get_db()
    
    for file in files:
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        
        try:
            # Save file
            file.save(filepath)
            print(f"\n📁 Processing: {filename}")
            
            # Auto-detect with error handling
            detection = None
            try:
                detection = auto_detect_periode(filepath, filename)
            except Exception as detect_error:
                print(f"❌ Auto-detect error for {filename}: {detect_error}")
                traceback.print_exc()
                results.append({
                    'filename': filename,
                    'status': 'error',
                    'message': f'Auto-detect failed: {str(detect_error)}'
                })
                continue
            
            if not detection:
                results.append({
                    'filename': filename,
                    'status': 'error',
                    'message': 'Cannot detect file type or periode - please set manually'
                })
                continue
            
            # Validate detection results
            file_type = detection.get('file_type')
            periode_bulan = detection.get('periode_bulan')
            periode_tahun = detection.get('periode_tahun')
            
            if not file_type or not periode_bulan or not periode_tahun:
                results.append({
                    'filename': filename,
                    'status': 'error',
                    'message': f'Incomplete detection: type={file_type}, bulan={periode_bulan}, tahun={periode_tahun}'
                })
                continue
            
            file_type = detection['file_type']
            periode_bulan = detection['periode_bulan']
            periode_tahun = detection['periode_tahun']
            periode_label = detection['periode_label']
            detect_method = detection['method']
            
            print(f"  Type: {file_type}, Periode: {periode_label}, Method: {detect_method}")
            
            # Apply manual override
            if filename in override_dict:
                periode_bulan = override_dict[filename].get('bulan', periode_bulan)
                periode_tahun = override_dict[filename].get('tahun', periode_tahun)
                periode_label = get_periode_label(periode_bulan, periode_tahun)
                detect_method = 'manual_override'
                print(f"  Override applied: {periode_label}")
            
            # Check duplicate
            existing = db.execute('''
                SELECT id FROM upload_metadata 
                WHERE file_type = ? AND periode_bulan = ? AND periode_tahun = ?
            ''', (file_type, periode_bulan, periode_tahun)).fetchone()
            
            action = 'new'
            if existing:
                action = 'replace'
                db.execute('DELETE FROM upload_metadata WHERE id = ?', (existing['id'],))
                print(f"  ⚠️  Replacing existing upload ID: {existing['id']}")
            
            # Save metadata
            cursor = db.execute('''
                INSERT INTO upload_metadata 
                (file_type, file_name, periode_bulan, periode_tahun, row_count)
                VALUES (?, ?, ?, ?, 0)
            ''', (file_type, filename, periode_bulan, periode_tahun))
            upload_id = cursor.lastrowid
            db.commit()
            
            print(f"  ✅ Metadata saved, upload_id: {upload_id}")
            
            # Process file
            row_count = 0
            try:
                if file_type == 'MC':
                    row_count = process_mc_file(filepath, upload_id, periode_bulan, periode_tahun, db)
                elif file_type == 'COLLECTION':
                    row_count = process_collection_file(filepath, upload_id, periode_bulan, periode_tahun, db)
                elif file_type == 'SBRS':
                    row_count = process_sbrs_file(filepath, upload_id, periode_bulan, periode_tahun, db)
                elif file_type == 'MB':
                    row_count = process_mb_file(filepath, upload_id, periode_bulan, periode_tahun, db)
                elif file_type == 'MAINBILL':
                    row_count = process_mainbill_file(filepath, upload_id, periode_bulan, periode_tahun, db)
                elif file_type == 'ARDEBT':
                    row_count = process_ardebt_file(filepath, upload_id, periode_bulan, periode_tahun, db)
                
                # Update row count
                db.execute('UPDATE upload_metadata SET row_count = ? WHERE id = ?', (row_count, upload_id))
                db.commit()
                
                print(f"  ✅ SUCCESS: {row_count} rows processed")
                
                results.append({
                    'filename': filename,
                    'status': 'success',
                    'file_type': file_type,
                    'periode': periode_label,
                    'periode_bulan': periode_bulan,
                    'periode_tahun': periode_tahun,
                    'detect_method': detect_method,
                    'row_count': row_count,
                    'action': action,
                    'message': f'✅ {file_type} uploaded: {row_count:,} rows | {periode_label}'
                })
                
            except Exception as proc_error:
                print(f"  ❌ Processing error: {proc_error}")
                traceback.print_exc()
                
                # Rollback metadata
                db.execute('DELETE FROM upload_metadata WHERE id = ?', (upload_id,))
                db.commit()
                
                results.append({
                    'filename': filename,
                    'status': 'error',
                    'message': f'Processing error: {str(proc_error)}',
                    'file_type': file_type,
                    'periode': periode_label
                })
            
        except Exception as e:
            print(f"  ❌ Outer Error: {e}")
            traceback.print_exc()
            
            # Make sure we have filename
            safe_filename = filename if 'filename' in locals() else 'unknown'
            
            results.append({
                'filename': safe_filename,
                'status': 'error',
                'message': f'Unexpected error: {str(e)}'
            })
            
            # Safe rollback
            try:
                db.rollback()
            except:
                pass
    
    # Summary
    success_count = len([r for r in results if r['status'] == 'success'])
    error_count = len([r for r in results if r['status'] == 'error'])
    
    print(f"\n📊 SUMMARY: {success_count} success, {error_count} failed")
    
    response = {
        'summary': {
            'total': len(files),
            'success': success_count,
            'error': error_count,
            'warning': 0
        },
        'results': results
    }
    
    return jsonify(response)


@app.route('/api/upload_history')
def api_upload_history():
    """Get history semua upload grouped by periode"""
    db = get_db()
    
    try:
        uploads = db.execute('''
            SELECT 
                id,
                file_type,
                file_name,
                periode_bulan,
                periode_tahun,
                upload_date,
                row_count,
                status
            FROM upload_metadata
            ORDER BY periode_tahun DESC, periode_bulan DESC, upload_date DESC
        ''').fetchall()
        
        # Group by periode
        grouped = {}
        for upload in uploads:
            key = f"{upload['periode_tahun']}-{str(upload['periode_bulan']).zfill(2)}"
            
            if key not in grouped:
                grouped[key] = {
                    'periode_tahun': upload['periode_tahun'],
                    'periode_bulan': upload['periode_bulan'],
                    'periode_label': get_periode_label(upload['periode_bulan'], upload['periode_tahun']),
                    'files': []
                }
            
            grouped[key]['files'].append({
                'id': upload['id'],
                'file_type': upload['file_type'],
                'file_name': upload['file_name'],
                'upload_date': upload['upload_date'],
                'row_count': upload['row_count'],
                'status': upload.get('status', 'success')
            })
        
        return jsonify(list(grouped.values()))
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


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
    print("📅 PERIODE LOGIC (PDAM Business Rules):")
    print("   Periode Desember 2025 contains:")
    print("   • MC November 2025 (offset +1)")
    print("   • MB November 2025 (offset +1)")
    print("   • ARDEBT November 2025 (offset +1)")
    print("   • Collection Desember 2025 (no offset)")
    print("   • SBRS Desember 2025 (no offset)")
    print("   • MainBill Desember 2025 (no offset)")
    print("="*60)
    print("🎯 Upload Sequence:")
    print("   1. MC (Master Customer) + periode")
    print("   2. SBRS (Sistem Baca Meter) + periode")
    print("   3. Collection + periode")
    print("="*60)
    app.run(host='0.0.0.0', port=5000, debug=True)
