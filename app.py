import os
import pandas as pd
from flask import Flask, request, jsonify, render_template, redirect, url_for, flash
from pymongo import MongoClient
from dotenv import load_dotenv
from werkzeug.utils import secure_filename
from werkzeug.security import check_password_hash, generate_password_hash
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from flask_wtf import FlaskForm 
from wtforms import StringField, PasswordField, SubmitField 
from wtforms.validators import DataRequired 
from functools import wraps
import re 

load_dotenv() 

# --- KONFIGURASI APLIKASI & DATABASE ---
app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv("SECRET_KEY")

# Konfigurasi MongoDB
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB_NAME")

NOME_COLUMN_NAME = 'NOMEN' 
ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls'} 

# Koneksi ke MongoDB
client = None
collection_mc = None
collection_mb = None
collection_cid = None
collection_sbrs = None
collection_ardebt = None

try:
    client = MongoClient(MONGO_URI)
    client.admin.command('ping') 
    db = client[DB_NAME]
    
    # 🚨 KOLEKSI DIPISAH BERDASARKAN SUMBER DATA
    collection_mc = db['MasterCetak']   # MC (Piutang/Tagihan Bulanan - REPLACE)
    collection_mb = db['MasterBayar']   # MB (Koleksi Harian - APPEND)
    collection_cid = db['CustomerData'] # CID (Data Pelanggan Statis - REPLACE)
    collection_sbrs = db['MeterReading'] # SBRS (Baca Meter Harian/Parsial - APPEND)
    collection_ardebt = db['AccountReceivable'] # ARDEBT (Tunggakan Detail - REPLACE)
    
    collection_data = collection_mc

    print("Koneksi MongoDB berhasil!")
except Exception as e:
    print(f"Gagal terhubung ke MongoDB: {e}")
    client = None


# --- PEMROSESAN DAFTAR PENGGUNA DARI .ENV (STATIC LOGIN) ---
STATIC_USERS = {}
user_list_str = os.getenv("USER_LIST", "")
if user_list_str:
    for user_entry in user_list_str.split(','):
        try:
            username, plain_password, is_admin_str = user_entry.strip().split(':')
            hashed_password = generate_password_hash(plain_password)
            is_admin = is_admin_str.lower() == 'true'
            STATIC_USERS[username] = {
                'id': username, 'password_hash': hashed_password,
                'is_admin': is_admin, 'username': username
            }
        except ValueError as e:
            print(f"Peringatan: Format USER_LIST salah pada entry '{user_entry}'. Error: {e}")


# --- KONFIGURASI FLASK-LOGIN ---
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login' 
login_manager.login_message_category = 'info'

# --- KELAS DAN DEKORATOR TETAP SAMA ---
class User(UserMixin):
    def __init__(self, user_data):
        self.id = user_data['id']
        self.username = user_data['username']
        self.password_hash = user_data['password_hash']
        self.is_admin = user_data['is_admin']

@login_manager.user_loader
def load_user(user_id):
    user_data = STATIC_USERS.get(user_id)
    if user_data:
        return User(user_data)
    return None

class LoginForm(FlaskForm):
    username = StringField('Username', validators=[DataRequired()])
    password = PasswordField('Password', validators=[DataRequired()])
    submit = SubmitField('Masuk')

def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.is_admin:
            flash('Anda tidak memiliki izin (Admin) untuk mengakses halaman ini.', 'danger')
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# --- ENDPOINT AUTENTIKASI ---
@app.route('/login', methods=['GET', 'POST'])
def login():
    form = LoginForm()
    if form.validate_on_submit():
        user_data_entry = STATIC_USERS.get(form.username.data)

        if user_data_entry and check_password_hash(user_data_entry['password_hash'], form.password.data):
            user = User(user_data_entry) 
            login_user(user)
            flash('Login berhasil.', 'success')
            next_page = request.args.get('next')
            return redirect(next_page or url_for('index'))
        else:
            flash('Login Gagal. Cek username dan password Anda.', 'danger')
            
    return render_template('login.html', form=form)

@app.route('/logout')
@login_required
def logout():
    logout_user()
    flash('Anda telah keluar.', 'success')
    return redirect(url_for('login'))

# --- ENDPOINT UTAMA (MENU CARI) ---
@app.route('/')
@login_required 
def index():
    return render_template('index.html', is_admin=current_user.is_admin)

@app.route('/api/search', methods=['GET'])
@login_required 
def search_nomen():
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500
        
    query_nomen = request.args.get('nomen', '').strip()

    if not query_nomen:
        return jsonify({"status": "fail", "message": "Masukkan NOMEN untuk memulai pencarian terintegrasi."}), 400

    try:
        # 1. DATA STATIS (CID) - Master Data Pelanggan
        cid_result = collection_cid.find_one({'NOMEN': query_nomen})
        
        if not cid_result:
            return jsonify({
                "status": "not_found",
                "message": f"NOMEN {query_nomen} tidak ditemukan di Master Data Pelanggan (CID)."
            }), 404

        # 2. PIUTANG BERJALAN (MC) - Snapshot Bulan Ini
        mc_results = list(collection_mc.find({'NOMEN': query_nomen}))
        piutang_nominal_total = sum(item.get('NOMINAL', 0) for item in mc_results)
        
        # 3. TUNGGAKAN DETAIL (ARDEBT)
        ardebt_results = list(collection_ardebt.find({'NOMEN': query_nomen}))
        tunggakan_nominal_total = sum(item.get('JUMLAH', 0) for item in ardebt_results)
        
        # 4. RIWAYAT PEMBAYARAN TERAKHIR (MB)
        # Mencari berdasarkan NOMEN di MB
        mb_last_payment_cursor = collection_mb.find({'NOMEN': query_nomen}).sort('TGL_BAYAR', -1).limit(1)
        last_payment = list(mb_last_payment_cursor)[0] if list(mb_last_payment_cursor) else None
        
        # 5. RIWAYAT BACA METER (SBRS)
        sbrs_last_read_cursor = collection_sbrs.find({'CMR_ACCOUNT': query_nomen}).sort('CMR_RD_DATE', -1).limit(2)
        sbrs_history = list(sbrs_last_read_cursor)
        
        # --- LOGIKA KECERDASAN (INTEGRASI & DIAGNOSTIK) ---
        
        # A. Status Tunggakan/Piutang
        if tunggakan_nominal_total > 0:
            status_financial = f"TUNGGAKAN AKTIF ({len(ardebt_results)} Bulan)"
        elif piutang_nominal_total > 0:
            status_financial = f"PIUTANG BULAN BERJALAN"
        else:
            status_financial = "LUNAS / TIDAK ADA TAGIHAN"
            
        # B. Status Pembayaran
        last_payment_date = last_payment.get('TGL_BAYAR', 'N/A') if last_payment else 'BELUM ADA PEMBAYARAN MB'

        # C. Status Pemakaian (Anomaly Check)
        status_pemakaian = "DATA SBRS KURANG"
        kubik_terakhir = 0
        if len(sbrs_history) >= 1:
            kubik_terakhir = sbrs_history[0].get('CMR_KUBIK', 0)
            
            if kubik_terakhir > 100: # Threshold Ekstrim
                status_pemakaian = f"EKSTRIM ({kubik_terakhir} m³)"
            elif kubik_terakhir <= 5 and kubik_terakhir > 0: # Threshold Rendah
                status_pemakaian = f"TURUN DRASITS / RENDAH ({kubik_terakhir} m³)"
            elif kubik_terakhir == 0:
                status_pemakaian = "ZERO (0 m³) / NON-AKTIF"
            else:
                status_pemakaian = f"NORMAL ({kubik_terakhir} m³)"


        health_summary = {
            "NOMEN": query_nomen,
            "NAMA": cid_result.get('NAMA', 'N/A'),
            "ALAMAT": cid_result.get('ALAMAT', 'N/A'),
            "RAYON": cid_result.get('RAYON', 'N/A'),
            "TIPE_PLGGN": cid_result.get('TIPEPLGGN', 'N/A'),
            "STATUS_FINANSIAL": status_financial,
            "TOTAL_PIUTANG_NOMINAL": piutang_nominal_total + tunggakan_nominal_total,
            "PEMBAYARAN_TERAKHIR": last_payment_date,
            "STATUS_PEMAKAIAN": status_pemakaian
        }
        
        # Hapus _id dari semua hasil
        def clean_mongo_id(doc):
            doc.pop('_id', None)
            return doc

        return jsonify({
            "status": "success",
            "summary": health_summary,
            "cid_data": clean_mongo_id(cid_result),
            "mc_data": [clean_mongo_id(doc) for doc in mc_results],
            "ardebt_data": [clean_mongo_id(doc) for doc in ardebt_results],
            "sbrs_data": [clean_mongo_id(doc) for doc in sbrs_history]
        }), 200

    except Exception as e:
        print(f"Error saat mencari data terintegrasi: {e}")
        return jsonify({"message": f"Gagal mengambil data terintegrasi: {e}"}), 500

# --- ENDPOINT KOLEKSI DAN ANALISIS LAINNYA ---
@app.route('/daily_collection', methods=['GET'])
@login_required 
def daily_collection_unified_page():
    return render_template('collection_unified.html', is_admin=current_user.is_admin)

@app.route('/api/collection/report', methods=['GET'])
@login_required 
def collection_report_api():
    """Menghitung Nomen Bayar, Nominal Bayar, Total Nominal, dan Persentase per Rayon/PCEZ."""
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500

    # Pipeline Awal: Memastikan Rayon dan PCEZ selalu ada untuk menghindari KeyError
    initial_project = {
        '$project': {
            'RAYON': { '$ifNull': [ '$RAYON', 'N/A' ] }, 
            'PCEZ': { '$ifNull': [ '$PCEZ', 'N/A' ] },   
            'NOMEN': 1,
            'NOMINAL': 1,
            'STATUS': 1
        }
    }
    
    # MENGGUNAKAN collection_mc SEBAGAI SUMBER PIUTANG UTAMA
    pipeline_billed = [
        initial_project, 
        { '$group': {
            '_id': { 'rayon': '$RAYON', 'pcez': '$PCEZ' },
            'total_nomen_all': { '$addToSet': '$NOMEN' },
            'total_nominal': { '$sum': '$NOMINAL' } 
        }}
    ]
    billed_data = list(collection_mc.aggregate(pipeline_billed))

    pipeline_collected = [
        initial_project, 
        { '$match': { 'STATUS': 'Payment' } }, 
        { '$group': {
            '_id': { 'rayon': '$RAYON', 'pcez': '$PCEZ' },
            'collected_nomen': { '$addToSet': '$NOMEN' }, 
            'collected_nominal': { '$sum': '$NOMINAL' } 
        }}
    ]
    collected_data = list(collection_mc.aggregate(pipeline_collected))

    report_map = {}
    
    for item in billed_data:
        key = (item['_id']['rayon'], item['_id']['pcez'])
        report_map[key] = {
            'RAYON': item['_id']['rayon'],
            'PCEZ': item['_id']['pcez'],
            'TotalNominal': float(item['total_nominal']),
            'TotalNomen': len(item['total_nomen_all']),
            'CollectedNominal': 0.0, 'CollectedNomen': 0, 'PercentNominal': 0.0, 'PercentNomenCount': 0.0
        }

    for item in collected_data:
        key = (item['_id']['rayon'], item['_id']['pcez'])
        if key in report_map:
            report_map[key]['CollectedNominal'] = float(item['collected_nominal'])
            report_map[key]['CollectedNomen'] = len(item['collected_nomen'])
            
            if report_map[key]['TotalNominal'] > 0:
                report_map[key]['PercentNominal'] = (report_map[key]['CollectedNominal'] / report_map[key]['TotalNominal']) * 100
            
            if report_map[key]['TotalNomen'] > 0:
                report_map[key]['PercentNomenCount'] = (report_map[key]['CollectedNomen'] / report_map[key]['TotalNomen']) * 100

    grand_total = {
        'TotalNominal': sum(d['TotalNominal'] for d in report_map.values()),
        'CollectedNominal': sum(d['CollectedNominal'] for d in report_map.values()),
        'TotalNomen': sum(d['TotalNomen'] for d in report_map.values()),
        'CollectedNomen': sum(d['CollectedNomen'] for d in report_map.values())
    }
    
    grand_total['PercentNominal'] = (grand_total['CollectedNominal'] / grand_total['TotalNominal']) * 100 if grand_total['TotalNominal'] > 0 else 0
    grand_total['PercentNomenCount'] = (grand_total['CollectedNomen'] / grand_total['TotalNomen']) * 100 if grand_total['TotalNomen'] > 0 else 0


    return jsonify({
        'report_data': list(report_map.values()),
        'grand_total': grand_total
    }), 200

@app.route('/api/collection/detail', methods=['GET'])
@login_required 
def collection_detail_api():
    """Endpoint API untuk mengambil data koleksi yang difilter dan diurutkan."""
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500
        
    query_str = request.args.get('q', '').strip()
    
    mongo_query = {} 
    
    if query_str:
        safe_query_str = re.escape(query_str)
        
        # Kolom yang mungkin ada di MB untuk filtering
        search_filter = {
            '$or': [
                {'RAYON': {'$regex': safe_query_str, '$options': 'i'}}, 
                {'PCEZ': {'$regex': safe_query_str, '$options': 'i'}},
                {'NOMEN': {'$regex': safe_query_str, '$options': 'i'}},
                {'ZONA_NOREK': {'$regex': safe_query_str, '$options': 'i'}} # Kolom alternatif di MB
            ]
        }
        mongo_query.update(search_filter)

    sort_order = [('TGL_BAYAR', -1)] # Sortasi berdasarkan TGL_BAYAR (dari MB)

    try:
        # MENGGUNAKAN collection_mb UNTUK DETAIL TRANSAKSI KOLEKSI
        results = list(collection_mb.find(mongo_query).sort(sort_order).limit(1000))

        cleaned_results = []
        for doc in results:
            nominal_val = float(doc.get('NOMINAL', 0)) 
            
            cleaned_results.append({
                'NOMEN': doc.get('NOMEN', 'N/A'),
                # Mapping kolom MB ke nama kolom Frontend
                'RAYON': doc.get('RAYON', doc.get('ZONA_NOREK', 'N/A')), 
                'PCEZ': doc.get('PCEZ', doc.get('LKS_BAYAR', 'N/A')),
                'NOMINAL': nominal_val,
                'PAY_DT': doc.get('TGL_BAYAR', 'N/A')
            })
            
        return jsonify(cleaned_results), 200

    except Exception as e:
        print(f"Error fetching detailed collection data: {e}")
        return jsonify({"message": f"Gagal mengambil data detail koleksi: {e}"}), 500


# --- ENDPOINT ANALISIS DATA LANJUTAN (SUB-MENU DASHBOARD) ---

@app.route('/analyze', methods=['GET'])
@login_required
def analyze_reports_landing():
    """Landing Page untuk Sub-menu Analisis."""
    return render_template('analyze_landing.html', is_admin=current_user.is_admin)

# Endpoint Laporan Anomali (Placeholder)
@app.route('/analyze/extreme', methods=['GET'])
@login_required
def analyze_extreme_usage():
    return render_template('analyze_report_template.html', 
                           title="Pemakaian Air Ekstrim", 
                           description="Menampilkan pelanggan dengan konsumsi air di atas ambang batas (memerlukan join MC, CID, dan SBRS).",
                           is_admin=current_user.is_admin)

@app.route('/analyze/reduced', methods=['GET'])
@login_required
def analyze_reduced_usage():
    return render_template('analyze_report_template.html', 
                           title="Pemakaian Air Naik/Turun (Fluktuasi Volume)", 
                           description="Menampilkan pelanggan dengan fluktuasi konsumsi air signifikan (naik atau turun) dengan membandingkan 2 periode SBRS terakhir.",
                           is_admin=current_user.is_admin)

@app.route('/analyze/zero', methods=['GET'])
@login_required
def analyze_zero_usage():
    return render_template('analyze_report_template.html', 
                           title="Tidak Ada Pemakaian (Zero)", 
                           description="Menampilkan pelanggan dengan konsumsi air nol (Zero) di periode tagihan terakhir.",
                           is_admin=current_user.is_admin)

@app.route('/analyze/standby', methods=['GET'])
@login_required
def analyze_stand_tungggu():
    return render_template('analyze_report_template.html', 
                           title="Stand Tunggu", 
                           description="Menampilkan pelanggan yang berstatus Stand Tunggu (Freeze/Blokir).",
                           is_admin=current_user.is_admin)

# =========================================================================
# === API BARU UNTUK ANALISIS AKURAT (Fluktuasi Volume Naik/Turun) ===
# =========================================================================
@app.route('/api/analyze/volume_fluctuation', methods=['GET'])
@login_required 
def analyze_volume_fluctuation_api():
    """Analisis Fluktuasi Pemakaian (Volume Naik/Turun) dengan membandingkan 2 riwayat SBRS terakhir."""
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500

    try:
        # Pipeline untuk analisis volume Naik/Turun
        pipeline_sbrs_history = [
            {
                # Sortasi data per account berdasarkan tanggal baca terbaru
                '$sort': {'CMR_ACCOUNT': 1, 'CMR_RD_DATE': -1} 
            },
            {
                # Kelompokkan berdasarkan CMR_ACCOUNT (NOMEN)
                '$group': {
                    '_id': '$CMR_ACCOUNT',
                    'history': {
                        '$push': {
                            # Konversi eksplisit ke double untuk memastikan operasi matematika berhasil
                            'kubik': {'$toDouble': {'$ifNull': ['$CMR_KUBIK', 0]}}, 
                            'tanggal': '$CMR_RD_DATE'
                        }
                    }
                }
            },
            {
                # Ambil hanya 2 entri pertama (riwayat kubikasi terbaru dan sebelumnya)
                '$project': {
                    'NOMEN': '$_id',
                    'latest': {'$arrayElemAt': ['$history', 0]},
                    'previous': {'$arrayElemAt': ['$history', 1]},
                    '_id': 0
                }
            },
            {
                # Filter hanya yang punya 2 data riwayat untuk perbandingan, dan pastikan nilai kubik ada
                '$match': {
                    'previous': {'$ne': None},
                    'latest': {'$ne': None},
                    'latest.kubik': {'$ne': None},
                    'previous.kubik': {'$ne': None}
                }
            },
            {
                # Kalkulasi Fluktuasi dan Status
                '$project': {
                    'NOMEN': 1,
                    'KUBIK_TERBARU': '$latest.kubik',
                    'KUBIK_SEBELUMNYA': '$previous.kubik',
                    'SELISIH_KUBIK': {'$subtract': ['$latest.kubik', '$previous.kubik']},
                    'PERSEN_SELISIH': {
                        '$cond': {
                            # Jika kubik sebelumnya > 0, lakukan perhitungan persentase
                            'if': {'$gt': ['$previous.kubik', 0]},
                            'then': {'$multiply': [{'$divide': [{'$subtract': ['$latest.kubik', '$previous.kubik']}, '$previous.kubik']}, 100]},
                            # Jika 0, set persentase ke 0 (karena tidak ada basis perbandingan)
                            'else': 0 
                        }
                    }
                }
            },
            {
                # Tentukan Status Fluktuasi
                '$addFields': {
                    'STATUS_PEMAKAIAN': {
                        '$switch': {
                            'branches': [
                                { 'case': {'$gte': ['$PERSEN_SELISIH', 50]}, 'then': 'NAIK EKSTRIM (>=50%)' }, 
                                { 'case': {'$gte': ['$PERSEN_SELISIH', 10]}, 'then': 'NAIK SIGNIFIKAN (>=10%)' }, 
                                { 'case': {'$lte': ['$PERSEN_SELISIH', -50]}, 'then': 'TURUN EKSTRIM (<= -50%)' }, 
                                { 'case': {'$lte': ['$PERSEN_SELISIH', -10]}, 'then': 'TURUN SIGNIFIKAN (<= -10%)' }, 
                                { 'case': {'$eq': ['$KUBIK_TERBARU', 0]}, 'then': 'ZERO / NOL' },
                            ],
                            'default': 'STABIL / NORMAL'
                        }
                    }
                }
            },
            {
                 # Gabungkan (Join) dengan CID untuk mendapatkan Nama dan Rayon (Lookup)
                 '$lookup': {
                    'from': 'CustomerData', 
                    'localField': 'NOMEN',
                    'foreignField': 'NOMEN',
                    'as': 'customer_info'
                 }
            },
            {
                '$unwind': {'path': '$customer_info', 'preserveNullAndEmptyArrays': True}
            },
            {
                # Proyeksi Akhir
                '$project': {
                    'NOMEN': 1,
                    'NAMA': {'$ifNull': ['$customer_info.NAMA', 'N/A']},
                    'RAYON': {'$ifNull': ['$customer_info.RAYON', 'N/A']},
                    'KUBIK_TERBARU': 1,
                    'KUBIK_SEBELUMNYA': 1,
                    'SELISIH_KUBIK': 1,
                    'PERSEN_SELISIH': {'$round': ['$PERSEN_SELISIH', 2]},
                    'STATUS_PEMAKAIAN': 1
                }
            },
            {
                 # Filter hanya yang anomali untuk ditampilkan di laporan ini
                 '$match': {
                    '$or': [
                        {'STATUS_PEMAKAIAN': 'NAIK EKSTRIM (>=50%)'},
                        {'STATUS_PEMAKAIAN': 'NAIK SIGNIFIKAN (>=10%)'},
                        {'STATUS_PEMAKAIAN': 'TURUN EKSTRIM (<= -50%)'},
                        {'STATUS_PEMAKAIAN': 'TURUN SIGNIFIKAN (<= -10%)'},
                        {'STATUS_PEMAKAIAN': 'ZERO / NOL'}
                    ]
                 }
            }
        ]
        
        # Eksekusi pipeline
        fluctuation_data = list(collection_sbrs.aggregate(pipeline_sbrs_history))
        
        # Hapus _id
        cleaned_data = []
        for doc in fluctuation_data:
            doc.pop('_id', None)
            cleaned_data.append(doc)

        return jsonify(cleaned_data), 200

    except Exception as e:
        # Pesan error yang lebih membantu jika pipeline gagal
        print(f"Error saat menganalisis fluktuasi volume: {e}")
        return jsonify({"message": f"Gagal mengambil data fluktuasi volume. Pastikan Anda sudah mengunggah data SBRS yang valid dan memiliki minimal 2 riwayat baca meter per pelanggan. Detail teknis error: {e}"}), 500
# =========================================================================
# === END API BARU ===
# =========================================================================

# --- ENDPOINT KELOLA UPLOAD (ADMIN) ---
@app.route('/admin/upload', methods=['GET'])
@login_required 
@admin_required 
def admin_upload_unified_page():
    return render_template('upload_admin_unified.html', is_admin=current_user.is_admin)

@app.route('/upload/mc', methods=['POST'])
@login_required 
@admin_required 
def upload_mc_data():
    """Mode GANTI: Untuk Master Cetak (Piutang Bulanan)."""
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500
    
    if 'file' not in request.files:
        return jsonify({"message": "Tidak ada file di permintaan"}), 400

    file = request.files['file']
    if file.filename == '' or not allowed_file(file.filename):
        return jsonify({"message": "Format file tidak valid. Harap unggah CSV, XLSX, atau XLS."}), 400

    filename = secure_filename(file.filename)
    file_extension = filename.rsplit('.', 1)[1].lower()

    try:
        if file_extension == 'csv':
            df = pd.read_csv(file)
        elif file_extension in ['xlsx', 'xls']:
            df = pd.read_excel(file, sheet_name=0) 
        
        df.columns = [col.strip().upper() for col in df.columns]
        
        # PEMBERSIHAN DATA AMAN
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.strip()
            # Kolom finansial MC
            if col in ['NOMINAL', 'NOMINAL_AKHIR', 'KUBIK', 'SUBNOMINAL', 'ANG_BP', 'DENDA', 'PPN']: 
                 df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        data_to_insert = df.to_dict('records')
        
        # OPERASI KRITIS: HAPUS DAN GANTI (REPLACE)
        collection_mc.delete_many({})
        collection_mc.insert_many(data_to_insert)
        count = len(data_to_insert)
        return jsonify({"message": f"Sukses! {count} baris Master Cetak (MC) berhasil MENGGANTI data lama."}), 200

    except Exception as e:
        print(f"Error saat memproses file MC: {e}")
        return jsonify({"message": f"Gagal memproses file MC: {e}. Pastikan format data benar."}), 500

@app.route('/upload/mb', methods=['POST'])
@login_required 
@admin_required 
def upload_mb_data():
    """Mode APPEND: Untuk Master Bayar (MB) / Koleksi Harian."""
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500
        
    if 'file' not in request.files:
        return jsonify({"message": "Tidak ada file di permintaan"}), 400

    file = request.files['file']
    if file.filename == '' or not allowed_file(file.filename):
        return jsonify({"message": "Format file tidak valid. Harap unggah CSV, XLSX, atau XLS."}), 400

    filename = secure_filename(file.filename)
    file_extension = filename.rsplit('.', 1)[1].lower()

    try:
        if file_extension == 'csv':
            df = pd.read_csv(file)
        elif file_extension in ['xlsx', 'xls']:
            df = pd.read_excel(file, sheet_name=0) 
        
        df.columns = [col.strip().upper() for col in df.columns]

        # PEMBERSIHAN DATA AMAN
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.strip()
            if col in ['NOMINAL', 'SUBNOMINAL', 'BEATETAP', 'BEA_SEWA']: # Kolom finansial MB
                 df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        data_to_insert = df.to_dict('records')
        
        if not data_to_insert:
            return jsonify({"message": "File kosong, tidak ada data yang dimasukkan."}), 200
        
        # OPERASI KRITIS: APPEND DATA BARU DENGAN PENCEGAHAN DUPLIKASI
        inserted_count = 0
        skipped_count = 0
        
        # Kunci unik: NOTAGIHAN (dari MB) + TGL_BAYAR
        UNIQUE_KEYS = ['NOTAGIHAN', 'TGL_BAYAR', 'NOMINAL'] 
        
        if not all(key in df.columns for key in UNIQUE_KEYS):
            return jsonify({"message": f"Gagal Append: File MB harus memiliki kolom kunci unik: {', '.join(UNIQUE_KEYS)}. Cek file Anda."}), 400

        for record in data_to_insert:
            filter_query = {key: record.get(key) for key in UNIQUE_KEYS}
            
            if collection_mb.find_one(filter_query):
                skipped_count += 1
            else:
                collection_mb.insert_one(record)
                inserted_count += 1
        
        return jsonify({"message": f"Sukses Append! {inserted_count} baris Master Bayar (MB) baru ditambahkan. ({skipped_count} duplikat diabaikan)."}), 200

    except Exception as e:
        print(f"Error saat memproses file MB: {e}")
        return jsonify({"message": f"Gagal memproses file MB: {e}. Pastikan format data benar."}), 500

@app.route('/upload/cid', methods=['POST'])
@login_required 
@admin_required 
def upload_cid_data():
    """Mode GANTI: Untuk Customer Data (CID) / Data Pelanggan Statis."""
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500
        
    if 'file' not in request.files:
        return jsonify({"message": "Tidak ada file di permintaan"}), 400

    file = request.files['file']
    if file.filename == '' or not allowed_file(file.filename):
        return jsonify({"message": "Format file tidak valid. Harap unggah CSV, XLSX, atau XLS."}), 400

    filename = secure_filename(file.filename)
    file_extension = filename.rsplit('.', 1)[1].lower()

    try:
        if file_extension == 'csv':
            df = pd.read_csv(file)
        elif file_extension in ['xlsx', 'xls']:
            df = pd.read_excel(file, sheet_name=0) 
        
        df.columns = [col.strip().upper() for col in df.columns]

        # PEMBERSIHAN DATA AMAN
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.strip()
        
        data_to_insert = df.to_dict('records')
        
        # OPERASI KRITIS: HAPUS DAN GANTI (REPLACE)
        collection_cid.delete_many({})
        collection_cid.insert_many(data_to_insert)
        count = len(data_to_insert)
        return jsonify({"message": f"Sukses! {count} baris Customer Data (CID) berhasil MENGGANTI data lama."}), 200

    except Exception as e:
        print(f"Error saat memproses file CID: {e}")
        return jsonify({"message": f"Gagal memproses file CID: {e}. Pastikan format data benar."}), 500

@app.route('/upload/sbrs', methods=['POST'])
@login_required 
@admin_required 
def upload_sbrs_data():
    """Mode APPEND: Untuk data Baca Meter (SBRS) / Riwayat Stand Meter."""
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500
        
    if 'file' not in request.files:
        return jsonify({"message": "Tidak ada file di permintaan"}), 400

    file = request.files['file']
    if file.filename == '' or not allowed_file(file.filename):
        return jsonify({"message": "Format file tidak valid. Harap unggah CSV, XLSX, atau XLS."}), 400

    filename = secure_filename(file.filename)
    file_extension = filename.rsplit('.', 1)[1].lower()

    try:
        if file_extension == 'csv':
            df = pd.read_csv(file)
        elif file_extension in ['xlsx', 'xls']:
            df = pd.read_excel(file, sheet_name=0) 
        
        df.columns = [col.strip().upper() for col in df.columns]

        # PEMBERSIHAN DATA AMAN
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.strip()
            # Kolom numerik penting untuk SBRS
            if col in ['CMR_PREV_READ', 'CMR_READING', 'CMR_KUBIK']: 
                 df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        data_to_insert = df.to_dict('records')
        
        if not data_to_insert:
            return jsonify({"message": "File kosong, tidak ada data yang dimasukkan."}), 200
        
        # OPERASI KRITIS: APPEND DATA BARU DENGAN PENCEGAHAN DUPLIKASI
        inserted_count = 0
        skipped_count = 0
        
        # Kunci unik: cmr_account (NOMEN) + cmr_rd_date (Tanggal Baca)
        UNIQUE_KEYS = ['CMR_ACCOUNT', 'CMR_RD_DATE'] 
        
        if not all(key in df.columns for key in UNIQUE_KEYS):
            return jsonify({"message": f"Gagal Append: File SBRS harus memiliki kolom kunci unik: {', '.join(UNIQUE_KEYS)}. Cek file Anda."}), 400

        for record in data_to_insert:
            filter_query = {key: record.get(key) for key in UNIQUE_KEYS}
            
            if collection_sbrs.find_one(filter_query):
                skipped_count += 1
            else:
                collection_sbrs.insert_one(record)
                inserted_count += 1
        
        return jsonify({"message": f"Sukses Append! {inserted_count} baris Riwayat Baca Meter (SBRS) baru ditambahkan. ({skipped_count} duplikat diabaikan)."}), 200

    except Exception as e:
        print(f"Error saat memproses file SBRS: {e}")
        return jsonify({"message": f"Gagal memproses file SBRS: {e}. Pastikan format data benar."}), 500

@app.route('/upload/ardebt', methods=['POST'])
@login_required 
@admin_required 
def upload_ardebt_data():
    """Mode GANTI: Untuk data Detail Tunggakan (ARDEBT)."""
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500
    
    if 'file' not in request.files:
        return jsonify({"message": "Tidak ada file di permintaan"}), 400

    file = request.files['file']
    if file.filename == '' or not allowed_file(file.filename):
        return jsonify({"message": "Format file tidak valid. Harap unggah CSV, XLSX, atau XLS."}), 400

    filename = secure_filename(file.filename)
    file_extension = filename.rsplit('.', 1)[1].lower()

    try:
        if file_extension == 'csv':
            df = pd.read_csv(file)
        elif file_extension in ['xlsx', 'xls']:
            df = pd.read_excel(file, sheet_name=0) 
        
        df.columns = [col.strip().upper() for col in df.columns]

        # PEMBERSIHAN DATA AMAN
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.strip()
            # Kolom numerik penting
            if col in ['JUMLAH', 'VOLUME']: 
                 df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        data_to_insert = df.to_dict('records')
        
        if not data_to_insert:
            return jsonify({"message": "File kosong, tidak ada data yang dimasukkan."}), 200
        
        # OPERASI KRITIS: HAPUS DAN GANTI (REPLACE)
        collection_ardebt.delete_many({})
        collection_ardebt.insert_many(data_to_insert)
        count = len(data_to_insert)
        
        return jsonify({"message": f"Sukses! {count} baris Detail Tunggakan (ARDEBT) berhasil MENGGANTI data lama."}), 200

    except Exception as e:
        print(f"Error saat memproses file ARDEBT: {e}")
        return jsonify({"message": f"Gagal memproses file ARDEBT: {e}. Pastikan format data benar."}), 500


# --- ENDPOINT UTAMA ---
@app.route('/')
@login_required 
def index():
    return render_template('index.html', is_admin=current_user.is_admin)

@app.route('/api/search', methods=['GET'])
@login_required 
def search_nomen():
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500
        
    query_nomen = request.args.get('nomen', '').strip()

    if not query_nomen:
        return jsonify({"status": "fail", "message": "Masukkan NOMEN untuk memulai pencarian terintegrasi."}), 400

    try:
        # 1. DATA STATIS (CID) - Master Data Pelanggan
        cid_result = collection_cid.find_one({'NOMEN': query_nomen})
        
        if not cid_result:
            return jsonify({
                "status": "not_found",
                "message": f"NOMEN {query_nomen} tidak ditemukan di Master Data Pelanggan (CID)."
            }), 404

        # 2. PIUTANG BERJALAN (MC) - Snapshot Bulan Ini
        mc_results = list(collection_mc.find({'NOMEN': query_nomen}))
        piutang_nominal_total = sum(item.get('NOMINAL', 0) for item in mc_results)
        
        # 3. TUNGGAKAN DETAIL (ARDEBT)
        ardebt_results = list(collection_ardebt.find({'NOMEN': query_nomen}))
        tunggakan_nominal_total = sum(item.get('JUMLAH', 0) for item in ardebt_results)
        
        # 4. RIWAYAT PEMBAYARAN TERAKHIR (MB)
        # Mencari berdasarkan NOMEN di MB
        mb_last_payment_cursor = collection_mb.find({'NOMEN': query_nomen}).sort('TGL_BAYAR', -1).limit(1)
        last_payment = list(mb_last_payment_cursor)[0] if list(mb_last_payment_cursor) else None
        
        # 5. RIWAYAT BACA METER (SBRS)
        sbrs_last_read_cursor = collection_sbrs.find({'CMR_ACCOUNT': query_nomen}).sort('CMR_RD_DATE', -1).limit(2)
        sbrs_history = list(sbrs_last_read_cursor)
        
        # --- LOGIKA KECERDASAN (INTEGRASI & DIAGNOSTIK) ---
        
        # A. Status Tunggakan/Piutang
        if tunggakan_nominal_total > 0:
            status_financial = f"TUNGGAKAN AKTIF ({len(ardebt_results)} Bulan)"
        elif piutang_nominal_total > 0:
            status_financial = f"PIUTANG BULAN BERJALAN"
        else:
            status_financial = "LUNAS / TIDAK ADA TAGIHAN"
            
        # B. Status Pembayaran
        last_payment_date = last_payment.get('TGL_BAYAR', 'N/A') if last_payment else 'BELUM ADA PEMBAYARAN MB'

        # C. Status Pemakaian (Anomaly Check)
        status_pemakaian = "DATA SBRS KURANG"
        kubik_terakhir = 0
        if len(sbrs_history) >= 1:
            kubik_terakhir = sbrs_history[0].get('CMR_KUBIK', 0)
            
            if kubik_terakhir > 100: # Threshold Ekstrim
                status_pemakaian = f"EKSTRIM ({kubik_terakhir} m³)"
            elif kubik_terakhir <= 5 and kubik_terakhir > 0: # Threshold Rendah
                status_pemakaian = f"TURUN DRASITS / RENDAH ({kubik_terakhir} m³)"
            elif kubik_terakhir == 0:
                status_pemakaian = "ZERO (0 m³) / NON-AKTIF"
            else:
                status_pemakaian = f"NORMAL ({kubik_terakhir} m³)"


        health_summary = {
            "NOMEN": query_nomen,
            "NAMA": cid_result.get('NAMA', 'N/A'),
            "ALAMAT": cid_result.get('ALAMAT', 'N/A'),
            "RAYON": cid_result.get('RAYON', 'N/A'),
            "TIPE_PLGGN": cid_result.get('TIPEPLGGN', 'N/A'),
            "STATUS_FINANSIAL": status_financial,
            "TOTAL_PIUTANG_NOMINAL": piutang_nominal_total + tunggakan_nominal_total,
            "PEMBAYARAN_TERAKHIR": last_payment_date,
            "STATUS_PEMAKAIAN": status_pemakaian
        }
        
        # Hapus _id dari semua hasil
        def clean_mongo_id(doc):
            doc.pop('_id', None)
            return doc

        return jsonify({
            "status": "success",
            "summary": health_summary,
            "cid_data": clean_mongo_id(cid_result),
            "mc_data": [clean_mongo_id(doc) for doc in mc_results],
            "ardebt_data": [clean_mongo_id(doc) for doc in ardebt_results],
            "sbrs_data": [clean_mongo_id(doc) for doc in sbrs_history]
        }), 200

    except Exception as e:
        print(f"Error saat mencari data terintegrasi: {e}")
        return jsonify({"message": f"Gagal mengambil data terintegrasi: {e}"}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
