import os
import pandas as pd
from flask import Flask, request, jsonify, render_template, redirect, url_for, flash, make_response
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
from datetime import datetime, timedelta 
import io 
from pymongo.errors import BulkWriteError, DuplicateKeyError, OperationFailure 

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
    # PERBAIKAN KRITIS UNTUK BULK WRITE/SBRS: Meningkatkan batas waktu koneksi dan socket.
    # Peningkatan timeout membantu mencegah hang pada query besar
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=60000, socketTimeoutMS=300000)
    client.admin.command('ping') 
    db = client[DB_NAME]
    
    # 🚨 KOLEKSI DIPISAH BERDASARKAN SUMBER DATA
    collection_mc = db['MasterCetak']
    collection_mb = db['MasterBayar']
    collection_cid = db['CustomerData']
    collection_sbrs = db['MeterReading']
    collection_ardebt = db['AccountReceivable']
    
    # ==========================================================
    # === START OPTIMASI: INDEXING KRITIS (SOLUSI KECEPATAN PERMANEN) ===
    # ==========================================================
    
    # CID (CustomerData)
    collection_cid.create_index([('NOMEN', 1), ('TANGGAL_UPLOAD_CID', -1)], name='idx_cid_nomen_hist', background=True)
    collection_cid.create_index([('RAYON', 1), ('TIPEPLGGN', 1)], name='idx_cid_rayon_tipe', background=True)

    # MC (MasterCetak)
    collection_mc.create_index([('NOMEN', 1), ('BULAN_TAGIHAN', -1)], name='idx_mc_nomen_hist', background=True)
    collection_mc.create_index([('RAYON', 1), ('PCEZ', 1)], name='idx_mc_rayon_pcez', background=True) 
    collection_mc.create_index([('STATUS', 1)], name='idx_mc_status', background=True)
    collection_mc.create_index([('TARIF', 1), ('KUBIK', 1), ('NOMINAL', 1)], name='idx_mc_tarif_volume', background=True)

    # MB (MasterBayar)
    # Index utama untuk BULK WRITE/Duplikasi
    collection_mb.create_index([('NOTAGIHAN', 1), ('TGL_BAYAR', 1), ('NOMINAL', 1)], name='idx_mb_unique_transaction', unique=False, background=True)
    collection_mb.create_index([('TGL_BAYAR', -1)], name='idx_mb_paydate_desc', background=True)
    collection_mb.create_index([('NOMEN', 1)], name='idx_mb_nomen', background=True)
    collection_mb.create_index([('RAYON', 1), ('PCEZ', 1), ('TGL_BAYAR', -1)], name='idx_mb_rayon_pcez_date', background=True)
    collection_mb.create_index([('BULAN_REK', 1), ('BILL_REASON', 1)], name='idx_mb_bulanrek_reason', background=True)
    

    # SBRS (MeterReading)
    try:
        # Coba buat index unik. Jika gagal (karena sudah ada duplikasi data), fallback ke non-unique
        collection_sbrs.create_index([('CMR_ACCOUNT', 1), ('CMR_RD_DATE', 1)], name='idx_sbrs_unique_read', unique=True, background=True)
    except OperationFailure:
        collection_sbrs.drop_index('idx_sbrs_unique_read')
        collection_sbrs.create_index([('CMR_ACCOUNT', 1), ('CMR_RD_DATE', 1)], name='idx_sbrs_unique_read', unique=False, background=True)
        
    collection_sbrs.create_index([('CMR_ACCOUNT', 1), ('CMR_RD_DATE', -1)], name='idx_sbrs_history', background=True)
    
    # ARDEBT (AccountReceivable)
    collection_ardebt.create_index([('NOMEN', 1), ('PERIODE_BILL', -1), ('JUMLAH', 1)], name='idx_ardebt_nomen_hist', unique=False, background=True)
    collection_ardebt.create_index([('NOMEN', 1)], name='idx_ardebt_nomen', background=True)
    
    # ==========================================================
    # === END OPTIMASI: INDEXING KRITIS ===
    # ==========================================================
    
    print("Koneksi MongoDB berhasil dan index dikonfigurasi!")
except Exception as e:
    print(f"Gagal terhubung ke MongoDB atau mengkonfigurasi index: {e}")
    client = None

# --- FUNGSI UTILITY INTERNAL: ANALISIS SBRS ---
def _get_sbrs_anomalies(collection_sbrs, collection_cid):
    """
    Menjalankan pipeline agregasi untuk menemukan anomali pemakaian (Naik/Turun/Zero/Ekstrim) 
    dengan membandingkan 2 riwayat SBRS terakhir dan melakukan JOIN ke CID.
    """
    if collection_sbrs is None or collection_cid is None:
        return []
        
    pipeline_sbrs_history = [
        {'$sort': {'CMR_ACCOUNT': 1, 'CMR_RD_DATE': -1}},
        {'$group': {
            '_id': '$CMR_ACCOUNT',
            'history': {
                '$push': {
                    'kubik': {'$toDouble': {'$ifNull': ['$CMR_KUBIK', 0]}}, 
                    'tanggal': '$CMR_RD_DATE'
                }
            }
        }},
        {'$project': {
            'NOMEN': '$_id',
            'latest': {'$arrayElemAt': ['$history', 0]},
            'previous': {'$arrayElemAt': ['$history', 1]},
            '_id': 0
        }},
        {'$match': {
            'previous': {'$ne': None},
            'latest': {'$ne': None},
            'latest.kubik': {'$ne': None},
            'previous.kubik': {'$ne': None}
        }},
        {'$project': {
            'NOMEN': 1,
            'KUBIK_TERBARU': '$latest.kubik',
            'KUBIK_SEBELUMNYA': '$previous.kubik',
            'SELISIH_KUBIK': {'$subtract': ['$latest.kubik', '$previous.kubik']},
            'PERSEN_SELISIH': {
                '$cond': {
                    'if': {'$gt': ['$previous.kubik', 0]},
                    'then': {'$multiply': [{'$divide': [{'$subtract': ['$latest.kubik', '$previous.kubik']}, '$previous.kubik']}, 100]},
                    'else': 0 
                }
            }
        }},
        {'$addFields': {
            'STATUS_PEMAKAIAN': {
                '$switch': {
                    'branches': [
                        { 'case': {'$gte': ['$KUBIK_TERBARU', 150]}, 'then': 'EKSTRIM (>150 m³)' },
                        { 'case': {'$gte': ['$PERSEN_SELISIH', 50]}, 'then': 'NAIK EKSTRIM (>=50%)' }, 
                        { 'case': {'$gte': ['$PERSEN_SELISIH', 10]}, 'then': 'NAIK SIGNIFIKAN (>=10%)' }, 
                        { 'case': {'$lte': ['$PERSEN_SELISIH', -50]}, 'then': 'TURUN EKSTRIM (<= -50%)' }, 
                        { 'case': {'$lte': ['$PERSEN_SELISIH', -10]}, 'then': 'TURUN SIGNIFIKAN (<= -10%)' }, 
                        { 'case': {'$eq': ['$KUBIK_TERBARU', 0]}, 'then': 'ZERO / NOL' },
                    ],
                    'default': 'STABIL / NORMAL'
                }
            }
        }},
        {'$lookup': {
           'from': 'CustomerData', 
           'localField': 'NOMEN',
           'foreignField': 'NOMEN',
           'as': 'customer_info'
        }},
        {'$unwind': {'path': '$customer_info', 'preserveNullAndEmptyArrays': True}},
        {'$project': {
            'NOMEN': 1,
            'NAMA': {'$ifNull': ['$customer_info.NAMA', 'N/A']},
            'RAYON': {'$ifNull': ['$customer_info.RAYON', 'N/A']},
            'KUBIK_TERBARU': {'$round': ['$KUBIK_TERBARU', 0]},
            'KUBIK_SEBELUMNYA': {'$round': ['$KUBIK_SEBELUMNYA', 0]},
            'SELISIH_KUBIK': {'$round': ['$SELISIH_KUBIK', 0]},
            'PERSEN_SELISIH': {'$round': ['$PERSEN_SELISIH', 2]},
            'STATUS_PEMAKAIAN': 1
        }},
        {'$match': { 
           '$or': [ # Filter hanya yang anomali
               {'STATUS_PEMAKAIAN': {'$ne': 'STABIL / NORMAL'}},
           ]
        }},
        {'$limit': 100} # Batasi output untuk performa
    ]

    anomalies = list(collection_sbrs.aggregate(pipeline_sbrs_history))
    
    # Clean up _id
    for doc in anomalies:
        doc.pop('_id', None)
        
    return anomalies


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
    """
    ENDPOINT REFACTOR: Pencarian Terintegrasi NOMEN.
    Menggunakan definisi Tunggakan dari ARDEBT dan Piutang dari MC terbaru.
    """
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500
        
    query_nomen = request.args.get('nomen', '').strip()

    if not query_nomen:
        return jsonify({"status": "fail", "message": "Masukkan NOMEN untuk memulai pencarian terintegrasi."}), 400

    try:
        cleaned_nomen = query_nomen.strip().upper()
        
        # 1. DATA STATIS (CID) - Ambil data CID TERBARU
        cid_result = collection_cid.find({'NOMEN': cleaned_nomen}).sort('TANGGAL_UPLOAD_CID', -1).limit(1)
        cid_result = list(cid_result)[0] if list(cid_result) else None
        
        if not cid_result:
            return jsonify({
                "status": "not_found",
                "message": f"NOMEN {query_nomen} tidak ditemukan di Master Data Pelanggan (CID)."
            }), 404

        # 2. RIWAYAT PIUTANG (MC) - Semua riwayat yang pernah di-upload
        mc_results = list(collection_mc.find({'NOMEN': cleaned_nomen}).sort('BULAN_TAGIHAN', -1))
        
        # 3. RIWAYAT TUNGGAKAN DETAIL (ARDEBT) - Total Piutang Tunggakan berdasarkan JUMLAH di ARDEBT.
        ardebt_results = list(collection_ardebt.find({'NOMEN': cleaned_nomen}).sort('PERIODE_BILL', -1))
        tunggakan_nominal_total = sum(float(item.get('JUMLAH', 0)) for item in ardebt_results) # Tunggakan (ARDEBT Definition)
        
        # 4. RIWAYAT PEMBAYARAN TERAKHIR (MB)
        mb_last_payment_cursor = collection_mb.find({'NOMEN': cleaned_nomen}).sort('TGL_BAYAR', -1).limit(1)
        last_payment = list(mb_last_payment_cursor)[0] if list(mb_last_payment_cursor) else None
        
        # 5. RIWAYAT BACA METER (SBRS) - 2 Riwayat terakhir
        sbrs_last_read_cursor = collection_sbrs.find({'CMR_ACCOUNT': cleaned_nomen}).sort('CMR_RD_DATE', -1).limit(2)
        sbrs_history = list(sbrs_last_read_cursor)
        
        # --- LOGIKA KECERDASAN (INTEGRASI & DIAGNOSTIK) ---
        
        mc_latest = mc_results[0] if mc_results else None
        
        status_financial = "LUNAS / TIDAK ADA TAGIHAN"
        total_kewajiban = 0
        
        if tunggakan_nominal_total > 0:
            # Menggunakan ARDEBT sebagai sumber utama status tunggakan
            status_financial = f"TUNGGAKAN AKTIF (Total ARDEBT: Rp {tunggakan_nominal_total:,.0f})"
            total_kewajiban += tunggakan_nominal_total
            
        if mc_latest and mc_latest.get('STATUS') != 'PAYMENT' and mc_latest.get('NOMINAL', 0) > 0:
            # Jika ada piutang bulan berjalan (MC terbaru belum bayar)
            if 'TUNGGAKAN' in status_financial:
                 status_financial += " & PIUTANG BULAN BERJALAN"
            else:
                 status_financial = "PIUTANG BULAN BERJALAN"
            total_kewajiban += float(mc_latest.get('NOMINAL', 0))

        if total_kewajiban == 0:
             status_financial = "LUNAS / TIDAK ADA KEWAJIBAN"
            
        # B. Status Pembayaran
        last_payment_date = last_payment.get('TGL_BAYAR', 'N/A') if last_payment else 'BELUM ADA PEMBAYARAN MB'

        # C. Status Pemakaian (Anomaly Check)
        status_pemakaian = "DATA SBRS KURANG"
        kubik_terakhir = 0
        if len(sbrs_history) >= 1:
            kubik_terakhir = float(sbrs_history[0].get('CMR_KUBIK', 0))
            
            if kubik_terakhir > 100:
                status_pemakaian = f"EKSTRIM ({kubik_terakhir:,.0f} m³)"
            elif kubik_terakhir <= 5 and kubik_terakhir > 0:
                status_pemakaian = f"TURUN DRASITS / RENDAH ({kubik_terakhir:,.0f} m³)"
            elif kubik_terakhir == 0:
                status_pemakaian = "ZERO (0 m³) / NON-AKTIF"
            else:
                status_pemakaian = f"NORMAL ({kubik_terakhir:,.0f} m³)"


        health_summary = {
            "NOMEN": query_nomen,
            "NAMA": cid_result.get('NAMA', 'N/A'),
            "ALAMAT": cid_result.get('ALAMAT', 'N/A'),
            "RAYON": cid_result.get('RAYON', 'N/A'),
            "TIPE_PLGGN": cid_result.get('TIPEPLGGN', 'N/A'),
            "STATUS_FINANSIAL": status_financial,
            "TOTAL_KEWAJIBAN_NOMINAL": total_kewajiban,
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

# --- ENDPOINT KOLEKSI DAN ANALISIS LAINNYA (NAVIGASI BARU) ---

@app.route('/collection', methods=['GET'])
@login_required 
def collection_landing_page():
    return render_template('collection_landing.html', is_admin=current_user.is_admin)

@app.route('/collection/summary', methods=['GET'])
@login_required 
def collection_summary():
    return render_template('collection_summary.html', is_admin=current_user.is_admin)

@app.route('/collection/monitoring', methods=['GET'])
@login_required 
def collection_monitoring():
    return render_template('collection_monitoring.html', is_admin=current_user.is_admin)

@app.route('/collection/analysis', methods=['GET'])
@login_required 
def collection_analysis():
    return render_template('collection_analysis.html', is_admin=current_user.is_admin)

@app.route('/analysis/tarif', methods=['GET'])
@login_required 
def analysis_tarif_breakdown():
    return render_template('analysis_report_template.html', 
                           title="Distribusi Tarif Pelanggan (R34/R35)",
                           description="Laporan detail Distribusi Tarif Nomen, Piutang, dan Kubikasi per Rayon/Tarif. (Memuat chart dan tabel)",
                           report_type="TARIF_BREAKDOWN", 
                           is_admin=current_user.is_admin)

@app.route('/analysis/grouping', methods=['GET'])
@login_required 
def analysis_grouping_sunter():
    return render_template('analysis_report_template.html', 
                           title="Grouping MC: AB Sunter Detail",
                           description="Laporan agregasi Nomen, Nominal, dan Kubikasi berdasarkan Tarif, Merek, dan Metode Baca untuk R34/R35.",
                           report_type="MC_GROUPING_AB_SUNTER", 
                           is_admin=current_user.is_admin)

@app.route('/analysis/aging', methods=['GET'])
@login_required 
def analysis_aging_report():
    return render_template('analysis_report_template.html', 
                           title="Analisis Umur Piutang (Aging Report)",
                           description="Daftar pelanggan dengan Piutang Lama (> 1 Bulan Tagihan) yang statusnya belum lunas.",
                           report_type="AGING_REPORT",
                           is_admin=current_user.is_admin)

@app.route('/analysis/top', methods=['GET'])
@login_required 
def analysis_top_lists():
    return render_template('analysis_report_template.html', 
                           title="Daftar Konsumen Top & Status Pembayaran",
                           description="Menampilkan Top 500 Tunggakan, Top 500 Premium, serta Daftar Lunas dan Belum Bayar (Snapshot Terbaru).",
                           report_type="TOP_LISTS", 
                           is_admin=current_user.is_admin)

@app.route('/analysis/volume', methods=['GET'])
@login_required 
def analysis_volume_dasar():
    return render_template('analysis_report_template.html', 
                           title="Laporan Volume Dasar Historis",
                           description="Riwayat volume KUBIK bulanan agregat berdasarkan Rayon dari seluruh data Master Cetak (MC).",
                           report_type="BASIC_VOLUME",
                           is_admin=current_user.is_admin)

# --- HELPER DATE FUNCTIONS ---
def _get_previous_month_year(bulan_tagihan):
    """Mengubah format 'MMYYYY' menjadi 'MMYYYY' bulan sebelumnya."""
    if not bulan_tagihan or len(bulan_tagihan) != 6:
        return None
    try:
        month = int(bulan_tagihan[:2])
        year = int(bulan_tagihan[2:])
        target_date = datetime(year, month, 1) - timedelta(days=1)
        prev_month = target_date.month
        prev_year = target_date.year
        return f"{prev_month:02d}{prev_year}"
    except ValueError:
        return None
        
def _get_month_date_range(bulan_tagihan):
    """Converts MMYYYY to YYYY-MM-DD start and YYYY-MM-DD end (exclusive)."""
    if not bulan_tagihan or len(bulan_tagihan) != 6:
        return None, None
    try:
        month = int(bulan_tagihan[:2])
        year = int(bulan_tagihan[2:])
        start_date = datetime(year, month, 1)
        if month == 12:
            next_month = 1
            next_year = year + 1
        else:
            next_month = month + 1
            next_year = year
        end_date = datetime(next_year, next_month, 1)
        return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
    except ValueError:
        return None, None

def _mm_yyyy_to_datetime(mm_yyyy_str):
    """Konversi MMYYYY ke datetime object (1st day)"""
    try:
        return datetime.strptime(mm_yyyy_str, '%m%Y')
    except ValueError:
        return None
        
def _get_month_n_ago(mm_yyyy_str, n):
    """Mengembalikan string 'MMYYYY' untuk n bulan yang lalu dari mm_yyyy_str."""
    dt = _mm_yyyy_to_datetime(mm_yyyy_str)
    if not dt:
        return None
        
    target_dt = dt
    for _ in range(n):
        target_dt = target_dt.replace(day=1) - timedelta(days=1)
        
    return target_dt.strftime('%m%Y')

# =========================================================================
# === API GROUPING & DISTRIBUTION REPORTS (REFACTORED ZONA_NOVAK) ===
# =========================================================================

def _get_distribution_report(group_fields, collection_mc):
    """
    Menghitung distribusi Nomen, Piutang, dan Kubikasi berdasarkan field yang diberikan.
    Constraint #3: Ekstraksi RAYON, PCEZ, PC, EZ, BLOCK dari ZONA_NOVAK.
    """
    if collection_mc is None:
        return [], "N/A (Koneksi DB Gagal)"
        
    if isinstance(group_fields, str):
        group_fields = [group_fields]

    latest_mc_month_doc = collection_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
    latest_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None
    
    if not latest_month:
        return [], "N/A (Tidak Ada Data MC)"


    pipeline = [
        # 1. Filter data untuk bulan tagihan terbaru saja
        {"$match": {"BULAN_TAGIHAN": latest_month}},
        # 2. Project dan konversi tipe data, serta EKSTRAKSI ZONA_NOVAK
        {"$project": {
            **{field: f"${field}" for field in group_fields if field not in ['RAYON', 'PC', 'EZ', 'PCEZ', 'BLOCK']},
            "NOMEN": 1,
            "NOMINAL": {"$toDouble": {"$ifNull": ["$NOMINAL", 0]}},
            "KUBIK": {"$toDouble": {"$ifNull": ["$KUBIK", 0]}},
            "CLEAN_ZONA": {"$trim": {"input": {"$ifNull": ["$ZONA_NOVAK", ""]}}},
        }},
        # Constraint #3: Implementasi Ekstraksi ZONA_NOVAK
        {"$addFields": {
            "RAYON": {"$substrCP": ["$CLEAN_ZONA", 0, 2]},  # Index 0, Length 2
            "PC": {"$substrCP": ["$CLEAN_ZONA", 2, 3]},     # Index 2, Length 3
            "EZ": {"$substrCP": ["$CLEAN_ZONA", 5, 2]},     # Index 5, Length 2
            "BLOCK": {"$substrCP": ["$CLEAN_ZONA", 7, 2]},   # Index 7, Length 2
            "PCEZ": {"$concat": [{"$substrCP": ["$CLEAN_ZONA", 2, 3]}, {"$substrCP": ["$CLEAN_ZONA", 5, 2]}]} # PC + EZ
        }},
        # 3. Grouping berdasarkan field yang diminta
        {"$group": {
            "_id": {field: f"${field}" for field in group_fields},
            "total_nomen_set": {"$addToSet": "$NOMEN"},
            "total_piutang": {"$sum": "$NOMINAL"},
            "total_kubikasi": {"$sum": "$KUBIK"}
        }},
        # 4. Proyeksi untuk hasil yang bersih dan hitung size set
        {"$project": {
            **{field: f"$_id.{field}" for field in group_fields},
            "_id": 0,
            "total_nomen": {"$size": "$total_nomen_set"},
            "total_piutang": 1,
            "total_kubikasi": 1
        }},
        # 5. Sorting berdasarkan total piutang terbesar
        {"$sort": {"total_piutang": -1}}
    ]

    try:
        results = list(collection_mc.aggregate(pipeline, allowDiskUse=True))
    except Exception as e:
        print(f"Error during distribution aggregation: {e}")
        return [], latest_month

    return results, latest_month

# --- API Distribusi Rayon/PCEZ/Rayon-Tarif/Rayon-Meter (MENGGUNAKAN HELPER DI ATAS) ---
@app.route("/api/distribution/rayon_report")
@login_required
def rayon_distribution_report():
    results, latest_month = _get_distribution_report(group_fields="RAYON", collection_mc=collection_mc)
    data_for_display = []
    for item in results:
        data_for_display.append({
            "RAYON": item.get("RAYON", "N/A"),
            "Jumlah Nomen": f"{item['total_nomen']:,.0f}",
            "Total Piutang (Rp)": f"Rp {item['total_piutang']:,.0f}",
            "Total Kubikasi (m³)" : f"{item['total_kubikasi']:,.0f}",
            "chart_label": item.get("RAYON", "N/A"),
            "chart_data_nomen": item['total_nomen'],
            "chart_data_piutang": round(item['total_piutang'], 2),
        })

    return jsonify({
        "data": data_for_display,
        "title": f"Distribusi Pelanggan per Rayon (dari ZONA_NOVAK)",
        "subtitle": f"Data Piutang & Kubikasi per Bulan Tagihan Terbaru: {latest_month}",
    })

@app.route("/api/distribution/pcez_report")
@login_required
def pcez_distribution_report():
    results, latest_month = _get_distribution_report(group_fields="PCEZ", collection_mc=collection_mc)
    data_for_display = []
    for item in results:
        data_for_display.append({
            "PCEZ": item.get("PCEZ", "N/A"),
            "Jumlah Nomen": f"{item['total_nomen']:,.0f}",
            "Total Piutang (Rp)": f"Rp {item['total_piutang']:,.0f}",
            "Total Kubikasi (m³)" : f"{item['total_kubikasi']:,.0f}",
            "chart_label": item.get("PCEZ", "N/A"),
            "chart_data_nomen": item['total_nomen'],
            "chart_data_piutang": round(item['total_piutang'], 2),
        })

    return jsonify({
        "data": data_for_display,
        "title": f"Distribusi Pelanggan per PCEZ (dari ZONA_NOVAK)",
        "subtitle": f"Data Piutang & Kubikasi per Bulan Tagihan Terbaru: {latest_month}",
    })

@app.route("/api/distribution/rayon_tarif_report")
@login_required
def rayon_tarif_distribution_report():
    results, latest_month = _get_distribution_report(group_fields=["RAYON", "TARIF"], collection_mc=collection_mc)
    data_for_display = []
    for item in results:
        label = f"{item.get('RAYON', 'N/A')} - {item.get('TARIF', 'N/A')}"
        data_for_display.append({
            "RAYON": item.get("RAYON", "N/A"),
            "TARIF": item.get("TARIF", "N/A"),
            "Jumlah Nomen": f"{item['total_nomen']:,.0f}",
            "Total Piutang (Rp)": f"Rp {item['total_piutang']:,.0f}",
            "Total Kubikasi (m³)" : f"{item['total_kubikasi']:,.0f}",
            "chart_label": label,
            "chart_data_nomen": item['total_nomen'],
            "chart_data_piutang": round(item['total_piutang'], 2),
        })

    return jsonify({
        "data": data_for_display,
        "title": f"Distribusi Pelanggan per Rayon (ZONA) / Tarif",
        "subtitle": f"Data Piutang & Kubikasi per Bulan Tagihan Terbaru: {latest_month}",
    })

@app.route("/api/distribution/rayon_meter_report")
@login_required
def rayon_meter_distribution_report():
    # Asumsi field untuk Jenis Meter adalah 'JENIS_METER' di koleksi MC.
    results, latest_month = _get_distribution_report(group_fields=["RAYON", "JENIS_METER"], collection_mc=collection_mc)
    data_for_display = []
    for item in results:
        label = f"{item.get('RAYON', 'N/A')} - {item.get('JENIS_METER', 'N/A')}"
        data_for_display.append({
            "RAYON": item.get("RAYON", "N/A"),
            "JENIS_METER": item.get("JENIS_METER", "N/A"),
            "Jumlah Nomen": f"{item['total_nomen']:,.0f}",
            "Total Piutang (Rp)": f"Rp {item['total_piutang']:,.0f}",
            "Total Kubikasi (m³)" : f"{item['total_kubikasi']:,.0f}",
            "chart_label": label,
            "chart_data_nomen": item['total_nomen'],
            "chart_data_piutang": round(item['total_piutang'], 2),
        })

    return jsonify({
        "data": data_for_display,
        "title": f"Distribusi Pelanggan per Rayon (ZONA) / Jenis Meter",
        "subtitle": f"Data Piutang & Kubikasi per Bulan Tagihan Terbaru: {latest_month}",
    })


@app.route("/collection/report/rayon_distribution")
@login_required
def rayon_distribution_view():
    return render_template(
        "analysis_report_template.html", 
        title="Distribusi Rayon Pelanggan",
        description="Laporan detail Distribusi Nomen, Piutang, dan Kubikasi per Rayon.",
        report_type="DIST_RAYON"
    )

@app.route("/collection/report/pcez_distribution")
@login_required
def pcez_distribution_view():
    return render_template(
        "analysis_report_template.html", 
        title="Distribusi PCEZ Pelanggan",
        description="Laporan detail Distribusi Nomen, Piutang, dan Kubikasi per PCEZ.",
        report_type="DIST_PCEZ"
    )

@app.route("/collection/report/rayon_tarif_distribution")
@login_required
def rayon_tarif_distribution_view():
    return render_template(
        "analysis_report_template.html", 
        title="Distribusi Rayon / Tarif Pelanggan",
        description="Laporan detail Distribusi Nomen, Piutang, dan Kubikasi per Rayon dan Tarif.",
        report_type="DIST_RAYON_TARIF"
    )

@app.route("/collection/report/rayon_meter_distribution")
@login_required
def rayon_meter_distribution_view():
    return render_template(
        "analysis_report_template.html", 
        title="Distribusi Rayon / Jenis Meter Pelanggan",
        description="Laporan detail Distribusi Nomen, Piutang, dan Kubikasi per Rayon dan Jenis Meter.",
        report_type="DIST_RAYON_METER"
    )

# --- HELPER: AGGREGATE MB SUNTER DETAIL (AGING LOGIC FIXED) ---
def _aggregate_mb_sunter_detail(collection_mb):
    """
    REFACTOR KRITIS: Menghitung agregasi koleksi (Undue, Current, Tunggakan) berdasarkan
    definisi Aging yang baru: Undue=M, Current=M-1, Tunggakan=<M-1.
    """
    if collection_mb is None or collection_mc is None:
        return {"status": "error", "message": "Database connection failed."}

    # 1. TENTUKAN PERIODE DINAMIS
    latest_mc_month_doc = collection_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
    latest_mc_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None

    if not latest_mc_month:
        return {"status": "error", "message": "Tidak ada data MC terbaru untuk menentukan periode koleksi."}

    # M = latest_mc_month (e.g., 122025) -> UNDUE (Aging 0)
    M_MINUS_1_REK = _get_previous_month_year(latest_mc_month) # M-1 (e.g., 112025) -> CURRENT (Aging 1)
    
    # Collection Period (TGL_BAYAR) is in Month M (Asumsi koleksi dilakukan di bulan tagihan terbaru)
    COLLECTION_MONTH_START, COLLECTION_MONTH_END = _get_month_date_range(latest_mc_month)

    RAYON_KEYS = ['34', '35']

    def _get_mb_collection_metrics(rayon_filter, bulan_rek_filter_type):
        
        # Base filter for TGL_BAYAR (Payment in Month M)
        base_match = {
            'BILL_REASON': 'BIAYA PEMAKAIAN AIR',
            # Transaksi pembayaran HARUS terjadi di bulan tagihan terbaru (M)
            'TGL_BAYAR': {'$gte': COLLECTION_MONTH_START, '$lt': COLLECTION_MONTH_END},
        }

        # --- LOGIKA FILTER AGING BARU BERDASARKAN BULAN_REK ---
        if bulan_rek_filter_type == 'UNDUE':
            # Undue (Aging 0): BULAN_REK = M
            base_match['BULAN_REK'] = latest_mc_month
        elif bulan_rek_filter_type == 'CURRENT':
            # Current (Aging 1): BULAN_REK = M-1
            base_match['BULAN_REK'] = M_MINUS_1_REK
        elif bulan_rek_filter_type == 'AGING':
            # Tunggakan (Aging >= 2): BULAN_REK < M-1 (i.e., M-2 atau lebih lama)
            base_match['BULAN_REK'] = {'$lt': M_MINUS_1_REK} 
            
        pipeline = [
            {'$match': base_match},
            {'$project': {
                'NOMINAL': {'$toDouble': {'$ifNull': ['$NOMINAL', 0]}},
                'NOMEN': 1,
                # Pastikan RAYON dari MB dinormalisasi
                'RAYON': {'$toUpper': {'$trim': {'$ifNull': ['$RAYON', 'N/A']}}} 
            }},
            {'$match': {'RAYON': {'$in': RAYON_KEYS}}}
        ]
        
        if rayon_filter == '34' or rayon_filter == '35':
            pipeline.append({'$match': {'RAYON': rayon_filter}})
        
        pipeline.append({'$group': {
            '_id': None,
            'TotalNominal': {'$sum': '$NOMINAL'},
            'TotalNomen': {'$addToSet': '$NOMEN'}
        }})
        
        result = list(collection_mb.aggregate(pipeline))
        return {
            'nominal': result[0].get('TotalNominal', 0.0) if result else 0.0,
            'nomen_count': len(result[0].get('TotalNomen', [])) if result else 0
        }

    # --- AGGREGATE SUMMARY ---
    summary_data = {'undue': {}, 'current': {}, 'tunggakan': {}}
    metrics = [('undue', 'UNDUE'), ('current', 'CURRENT'), ('tunggakan', 'AGING')]
    
    for key, type_str in metrics:
        data34 = _get_mb_collection_metrics('34', type_str)
        data35 = _get_mb_collection_metrics('35', type_str)
        
        summary_data[key]['34'] = data34
        summary_data[key]['35'] = data35
        summary_data[key]['AB_SUNTER'] = {
            'nominal': data34['nominal'] + data35['nominal'],
            'nomen_count': data34['nomen_count'] + data35['nomen_count']
        }
    
    # --- DETAIL HARIAN R34 dan R35 (TANGGAL BAYAR NGURUT) ---
    def _get_mb_daily_detail(rayon_key):
        # Koleksi Harian adalah SEMUA koleksi yang dibayar di bulan M, TANPA filter BULAN_REK.
        COLLECTION_MONTH_START_M, COLLECTION_MONTH_END_M = _get_month_date_range(latest_mc_month)
        
        pipeline = [
            {'$match': {
                'BILL_REASON': 'BIAYA PEMAKAIAN AIR',
                # Filter TGL_BAYAR: Bulan M (sesuai COLLECTION_MONTH_START/END)
                'TGL_BAYAR': {'$gte': COLLECTION_MONTH_START_M, '$lt': COLLECTION_MONTH_END_M}, 
                'RAYON': {'$toUpper': {'$trim': {'$ifNull': ['$RAYON', 'N/A']}}}
            }},
            {'$match': {'RAYON': rayon_key}},
            {'$project': {
                'TGL_BAYAR': 1,
                'NOMINAL': {'$toDouble': {'$ifNull': ['$NOMINAL', 0]}},
                'NOMEN': 1
            }},
            {'$group': {
                '_id': '$TGL_BAYAR',
                'DailyNominal': {'$sum': '$NOMINAL'},
                'DailyNomenCount': {'$addToSet': '$NOMEN'}
            }},
            {'$project': {
                '_id': 0,
                'TANGGAL_BAYAR': '$_id',
                'NOMEN_COUNT': {'$size': '$DailyNomenCount'},
                'NOMINAL': '$DailyNominal'
            }},
            {'$sort': {'TANGGAL_BAYAR': 1}}
        ]
        return list(collection_mb.aggregate(pipeline))

    daily_detail = {
        '34': _get_mb_daily_detail('34'),
        '35': _get_mb_daily_detail('35'),
    }

    # --- FINAL PERIODS ---
    bayar_bulan_fmt = datetime.strptime(latest_mc_month, '%m%Y').strftime('%b %Y').upper()

    return {
        'status': 'success',
        'periods': {
            'bayar_bulan': bayar_bulan_fmt, 
            'undue_rek': latest_mc_month,
            'current_rek': M_MINUS_1_REK,
            'aging_rek_max': f"<{M_MINUS_1_REK}", # Tunggakan: < M-1
        },
        'summary': summary_data,
        'daily_detail': daily_detail
    }
    
@app.route('/api/analyze/mb_sunter_report', methods=['GET'])
@login_required
def analyze_mb_sunter_report_api():
    """API endpoint untuk laporan detail koleksi AB Sunter (Undue, Current, Tunggakan, Harian)."""
    if client is None:
        return jsonify({"status": "error", "message": "Server tidak terhubung ke Database."}), 500

    try:
        report_data = _aggregate_mb_sunter_detail(collection_mb)
        return jsonify(report_data), 200
    except Exception as e:
        print(f"Error fetching MB Sunter report: {e}")
        return jsonify({"status": "error", "message": f"Gagal mengambil data laporan MB Sunter: {e}"}), 500


@app.route('/analysis/grouping/mb_sunter', methods=['GET'])
@login_required 
def analysis_mb_sunter_detail():
    """Rute view untuk laporan detail koleksi MB Sunter."""
    return render_template('analysis_report_template.html', 
                           title="Grouping MB: AB Sunter Detail (Koleksi & Aging)",
                           description="Laporan agregasi detail koleksi (Undue, Current, Tunggakan) berdasarkan Rayon dan per hari.",
                           report_type="MB_SUNTER_DETAIL", 
                           is_admin=current_user.is_admin)

# --- FUNGSI BARU UNTUK REPORT KOLEKSI & PIUTANG ---
@app.route('/api/collection/report', methods=['GET'])
@login_required 
def collection_report_api():
    """Menghitung Nomen Bayar, Nominal Bayar, Total Nominal, dan Persentase per Rayon/PCEZ, termasuk KUBIKASI."""
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500

    latest_mc_month_doc = collection_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
    latest_mc_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None
    
    if not latest_mc_month:
        return jsonify({"report_data": [], "grand_total": {'TotalPelanggan': 0, 'MC_TotalNominal': 0, 'MB_UndueNominal': 0, 'PercentNominal': 0, 'UnduePercentNominal': 0}}), 200

    previous_mc_month = _get_previous_month_year(latest_mc_month)
    
    mc_filter = {'BULAN_TAGIHAN': latest_mc_month}
    
    initial_project = {
        '$project': {
            'RAYON': { '$ifNull': [ '$RAYON', 'N/A' ] }, 
            'PCEZ': { '$ifNull': [ '$PCEZ', 'N/A' ] }, 
            'NOMEN': 1,
            'NOMINAL': {'$toDouble': {'$ifNull': ['$NOMINAL', 0]}}, 
            'KUBIK': {'$toDouble': {'$ifNull': ['$KUBIK', 0]}}, 
            'STATUS': 1
        }
    }
    
    # 1. MC (PIUTANG) METRICS - Billed (BULAN TERBARU SAJA)
    pipeline_billed = [
        { '$match': mc_filter }, 
        initial_project, 
        { '$group': {
            '_id': { 'rayon': '$RAYON', 'pcez': '$PCEZ' },
            'total_nomen_all': { '$addToSet': '$NOMEN' },
            'total_nominal': { '$sum': '$NOMINAL' },
            'total_kubik': { '$sum': '$KUBIK' } 
        }}
    ]
    billed_data = list(collection_mc.aggregate(pipeline_billed))

    # 2. MC (KOLEKSI) METRICS - Collected (flagged in MC - BULAN TERBARU SAJA)
    pipeline_collected = [
        { '$match': mc_filter }, 
        initial_project, 
        { '$match': { 'STATUS': 'PAYMENT' } },
        { '$group': {
            '_id': { 'rayon': '$RAYON', 'pcez': '$PCEZ' },
            'collected_nomen': { '$addToSet': '$NOMEN' }, 
            'collected_nominal': { '$sum': '$NOMINAL' },
            'collected_kubik': { '$sum': '$KUBIK' } 
        }}
    ]
    collected_data = list(collection_mc.aggregate(pipeline_collected))

    # 3. MB (UNDUE BULAN INI) - MB yang BULAN_REK sama dengan bulan tagihan MC terbaru
    pipeline_mb_undue = [
        { '$match': { 
            'BULAN_REK': latest_mc_month, # Filter bulan tagihan (UNDUE = M)
            'BILL_REASON': 'BIAYA PEMAKAIAN AIR'
        }},
        { '$project': {
            'NOMEN': 1,
            'NOMINAL': {'$toDouble': {'$ifNull': ['$NOMINAL', 0]}}, 
            'KUBIKBAYAR': {'$toDouble': {'$ifNull': ['$KUBIKBAYAR', 0]}}, 
            'RAYON_MB': { '$ifNull': [ '$RAYON', 'N/A' ] },
            'PCEZ_MB': { '$ifNull': [ '$PCEZ', 'N/A' ] },
        }},
        {'$lookup': {
           'from': 'CustomerData', 
           'localField': 'NOMEN',
           'foreignField': 'NOMEN',
           'as': 'customer_info'
        }},
        {'$unwind': {'path': '$customer_info', 'preserveNullAndEmptyArrays': True}},
        { '$project': {
             'RAYON': {'$ifNull': ['$customer_info.RAYON', '$RAYON_MB']},
             'PCEZ': {'$ifNull': ['$customer_info.PCEZ', '$PCEZ_MB']},
             'NOMEN': 1, 'NOMINAL': 1, 'KUBIKBAYAR': 1
        }},
        { '$group': {
            '_id': { 'rayon': '$RAYON', 'pcez': '$PCEZ' },
            'mb_undue_nominal': { '$sum': '$NOMINAL' },
            'mb_undue_kubik': { '$sum': '$KUBIKBAYAR' },
            'mb_undue_nomen': { '$addToSet': '$NOMEN' },
        }}
    ]
    mb_undue_data = list(collection_mb.aggregate(pipeline_mb_undue))

    # 4. MB (UNDUE BULAN SEBELUMNYA) - Total nominal yang di-collect di bulan M-1, dengan BULAN_REK = M-1
    prev_month_rek = previous_mc_month
    prev_month_start, prev_month_end = _get_month_date_range(prev_month_rek)
    
    pipeline_mb_undue_prev = [
        { '$match': { 
            'BULAN_REK': prev_month_rek, 
            'BILL_REASON': 'BIAYA PEMAKAIAN AIR',
            # TGL_BAYAR harus di bulan M-1
            'TGL_BAYAR': {'$gte': prev_month_start, '$lt': prev_month_end} 
        }},
        { '$group': {
            '_id': None,
            'mb_undue_prev_nominal': { '$sum': {'$toDouble': {'$ifNull': ['$NOMINAL', 0]}} },
        }}
    ]
    mb_undue_prev_result = list(collection_mb.aggregate(pipeline_mb_undue_prev))
    total_undue_prev_nominal = mb_undue_prev_result[0]['mb_undue_prev_nominal'] if mb_undue_prev_result else 0.0
    
    report_map = {}
    
    # Merge Billed data
    for item in billed_data:
        key = (item['_id']['rayon'], item['_id']['pcez'])
        report_map[key] = {
            'RAYON': item['_id']['rayon'],
            'PCEZ': item['_id']['pcez'],
            'MC_TotalNominal': float(item['total_nominal']),
            'MC_TotalKubik': float(item['total_kubik']),
            'TotalNomen': len(item['total_nomen_all']),
            'MC_CollectedNominal': 0.0, 
            'MC_CollectedKubik': 0.0,
            'MC_CollectedNomen': 0, 
        }

    # Merge Collected data
    for item in collected_data:
        key = (item['_id']['rayon'], item['_id']['pcez'])
        if key in report_map:
            report_map[key]['MC_CollectedNominal'] = float(item['collected_nominal'])
            report_map[key]['MC_CollectedKubik'] = float(item['collected_kubik'])
            report_map[key]['MC_CollectedNomen'] = len(item['collected_nomen'])
            
    # Merge MB Undue data
    for item in mb_undue_data:
        key = (item['_id']['rayon'], item['_id']['pcez'])
        if key not in report_map:
             report_map[key] = {
                 'RAYON': item['_id']['rayon'],
                 'PCEZ': item['_id']['pcez'],
                 'MC_TotalNominal': 0.0, 
                 'MC_TotalKubik': 0.0,
                 'TotalNomen': 0,
                 'MC_CollectedNominal': 0.0, 
                 'MC_CollectedKubik': 0.0,
                 'MC_CollectedNomen': 0, 
             }
        report_map[key]['MB_UndueNominal'] = float(item.get('mb_undue_nominal', 0.0))
        report_map[key]['MB_UndueKubik'] = float(item.get('mb_undue_kubik', 0.0))
        report_map[key]['MB_UndueNomen'] = len(item.get('mb_undue_nomen', []))


    # Final calculations and cleanup
    final_report = []
    
    grand_total = {
        'TotalPelanggan': collection_cid.count_documents({}),
        'MC_TotalNominal': 0.0, 'MC_TotalKubik': 0.0,
        'MC_CollectedNominal': 0.0, 'MC_CollectedKubik': 0.0,
        'MC_TotalNomen': 0, 'MC_CollectedNomen': 0,
        'MB_UndueNominal': 0.0, 'MB_UndueKubik': 0.0,
        'MB_UndueNomen': 0,
        'TotalUnduePrevNominal': total_undue_prev_nominal
    }
    
    for key, data in report_map.items():
        data.setdefault('MB_UndueNominal', 0.0)
        data.setdefault('MB_UndueKubik', 0.0)
        data.setdefault('MB_UndueNomen', 0)
        
        data['PercentNominal'] = (data['MC_CollectedNominal'] / data['MC_TotalNominal']) * 100 if data['MC_TotalNominal'] > 0 else 0
        data['UnduePercentNominal'] = (data['MB_UndueNominal'] / data['MC_TotalNominal']) * 100 if data['MC_TotalNominal'] > 0 else 0
        
        final_report.append(data)
        
        grand_total['MC_TotalNominal'] += data['MC_TotalNominal']
        grand_total['MC_TotalKubik'] += data['MC_TotalKubik']
        grand_total['MC_CollectedNominal'] += data['MC_CollectedNominal']
        grand_total['MC_CollectedKubik'] += data['MC_CollectedKubik']
        grand_total['MC_TotalNomen'] += data['TotalNomen']
        grand_total['MC_CollectedNomen'] += data['MC_CollectedNomen']
        grand_total['MB_UndueNominal'] += data['MB_UndueNominal']
        grand_total['MB_UndueKubik'] += data['MB_UndueKubik']
        grand_total['MB_UndueNomen'] += data['MB_UndueNomen']
        
    grand_total['PercentNominal'] = (grand_total['MC_CollectedNominal'] / grand_total['MC_TotalNominal']) * 100 if grand_total['MC_TotalNominal'] > 0 else 0
    grand_total['PercentNomenCount'] = (grand_total['MC_CollectedNomen'] / grand_total['MC_TotalNomen']) * 100 if grand_total['MC_TotalNomen'] > 0 else 0
    grand_total['UnduePercentNominal'] = (grand_total['MB_UndueNominal'] / grand_total['MC_TotalNominal']) * 100 if grand_total['MC_TotalNominal'] > 0 else 0


    return jsonify({
        'report_data': final_report,
        'grand_total': grand_total,
        'total_mc_nominal_all': grand_total['MC_TotalNominal'] 
    }), 200

# --- FUNGSI BARU UNTUK DETAIL TRANSAKSI MB ---
@app.route('/api/collection/detail', methods=['GET'])
@login_required 
def collection_detail_api():
    """Endpoint API untuk mengambil data koleksi yang difilter dan diurutkan."""
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500
        
    query_str = request.args.get('q', '').strip()
    mongo_query = {} 
    
    if query_str:
        safe_query_str = re.escape(query_str.upper())
        search_filter = {
            '$or': [
                {'RAYON': {'$regex': safe_query_str}}, 
                {'PCEZ': {'$regex': safe_query_str}},
                {'NOMEN': {'$regex': safe_query_str}},
                {'ZONA_NOREK': {'$regex': safe_query_str}}, 
                {'LKS_BAYAR': {'$regex': safe_query_str}},
                {'BILL_REASON': {'$regex': safe_query_str}}, 
                {'BULAN_REK': {'$regex': safe_query_str}} 
            ]
        }
        mongo_query.update(search_filter)

    sort_order = [('TGL_BAYAR', -1)] 

    try:
        results = list(collection_mb.find(mongo_query).sort(sort_order).limit(1000))
        cleaned_results = []
        
        latest_mc_month_doc = collection_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
        latest_mc_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None
        
        current_month_prefix = datetime.now().strftime('%Y-%m')

        for doc in results:
            nominal_val = float(doc.get('NOMINAL', 0)) 
            kubik_val = float(doc.get('KUBIKBAYAR', 0)) 
            pay_dt = doc.get('TGL_BAYAR', '')
            bulan_rek = doc.get('BULAN_REK', 'N/A')
            
            # REFACTOR: IS_UNDUE adalah pembayaran bulan M untuk rekening bulan M (Aging 0)
            is_undue = bulan_rek == latest_mc_month and pay_dt.startswith(current_month_prefix)
            
            cleaned_results.append({
                'NOMEN': doc.get('NOMEN', 'N/A'),
                'RAYON': doc.get('RAYON', doc.get('ZONA_NOREK', 'N/A')), 
                'PCEZ': doc.get('PCEZ', doc.get('LKS_BAYAR', 'N/A')),
                'NOMINAL': nominal_val,
                'KUBIKBAYAR': kubik_val, 
                'PAY_DT': pay_dt,
                'BULAN_REK': bulan_rek, 
                'BILL_REASON': doc.get('BILL_REASON', 'N/A'), 
                'IS_UNDUE': is_undue 
            })
            
        return jsonify(cleaned_results), 200

    except Exception as e:
        print(f"Error fetching detailed collection data: {e}")
        return jsonify({"message": f"Gagal mengambil data detail koleksi: {e}"}), 500


# --- FUNGSI UNTUK EXPORT LAPORAN KOLEKSI/REPORT ---
@app.route('/api/export/collection_report', methods=['GET'])
@login_required
def export_collection_report():
    """Export data Laporan Koleksi & Piutang (MC/MB) ke Excel."""
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500
        
    try:
        report_response = collection_report_api()
        report_json = report_response.get_json()
        
        if not report_json['report_data']:
            return jsonify({"message": "Tidak ada data laporan untuk diekspor."}), 404
            
        df_report = pd.DataFrame(report_json['report_data'])
        df_grand_total = pd.DataFrame([report_json['grand_total']])
        df_grand_total.insert(0, 'RAYON', 'GRAND TOTAL')

        df_grand_total = df_grand_total.drop(columns=['MC_TotalNomen', 'MC_CollectedNomen', 'MB_UndueNomen', 'TotalPelanggan', 'TotalUnduePrevNominal'], errors='ignore')
        
        df_export = pd.concat([df_report, df_grand_total], ignore_index=True)

        df_export = df_export[[
            'RAYON', 'PCEZ', 
            'MC_TotalNominal', 'MC_TotalKubik',
            'MC_CollectedNominal', 'MC_CollectedKubik',
            'MB_UndueNominal', 'MB_UndueKubik',
            'PercentNominal', 'UnduePercentNominal'
        ]]
        df_export.columns = [
            'RAYON', 'PCEZ', 
            'MC_PIUTANG_NOMINAL', 'MC_PIUTANG_KUBIK',
            'MC_KOLEKSI_NOMINAL', 'MC_KOLEKSI_KUBIK',
            'MB_UNDUE_NOMINAL', 'MB_UNDUE_KUBIK',
            'MC_PERSEN_KOLEKSI', 'MB_PERSEN_UNDUE'
        ]

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_export.to_excel(writer, sheet_name='Laporan Koleksi & Piutang', index=False)
            
        output.seek(0)
        
        response = make_response(output.read())
        response.headers['Content-Disposition'] = 'attachment; filename=Laporan_Koleksi_Piutang_Terpadu.xlsx'
        response.headers['Content-type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        return response

    except Exception as e:
        print(f"Error during collection report export: {e}")
        return jsonify({"message": f"Gagal mengekspor data laporan koleksi: {e}"}), 500


# --- ENDPOINT ANALISIS DATA LANJUTAN (VIEWS) ---
@app.route('/analyze', methods=['GET'])
@login_required
def analyze_reports_landing():
    return render_template('analyze_landing.html', is_admin=current_user.is_admin)

@app.route('/analyze/tariff_change_report', methods=['GET'])
@login_required
def analyze_tariff_change_report():
    return render_template('analyze_report_template.html', 
                            title="Laporan Perubahan Tarif Pelanggan", 
                            description="Menampilkan pelanggan yang memiliki perbedaan data Tarif antara riwayat data CID terbaru dan sebelumnya. (Memerlukan CID dalam mode historis).",
                            report_type="TARIFF_CHANGE",
                            is_admin=current_user.is_admin)

@app.route('/analyze/extreme', methods=['GET'])
@login_required
def analyze_extreme_usage():
    return render_template('analyze_report_template.html', 
                            title="Pemakaian Air Ekstrim", 
                            description="Menampilkan pelanggan dengan konsumsi air di atas ambang batas dan fluktuasi signifikan.",
                            report_type="EXTREME_USAGE",
                            is_admin=current_user.is_admin)

@app.route('/analyze/reduced', methods=['GET'])
@login_required
def analyze_reduced_usage():
    return render_template('analyze_report_template.html', 
                            title="Pemakaian Air Naik/Turun (Fluktuasi Volume)", 
                            description="Menampilkan pelanggan dengan fluktuasi konsumsi air signifikan (naik atau turun) dengan membandingkan 2 periode SBRS terakhir.",
                            report_type="FLUX_REPORT",
                            is_admin=current_user.is_admin)


# =========================================================================
# === API GROUPING MC KUSTOM (HELPER FUNCTION) ===
# =========================================================================

def _aggregate_custom_mc_report(collection_mc, collection_cid, dimension=None, rayon_filter=None):
    """
    Menghitung Piutang Kustom (Rayon 34/35 REG) untuk laporan grup (mis. Grouping MC).
    Memastikan ZONA_NOVAK terekstraksi dengan benar.
    """
    
    latest_mc_month_doc = collection_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
    latest_mc_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None
    
    if not latest_mc_month:
        return {'CountOfNOMEN': 0, 'SumOfKUBIK': 0, 'SumOfNOMINAL': 0}

    dimension_map = {'TARIF': '$TARIF_CID', 'MERK': '$MERK_CID', 'READ_METHOD': '$READ_METHOD'}
    
    pipeline = [
        {'$match': {'BULAN_TAGIHAN': latest_mc_month}}, 
        {"$project": {
            "NOMEN": "$NOMEN",
            "NOMINAL": {"$toDouble": {"$ifNull": ["$NOMINAL", 0]}},
            "KUBIK": {"$toDouble": {"$ifNull": ["$KUBIK", 0]}},
            "CUST_TYPE_MC": "$CUST_TYPE", 
            "CLEAN_ZONA": {"$trim": {"input": {"$ifNull": ["$ZONA_NOVAK", ""]}}},
        }},
        {"$addFields": {
            "RAYON_ZONA": {"$substrCP": ["$CLEAN_ZONA", 0, 2]},
            "PC_ZONA": {"$substrCP": ["$CLEAN_ZONA", 2, 3]},
            "EZ_ZONA": {"$substrCP": ["$CLEAN_ZONA", 5, 2]},
            "BLOCK_ZONA": {"$substrCP": ["$CLEAN_ZONA", 7, 2]},
            "PCEZ_ZONA": {"$concat": ["$PC_ZONA", "$EZ_ZONA"]} 
        }},
        {'$lookup': {'from': 'CustomerData', 'localField': 'NOMEN', 'foreignField': 'NOMEN', 'as': 'customer_info'}},
        {'$unwind': {'path': '$customer_info', 'preserveNullAndEmptyArrays': True}}, 
        {'$addFields': {
            'CLEAN_TIPEPLGGN': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.TIPEPLGGN', '$CUST_TYPE_MC']}}}}}, 
            'CLEAN_RAYON': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.RAYON', '$RAYON_ZONA']}}}}}, 
            'TARIF_CID': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.TARIF', 'N/A']}}}}}, 
            'MERK_CID': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.MERK', 'N/A']}}}}},
            'READ_METHOD': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.READ_METHOD', 'N/A']}}}}},
        }},
        {'$match': {'CLEAN_TIPEPLGGN': 'REG'}}
    ]
    
    rayon_keys = ['34', '35']
    if rayon_filter in rayon_keys:
        pipeline.append({'$match': {'CLEAN_RAYON': rayon_filter}})
    elif rayon_filter == 'TOTAL_34_35':
        pipeline.append({'$match': {'CLEAN_RAYON': {'$in': rayon_keys}}})

    if dimension is None:
        pipeline.extend([
            {'$group': {
                '_id': None,
                'TotalNomen': {'$addToSet': '$NOMEN'},
                'SumOfKUBIK': {'$sum': '$KUBIK'},
                'SumOfNOMINAL': {'$sum': '$NOMINAL'},
            }},
            {'$project': {
                '_id': 0,
                'CountOfNOMEN': {'$size': '$TotalNomen'},
                'SumOfKUBIK': {'$round': ['$SumOfKUBIK', 0]},
                'SumOfNOMINAL': {'$round': ['$SumOfNOMINAL', 0]},
            }}
        ])
        result = list(collection_mc.aggregate(pipeline, allowDiskUse=True))
        return result[0] if result else {'CountOfNOMEN': 0, 'SumOfKUBIK': 0, 'SumOfNOMINAL': 0}
        
    else:
        group_key = dimension_map[dimension]
        pipeline.extend([
            {'$group': {
                '_id': group_key,
                'CountOfNOMEN': {'$addToSet': '$NOMEN'},
                'SumOfKUBIK': {'$sum': '$KUBIK'},
                'SumOfNOMINAL': {'$sum': '$NOMINAL'},
            }},
            {'$project': {
                '_id': 0,
                'DIMENSION_KEY': '$_id',
                'CountOfNOMEN': {'$size': '$CountOfNOMEN'},
                'SumOfKUBIK': {'$round': ['$SumOfKUBIK', 0]},
                'SumOfNOMINAL': {'$round': ['$SumOfNOMINAL', 0]},
            }},
            {'$sort': {'DIMENSION_KEY': 1}}
        ])
        results = list(collection_mc.aggregate(pipeline, allowDiskUse=True))
        
        for item in results:
             item[dimension] = item.pop('DIMENSION_KEY')
             
        return results

@app.route('/api/analyze/mc_grouping', methods=['GET'])
@login_required 
def analyze_mc_grouping_api():
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500

    try:
        totals = {
            'TOTAL_34_35': _aggregate_custom_mc_report(collection_mc, collection_cid, dimension=None, rayon_filter='TOTAL_34_35'),
            '34': _aggregate_custom_mc_report(collection_mc, collection_cid, dimension=None, rayon_filter='34'),
            '35': _aggregate_custom_mc_report(collection_mc, collection_cid, dimension=None, rayon_filter='35'),
        }

        dimensions = ['TARIF', 'MERK', 'READ_METHOD']
        breakdowns = {}

        for dim in dimensions:
            breakdowns[dim] = {
                'TOTAL_34_35': _aggregate_custom_mc_report(collection_mc, collection_cid, dim, rayon_filter='TOTAL_34_35'),
                '34': _aggregate_custom_mc_report(collection_mc, collection_cid, dim, rayon_filter='34'),
                '35': _aggregate_custom_mc_report(collection_mc, collection_cid, dim, rayon_filter='35'),
            }
            
        if all(totals[k]['CountOfNOMEN'] == 0 for k in totals):
            return jsonify({"status": "error", "message": "Tidak ada data Piutang MC terbaru untuk Rayon 34/35 yang ditemukan."}), 404

        response_data = {
            'status': 'success',
            'totals': totals,
            'breakdowns': breakdowns
        }
        
        return jsonify(response_data), 200

    except Exception as e:
        print(f"Error saat menganalisis custom grouping MC: {e}")
        return jsonify({"status": "error", "message": f"Gagal mengambil data grouping MC: {e}"}), 500


@app.route('/api/analyze/mc_grouping/summary', methods=['GET'])
@login_required 
def analyze_mc_grouping_summary_api():
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500

    try:
        latest_mc_month_doc = collection_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
        latest_mc_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None
        
        if not latest_mc_month:
             return jsonify({ 'TotalPiutangKustomNominal': 0, 'TotalPiutangKustomKubik': 0, 'TotalNomenKustom': 0 }), 200

        pipeline_summary = [
            {'$match': {'BULAN_TAGIHAN': latest_mc_month}}, 
             {"$project": {
                "NOMEN": "$NOMEN",
                "NOMINAL": {"$toDouble": {"$ifNull": ["$NOMINAL", 0]}},
                "KUBIK": {"$toDouble": {"$ifNull": ["$KUBIK", 0]}},
                "CUST_TYPE_MC": "$CUST_TYPE", 
                "CLEAN_ZONA": {"$trim": {"input": {"$ifNull": ["$ZONA_NOVAK", ""]}}},
            }},
            {"$addFields": {
                "RAYON_ZONA": {"$substrCP": ["$CLEAN_ZONA", 0, 2]}, 
            }},
            {'$lookup': {
               'from': 'CustomerData', 
               'localField': 'NOMEN',
               'foreignField': 'NOMEN',
               'as': 'customer_info'
            }},
            {'$unwind': {'path': '$customer_info', 'preserveNullAndEmptyArrays': True}},
            
            {'$addFields': {
                'CLEAN_TIPEPLGGN': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.TIPEPLGGN', '$CUST_TYPE_MC']}}}}}, 
                'CLEAN_RAYON': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.RAYON', '$RAYON_ZONA']}}}}}, 
            }},
            
            {'$match': {
                'CLEAN_TIPEPLGGN': 'REG',
                'CLEAN_RAYON': {'$in': ['34', '35']}
            }},
            {'$group': {
                '_id': None,
                'SumOfKUBIK': {'$sum': {'$toDouble': {'$ifNull': ['$KUBIK', 0]}}},
                'SumOfNOMINAL': {'$sum': {'$toDouble': {'$ifNull': ['$NOMINAL', 0]}}},
                'CountOfNOMEN': {'$addToSet': '$NOMEN'}
            }},
            {'$project': {
                '_id': 0,
                'TotalPiutangKustomNominal': {'$round': ['$SumOfNOMINAL', 0]},
                'TotalPiutangKustomKubik': {'$round': ['$SumOfKUBIK', 0]},
                'TotalNomenKustom': {'$size': '$CountOfNOMEN'}
            }}
        ]
        summary_result = list(collection_mc.aggregate(pipeline_summary))
        
        if not summary_result:
            return jsonify({
                'TotalPiutangKustomNominal': 0,
                'TotalPiutangKustomKubik': 0,
                'TotalNomenKustom': 0
            }), 200

        return jsonify(summary_result[0]), 200

    except Exception as e:
        print(f"Error saat mengambil summary grouping MC: {e}")
        return jsonify({"message": f"Gagal mengambil summary grouping MC. Detail teknis error: {e}"}), 500

@app.route('/api/analyze/mc_tarif_breakdown', methods=['GET'])
@login_required 
def analyze_mc_tarif_breakdown_api():
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500

    try:
        latest_mc_month_doc = collection_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
        latest_mc_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None
        
        if not latest_mc_month:
             return jsonify([]), 200
             
        pipeline_tarif_breakdown = [
            {'$match': {'BULAN_TAGIHAN': latest_mc_month}}, 
            {"$project": {
                "NOMEN": 1, "RAYON": 1, "TARIF": 1,
                "CLEAN_ZONA": {"$trim": {"input": {"$ifNull": ["$ZONA_NOVAK", ""]}}},
                "CUST_TYPE_MC": "$CUST_TYPE", 
            }},
            {"$addFields": {
                "RAYON_ZONA": {"$substrCP": ["$CLEAN_ZONA", 0, 2]}, 
            }},
            {'$lookup': {
               'from': 'CustomerData', 
               'localField': 'NOMEN',
               'foreignField': 'NOMEN',
               'as': 'customer_info'
            }},
            {'$unwind': {'path': '$customer_info', 'preserveNullAndEmptyArrays': True}}, 
            
            {'$addFields': {
                'CLEAN_TIPEPLGGN': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.TIPEPLGGN', '$CUST_TYPE_MC']}}}}}, 
                'CLEAN_RAYON': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.RAYON', '$RAYON_ZONA']}}}}}, 
            }},
            
            {'$match': {
                'CLEAN_TIPEPLGGN': 'REG',
                'CLEAN_RAYON': {'$in': ['34', '35']}
            }},
            
            {'$group': {
                '_id': {
                    'RAYON': '$CLEAN_RAYON',
                    'TARIF': '$TARIF',
                },
                'CountOfNOMEN': {'$addToSet': '$NOMEN'},
            }},
            
            {'$project': {
                '_id': 0,
                'RAYON': '$_id.RAYON',
                'TARIF': '$_id.TARIF',
                'JumlahPelanggan': {'$size': '$CountOfNOMEN'}
            }},
            {'$sort': {'RAYON': 1, 'TARIF': 1}}
        ]
        breakdown_data = list(collection_mc.aggregate(pipeline_tarif_breakdown))
        
        if not breakdown_data:
            return jsonify([]), 200 

        return jsonify(breakdown_data), 200

    except Exception as e:
        print(f"Error saat mengambil tarif breakdown MC: {e}")
        return jsonify({"message": f"Gagal mengambil tarif breakdown MC. Detail teknis error: {e}"}), 500

# =========================================================================
# === API MONITORING KOLEKSI HARIAN (CURRENT/RP1 LOGIC FIXED) ===
# =========================================================================

@app.route('/api/collection/monitoring', methods=['GET'])
@login_required
def collection_monitoring_api():
    """
    REFACTOR KRITIS: Menghasilkan data harian, kumulatif, dan persentase koleksi.
    Logika: Koleksi Current/Rp1 (BULAN_REK = M-1, TGL_BAYAR = Bulan M).
    """
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500
        
    try:
        latest_mc_month_doc = collection_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
        latest_mc_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None
        
        if not latest_mc_month:
            empty_summary = {'R34': {'MC': 0, 'CURRENT': 0}, 'R35': {'MC': 0, 'CURRENT': 0}, 'GLOBAL': {'TotalPiutangMC': 0, 'TotalUnduePrev': 0, 'CurrentKoleksiTotal': 0, 'TotalKoleksiPersen': 0}}
            return jsonify({'monitoring_data': {'R34': [], 'R35': []}, 'summary_top': empty_summary}), 200

        # M-1 adalah target BULAN_REK untuk Koleksi Current/Rp1 bulan ini
        previous_mc_month = _get_previous_month_year(latest_mc_month)
        
        if not previous_mc_month:
            previous_mc_month = latest_mc_month

        # 1. Hitung Total Piutang MC (Denominator) dari bulan tagihan terbaru (M)
        mc_total_response = collection_mc.aggregate([
            {'$match': {'BULAN_TAGIHAN': latest_mc_month}},
            {"$project": {
                "NOMEN": 1, "NOMINAL": {"$toDouble": {"$ifNull": ["$NOMINAL", 0]}},
                "CLEAN_ZONA": {"$trim": {"input": {"$ifNull": ["$ZONA_NOVAK", ""]}}},
                "CUST_TYPE_MC": "$CUST_TYPE", 
            }},
            {"$addFields": {
                "RAYON_ZONA": {"$substrCP": ["$CLEAN_ZONA", 0, 2]}, 
            }},
            {'$lookup': {'from': 'CustomerData', 'localField': 'NOMEN', 'foreignField': 'NOMEN', 'as': 'customer_info'}},
            {'$unwind': {'path': '$customer_info', 'preserveNullAndEmptyArrays': False}},
            {'$addFields': {'CLEAN_RAYON': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.RAYON', '$RAYON_ZONA']}}}}},
             'CLEAN_TIPEPLGGN': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.TIPEPLGGN', '$CUST_TYPE_MC']}}}}},}},
            
            {'$match': {'CLEAN_RAYON': {'$in': ['34', '35']}, 'CLEAN_TIPEPLGGN': 'REG'}},
            
            {'$group': {'_id': '$CLEAN_RAYON', 'TotalPiutang': {'$sum': {'$toDouble': {'$ifNull': ['$NOMINAL', 0]}}}}}
        ], allowDiskUse=True)
        
        mc_totals = {doc['_id']: doc['TotalPiutang'] for doc in mc_total_response}
        total_mc_34 = mc_totals.get('34', 0)
        total_mc_35 = mc_totals.get('35', 0)
        total_mc_nominal_all = total_mc_34 + total_mc_35
        
        # 2. Hitung Total UNDUE Bulan Sebelumnya (M-1)
        prev_month_start_date, prev_month_end_date = _get_month_date_range(previous_mc_month)

        pipeline_undue_prev = [
            { '$match': { 
                'BULAN_REK': previous_mc_month, 
                'BILL_REASON': 'BIAYA PEMAKAIAN AIR',
                # Filter TGL_BAYAR harus dalam rentang bulan M-1
                'TGL_BAYAR': {'$gte': prev_month_start_date, '$lt': prev_month_end_date} 
            }},
            { '$group': {
                '_id': None,
                'TotalUnduePrev': { '$sum': {'$toDouble': {'$ifNull': ['$NOMINAL', 0]}} },
            }}
        ]
        undue_prev_result = list(collection_mb.aggregate(pipeline_undue_prev))
        total_undue_prev_nominal = undue_prev_result[0]['TotalUnduePrev'] if undue_prev_result else 0.0

        # 3. Ambil Data Transaksi MB (Koleksi Current/Rp1) Harian
        now = datetime.now()
        this_month_start = now.strftime('%Y-%m-01')
        next_month_start = (now.replace(day=1) + timedelta(days=32)).replace(day=1).strftime('%Y-%m-%d')
        
        pipeline_mb_daily = [
            {'$match': {
                'TGL_BAYAR': {'$gte': this_month_start, '$lt': next_month_start}, # Filter A: TGL_BAYAR di bulan ini (M)
                'BULAN_REK': previous_mc_month, # Filter B: BULAN_REK bulan lalu (M-1) (Rp1/Current)
                'BILL_REASON': 'BIAYA PEMAKAIAN AIR'
            }}, 
            {'$project': {
                'TGL_BAYAR': 1,
                'NOMINAL': {'$toDouble': {'$ifNull': ['$NOMINAL', 0]}}, 
                'NOMEN': 1,
                'RAYON_MB': { '$ifNull': [ '$RAYON', 'N/A' ] },
            }},
            {'$lookup': {'from': 'CustomerData', 'localField': 'NOMEN', 'foreignField': 'NOMEN', 'as': 'customer_info'}},
            {'$unwind': {'path': '$customer_info', 'preserveNullAndEmptyArrays': True}},
            {'$project': {
                 'TGL_BAYAR': 1,
                 'NOMINAL': 1,
                 'CLEAN_RAYON': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.RAYON', '$RAYON_MB']}}}}},
                 'NOMEN': 1
            }},
            {'$match': {'CLEAN_RAYON': {'$in': ['34', '35']}}},
            {'$group': {
                '_id': {'date': '$TGL_BAYAR', 'rayon': '$CLEAN_RAYON'},
                'DailyNominal': {'$sum': '$NOMINAL'},
                'DailyCustCount': {'$addToSet': '$NOMEN'}
            }},
            {'$sort': {'_id.date': 1}}
        ]
        
        mb_daily_data = list(collection_mb.aggregate(pipeline_mb_daily, allowDiskUse=True))

        # 4. Proses di Pandas untuk Kumulatif & Persentase
        df_monitoring = pd.DataFrame([
            {'TGL': doc['_id']['date'], 
             'RAYON': doc['_id']['rayon'], 
             'COLL_NOMINAL': doc['DailyNominal'], 
             'CUST_COUNT': len(doc['DailyCustCount'])}
            for doc in mb_daily_data
        ])

        if df_monitoring.empty:
            empty_summary = {'R34': {'MC': total_mc_34, 'CURRENT': 0}, 'R35': {'MC': total_mc_35, 'CURRENT': 0}, 'GLOBAL': {'TotalPiutangMC': total_mc_nominal_all, 'TotalUnduePrev': total_undue_prev_nominal, 'CurrentKoleksiTotal': 0, 'TotalKoleksiPersen': 0}}
            return jsonify({'monitoring_data': {'R34': [], 'R35': []}, 'summary_top': empty_summary}), 200

        df_monitoring['TGL'] = pd.to_datetime(df_monitoring['TGL'])
        df_monitoring = df_monitoring.sort_values(by='TGL')
        
        df_monitoring['Rp1_Kumulatif'] = df_monitoring.groupby('RAYON')['COLL_NOMINAL'].cumsum()
        df_monitoring_global = df_monitoring.groupby('TGL').agg({'COLL_NOMINAL': 'sum'}).reset_index()
        df_monitoring_global['Rp1_Kumulatif_Global'] = df_monitoring_global['COLL_NOMINAL'].cumsum()
        
        df_monitoring = pd.merge(df_monitoring, df_monitoring_global[['TGL', 'Rp1_Kumulatif_Global']], on='TGL', how='left')

        df_monitoring['COLL_Kumulatif_Persen'] = (
            (df_monitoring['Rp1_Kumulatif_Global'] + total_undue_prev_nominal) / total_mc_nominal_all
        ) * 100
        df_monitoring['COLL_Kumulatif_Persen'] = df_monitoring['COLL_Kumulatif_Persen'].fillna(0)

        df_monitoring['COLL_VAR'] = df_monitoring.groupby('RAYON')['COLL_Kumulatif_Persen'].diff().fillna(df_monitoring['COLL_Kumulatif_Persen'])
        
        df_monitoring = df_monitoring.drop(columns=['Rp1_Kumulatif_Global'], errors='ignore')
        
        df_monitoring['TGL'] = df_monitoring['TGL'].dt.strftime('%d/%m/%Y')
        
        df_monitoring['RAYON_OUTPUT'] = 'R' + df_monitoring['RAYON']

        df_r34 = df_monitoring[df_monitoring['RAYON_OUTPUT'] == 'R34'].drop(columns=['RAYON', 'RAYON_OUTPUT']).reset_index(drop=True)
        df_r35 = df_monitoring[df_monitoring['RAYON_OUTPUT'] == 'R35'].drop(columns=['RAYON', 'RAYON_OUTPUT']).reset_index(drop=True)

        summary_r34 = {
            'MC_Piutang_M': total_mc_34,
            'CURRENT_Collected': df_r34['Rp1_Kumulatif'].iloc[-1] if not df_r34.empty else 0
        }
        summary_r35 = {
            'MC_Piutang_M': total_mc_35,
            'CURRENT_Collected': df_r35['Rp1_Kumulatif'].iloc[-1] if not df_r35.empty else 0
        }
        
        current_koleksi_total = df_monitoring_global['COLL_NOMINAL'].sum()
        total_koleksi_persen = df_monitoring['COLL_Kumulatif_Persen'].iloc[-1] if not df_monitoring.empty else 0
        
        grand_total_summary = {
            'TotalPiutangMC': total_mc_nominal_all,
            'TotalUnduePrev': total_undue_prev_nominal,
            'CurrentKoleksiTotal': current_koleksi_total,
            'TotalKoleksiPersen': total_koleksi_persen
        }

        return jsonify({
            'monitoring_data': {
                'R34': df_r34.to_dict('records'),
                'R35': df_r35.to_dict('records'),
            },
            'summary_top': {
                'R34': summary_r34,
                'R35': summary_r35,
                'GLOBAL': grand_total_summary
            }
        }), 200

    except Exception as e:
        print(f"Error collection monitoring: {e}")
        return jsonify({"message": f"Gagal membuat data monitoring koleksi: {e}"}), 500

# =========================================================================
# === API LAPORAN AGING PIUTANG (MC AGING LOGIC FIXED) ===
# =========================================================================

@app.route('/api/report/aging_report', methods=['GET'])
@login_required 
def aging_report_api():
    """
    REFACTOR KRITIS: Menghasilkan laporan aging piutang (MC) yang BUKAN bulan tagihan terbaru, 
    BELUM LUNAS, dan NOMINAL > 0 (Piutang Lama).
    """
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500

    try:
        latest_mc_month_doc = collection_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
        latest_mc_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None
        
        if not latest_mc_month:
            return jsonify({'status': 'error', 'message': 'Tidak ada data MC historis ditemukan.'}), 404

        pipeline = [
            {'$match': {
                'BULAN_TAGIHAN': {'$ne': latest_mc_month}, # BUKAN bulan terbaru
                'STATUS': {'$ne': 'PAYMENT'}, # BELUM BAYAR
                'NOMINAL': {'$gt': 0} # NOMINAL AKTIF
            }},
            {'$project': {
                'NOMEN': 1,
                'RAYON': 1,
                'NOMINAL': {'$toDouble': {'$ifNull': ['$NOMINAL', 0]}},
                'BULAN_TAGIHAN': 1
            }},
            {'$group': {
                '_id': '$NOMEN',
                'TotalPiutangLama': {'$sum': '$NOMINAL'},
                'Bulan_Tagihan_Terlama': {'$min': '$BULAN_TAGIHAN'},
                'RayonMC': {'$first': '$RAYON'}
            }},
            {'$match': {'TotalPiutangLama': {'$gt': 0}}},
            {'$lookup': { 
               'from': 'CustomerData', 
               'localField': '_id',
               'foreignField': 'NOMEN',
               'as': 'customer_info'
            }},
            {'$unwind': {'path': '$customer_info', 'preserveNullAndEmptyArrays': True}},
            {'$project': {
                '_id': 0,
                'NOMEN': '$_id',
                'NAMA': {'$ifNull': ['$customer_info.NAMA', 'N/A']},
                'RAYON': {'$ifNull': ['$customer_info.RAYON', '$RayonMC']},
                'PiutangLama': {'$round': ['$TotalPiutangLama', 0]},
                'Bulan_Tagihan_Terlama': '$Bulan_Tagihan_Terlama',
            }},
            {'$sort': {'PiutangLama': -1}},
            {'$limit': 500}
        ]

        aging_data = list(collection_mc.aggregate(pipeline, allowDiskUse=True))
        
        if not aging_data:
            return jsonify({'status': 'success', 'message': 'Tidak ada piutang lama ditemukan (semua lunas atau bulan berjalan).', 'data': []}), 200

        return jsonify({'status': 'success', 'data': aging_data}), 200

    except Exception as e:
        print(f"Error saat membuat laporan aging: {e}")
        return jsonify({"status": 'error', "message": f"Gagal mengambil laporan aging: {e}"}), 500

# =========================================================================
# === API UPLOAD DATA (REFACTORING KRITIS MAPPING MB) ===
# =========================================================================

@app.route('/upload/mc', methods=['POST'])
@login_required 
@admin_required 
def upload_mc_data():
    """Mode HISTORIS: Untuk Master Cetak (MC) / Piutang Bulanan. (DIOPTIMASI)"""
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
        upload_month = request.form.get('month')
        upload_year = request.form.get('year')
        
        if not upload_month or not upload_year:
            return jsonify({"message": "Gagal: Bulan dan Tahun Tagihan harus diisi."}), 400
        
        bulan_tagihan_value = f"{upload_month}{upload_year}"

        if file_extension == 'csv':
            df = pd.read_csv(file)
        elif file_extension in ['xlsx', 'xls']:
            df = pd.read_excel(file, sheet_name=0) 
        
        df.columns = [col.strip().upper() for col in df.columns]
        
        if 'NOMEN' not in df.columns:
            return jsonify({"message": "Gagal Append: File MC harus memiliki kolom kunci 'NOMEN' untuk penyimpanan historis."}), 400
            
        df['BULAN_TAGIHAN'] = bulan_tagihan_value
        
        columns_to_normalize_mc = ['PC', 'EMUH', 'NOMEN', 'STATUS', 'TARIF', 'BULAN_TAGIHAN', 'ZONA_NOVAK', 'CUST_TYPE']
        
        for col in df.columns:
            if df[col].dtype == 'object' or col in columns_to_normalize_mc:
                df[col] = df[col].astype(str).str.strip().str.upper()
                df[col] = df[col].replace(['NAN', 'NONE', '', ' '], 'N/A')
            
            if col in ['NOMINAL', 'NOMINAL_AKHIR', 'KUBIK', 'SUBNOMINAL', 'ANG_BP', 'DENDA', 'PPN']: 
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        if 'PC' in df.columns:
            df = df.rename(columns={'PC': 'RAYON'})
        
        if 'STATUS' not in df.columns:
            df['STATUS'] = 'N/A' 

        data_to_insert = df.to_dict('records')
        
        if not data_to_insert:
            return jsonify({"message": "File kosong, tidak ada data yang dimasukkan."}), 200
        
        UNIQUE_KEYS = ['NOMEN', 'BULAN_TAGIHAN'] 
        
        if not all(key in df.columns for key in UNIQUE_KEYS):
             return jsonify({"message": "Kesalahan Internal: Kolom kunci 'NOMEN' atau 'BULAN_TAGIHAN' hilang setelah pemrosesan Pandas."}), 500

        inserted_count = 0
        skipped_count = 0
        total_rows = len(data_to_insert)

        try:
            result = collection_mc.insert_many(data_to_insert, ordered=False)
            inserted_count = len(result.inserted_ids)
            skipped_count = total_rows - inserted_count
            
        except BulkWriteError as bwe:
            inserted_count = bwe.details.get('nInserted', 0)
            skipped_count = total_rows - inserted_count
            
        except Exception as e:
            print(f"Error massal saat insert: {e}")
            return jsonify({"message": f"Gagal menyimpan data secara massal: {e}"}), 500
        
        return jsonify({
            "status": "success",
            "message": f"Sukses Historis! {inserted_count} baris Master Cetak (MC) baru ditambahkan. ({skipped_count} duplikat diabaikan).",
            "summary_report": {
                "total_rows": total_rows,
                "type": "APPEND",
                "inserted_count": inserted_count,
                "skipped_count": skipped_count
            },
            "anomaly_list": []
        }), 200

    except Exception as e:
        print(f"Error saat memproses file MC: {e}")
        return jsonify({"message": f"Gagal memproses file MC: {e}. Pastikan format data benar."}), 500


@app.route('/upload/mb', methods=['POST'])
@login_required 
@admin_required 
def upload_mb_data():
    """
    REFACTOR KRITIS: Mode APPEND untuk Master Bayar (MB) / Koleksi Harian.
    Constraint #1 & #2: MENGUBAH nama kolom NOTAG dan PAY_DT.
    """
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
        
        # >>> START PERBAIKAN: MAPPING HEADER KRITIS UNTUK MB <<<
        rename_map = {
            'NOTAG': 'NOTAGIHAN', # Constraint #1: Map NOTAG (Daily) ke NOTAGIHAN (MB)
            'PAY_DT': 'TGL_BAYAR', # Constraint #2: Map PAY_DT (Daily) ke TGL_BAYAR (MB)
            'BILL_PERIOD': 'BULAN_REK',
            'MC VOL OKT 25_NOMEN': 'NOMEN' 
        }
        df = df.rename(columns=lambda x: rename_map.get(x, x), errors='ignore')
        # >>> END PERBAIKAN: MAPPING HEADER KRITIS UNTUK MB <<<
        
        # Inject Missing Critical Columns (if needed)
        if 'BILL_REASON' not in df.columns:
            df['BILL_REASON'] = 'UNKNOWN'
        
        if 'BULAN_REK' not in df.columns:
            df['BULAN_REK'] = 'N/A' 

        # --- TGL_BAYAR DATE NORMALIZATION (ROBUST) ---
        if 'TGL_BAYAR' in df.columns:
            df['TGL_BAYAR_OBJ'] = pd.to_datetime(
                df['TGL_BAYAR'].astype(str).str.strip(), 
                format='%d-%m-%Y', 
                errors='coerce'
            )
            numeric_dates = pd.to_numeric(df['TGL_BAYAR'].replace({'N/A': float('nan')}), errors='coerce')
            df['TGL_BAYAR_OBJ'] = df['TGL_BAYAR_OBJ'].fillna(
                pd.to_datetime(numeric_dates, unit='D', origin='1899-12-30', errors='coerce')
            )
            df['TGL_BAYAR'] = df['TGL_BAYAR_OBJ'].dt.strftime('%Y-%m-%d').fillna('N/A')
            df = df.drop(columns=['TGL_BAYAR_OBJ'], errors='ignore')
        # --- END TGL_BAYAR DATE NORMALIZATION ---
        
        # --- NORMALISASI DATA PANDAS ---
        columns_to_normalize_mb = ['NOMEN', 'RAYON', 'PCEZ', 'ZONA_NOREK', 'LKS_BAYAR', 'BULAN_REK', 'NOTAGIHAN', 'BILL_REASON'] 
        
        for col in df.columns:
            if df[col].dtype == 'object' or col in columns_to_normalize_mb:
                df[col] = df[col].astype(str).str.strip().str.upper()
                df[col] = df[col].replace(['NAN', 'NONE', '', ' '], 'N/A')

            if col in ['NOMINAL', 'SUBNOMINAL', 'BEATETAP', 'BEA_SEWA']: 
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        data_to_insert = df.to_dict('records')
        
        if not data_to_insert:
            return jsonify({"message": "File kosong, tidak ada data yang dimasukkan."}), 200
        
        # --- START: OPERASI BULK WRITE TEROPTIMASI ---
        UNIQUE_KEYS = ['NOTAGIHAN', 'TGL_BAYAR', 'NOMINAL'] 
        
        if not all(key in df.columns for key in UNIQUE_KEYS):
            return jsonify({"message": f"Gagal Append: File MB harus memiliki kolom kunci unik: {', '.join(UNIQUE_KEYS)}. Cek file Anda."}), 400

        inserted_count = 0
        skipped_count = 0
        total_rows = len(data_to_insert)

        try:
            result = collection_mb.insert_many(data_to_insert, ordered=False)
            inserted_count = len(result.inserted_ids)
            skipped_count = total_rows - inserted_count
            
        except BulkWriteError as bwe:
            inserted_count = bwe.details.get('nInserted', 0)
            skipped_count = total_rows - inserted_count
            
        except Exception as e:
            print(f"Error massal saat insert: {e}")
            return jsonify({"message": f"Gagal menyimpan data secara massal: {e}"}), 500
        
        return jsonify({
            "status": "success",
            "message": f"Sukses Append! {inserted_count} baris Master Bayar (MB) baru ditambahkan. ({skipped_count} duplikat diabaikan).",
            "summary_report": {
                "total_rows": total_rows,
                "type": "APPEND",
                "inserted_count": inserted_count,
                "skipped_count": skipped_count
            },
            "anomaly_list": []
        }), 200

    except Exception as e:
        print(f"Error saat memproses file MB: {e}")
        return jsonify({"message": f"Gagal memproses file MB: {e}. Pastikan format data benar."}), 500

@app.route('/upload/cid', methods=['POST'])
@login_required 
@admin_required 
def upload_cid_data():
    """Mode HISTORIS: Untuk Customer Data (CID) / Data Pelanggan Statis. (DIOPTIMASI)"""
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

        columns_to_normalize = ['MERK', 'READ_METHOD', 'TIPEPLGGN', 'RAYON', 'NOMEN', 'TARIFF']
        
        for col in df.columns:
            if df[col].dtype == 'object' or col in columns_to_normalize:
                df[col] = df[col].astype(str).str.strip().str.upper()
                df[col] = df[col].replace(['NAN', 'NONE', '', ' '], 'N/A')
        
        if 'TARIFF' in df.columns:
            df = df.rename(columns={'TARIFF': 'TARIF'})
        
        data_to_insert = df.to_dict('records')
        
        if not data_to_insert:
            return jsonify({"message": "File kosong, tidak ada data yang dimasukkan."}), 200
        
        upload_date_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for record in data_to_insert:
            record['TANGGAL_UPLOAD_CID'] = upload_date_str

        inserted_count = 0
        total_rows = len(data_to_insert)

        try:
            result = collection_cid.insert_many(data_to_insert, ordered=False)
            inserted_count = len(result.inserted_ids)
        except BulkWriteError as bwe:
            inserted_count = bwe.details.get('nInserted', 0)
        except Exception as e:
            return jsonify({"message": f"Gagal menyimpan data secara massal: {e}"}), 500
            
        return jsonify({
            "status": "success",
            "message": f"Sukses Historis! {inserted_count} baris Customer Data (CID) baru ditambahkan.",
            "summary_report": {
                "total_rows": total_rows,
                "type": "APPEND", 
                "inserted_count": inserted_count,
                "skipped_count": total_rows - inserted_count
            },
            "anomaly_list": []
        }), 200

    except Exception as e:
        print(f"Error saat memproses file CID: {e}")
        return jsonify({"message": f"Gagal memproses file CID: {e}. Pastikan format data benar."}), 500


@app.route('/upload/sbrs', methods=['POST'])
@login_required 
@admin_required 
def upload_sbrs_data():
    """Mode APPEND: Untuk data Baca Meter (SBRS) / Riwayat Stand Meter. (DIOPTIMASI)"""
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

        columns_to_normalize_sbrs = ['CMR_ACCOUNT', 'CMR_RD_DATE', 'CMR_READER'] 

        for col in df.columns:
            if df[col].dtype == 'object' or col in columns_to_normalize_sbrs:
                df[col] = df[col].astype(str).str.strip().str.upper()
                df[col] = df[col].replace(['NAN', 'NONE', '', ' '], 'N/A')

            if col in ['CMR_PREV_READ', 'CMR_READING', 'CMR_KUBIK']: 
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        data_to_insert = df.to_dict('records')
        
        if not data_to_insert:
            return jsonify({"message": "File kosong, tidak ada data yang dimasukkan."}), 200
        
        UNIQUE_KEYS = ['CMR_ACCOUNT', 'CMR_RD_DATE'] 
        
        if not all(key in df.columns for key in UNIQUE_KEYS):
            return jsonify({"message": f"Gagal Append: File SBRS harus memiliki kolom kunci unik: {', '.join(UNIQUE_KEYS)}. Cek file Anda."}), 400

        inserted_count = 0
        skipped_count = 0
        total_rows = len(data_to_insert)

        try:
            result = collection_sbrs.insert_many(data_to_insert, ordered=False)
            inserted_count = len(result.inserted_ids)
            skipped_count = total_rows - inserted_count
            
        except BulkWriteError as bwe:
            inserted_count = bwe.details.get('nInserted', 0)
            skipped_count = total_rows - inserted_count
            
        except Exception as e:
            print(f"Error massal saat insert: {e}")
            return jsonify({"message": f"Gagal menyimpan data secara massal: {e}"}), 500
        
        anomaly_list = []
        try:
            if inserted_count > 0:
                anomaly_list = _get_sbrs_anomalies(collection_sbrs, collection_cid)
        except Exception as e:
            print(f"Peringatan: Gagal menjalankan analisis anomali instan: {e}")

        return jsonify({
            "status": "success",
            "message": f"Sukses Append! {inserted_count} baris Riwayat Baca Meter (SBRS) baru ditambahkan. ({skipped_count} duplikat diabaikan).",
            "summary_report": {
                "total_rows": total_rows,
                "type": "APPEND",
                "inserted_count": inserted_count,
                "skipped_count": skipped_count
            },
            "anomaly_list": []
        }), 200

    except Exception as e:
        print(f"Error saat memproses file SBRS: {e}")
        return jsonify({"message": f"Gagal memproses file SBRS: {e}. Pastikan format data benar."}), 500

@app.route('/upload/ardebt', methods=['POST'])
@login_required 
@admin_required 
def upload_ardebt_data():
    """Mode HISTORIS: Untuk data Detail Tunggakan (ARDEBT). (DIOPTIMASI & ROBUST KEY CHECK)"""
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

        if 'NOMEN' not in df.columns:
            return jsonify({"message": "Gagal Append: File ARDEBT harus memiliki kolom kunci 'NOMEN' untuk penyimpanan historis."}), 400
            
        monetary_keys = ['JUMLAH', 'AMOUNT', 'TOTAL', 'NOMINAL']
        found_monetary_key = next((key for key in monetary_keys if key in df.columns), None)

        if found_monetary_key and found_monetary_key != 'JUMLAH':
             df = df.rename(columns={found_monetary_key: 'JUMLAH'})
        elif 'JUMLAH' not in df.columns:
             return jsonify({"message": "Gagal Append: Kolom kunci JUMLAH (atau AMOUNT/TOTAL/NOMINAL) untuk nominal tunggakan tidak ditemukan di file Anda."}), 400

        upload_month = request.form.get('month')
        upload_year = request.form.get('year')
        
        if not upload_month or not upload_year:
            return jsonify({"message": "Gagal: Bulan dan Tahun Tunggakan harus diisi."}), 400
        
        periode_bill_value = f"{upload_month}{upload_year}"

        df['PERIODE_BILL'] = periode_bill_value 

        columns_to_normalize_ardebt = ['NOMEN', 'RAYON', 'TIPEPLGGN', 'PERIODE_BILL'] 
        
        for col in df.columns:
            if df[col].dtype == 'object' or col in columns_to_normalize_ardebt:
                df[col] = df[col].astype(str).str.strip().str.upper()
                df[col] = df[col].replace(['NAN', 'NONE', '', ' '], 'N/A')
            
            if col in ['JUMLAH', 'VOLUME']: 
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        data_to_insert = df.to_dict('records')
        
        if not data_to_insert:
            return jsonify({"message": "File kosong, tidak ada data yang dimasukkan."}), 200
        
        UNIQUE_KEYS = ['NOMEN', 'PERIODE_BILL', 'JUMLAH'] 
        
        inserted_count = 0
        skipped_count = 0
        total_rows = len(data_to_insert)

        try:
            result = collection_ardebt.insert_many(data_to_insert, ordered=False)
            inserted_count = len(result.inserted_ids)
            skipped_count = total_rows - inserted_count
            
        except BulkWriteError as bwe:
            inserted_count = bwe.details.get('nInserted', 0)
            skipped_count = total_rows - inserted_count
            
        except Exception as e:
            print(f"Error massal saat insert: {e}")
            return jsonify({"message": f"Gagal menyimpan data secara massal: {e}"}), 500
        
        return jsonify({
            "status": "success",
            "message": f"Sukses Historis! {inserted_count} baris Detail Tunggakan (ARDEBT) baru ditambahkan. ({skipped_count} duplikat diabaikan).",
            "summary_report": {
                "total_rows": total_rows,
                "type": "APPEND", 
                "inserted_count": inserted_count,
                "skipped_count": skipped_count
            },
            "anomaly_list": []
        }), 200

    except Exception as e:
        print(f"Error saat memproses file ARDEBT: {e}")
        return jsonify({"message": f"Gagal memproses file ARDEBT: {e}. Pastikan format data benar."}), 500


# =========================================================================
# === DASHBOARD ANALYTICS ENDPOINTS (INTEGRATED) ===
# =========================================================================
# (API Dashboard dan Export tetap sama karena didasarkan pada helper yang sudah diperbaiki)

@app.route('/dashboard', methods=['GET'])
@login_required
def analytics_dashboard():
    return render_template('dashboard_analytics.html', is_admin=current_user.is_admin)

@app.route('/api/dashboard/summary', methods=['GET'])
@login_required
def dashboard_summary_api():
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500
    
    try:
        summary_data = {}
        
        latest_mc_month_doc = collection_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
        latest_mc_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None
        
        summary_data['total_pelanggan'] = len(collection_cid.distinct('NOMEN'))
        
        pipeline_piutang = [
            {'$match': {'BULAN_TAGIHAN': latest_mc_month}},
            {'$group': {
                '_id': None,
                'total_piutang': {'$sum': '$NOMINAL'},
                'jumlah_tagihan': {'$sum': 1}
            }}
        ]
        piutang_result = list(collection_mc.aggregate(pipeline_piutang))
        summary_data['total_piutang'] = piutang_result[0]['total_piutang'] if piutang_result else 0
        summary_data['jumlah_tagihan'] = piutang_result[0]['jumlah_tagihan'] if piutang_result else 0
        
        pipeline_tunggakan = [
            {'$group': {
                '_id': None,
                'total_tunggakan': {'$sum': '$JUMLAH'},
                'jumlah_tunggakan': {'$sum': 1}
            }}
        ]
        tunggakan_result = list(collection_ardebt.aggregate(pipeline_tunggakan))
        summary_data['total_tunggakan'] = tunggakan_result[0]['total_tunggakan'] if tunggakan_result else 0
        summary_data['jumlah_tunggakan'] = tunggakan_result[0]['jumlah_tunggakan'] if tunggakan_result else 0
        
        today_date = datetime.now().strftime('%Y-%m-%d')
        pipeline_koleksi_today = [
            {'$match': {'TGL_BAYAR': today_date,
                         'BILL_REASON': 'BIAYA PEMAKAIAN AIR'
            }}, 
            {'$group': {
                '_id': None,
                'koleksi_hari_ini': {'$sum': '$NOMINAL'},
                'transaksi_hari_ini': {'$sum': 1}
            }}
        ]
        koleksi_result = list(collection_mb.aggregate(pipeline_koleksi_today))
        summary_data['koleksi_hari_ini'] = koleksi_result[0]['koleksi_hari_ini'] if koleksi_result else 0
        summary_data['transaksi_hari_ini'] = koleksi_result[0]['transaksi_hari_ini'] if koleksi_result else 0
        
        this_month = datetime.now().strftime('%Y-%m')
        pipeline_koleksi_month = [
            {'$match': {'TGL_BAYAR': {'$regex': this_month},
                         'BILL_REASON': 'BIAYA PEMAKAIAN AIR'
            }},
            {'$group': {
                '_id': None,
                'koleksi_bulan_ini': {'$sum': '$NOMINAL'},
                'transaksi_bulan_ini': {'$sum': 1}
            }}
        ]
        koleksi_month_result = list(collection_mb.aggregate(pipeline_koleksi_month))
        summary_data['koleksi_bulan_ini'] = koleksi_month_result[0]['koleksi_bulan_ini'] if koleksi_month_result else 0
        summary_data['transaksi_bulan_ini'] = koleksi_month_result[0]['transaksi_bulan_ini'] if koleksi_month_result else 0
        
        anomalies = _get_sbrs_anomalies(collection_sbrs, collection_cid)
        summary_data['total_anomali'] = len(anomalies)
        
        anomali_breakdown = {'kategori': {}}
        for item in anomalies:
            status = item.get('STATUS_PEMAKAIAN', 'UNKNOWN')
            if 'EKSTRIM' in status or 'NAIK' in status:
                key = 'KENAIKAN_SIGNIFIKAN'
            elif 'TURUN' in status:
                key = 'PENURUNAN_SIGNIFIKAN'
            elif 'ZERO' in status:
                key = 'ZERO_USAGE'
            else:
                key = 'LAINNYA'
            
            if key not in anomali_breakdown['kategori']:
                 anomali_breakdown['kategori'][key] = {'jumlah': 0, 'data': []}
                
            anomali_breakdown['kategori'][key]['jumlah'] += 1
            if len(anomali_breakdown['kategori'][key]['data']) < 10:
                 anomali_breakdown['kategori'][key]['data'].append(item)
            
        summary_data['anomali_breakdown'] = anomali_breakdown
        
        pelanggan_tunggakan = collection_ardebt.distinct('NOMEN')
        summary_data['pelanggan_dengan_tunggakan'] = len(pelanggan_tunggakan)
        
        pipeline_top_rayon = [
            {'$match': {'BULAN_TAGIHAN': latest_mc_month}},
            {'$group': {
                '_id': '$RAYON',
                'total_piutang': {'$sum': '$NOMINAL'},
                'total_pelanggan': {'$addToSet': '$NOMEN'}
            }},
            {'$project': {
                '_id': 0,
                'RAYON': '$_id',
                'total_piutang': 1,
                'total_pelanggan': {'$size': '$total_pelanggan'}
            }},
            {'$sort': {'total_piutang': -1}},
            {'$limit': 5}
        ]
        top_rayon = list(collection_mc.aggregate(pipeline_top_rayon))
        summary_data['top_rayon_piutang'] = [
            {'rayon': item['RAYON'], 'total': item['total_piutang']} 
            for item in top_rayon
        ]
        
        trend_data = []
        for i in range(7):
            date_obj = datetime.now() - timedelta(days=i)
            date = date_obj.strftime('%Y-%m-%d')
            
            pipeline = [
                {'$match': {'TGL_BAYAR': date, 
                             'BILL_REASON': 'BIAYA PEMAKAIAN AIR'}}, 
                {'$group': {
                    '_id': None,
                    'total': {'$sum': '$NOMINAL'},
                    'count': {'$sum': 1}
                }}
            ]
            result = list(collection_mb.aggregate(pipeline))
            trend_data.append({
                'tanggal': date,
                'total': result[0]['total'] if result else 0,
                'transaksi': result[0]['count'] if result else 0
            })
        summary_data['tren_koleksi_7_hari'] = sorted(trend_data, key=lambda x: x['tanggal'])
        
        if summary_data['total_piutang'] > 0:
            summary_data['persentase_koleksi'] = (summary_data['koleksi_bulan_ini'] / summary_data['total_piutang']) * 100
        else:
            summary_data['persentase_koleksi'] = 0
        
        return jsonify(summary_data), 200
        
    except Exception as e:
        print(f"Error fetching dashboard summary: {e}")
        return jsonify({"message": f"Gagal mengambil data dashboard: {e}"}), 500


@app.route('/api/dashboard/rayon_analysis', methods=['GET'])
@login_required
def rayon_analysis_api():
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500
    
    latest_mc_month_doc = collection_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
    latest_mc_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None
    
    try:
        pipeline_piutang_rayon = [
            {'$match': {'BULAN_TAGIHAN': latest_mc_month}}, 
            {'$group': {
                '_id': '$RAYON',
                'total_piutang': {'$sum': '$NOMINAL'},
                'total_pelanggan': {'$addToSet': '$NOMEN'}
            }},
            {'$project': {
                '_id': 0,
                'RAYON': '$_id',
                'total_piutang': 1,
                'total_pelanggan': {'$size': '$total_pelanggan'}
            }},
            {'$sort': {'total_piutang': -1}}
        ]
        rayon_piutang_data = list(collection_mc.aggregate(pipeline_piutang_rayon))
        
        rayon_map = {item['RAYON']: item for item in rayon_piutang_data}
        
        this_month = datetime.now().strftime('%Y-%m')
        pipeline_koleksi_rayon = [
            {'$match': {'TGL_BAYAR': {'$regex': this_month},
                         'BILL_REASON': 'BIAYA PEMAKAIAN AIR'
            }},
            {'$group': {
                '_id': '$RAYON',
                'total_koleksi': {'$sum': '$NOMINAL'},
            }},
        ]
        koleksi_result = list(collection_mb.aggregate(pipeline_koleksi_rayon))
        
        for item in koleksi_result:
            rayon_name = item['_id']
            if rayon_name in rayon_map:
                rayon_map[rayon_name]['koleksi'] = item['total_koleksi']
                
                if rayon_map[rayon_name]['total_piutang'] > 0:
                    rayon_map[rayon_name]['persentase_koleksi'] = (item['total_koleksi'] / rayon_map[rayon_name]['total_piutang']) * 100
                else:
                    rayon_map[rayon_name]['persentase_koleksi'] = 0
            
        for rayon in rayon_map.values():
            rayon.setdefault('koleksi', 0)
            rayon.setdefault('persentase_koleksi', 0)


        return jsonify(list(rayon_map.values())), 200
        
    except Exception as e:
        print(f"Error in rayon analysis: {e}")
        return jsonify({"message": f"Gagal menganalisis data rayon: {e}"}), 500


@app.route('/api/dashboard/anomaly_summary', methods=['GET'])
@login_required
def anomaly_summary_api():
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500
    
    try:
        all_anomalies = _get_sbrs_anomalies(collection_sbrs, collection_cid)
        
        ekstrim = [a for a in all_anomalies if 'EKSTRIM' in a['STATUS_PEMAKAIAN']]
        naik = [a for a in all_anomalies if 'NAIK' in a['STATUS_PEMAKAIAN'] and 'EKSTRIM' not in a['STATUS_PEMAKAIAN']]
        turun = [a for a in all_anomalies if 'TURUN' in a['STATUS_PEMAKAIAN']]
        zero = [a for a in all_anomalies if 'ZERO' in a['STATUS_PEMAKAIAN']]
        
        summary = {
            'total_anomali': len(all_anomalies),
            'kategori': {
                'ekstrim': {
                    'jumlah': len(ekstrim),
                    'data': ekstrim[:10]
                },
                'naik_signifikan': {
                    'jumlah': len(naik),
                    'data': naik[:10]
                },
                'turun_signifikan': {
                    'jumlah': len(turun),
                    'data': turun[:10]
                },
                'zero': {
                    'jumlah': len(zero),
                    'data': zero[:10]
                }
            }
        }
        
        return jsonify(summary), 200
        
    except Exception as e:
        print(f"Error in anomaly summary: {e}")
        return jsonify({"message": f"Gagal mengambil summary anomali: {e}"}), 500


@app.route('/api/dashboard/critical_alerts', methods=['GET'])
@login_required
def critical_alerts_api():
    if client is None:
        return jsonify([]), 200
        
    try:
        alerts = []
        
        anomalies = _get_sbrs_anomalies(collection_sbrs, collection_cid)
        ekstrim_alerts = [
            {'nomen': a['NOMEN'], 'status': a['STATUS_PEMAKAIAN'], 'ray': a['RAYON'], 'category': 'VOLUME_EKSTRIM'}
            for a in anomalies if 'EKSTRIM' in a['STATUS_PEMAKAIAN'] or 'ZERO' in a['STATUS_PEMAKAIAN']
        ]
        alerts.extend(ekstrim_alerts[:20])

        # Critical Debt pipeline disederhanakan karena ARDEBT kini historis
        pipeline_critical_debt = [
            {'$group': {
                '_id': '$NOMEN',
                'months': {'$sum': 1}, 
                'amount': {'$sum': '$JUMLAH'}
            }},
            {'$match': {'months': {'$gte': 5}}}, 
            {'$limit': 20}
        ]
        
        critical_debt_result = list(collection_ardebt.aggregate(pipeline_critical_debt))
        
        debt_alerts = [
            {'nomen': d['_id'], 'status': f"TUNGGAKAN KRITIS {d['months']} BULAN", 'amount': d['amount'], 'category': 'DEBT_CRITICAL'}
            for d in critical_debt_result
        ]
        
        alerts.extend(debt_alerts)
        
        return jsonify(alerts), 200
        
    except Exception as e:
        print(f"Error fetching critical alerts: {e}")
        return jsonify([]), 500


@app.route('/api/export/dashboard', methods=['GET'])
@login_required
def export_dashboard_data():
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500
        
    try:
        summary_response = dashboard_summary_api()
        summary_data = summary_response.get_json()
        
        rayon_response = rayon_analysis_api()
        rayon_data = rayon_response.get_json()
        
        df_rayon = pd.DataFrame(rayon_data)
        
        df_summary = pd.DataFrame({
            'Metrik': ['Total Pelanggan', 'Total Piutang (MC)', 'Total Tunggakan (ARDEBT)', 'Koleksi Bulan Ini', 'Persentase Koleksi'],
            'Nilai': [
                summary_data['total_pelanggan'],
                summary_data['total_piutang'],
                summary_data['total_tunggakan'],
                summary_data['koleksi_bulan_ini'],
                f"{summary_data['persentase_koleksi']:.2f}%"
            ]
        })
        
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_summary.to_excel(writer, sheet_name='Ringkasan KPI', index=False)
            df_rayon.to_excel(writer, sheet_name='Analisis Rayon', index=False)
            pd.DataFrame(summary_data['tren_koleksi_7_hari']).to_excel(writer, sheet_name='Tren Koleksi 7 Hari', index=False)
            
        output.seek(0)
        
        response = make_response(output.read())
        response.headers['Content-Disposition'] = 'attachment; filename=Laporan_Dashboard_Analytics.xlsx'
        response.headers['Content-type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        return response

    except Exception as e:
        print(f"Error during dashboard export: {e}")
        return jsonify({"message": f"Gagal mengekspor data dashboard: {e}"}), 500


@app.route('/api/export/anomalies', methods=['GET'])
@login_required
def export_anomalies_data():
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500
        
    try:
        all_anomalies = _get_sbrs_anomalies(collection_sbrs, collection_cid)
        
        if not all_anomalies:
            return jsonify({"message": "Tidak ada data anomali untuk diekspor."}), 404
            
        df_anomalies = pd.DataFrame(all_anomalies)
        
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_anomalies.to_excel(writer, sheet_name='Anomali Pemakaian Air', index=False)
            
        output.seek(0)
        
        response = make_response(output.read())
        response.headers['Content-Disposition'] = 'attachment; filename=Laporan_Anomali_SBRS.xlsx'
        response.headers['Content-type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        return response

    except Exception as e:
        print(f"Error during anomaly export: {e}")
        return jsonify({"message": f"Gagal mengekspor data anomali: {e}"}), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
