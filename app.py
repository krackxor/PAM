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
from pymongo.errors import BulkWriteError, DuplicateKeyError 

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
    # Mengatasi error timeout 20000ms saat operasi berat (SBRS)
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=60000, socketTimeoutMS=300000) # 60s for server selection, 5 menit (300 detik) untuk operasi data socket
    client.admin.command('ping') 
    db = client[DB_NAME]
    
    # 🚨 KOLEKSI DIPISAH BERDASARKAN SUMBER DATA
    collection_mc = db['MasterCetak']   # MC (Piutang/Tagihan Bulanan - DIUBAH KE APPEND HISTORIS)
    collection_mb = db['MasterBayar']   # MB (Koleksi Harian - APPEND)
    collection_cid = db['CustomerData'] # CID (Data Pelanggan Statis - DIUBAH KE APPEND HISTORIS)
    collection_sbrs = db['MeterReading'] # SBRS (Baca Meter Harian/Parsial - APPEND)
    collection_ardebt = db['AccountReceivable'] # ARDEBT (Tunggakan Detail - DIUBAH KE APPEND HISTORIS)
    
    # ==========================================================
    # === START OPTIMASI: INDEXING KRITIS (SOLUSI KECEPATAN PERMANEN) ===
    # ==========================================================
    
    # CID (CustomerData): Paling KRITIS untuk JOIN
    # TIDAK LAGI UNIQUE, KARENA CID MENGGUNAKAN MODE HISTORIS APPEND
    collection_cid.create_index([('NOMEN', 1), ('TANGGAL_UPLOAD_CID', -1)], name='idx_cid_nomen_hist')
    collection_cid.create_index([('RAYON', 1), ('TIPEPLGGN', 1)], name='idx_cid_rayon_tipe')

    # MC (MasterCetak): Untuk Report Koleksi dan Grouping
    collection_mc.create_index([('NOMEN', 1), ('BULAN_TAGIHAN', -1)], name='idx_mc_nomen_hist') # HISTORIS
    collection_mc.create_index([('RAYON', 1), ('PCEZ', 1)], name='idx_mc_rayon_pcez') 
    collection_mc.create_index([('STATUS', 1)], name='idx_mc_status')
    collection_mc.create_index([('TARIF', 1), ('KUBIK', 1), ('NOMINAL', 1)], name='idx_mc_tarif_volume')

    # MB (MasterBayar): Untuk Detail Transaksi dan Undue Check
    # Index unik digantikan pengecekan di Python/BulkWrite Error Handling
    collection_mb.create_index([('NOTAGIHAN', 1), ('TGL_BAYAR', 1), ('NOMINAL', 1)], name='idx_mb_unique_transaction', unique=True) # DIUBAH MENJADI UNIK untuk BulkWrite
    collection_mb.create_index([('TGL_BAYAR', -1)], name='idx_mb_paydate_desc')
    collection_mb.create_index([('NOMEN', 1)], name='idx_mb_nomen')
    collection_mb.create_index([('RAYON', 1), ('PCEZ', 1), ('TGL_BAYAR', -1)], name='idx_mb_rayon_pcez_date')

    # SBRS (MeterReading): Untuk Anomaly Check
    # Index unik digantikan pengecekan di Python/BulkWrite Error Handling
    collection_sbrs.create_index([('CMR_ACCOUNT', 1), ('CMR_RD_DATE', 1)], name='idx_sbrs_unique_read', unique=True) # DIUBAH MENJADI UNIK untuk BulkWrite
    collection_sbrs.create_index([('CMR_ACCOUNT', 1), ('CMR_RD_DATE', -1)], name='idx_sbrs_history')
    
    # ARDEBT (AccountReceivable): Untuk Cek Tunggakan
    collection_ardebt.create_index([('NOMEN', 1), ('PERIODE_BILL', -1), ('JUMLAH', 1)], name='idx_ardebt_nomen_hist') # HISTORIS
    
    # ==========================================================
    # === END OPTIMASI: INDEXING KRITIS ===
    # ==========================================================
    
    collection_data = collection_mc

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
                        { 'case': {'$gte': ['$KUBIK_TERBARU', 150]}, 'then': 'EKSTRIM (>150 m³)' }, # Threshold Ekstrim Tinggi
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
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500
        
    query_nomen = request.args.get('nomen', '').strip()

    if not query_nomen:
        return jsonify({"status": "fail", "message": "Masukkan NOMEN untuk memulai pencarian terintegrasi."}), 400

    try:
        cleaned_nomen = query_nomen.strip().upper()
        
        # 1. DATA STATIS (CID) - Ambil data CID TERBARU
        # Menggunakan CID historis, ambil dokumen CID terbaru berdasarkan TANGGAL_UPLOAD_CID
        cid_result = collection_cid.find({'NOMEN': cleaned_nomen}).sort('TANGGAL_UPLOAD_CID', -1).limit(1)
        cid_result = list(cid_result)[0] if list(cid_result) else None
        
        if not cid_result:
            return jsonify({
                "status": "not_found",
                "message": f"NOMEN {query_nomen} tidak ditemukan di Master Data Pelanggan (CID)."
            }), 404

        # 2. RIWAYAT PIUTANG (MC) - Semua riwayat yang pernah di-upload
        mc_results = list(collection_mc.find({'NOMEN': cleaned_nomen}).sort('BULAN_TAGIHAN', -1))
        piutang_nominal_total = sum(item.get('NOMINAL', 0) for item in mc_results)
        
        # 3. RIWAYAT TUNGGAKAN DETAIL (ARDEBT) - Semua riwayat yang pernah di-upload
        ardebt_results = list(collection_ardebt.find({'NOMEN': cleaned_nomen}).sort('PERIODE_BILL', -1))
        tunggakan_nominal_total = sum(item.get('JUMLAH', 0) for item in ardebt_results)
        
        # 4. RIWAYAT PEMBAYARAN TERAKHIR (MB)
        mb_last_payment_cursor = collection_mb.find({'NOMEN': cleaned_nomen}).sort('TGL_BAYAR', -1).limit(1)
        last_payment = list(mb_last_payment_cursor)[0] if list(mb_last_payment_cursor) else None
        
        # 5. RIWAYAT BACA METER (SBRS) - 2 Riwayat terakhir untuk Anomaly Check
        sbrs_last_read_cursor = collection_sbrs.find({'CMR_ACCOUNT': cleaned_nomen}).sort('CMR_RD_DATE', -1).limit(2)
        sbrs_history = list(sbrs_last_read_cursor)
        
        # --- LOGIKA KECERDASAN (INTEGRASI & DIAGNOSTIK) ---
        
        # A. Status Tunggakan/Piutang (Menggunakan Total Piutang Aktif)
        # Ambil status dari MC/ARDEBT terbaru (Asumsi MC/ARDEBT terbaru adalah tagihan/tunggakan aktif)
        mc_latest = mc_results[0] if mc_results else None
        
        if tunggakan_nominal_total > 0:
            # Periksa tunggakan yang belum dibayar di periode terbaru
            aktif_ardebt = [d for d in ardebt_results if d.get('STATUS', 'N/A') != 'LUNAS']
            status_financial = f"TUNGGAKAN AKTIF ({len(aktif_ardebt)} Periode)"
        elif mc_latest and mc_latest.get('STATUS') != 'PAYMENT' and mc_latest.get('NOMINAL', 0) > 0:
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
            "mc_data": [clean_mongo_id(doc) for doc in mc_results], # Kini berisi RIWAYAT MC
            "ardebt_data": [clean_mongo_id(doc) for doc in ardebt_results], # Kini berisi RIWAYAT ARDEBT
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

# --- FUNGSI BARU UNTUK REPORT KOLEKSI & PIUTANG ---
@app.route('/api/collection/report', methods=['GET'])
@login_required 
def collection_report_api():
    """Menghitung Nomen Bayar, Nominal Bayar, Total Nominal, dan Persentase per Rayon/PCEZ, termasuk KUBIKASI."""
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500

    # FIX: Ambil BULAN_TAGIHAN terbaru dari MC Historis
    latest_mc_month_doc = collection_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
    latest_mc_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None
    
    if not latest_mc_month:
        return jsonify({"report_data": [], "grand_total": {'TotalPelanggan': 0, 'MC_TotalNominal': 0, 'MB_UndueNominal': 0, 'PercentNominal': 0, 'UnduePercentNominal': 0}}), 200

    # Filter MC hanya untuk bulan terbaru
    mc_filter = {'BULAN_TAGIHAN': latest_mc_month}
    
    initial_project = {
        '$project': {
            'RAYON': { '$ifNull': [ '$RAYON', 'N/A' ] }, 
            'PCEZ': { '$ifNull': [ '$PCEZ', 'N/A' ] },   
            'NOMEN': 1,
            'NOMINAL': {'$toDouble': {'$ifNull': ['$NOMINAL', 0]}}, 
            'KUBIK': {'$toDouble': {'$ifNull': ['$KUBIK', 0]}}, # NEW: Include KUBIK for billed volume
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
            'total_kubik': { '$sum': '$KUBIK' } # Sum of Billed Kubik
        }}
    ]
    billed_data = list(collection_mc.aggregate(pipeline_billed))

    # 2. MC (KOLEKSI) METRICS - Collected (flagged in MC - BULAN TERBARU SAJA)
    pipeline_collected = [
        { '$match': mc_filter }, 
        initial_project, 
        { '$match': { 'STATUS': 'PAYMENT' } }, # Gunakan UPPERCASE yang sudah bersih
        { '$group': {
            '_id': { 'rayon': '$RAYON', 'pcez': '$PCEZ' },
            'collected_nomen': { '$addToSet': '$NOMEN' }, 
            'collected_nominal': { '$sum': '$NOMINAL' },
            'collected_kubik': { '$sum': '$KUBIK' } # Sum of Collected Kubik
        }}
    ]
    collected_data = list(collection_mc.aggregate(pipeline_collected))

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
            
    # Calculate MB (Undue) Metrics separately
    
    pipeline_mb_undue = [
        # Match payments for the current month's bill (approximation)
        { '$match': { 
            'BULAN_REK': latest_mc_month # Match bulan MB dengan bulan tagihan MC terbaru
        }},
        { '$project': {
            'NOMEN': 1,
            'NOMINAL': {'$toDouble': {'$ifNull': ['$NOMINAL', 0]}}, 
            'KUBIKBAYAR': {'$toDouble': {'$ifNull': ['$KUBIKBAYAR', 0]}}, # Use KUBIKBAYAR from MB
            'RAYON_MB': { '$ifNull': [ '$RAYON', 'N/A' ] },
            'PCEZ_MB': { '$ifNull': [ '$PCEZ', 'N/A' ] },
        }},
        # Join back to CID to get Rayon/PCEZ for MB data
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
             'NOMEN': 1,
             'NOMINAL': 1,
             'KUBIKBAYAR': 1
        }},
        { '$group': {
            '_id': { 'rayon': '$RAYON', 'pcez': '$PCEZ' },
            'mb_undue_nominal': { '$sum': '$NOMINAL' },
            'mb_undue_kubik': { '$sum': '$KUBIKBAYAR' },
            'mb_undue_nomen': { '$addToSet': '$NOMEN' },
        }}
    ]
    mb_undue_data = list(collection_mb.aggregate(pipeline_mb_undue))
    
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
            
        report_map[key]['MB_UndueNominal'] = float(item['mb_undue_nominal'])
        report_map[key]['MB_UndueKubik'] = float(item['mb_undue_kubik'])
        report_map[key]['MB_UndueNomen'] = len(item['mb_undue_nomen'])


    # Final calculations and cleanup
    final_report = []
    
    grand_total = {
        'TotalPelanggan': collection_cid.count_documents({}),
        'MC_TotalNominal': 0.0, 'MC_TotalKubik': 0.0,
        'MC_CollectedNominal': 0.0, 'MC_CollectedKubik': 0.0,
        'MC_TotalNomen': 0, 'MC_CollectedNomen': 0,
        'MB_UndueNominal': 0.0, 'MB_UndueKubik': 0.0,
        'MB_UndueNomen': 0,
        'PercentNominal': 0.0, 'PercentNomenCount': 0.0,
        'UnduePercentNominal': 0.0 
    }

    for key, data in report_map.items():
        data.setdefault('MB_UndueNominal', 0.0)
        data.setdefault('MB_UndueKubik', 0.0)
        data.setdefault('MB_UndueNomen', 0)
        
        data['PercentNominal'] = (data['MC_CollectedNominal'] / data['MC_TotalNominal']) * 100 if data['MC_TotalNominal'] > 0 else 0
        data['PercentNomenCount'] = (data['MC_CollectedNomen'] / data['TotalNomen']) * 100 if data['TotalNomen'] > 0 else 0
        data['UnduePercentNominal'] = (data['MB_UndueNominal'] / data['MC_TotalNominal']) * 100 if data['MC_TotalNominal'] > 0 else 0
        
        final_report.append(data)
        
        # Update Grand Totals
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
        'grand_total': grand_total
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
                {'LKS_BAYAR': {'$regex': safe_query_str}} 
            ]
        }
        mongo_query.update(search_filter)

    sort_order = [('TGL_BAYAR', -1)] 

    try:
        results = list(collection_mb.find(mongo_query).sort(sort_order).limit(1000))
        cleaned_results = []
        
        # Fix: Mendapatkan bulan tagihan terbaru dari MC untuk perbandingan IS_UNDUE yang akurat
        latest_mc_month_doc = collection_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
        latest_mc_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None

        for doc in results:
            nominal_val = float(doc.get('NOMINAL', 0)) 
            kubik_val = float(doc.get('KUBIKBAYAR', 0)) # NEW: Include KUBIKBAYAR
            pay_dt = doc.get('TGL_BAYAR', '')
            bulan_rek = doc.get('BULAN_REK', '')
            
            # Perbandingan IS_UNDUE menggunakan BULAN_REK MB dan BULAN_TAGIHAN MC terbaru
            is_undue = bulan_rek == latest_mc_month
            
            cleaned_results.append({
                'NOMEN': doc.get('NOMEN', 'N/A'),
                'RAYON': doc.get('RAYON', doc.get('ZONA_NOREK', 'N/A')), 
                'PCEZ': doc.get('PCEZ', doc.get('LKS_BAYAR', 'N/A')),
                'NOMINAL': nominal_val,
                'KUBIKBAYAR': kubik_val, # NEW
                'PAY_DT': pay_dt,
                'IS_UNDUE': is_undue # NEW
            })
            
        return jsonify(cleaned_results), 200

    except Exception as e:
        print(f"Error fetching detailed collection data: {e}")
        return jsonify({"message": f"Gagal mengambil data detail koleksi: {e}"}), 500


# --- FUNGSI BARU UNTUK EXPORT LAPORAN KOLEKSI/REPORT ---
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

        # Hapus kolom count dan persen di grand total untuk dipisahkan
        df_grand_total = df_grand_total.drop(columns=['MC_TotalNomen', 'MC_CollectedNomen', 'MB_UndueNomen', 'TotalPelanggan'], errors='ignore')
        
        # Gabungkan data dan total
        df_export = pd.concat([df_report, df_grand_total], ignore_index=True)

        # Re-order dan rename kolom
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


# --- ENDPOINT ANALISIS DATA LANJUTAN (SUB-MENU DASHBOARD) ---

@app.route('/analyze', methods=['GET'])
@login_required
def analyze_reports_landing():
    return render_template('analyze_landing.html', is_admin=current_user.is_admin)

# ENDPOINT BARU: PERUBAHAN TARIF
@app.route('/analyze/tariff_change_report', methods=['GET'])
@login_required
def analyze_tariff_change_report():
    return render_template('analyze_report_template.html', 
                           title="Laporan Perubahan Tarif Pelanggan", 
                           description="Menampilkan pelanggan yang memiliki perbedaan data Tarif antara riwayat data CID terbaru dan sebelumnya. (Memerlukan CID dalam mode historis).",
                           is_admin=current_user.is_admin)

@app.route('/analyze/full_mc_report', methods=['GET'])
@login_required
def analyze_full_mc_report():
    return render_template('analyze_report_template.html', 
                           title="Laporan Grup Master Cetak (MC) Lengkap", 
                           description="Menyajikan data agregasi NOMEN, Kubik, dan Nominal berdasarkan Rayon, Metode Baca, Tarif, dan Jenis Meter.",
                           is_admin=current_user.is_admin)

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
# === API GROUPING MC KUSTOM (HELPER FUNCTION) ===
# =========================================================================

def _aggregate_custom_mc_report(collection_mc, collection_cid, dimension=None, rayon_filter=None):
    # This helper will return a list of aggregated results grouped by the dimension.
    # It filters to 'REG' type and applies rayon filter if provided ('34', '35', or 'TOTAL_34_35').
    
    # FIX: Ambil BULAN_TAGIHAN terbaru dari MC Historis
    latest_mc_month_doc = collection_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
    latest_mc_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None
    
    if not latest_mc_month:
        return {'CountOfNOMEN': 0, 'SumOfKUBIK': 0, 'SumOfNOMINAL': 0}

    dimension_map = {'TARIF': '$TARIF_CID', 'MERK': '$MERK_CID', 'READ_METHOD': '$READ_METHOD'}
    
    # Base pipeline structure (Projection and CID Join for all necessary fields)
    pipeline = [
        {'$match': {'BULAN_TAGIHAN': latest_mc_month}}, # HANYA AMBIL MC BULAN TERBARU
        {'$project': {
            'NOMEN': '$NOMEN',
            'NOMINAL': {'$toDouble': {'$ifNull': ['$NOMINAL', 0]}},
            'KUBIK': {'$toDouble': {'$ifNull': ['$KUBIK', 0]}},
        }},
        {'$lookup': {'from': 'CustomerData', 'localField': 'NOMEN', 'foreignField': 'NOMEN', 'as': 'customer_info'}},
        {'$unwind': {'path': '$customer_info', 'preserveNullAndEmptyArrays': False}}, 
        {'$addFields': {
            'CLEAN_RAYON': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.RAYON', 'N/A']}}}}},
            'TARIF_CID': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.TARIF', 'N/A']}}}}}, 
            'MERK_CID': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.MERK', 'N/A']}}}}},
            'READ_METHOD': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.READ_METHOD', 'N/A']}}}}},
            'CLEAN_TIPEPLGGN': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.TIPEPLGGN', 'N/A']}}}}},
        }},
        {'$match': {'CLEAN_TIPEPLGGN': 'REG'}} # Always filter to REG
    ]
    
    # Apply Rayon filter
    rayon_keys = ['34', '35']
    if rayon_filter in rayon_keys:
        pipeline.append({'$match': {'CLEAN_RAYON': rayon_filter}})
    elif rayon_filter == 'TOTAL_34_35':
        pipeline.append({'$match': {'CLEAN_RAYON': {'$in': rayon_keys}}})

    # Grouping stage
    if dimension is None:
        # Total Aggregation
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
        # Dimension Breakdown Aggregation
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
        
        # Rename DIMENSION_KEY field back to the dimension name for easy consumption in JS
        for item in results:
             item[dimension] = item.pop('DIMENSION_KEY')
             
        return results

# =========================================================================
# === API GROUPING MC KUSTOM (GENERATES COMPLEX JSON FOR CUSTOM REPORT) ===
# =========================================================================

@app.route('/api/analyze/mc_grouping', methods=['GET'])
@login_required 
def analyze_mc_grouping_api():
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500

    try:
        # 1. Total Aggregations
        totals = {
            'TOTAL_34_35': _aggregate_custom_mc_report(collection_mc, collection_cid, dimension=None, rayon_filter='TOTAL_34_35'),
            '34': _aggregate_custom_mc_report(collection_mc, collection_cid, dimension=None, rayon_filter='34'),
            '35': _aggregate_custom_mc_report(collection_mc, collection_cid, dimension=None, rayon_filter='35'),
        }

        # 2. Dimension Breakdowns (for R34, R35, and R34+R35 Total)
        dimensions = ['TARIF', 'MERK', 'READ_METHOD']
        breakdowns = {}

        for dim in dimensions:
            breakdowns[dim] = {
                'TOTAL_34_35': _aggregate_custom_mc_report(collection_mc, collection_cid, dim, rayon_filter='TOTAL_34_35'),
                '34': _aggregate_custom_mc_report(collection_mc, collection_cid, dim, rayon_filter='34'),
                '35': _aggregate_custom_mc_report(collection_mc, collection_cid, dim, rayon_filter='35'),
            }
            
        # Jika hasil kosong semua, return error
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

# 2. API SUMMARY (Untuk KPI Cards di collection_unified.html)
@app.route('/api/analyze/mc_grouping/summary', methods=['GET'])
@login_required 
def analyze_mc_grouping_summary_api():
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500

    try:
        # FIX: Ambil BULAN_TAGIHAN terbaru dari MC Historis
        latest_mc_month_doc = collection_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
        latest_mc_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None
        
        if not latest_mc_month:
             return jsonify({ 'TotalPiutangKustomNominal': 0, 'TotalPiutangKustomKubik': 0, 'TotalNomenKustom': 0 }), 200

        pipeline_summary = [
            {'$match': {'BULAN_TAGIHAN': latest_mc_month}}, # HANYA AMBIL MC BULAN TERBARU
            {'$lookup': {
               'from': 'CustomerData', 
               'localField': 'NOMEN',
               'foreignField': 'NOMEN',
               'as': 'customer_info'
            }},
            {'$unwind': {'path': '$customer_info', 'preserveNullAndEmptyArrays': False}},
            
            # --- NORMALISASI DATA UNTUK FILTER ---
            {'$addFields': {
                'CLEAN_TIPEPLGGN': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.TIPEPLGGN', 'N/A']}}}}}, 
                'CLEAN_RAYON': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.RAYON', 'N/A']}}}}},
            }},
            # --- END NORMALISASI ---
            
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

# 3. API BREAKDOWN TARIF (Untuk Tabel Distribusi di collection_unified.html)
@app.route('/api/analyze/mc_tarif_breakdown', methods=['GET'])
@login_required 
def analyze_mc_tarif_breakdown_api():
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500

    try:
        # FIX: Ambil BULAN_TAGIHAN terbaru dari MC Historis
        latest_mc_month_doc = collection_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
        latest_mc_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None
        
        if not latest_mc_month:
             return jsonify([]), 200
             
        pipeline_tarif_breakdown = [
            {'$match': {'BULAN_TAGIHAN': latest_mc_month}}, # HANYA AMBIL MC BULAN TERBARU
            # 1. Join MC ke CID
            {'$lookup': {
               'from': 'CustomerData', 
               'localField': 'NOMEN',
               'foreignField': 'NOMEN',
               'as': 'customer_info'
            }},
            {'$unwind': {'path': '$customer_info', 'preserveNullAndEmptyArrays': False}},
            
            # --- NORMALISASI DATA UNTUK FILTER ---
            {'$addFields': {
                'CLEAN_TIPEPLGGN': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.TIPEPLGGN', 'N/A']}}}}}, 
                'CLEAN_RAYON': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.RAYON', 'N/A']}}}}},
            }},
            # --- END NORMALISASI ---
            
            {'$match': {
                'CLEAN_TIPEPLGGN': 'REG',
                'CLEAN_RAYON': {'$in': ['34', '35']}
            }},
            
            # 3. Grouping berdasarkan RAYON dan TARIF
            {'$group': {
                '_id': {
                    'RAYON': '$CLEAN_RAYON',
                    'TARIF': '$TARIF',
                },
                'CountOfNOMEN': {'$addToSet': '$NOMEN'},
            }},
            
            # 4. Proyeksi Akhir dan Penghitungan Size
            {'$project': {
                '_id': 0,
                'RAYON': '$_id.RAYON',
                'TARIF': '$_id.TARIF',
                'JumlahPelanggan': {'$size': '$CountOfNOMEN'}
            }},
            {'$sort': {'RAYON': 1, 'TARIF': 1}}
        ]
        breakdown_data = list(collection_mc.aggregate(pipeline_tarif_breakdown))
        
        # Perbaiki penanganan error/empty result: jika kosong, kembalikan [] dan status 200
        if not breakdown_data:
            return jsonify([]), 200 

        return jsonify(breakdown_data), 200

    except Exception as e:
        print(f"Error saat mengambil tarif breakdown MC: {e}")
        return jsonify({"message": f"Gagal mengambil tarif breakdown MC. Detail teknis error: {e}"}), 500

# =========================================================================
# === API MONITORING KOLEKSI HARIAN (BARU) ===
# =========================================================================

@app.route('/api/collection/monitoring', methods=['GET'])
@login_required
def collection_monitoring_api():
    """
    Menghasilkan data harian, kumulatif, dan persentase koleksi berdasarkan Rayon (34 & 35)
    menggunakan riwayat MB dan membandingkannya dengan piutang total MC.
    """
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500
        
    try:
        # Tentukan Total Piutang MC (Denominator) dari bulan tagihan terbaru
        latest_mc_month_doc = collection_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
        latest_mc_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None
        
        if not latest_mc_month:
            return jsonify({'monitoring_data': {'R34': [], 'R35': []}, 'summary_top': {'R34': {'MC1125': 0}, 'R35': {'MC1125': 0}}}), 200

        mc_total_response = collection_mc.aggregate([
            {'$match': {'BULAN_TAGIHAN': latest_mc_month}},
            {'$lookup': {'from': 'CustomerData', 'localField': 'NOMEN', 'foreignField': 'NOMEN', 'as': 'customer_info'}},
            {'$unwind': {'path': '$customer_info', 'preserveNullAndEmptyArrays': False}},
            {'$addFields': {'CLEAN_RAYON': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.RAYON', 'N/A']}}}}},
             'CLEAN_TIPEPLGGN': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.TIPEPLGGN', 'N/A']}}}}},}},
            {'$match': {'CLEAN_RAYON': {'$in': ['34', '35']}, 'CLEAN_TIPEPLGGN': 'REG'}},
            {'$group': {'_id': '$CLEAN_RAYON', 'TotalPiutang': {'$sum': {'$toDouble': {'$ifNull': ['$NOMINAL', 0]}}}}}
        ], allowDiskUse=True)
        
        mc_totals = {doc['_id']: doc['TotalPiutang'] for doc in mc_total_response}
        total_mc_34 = mc_totals.get('34', 0)
        total_mc_35 = mc_totals.get('35', 0)
        
        # Ambil Data Transaksi MB (Koleksi)
        today = datetime.now()
        start_date = today - timedelta(days=45) 

        pipeline_mb_daily = [
            # Filter berdasarkan BULAN_REK MB yang sama dengan BULAN_TAGIHAN MC terbaru
            {'$match': {'BULAN_REK': latest_mc_month}}, 
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

        # Proses di Pandas untuk Kumulatif & Persentase
        df_monitoring = pd.DataFrame([
            {'TGL': doc['_id']['date'], 
             'RAYON': doc['_id']['rayon'], 
             'COLL_NOMINAL': doc['DailyNominal'], 
             'CUST_COUNT': len(doc['DailyCustCount'])}
            for doc in mb_daily_data
        ])

        if df_monitoring.empty:
            return jsonify({'monitoring_data': {'R34': [], 'R35': []}, 'summary_top': {'R34': {'MC1125': total_mc_34}, 'R35': {'MC1125': total_mc_35}}}), 200

        df_monitoring['TGL'] = pd.to_datetime(df_monitoring['TGL'])
        df_monitoring = df_monitoring.sort_values(by='TGL')
        
        # Hitung Kumulatif per Rayon
        df_monitoring['Rp1_Kumulatif'] = df_monitoring.groupby('RAYON')['COLL_NOMINAL'].cumsum()
        df_monitoring['CUST_Kumulatif'] = df_monitoring.groupby('RAYON')['CUST_COUNT'].cumsum()

        # Hitung Persentase Koleksi & Variance
        df_monitoring['COLL_VAR'] = 0.0
        
        def calculate_percentages(group):
            rayon = group['RAYON'].iloc[0]
            total_piutang = total_mc_34 if rayon == '34' else total_mc_35
            
            if total_piutang > 0:
                group['COLL_Kumulatif_Persen'] = (group['Rp1_Kumulatif'] / total_piutang) * 100
                group['COLL_VAR'] = group['COLL_Kumulatif_Persen'].diff().fillna(0)
            else:
                group['COLL_Kumulatif_Persen'] = 0.0
            return group

        df_monitoring = df_monitoring.groupby('RAYON').apply(calculate_percentages)
        
        # Format Output
        df_monitoring['TGL'] = df_monitoring['TGL'].dt.strftime('%d/%m/%Y')
        
        df_r34 = df_monitoring[df_monitoring['RAYON'] == '34'].drop(columns=['RAYON']).reset_index(drop=True)
        df_r35 = df_monitoring[df_monitoring['RAYON'] == '35'].drop(columns=['RAYON']).reset_index(drop=True)

        summary_r34 = {
            'MC1125': total_mc_34,
            'CURRENT': total_mc_34 - df_r34['Rp1_Kumulatif'].iloc[-1] if not df_r34.empty else total_mc_34
        }
        summary_r35 = {
            'MC1125': total_mc_35,
            'CURRENT': total_mc_35 - df_r35['Rp1_Kumulatif'].iloc[-1] if not df_r35.empty else total_mc_35
        }

        return jsonify({
            'monitoring_data': {
                'R34': df_r34.to_dict('records'),
                'R35': df_r35.to_dict('records'),
            },
            'summary_top': {
                'R34': summary_r34,
                'R35': summary_r35,
            }
        }), 200

    except Exception as e:
        print(f"Error collection monitoring: {e}")
        return jsonify({"message": f"Gagal membuat data monitoring koleksi: {e}"}), 500

# =========================================================================
# === API PERUBAHAN TARIF PELANGGAN (BARU) ===
# =========================================================================

def _aggregate_tariff_changes(collection_cid):
    """
    Menjalankan pipeline agregasi untuk menemukan pelanggan yang tarifnya berubah
    dengan membandingkan riwayat TARIF dari data CID yang di-APPEND.
    """
    if collection_cid.count_documents({}) == 0:
        return []

    pipeline_tariff_history = [
        # 1. Group berdasarkan NOMEN untuk mengumpulkan riwayat tarif
        {'$group': {
            '_id': '$NOMEN',
            'history': {
                '$push': {
                    'tanggal': {'$ifNull': ['$TANGGAL_UPLOAD_CID', '1900-01-01 00:00:00']},
                    'tarif': '$TARIF'
                }
            }
        }},
        # 2. Sort the history array by date/timestamp
        {'$project': {
            'NOMEN': '$_id',
            'sorted_history': {
                '$sortArray': {
                    'input': '$history',
                    'sortBy': {'tanggal': 1}
                }
            },
            '_id': 0
        }},
        # 3. Dapatkan tarif terbaru dan tarif sebelumnya
        {'$addFields': {
            'latest_tarif': {'$arrayElemAt': ['$sorted_history.tarif', -1]}, 
            'previous_tarif': {
                '$arrayElemAt': [
                    '$sorted_history.tarif',
                    {'$subtract': [{'$size': '$sorted_history.tarif'}, 2]}
                ]
            }
        }},
        # 4. Cocokkan jika tarif terbaru TIDAK SAMA dengan tarif sebelumnya
        {'$match': {
            '$expr': {'$ne': ['$latest_tarif', '$previous_tarif']},
            'previous_tarif': {'$ne': None} 
        }},
        # 5. Proyeksi Akhir
        {'$project': {
            'NOMEN': 1,
            'TARIF_SEBELUMNYA': '$previous_tarif',
            'TARIF_TERBARU': '$latest_tarif',
            'TOTAL_RIWAYAT_CID': {'$size': '$sorted_history'},
            'TANGGAL_PERUBAHAN_TERAKHIR': {'$arrayElemAt': ['$sorted_history.tanggal', -1]}
        }},
        {'$limit': 500}
    ]

    changes = list(collection_cid.aggregate(pipeline_tariff_history, allowDiskUse=True))
    return changes

@app.route('/api/analyze/tariff_change', methods=['GET'])
@login_required 
def analyze_tariff_change_api():
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500

    try:
        tariff_changes = _aggregate_tariff_changes(collection_cid)
        return jsonify(tariff_changes), 200

    except Exception as e:
        print(f"Error saat menganalisis perubahan tarif: {e}")
        return jsonify({"message": f"Gagal mengambil data perubahan tarif. Detail teknis error: {e}"}), 500

# =========================================================================
# === API LAPORAN TOP LISTS (BARU) ===
# =========================================================================

def _aggregate_top_debt(collection_mc, collection_ardebt, collection_cid):
    """Menghitung total kewajiban (Piutang MC + Tunggakan ARDEBT) dan mengembalikan 500 teratas."""
    # 1. Ambil BULAN_TAGIHAN terbaru dari MC Historis
    latest_mc_month_doc = collection_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
    latest_mc_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None
    
    # 1. Agregasi Tunggakan (ARDEBT)
    pipeline_ardebt_total = [
        {'$group': {
            '_id': '$NOMEN',
            'TotalARDEBT': {'$sum': {'$toDouble': {'$ifNull': ['$JUMLAH', 0]}}},
            'TotalPeriodeTunggakan': {'$sum': 1}
        }}
    ]
    ardebt_totals = {doc['_id']: doc for doc in collection_ardebt.aggregate(pipeline_ardebt_total, allowDiskUse=True)}

    # 2. Agregasi Piutang Bulan Ini (MC) - Filter ke bulan tagihan terbaru
    pipeline_mc_piutang = [
        {'$match': {'BULAN_TAGIHAN': latest_mc_month}} if latest_mc_month else {'$match': {'NOMEN': {'$exists': True}}}, 
        {'$group': {
            '_id': '$NOMEN',
            'MC_Piutang': {'$sum': {'$toDouble': {'$ifNull': ['$NOMINAL', 0]}}}
        }}
    ]
    mc_piutang = {doc['_id']: doc['MC_Piutang'] for doc in collection_mc.aggregate(pipeline_mc_piutang, allowDiskUse=True)}

    # 3. Gabungkan, Hitung Total Debt, dan Join ke CID (Client-side join untuk kemudahan)
    debt_list = []
    all_nomens = set(mc_piutang.keys()) | set(ardebt_totals.keys())

    for nomen in all_nomens:
        ardebt = ardebt_totals.get(nomen, {})
        mc_val = mc_piutang.get(nomen, 0)
        
        total_ardebt = ardebt.get('TotalARDEBT', 0)
        total_debt = mc_val + total_ardebt

        if total_debt > 0:
            cid_info = collection_cid.find_one({'NOMEN': nomen}, {'_id': 0, 'NAMA': 1, 'RAYON': 1, 'TARIF': 1}, sort=[('TANGGAL_UPLOAD_CID', -1)]) or {}
            
            debt_list.append({
                'NOMEN': nomen,
                'NAMA': cid_info.get('NAMA', 'N/A'),
                'RAYON': cid_info.get('RAYON', 'N/A'),
                'TARIF': cid_info.get('TARIF', 'N/A'),
                'MC_PIUTANG_TERBARU': mc_val,
                'TOTAL_TUNGGAKAN_ARDEBT': total_ardebt,
                'TOTAL_KEWAJIBAN': total_debt,
                'JUMLAH_PERIODE_TUNGGAKAN': ardebt.get('TotalPeriodeTunggakan', 0)
            })

    debt_list.sort(key=lambda x: x['TOTAL_KEWAJIBAN'], reverse=True)
    return debt_list[:500]

def _aggregate_top_premium(collection_mc, collection_cid):
    """Menentukan 'Premium' berdasarkan Konsumsi (KUBIK) tertinggi di periode MC saat ini."""
    # Ambil BULAN_TAGIHAN terbaru dari MC Historis
    latest_mc_month_doc = collection_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
    latest_mc_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None

    pipeline_premium = [
        {'$match': {'BULAN_TAGIHAN': latest_mc_month}} if latest_mc_month else {'$match': {'NOMEN': {'$exists': True}}}, 
        {'$match': {'KUBIK': {'$gt': 0}}}, 
        {'$group': {
            '_id': '$NOMEN',
            'TotalKubik': {'$sum': {'$toDouble': {'$ifNull': ['$KUBIK', 0]}}},
            'TotalNominal': {'$sum': {'$toDouble': {'$ifNull': ['$NOMINAL', 0]}}}
        }},
        {'$sort': {'TotalKubik': -1}}, 
        {'$limit': 500},
        {'$lookup': {'from': 'CustomerData', 'localField': '_id', 'foreignField': 'NOMEN', 'as': 'customer_info'}},
        {'$unwind': {'path': '$customer_info', 'preserveNullAndEmptyArrays': True}},
        {'$project': {
            '_id': 0,
            'NOMEN': '$_id',
            'NAMA': {'$ifNull': ['$customer_info.NAMA', 'N/A']},
            'RAYON': {'$ifNull': ['$customer_info.RAYON', 'N/A']},
            'TARIF': {'$ifNull': ['$customer_info.TARIF', 'N/A']},
            'TOTAL_KUBIK_MC': {'$round': ['$TotalKubik', 0]},
            'TOTAL_NOMINAL_MC': {'$round': ['$TotalNominal', 0]},
            'STATUS': 'MC PIUTANG'
        }}
    ]
    return list(collection_mc.aggregate(pipeline_premium, allowDiskUse=True))

@app.route('/api/report/top_lists', methods=['GET'])
@login_required 
def report_top_lists_api():
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500
    
    try:
        # Ambil BULAN_TAGIHAN terbaru dari MC Historis
        latest_mc_month_doc = collection_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
        latest_mc_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None

        # 1. Top 500 Belum Bayar (Total Debt)
        top_debt = _aggregate_top_debt(collection_mc, collection_ardebt, collection_cid)
        
        # 2. Top 500 Premium (High Volume)
        top_premium = _aggregate_top_premium(collection_mc, collection_cid)
        
        # 3. Konsumen Sudah Bayar (MC Status Paid - Limit 500)
        paid_pipeline = [
            {'$match': {'BULAN_TAGIHAN': latest_mc_month, 'STATUS': 'PAYMENT'}} if latest_mc_month else {'$match': {'STATUS': 'PAYMENT'}},
            {'$limit': 500}, 
            {'$lookup': {'from': 'CustomerData', 'localField': 'NOMEN', 'foreignField': 'NOMEN', 'as': 'cid'}},
            {'$unwind': {'path': '$cid', 'preserveNullAndEmptyArrays': True}},
            {'$project': {
                '_id': 0, 'NOMEN': 1, 'NAMA': {'$ifNull': ['$cid.NAMA', 'N/A']},
                'RAYON': {'$ifNull': ['$cid.RAYON', 'N/A']},
                'NOMINAL_MC': {'$round': [{'$toDouble': {'$ifNull': ['$NOMINAL', 0]}}, 0]},
                'KUBIK_MC': {'$round': [{'$toDouble': {'$ifNull': ['$KUBIK', 0]}}, 0]},
                'STATUS': 1
            }}
        ]
        paid_list = list(collection_mc.aggregate(paid_pipeline))
        
        # 4. Konsumen Belum Bayar (MC Status Unpaid - Limit 500)
        unpaid_pipeline = [
            {'$match': {'BULAN_TAGIHAN': latest_mc_month, 'STATUS': {'$ne': 'PAYMENT'}, 'NOMINAL': {'$gt': 0}}} if latest_mc_month else {'$match': {'STATUS': {'$ne': 'PAYMENT'}, 'NOMINAL': {'$gt': 0}}},
            {'$limit': 500}, 
            {'$lookup': {'from': 'CustomerData', 'localField': 'NOMEN', 'foreignField': 'NOMEN', 'as': 'cid'}},
            {'$unwind': {'path': '$cid', 'preserveNullAndEmptyArrays': True}},
            {'$project': {
                '_id': 0, 'NOMEN': 1, 'NAMA': {'$ifNull': ['$cid.NAMA', 'N/A']},
                'RAYON': {'$ifNull': ['$cid.RAYON', 'N/A']},
                'NOMINAL_MC': {'$round': [{'$toDouble': {'$ifNull': ['$NOMINAL', 0]}}, 0]},
                'KUBIK_MC': {'$round': [{'$toDouble': {'$ifNull': ['$KUBIK', 0]}}, 0]},
                'STATUS': 1
            }}
        ]
        unpaid_list = list(collection_mc.aggregate(unpaid_pipeline))

        return jsonify({
            'status': 'success',
            'top_debt': top_debt,
            'top_premium': top_premium,
            'paid_list': paid_list,
            'unpaid_list': unpaid_list
        }), 200

    except Exception as e:
        print(f"Error fetching top lists: {e}")
        return jsonify({"message": f"Gagal mengambil data top list: {e}"}), 500


# =========================================================================
# === API LAPORAN VOLUME DASAR BULANAN (BARU) ===
# =========================================================================

@app.route('/api/report/basic_volume', methods=['GET'])
@login_required 
def basic_volume_report_api():
    """
    Menghasilkan agregasi Volume (KUBIK) bulanan berdasarkan Rayon dari seluruh 
    data Master Cetak (MC) yang telah di-append secara historis.
    """
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500

    try:
        pipeline_basic_volume = [
            {'$project': {
                'NOMEN': 1,
                'KUBIK': {'$toDouble': {'$ifNull': ['$KUBIK', 0]}},
                'RAYON': 1,
                'BULAN_TAGIHAN': 1 # Kunci pembeda historis
            }},
            {'$match': {
                'KUBIK': {'$gt': 0},
                'RAYON': {'$in': ['34', '35']} 
            }},
            {'$group': {
                '_id': { 'rayon': '$RAYON', 'bulan': '$BULAN_TAGIHAN' },
                'TotalKubik': {'$sum': '$KUBIK'},
                'CountNomen': {'$addToSet': '$NOMEN'}
            }},
            {'$project': {
                '_id': 0,
                'RAYON': '$_id.rayon',
                'BULAN_TAGIHAN': '$_id.bulan',
                'SUM_KUBIK': {'$round': ['$TotalKubik', 0]},
                'COUNT_CUST': {'$size': '$CountNomen'}
            }},
            {'$sort': {'BULAN_TAGIHAN': 1, 'RAYON': 1}}
        ]
        
        volume_data_raw = list(collection_mc.aggregate(pipeline_basic_volume, allowDiskUse=True))

        if not volume_data_raw:
            return jsonify({'basic_volume_report': []}), 200

        df_vol = pd.DataFrame(volume_data_raw)
        
        # Format BULAN_TAGIHAN untuk pivot
        def format_bulan(tagihan):
             try:
                 if len(tagihan) >= 6:
                     month_str = tagihan[:2]
                     year_str = tagihan[-2:]
                     month_name = datetime.strptime(month_str, '%m').strftime('%b')
                     return f"{month_name}-{year_str}"
                 else:
                     return tagihan
             except:
                 return tagihan

        df_vol['BULAN_TAGIHAN_FMT'] = df_vol['BULAN_TAGIHAN'].astype(str).apply(format_bulan)
        
        # Buat Pivot Table
        pivot_kubik = df_vol.pivot_table(
            index='RAYON', 
            columns='BULAN_TAGIHAN_FMT', 
            values='SUM_KUBIK', 
            fill_value=0,
            aggfunc='sum'
        )
        
        # Hitung Total Horizontal dan Vertikal
        pivot_kubik['TOTAL_VOL'] = pivot_kubik.sum(axis=1)
        total_row = pd.Series(pivot_kubik.sum(axis=0), name='TOTAL')
        pivot_kubik = pd.concat([pivot_kubik, total_row.to_frame().T])
        
        pivot_kubik = pivot_kub.reset_index().rename(columns={'index': 'RAYON'})

        return jsonify({
            'status': 'success',
            'basic_volume_report': pivot_kubik.to_dict('records')
        }), 200

    except Exception as e:
        print(f"Error basic volume report: {e}")
        return jsonify({"message": f"Gagal membuat laporan volume dasar: {e}"}), 500

# =========================================================================
# === END API REPORTS ===
# =========================================================================


@app.route('/api/analyze/volume_fluctuation', methods=['GET'])
@login_required 
def analyze_volume_fluctuation_api():
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500

    try:
        fluctuation_data = _get_sbrs_anomalies(collection_sbrs, collection_cid)
        return jsonify(fluctuation_data), 200

    except Exception as e:
        print(f"Error saat menganalisis fluktuasi volume: {e}")
        return jsonify({"message": f"Gagal mengambil data fluktuasi volume. Detail teknis error: {e}"}), 500
        
# Endpoint File Upload/Merge (Dinamis)
@app.route('/analyze/upload', methods=['GET'])
@login_required 
def analyze_data_page():
    return render_template('analyze_upload.html', is_admin=current_user.is_admin)

@app.route('/api/analyze', methods=['POST'])
@login_required 
def analyze_data():
    """Endpoint untuk mengunggah file jamak, menggabungkannya, dan menjalankan analisis data."""
    if not request.files:
        return jsonify({"message": "Tidak ada file yang diunggah."}), 400

    uploaded_files = request.files.getlist('file')
    all_dfs = []
    
    JOIN_KEY = 'NOMEN' 
    
    for file in uploaded_files:
        filename = secure_filename(file.filename)
        file_extension = filename.rsplit('.', 1)[1].lower()

        if file_extension not in ALLOWED_EXTENSIONS:
            continue

        try:
            if file_extension == 'csv':
                df = pd.read_csv(file) 
            elif file_extension in ['xlsx', 'xls']:
                df = pd.read_excel(file, sheet_name=0) 
            
            df.columns = [col.strip().upper() for col in df.columns]
            
            if JOIN_KEY in df.columns:
                 df[JOIN_KEY] = df[JOIN_KEY].astype(str).str.strip() 
                 all_dfs.append(df)
            else:
                 return jsonify({"message": f"Gagal: File '{filename}' tidak memiliki kolom kunci '{JOIN_KEY}'."}), 400

        except Exception as e:
            print(f"Error membaca file {filename}: {e}")
            return jsonify({"message": f"Gagal membaca file {filename}: {e}"}), 500

    if not all_dfs:
        return jsonify({"message": "Tidak ada file yang valid untuk digabungkan."}), 400

    merged_df = all_dfs[0]
    
    for i in range(1, len(all_dfs)):
        merged_df = pd.merge(merged_df, all_dfs[i], on=JOIN_KEY, how='outer', suffixes=(f'_f{i}', f'_f{i+1}'))

    # Analisis Data Gabungan
    data_summary = {
        "file_name": f"Gabungan ({len(uploaded_files)} files)",
        "join_key": JOIN_KEY,
        "row_count": len(merged_df),
        "column_count": len(merged_df.columns),
        "columns": merged_df.columns.tolist() 
    }

    descriptive_stats = merged_df.describe(include='all').to_json(orient='index')
    
    return jsonify({
        "status": "success",
        "summary": data_summary,
        "stats": descriptive_stats,
        "head": merged_df.head().to_html(classes='table table-striped') 
    }), 200

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
        # Get Month and Year from the request form data (sent from JS)
        upload_month = request.form.get('month')
        upload_year = request.form.get('year')
        
        # 1. Validation for required fields from HTML form
        if not upload_month or not upload_year:
            return jsonify({"message": "Gagal: Bulan dan Tahun Tagihan harus diisi."}), 400
        
        # Construct the BULAN_TAGIHAN string (e.g., 012025)
        bulan_tagihan_value = f"{upload_month}{upload_year}"

        if file_extension == 'csv':
            df = pd.read_csv(file)
        elif file_extension in ['xlsx', 'xls']:
            df = pd.read_excel(file, sheet_name=0) 
        
        # Normalize original column names (Uppercase)
        df.columns = [col.strip().upper() for col in df.columns]
        
        # 2. Check for the critical NOMEN key (which should be present in the file)
        if 'NOMEN' not in df.columns:
            return jsonify({"message": "Gagal Append: File MC harus memiliki kolom kunci 'NOMEN' untuk penyimpanan historis."}), 400
            
        # >>> START: INJECT BULAN_TAGIHAN COLUMN <<<
        # Suntikkan kolom BULAN_TAGIHAN yang dibuat dari form ke DataFrame
        df['BULAN_TAGIHAN'] = bulan_tagihan_value
        # >>> END: INJECT BULAN_TAGIHAN COLUMN <<<

        # >>> PERBAIKAN KRITIS MC: NORMALISASI DATA PANDAS <<<
        # Pastikan BULAN_TAGIHAN ada di daftar normalisasi string
        columns_to_normalize_mc = ['PC', 'EMUH', 'NOMEN', 'STATUS', 'TARIF', 'BULAN_TAGIHAN'] 
        
        for col in df.columns:
            if df[col].dtype == 'object' or col in columns_to_normalize_mc:
                df[col] = df[col].astype(str).str.strip().str.upper()
                df[col] = df[col].replace(['NAN', 'NONE', '', ' '], 'N/A')
            
            if col in ['NOMINAL', 'NOMINAL_AKHIR', 'KUBIK', 'SUBNOMINAL', 'ANG_BP', 'DENDA', 'PPN']: 
                 df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        if 'PC' in df.columns:
             df = df.rename(columns={'PC': 'RAYON'})

        # >>> START PERUBAHAN KRITIS KE APPEND (BULK WRITE) <<<
        data_to_insert = df.to_dict('records')
        
        if not data_to_insert:
            return jsonify({"message": "File kosong, tidak ada data yang dimasukkan."}), 200
        
        # Kunci Unik: NOMEN + BULAN_TAGIHAN (untuk cek duplikasi internal)
        UNIQUE_KEYS = ['NOMEN', 'BULAN_TAGIHAN'] 
        
        # Check this again for safety, though it should pass now
        if not all(key in df.columns for key in UNIQUE_KEYS):
             # Jika ini dieksekusi, ada masalah serius di Pandas/Loading.
             return jsonify({"message": "Kesalahan Internal: Kolom kunci 'NOMEN' atau 'BULAN_TAGIHAN' hilang setelah pemrosesan Pandas."}), 500

        inserted_count = 0
        skipped_count = 0
        total_rows = len(data_to_insert)

        try:
            # Menggunakan insert_many(ordered=False) untuk kecepatan dan melewati duplikasi
            result = collection_mc.insert_many(data_to_insert, ordered=False)
            inserted_count = len(result.inserted_ids)
            skipped_count = total_rows - inserted_count
            
        except BulkWriteError as bwe:
            # Menangani error duplikasi dari index MongoDB (internal atau eksternal)
            inserted_count = bwe.details.get('nInserted', 0)
            skipped_count = total_rows - inserted_count
            
        except Exception as e:
            # Jika gagal karena alasan lain, lempar error untuk debugging
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
        # >>> END PERUBAHAN KRITIS KE APPEND (BULK WRITE) <<<

    except Exception as e:
        print(f"Error saat memproses file MC: {e}")
        return jsonify({"message": f"Gagal memproses file MC: {e}. Pastikan format data benar."}), 500


@app.route('/upload/mb', methods=['POST'])
@login_required 
@admin_required 
def upload_mb_data():
    """Mode APPEND: Untuk Master Bayar (MB) / Koleksi Harian. (DIOPTIMASI)"""
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
        
        # >>> PERBAIKAN KRITIS MB: NORMALISASI DATA PANDAS <<<

        columns_to_normalize_mb = ['NOMEN', 'RAYON', 'PCEZ', 'ZONA_NOREK', 'LKS_BAYAR', 'BULAN_REK', 'NOTAGIHAN'] 
        
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
        # Kunci unik untuk MB (harus ada di index MongoDB sebagai unique=True)
        UNIQUE_KEYS = ['NOTAGIHAN', 'TGL_BAYAR', 'NOMINAL'] 
        
        if not all(key in df.columns for key in UNIQUE_KEYS):
            return jsonify({"message": f"Gagal Append: File MB harus memiliki kolom kunci unik: {', '.join(UNIQUE_KEYS)}. Cek file Anda."}), 400

        inserted_count = 0
        skipped_count = 0
        total_rows = len(data_to_insert)

        try:
            # Menggunakan insert_many(ordered=False) untuk kecepatan dan melewati duplikasi
            result = collection_mb.insert_many(data_to_insert, ordered=False)
            inserted_count = len(result.inserted_ids)
            skipped_count = total_rows - inserted_count
            
        except BulkWriteError as bwe:
            # Menangani error duplikasi
            inserted_count = bwe.details.get('nInserted', 0)
            skipped_count = total_rows - inserted_count
            
        except Exception as e:
            print(f"Error massal saat insert: {e}")
            return jsonify({"message": f"Gagal menyimpan data secara massal: {e}"}), 500
        
        # --- RETURN REPORT ---
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
        # --- END RETURN REPORT ---

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

        # >>> PERBAIKAN KRITIS CID: NORMALISASI DATA PANDAS <<<
        columns_to_normalize = ['MERK', 'READ_METHOD', 'TIPEPLGGN', 'RAYON', 'NOMEN', 'TARIFF']
        
        for col in df.columns:
            if df[col].dtype == 'object' or col in columns_to_normalize:
                df[col] = df[col].astype(str).str.strip().str.upper()
                df[col] = df[col].replace(['NAN', 'NONE', '', ' '], 'N/A')
        
        if 'TARIFF' in df.columns:
             df = df.rename(columns={'TARIFF': 'TARIF'})
        
        # >>> START PERUBAHAN KRITIS KE APPEND (BULK WRITE) <<<
        data_to_insert = df.to_dict('records')
        
        if not data_to_insert:
            return jsonify({"message": "File kosong, tidak ada data yang dimasukkan."}), 200
        
        # Tambahkan field penanda waktu upload
        upload_date_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for record in data_to_insert:
            record['TANGGAL_UPLOAD_CID'] = upload_date_str

        inserted_count = 0
        total_rows = len(data_to_insert)

        # CID menggunakan insert_many biasa, karena TANGGAL_UPLOAD_CID membuat data unik
        # Kita bungkus dalam try/except untuk menangani potential BulkWriteError
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
        # >>> END PERUBAHAN KRITIS KE APPEND (BULK WRITE) <<<

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
        # Load data
        if file_extension == 'csv':
            df = pd.read_csv(file)
        elif file_extension in ['xlsx', 'xls']:
            df = pd.read_excel(file, sheet_name=0) 
        
        df.columns = [col.strip().upper() for col in df.columns]

        # >>> PERBAIKAN KRITIS SBRS: NORMALISASI DATA PANDAS <<<
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
        
        # --- START: OPERASI BULK WRITE TEROPTIMASI ---
        # Kunci unik: CMR_ACCOUNT (NOMEN) + CMR_RD_DATE (Tanggal Baca)
        UNIQUE_KEYS = ['CMR_ACCOUNT', 'CMR_RD_DATE'] 
        
        if not all(key in df.columns for key in UNIQUE_KEYS):
            return jsonify({"message": f"Gagal Append: File SBRS harus memiliki kolom kunci unik: {', '.join(UNIQUE_KEYS)}. Cek file Anda."}), 400

        inserted_count = 0
        skipped_count = 0
        total_rows = len(data_to_insert)

        try:
            # Menggunakan insert_many(ordered=False) untuk kecepatan dan melewati duplikasi
            result = collection_sbrs.insert_many(data_to_insert, ordered=False)
            inserted_count = len(result.inserted_ids)
            skipped_count = total_rows - inserted_count
            
        except BulkWriteError as bwe:
            # Menangani error duplikasi
            inserted_count = bwe.details.get('nInserted', 0)
            skipped_count = total_rows - inserted_count
            
        except Exception as e:
            print(f"Error massal saat insert: {e}")
            return jsonify({"message": f"Gagal menyimpan data secara massal: {e}"}), 500
        
        # === ANALISIS ANOMALI INSTAN SETELAH INSERT ===
        anomaly_list = []
        try:
            if inserted_count > 0:
                anomaly_list = _get_sbrs_anomalies(collection_sbrs, collection_cid)
        except Exception as e:
            print(f"Peringatan: Gagal menjalankan analisis anomali instan: {e}")
        # ============================================

        # --- RETURN REPORT ---
        return jsonify({
            "status": "success",
            "message": f"Sukses Append! {inserted_count} baris Riwayat Baca Meter (SBRS) baru ditambahkan. ({skipped_count} duplikat diabaikan).",
            "summary_report": {
                "total_rows": total_rows,
                "type": "APPEND",
                "inserted_count": inserted_count,
                "skipped_count": skipped_count
            },
            "anomaly_list": anomaly_list # Mengembalikan hasil analisis anomali
        }), 200
        # --- END RETURN REPORT ---

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

        # 1. Check for the critical NOMEN key
        if 'NOMEN' not in df.columns:
            return jsonify({"message": "Gagal Append: File ARDEBT harus memiliki kolom kunci 'NOMEN' untuk penyimpanan historis."}), 400
            
        # 2. VALIDASI DAN PENYESUAIAN JUMLAH (Kunci Tunggakan)
        monetary_keys = ['JUMLAH', 'AMOUNT', 'TOTAL', 'NOMINAL']
        found_monetary_key = next((key for key in monetary_keys if key in df.columns), None)

        if found_monetary_key and found_monetary_key != 'JUMLAH':
             df = df.rename(columns={found_monetary_key: 'JUMLAH'})
        elif 'JUMLAH' not in df.columns:
             return jsonify({"message": "Gagal Append: Kolom kunci JUMLAH (atau AMOUNT/TOTAL/NOMINAL) untuk nominal tunggakan tidak ditemukan di file Anda."}), 400

        # Get Month and Year from the request form data (sent from JS)
        upload_month = request.form.get('month')
        upload_year = request.form.get('year')
        
        if not upload_month or not upload_year:
            return jsonify({"message": "Gagal: Bulan dan Tahun Tunggakan harus diisi."}), 400
        
        # Construct the PERIODE_BILL string (e.g., 012025)
        periode_bill_value = f"{upload_month}{upload_year}"

        # >>> START: INJECT PERIODE_BILL COLUMN <<<
        df['PERIODE_BILL'] = periode_bill_value 
        # >>> END: INJECT PERIODE_BILL COLUMN <<<

        # >>> PERBAIKAN KRITIS ARDEBT: NORMALISASI DATA PANDAS <<<
        columns_to_normalize_ardebt = ['NOMEN', 'RAYON', 'TIPEPLGGN', 'PERIODE_BILL'] 
        
        for col in df.columns:
            if df[col].dtype == 'object' or col in columns_to_normalize_ardebt:
                df[col] = df[col].astype(str).str.strip().str.upper()
                df[col] = df[col].replace(['NAN', 'NONE', '', ' '], 'N/A')
            
            if col in ['JUMLAH', 'VOLUME']: 
                 df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # >>> START PERUBAHAN KRITIS KE APPEND (BULK WRITE) <<<
        data_to_insert = df.to_dict('records')
        
        if not data_to_insert:
            return jsonify({"message": "File kosong, tidak ada data yang dimasukkan."}), 200
        
        # Kunci Unik: NOMEN + PERIODE_BILL + JUMLAH 
        UNIQUE_KEYS = ['NOMEN', 'PERIODE_BILL', 'JUMLAH'] 
        
        inserted_count = 0
        skipped_count = 0
        total_rows = len(data_to_insert)

        try:
            # Menggunakan insert_many(ordered=False) untuk kecepatan dan melewati duplikasi
            result = collection_ardebt.insert_many(data_to_insert, ordered=False)
            inserted_count = len(result.inserted_ids)
            skipped_count = total_rows - inserted_count
            
        except BulkWriteError as bwe:
            # Menangani error duplikasi
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
        # >>> END PERUBAHAN KRITIS KE APPEND (BULK WRITE) <<<

    except Exception as e:
        print(f"Error saat memproses file ARDEBT: {e}")
        return jsonify({"message": f"Gagal memproses file ARDEBT: {e}. Pastikan format data benar."}), 500


# =========================================================================
# === DASHBOARD ANALYTICS ENDPOINTS (INTEGRATED) ===
# =========================================================================

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
        
        # Ambil BULAN_TAGIHAN terbaru dari MC Historis untuk data PIUTANG AKTIF
        latest_mc_month_doc = collection_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
        latest_mc_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None
        
        # 1. TOTAL PELANGGAN (dari CID - Ambil total nomen unik terbaru)
        summary_data['total_pelanggan'] = len(collection_cid.distinct('NOMEN'))
        
        # 2. TOTAL PIUTANG & TUNGGAKAN
        pipeline_piutang = [
            {'$match': {'BULAN_TAGIHAN': latest_mc_month}} if latest_mc_month else {'$match': {'NOMEN': {'$exists': True}}}, 
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
        
        # 3. KOLEKSI HARI INI (dari MB)
        today_date = datetime.now().strftime('%Y-%m-%d')
        pipeline_koleksi_today = [
            {'$match': {'TGL_BAYAR': today_date}}, # Match string tanggal hari ini
            {'$group': {
                '_id': None,
                'koleksi_hari_ini': {'$sum': '$NOMINAL'},
                'transaksi_hari_ini': {'$sum': 1}
            }}
        ]
        koleksi_result = list(collection_mb.aggregate(pipeline_koleksi_today))
        summary_data['koleksi_hari_ini'] = koleksi_result[0]['koleksi_hari_ini'] if koleksi_result else 0
        summary_data['transaksi_hari_ini'] = koleksi_result[0]['transaksi_hari_ini'] if koleksi_result else 0
        
        # 4. TOTAL KOLEKSI BULAN INI
        this_month = datetime.now().strftime('%Y-%m')
        pipeline_koleksi_month = [
            {'$match': {'TGL_BAYAR': {'$regex': this_month}}},
            {'$group': {
                '_id': None,
                'koleksi_bulan_ini': {'$sum': '$NOMINAL'},
                'transaksi_bulan_ini': {'$sum': 1}
            }}
        ]
        koleksi_month_result = list(collection_mb.aggregate(pipeline_koleksi_month))
        summary_data['koleksi_bulan_ini'] = koleksi_month_result[0]['koleksi_bulan_ini'] if koleksi_month_result else 0
        summary_data['transaksi_bulan_ini'] = koleksi_month_result[0]['transaksi_bulan_ini'] if koleksi_month_result else 0
        
        # 5. ANOMALI PEMAKAIAN (dari fungsi existing)
        anomalies = _get_sbrs_anomalies(collection_sbrs, collection_cid)
        summary_data['total_anomali'] = len(anomalies)
        
        # Breakdown anomali per tipe
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
        
        # 6. PELANGGAN DENGAN TUNGGAKAN (Distinct NOMEN)
        pelanggan_tunggakan = collection_ardebt.distinct('NOMEN')
        summary_data['pelanggan_dengan_tunggakan'] = len(pelanggan_tunggakan)
        
        # 7. TOP 5 RAYON DENGAN PIUTANG TERTINGGI (Bulan Tagihan Terbaru)
        pipeline_top_rayon = [
            {'$match': {'BULAN_TAGIHAN': latest_mc_month}} if latest_mc_month else {'$match': {'NOMEN': {'$exists': True}}}, 
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
        
        # 8. TREN KOLEKSI 7 HARI TERAKHIR
        trend_data = []
        for i in range(7):
            date_obj = datetime.now() - timedelta(days=i)
            date = date_obj.strftime('%Y-%m-%d')
            
            pipeline = [
                {'$match': {'TGL_BAYAR': date}}, 
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
        
        # 9. PERSENTASE KOLEKSI
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
    
    # Ambil BULAN_TAGIHAN terbaru dari MC Historis
    latest_mc_month_doc = collection_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
    latest_mc_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None
    
    try:
        pipeline_piutang_rayon = [
            {'$match': {'BULAN_TAGIHAN': latest_mc_month}} if latest_mc_month else {'$match': {'NOMEN': {'$exists': True}}}, 
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
            {'$match': {'TGL_BAYAR': {'$regex': this_month}}},
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

        # FIX: Critical Debt pipeline disederhanakan karena ARDEBT kini historis
        pipeline_critical_debt = [
            {'$group': {
                '_id': '$NOMEN',
                'months': {'$sum': 1}, # Hitung jumlah baris tunggakan
                'amount': {'$sum': '$JUMLAH'}
            }},
            {'$match': {'months': {'$gte': 5}}}, # Nomen dengan 5 periode tunggakan atau lebih
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
