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
    # FIX: Menambahkan timeout pada koneksi MongoDB untuk stabilitas
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000) 
    client.admin.command('ping') 
    db = client[DB_NAME]
    
    # 🚨 KOLEKSI DIPISAH BERDASARKAN SUMBER DATA
    collection_mc = db['MasterCetak']   # MC (Piutang/Tagihan Bulanan - REPLACE)
    collection_mb = db['MasterBayar']   # MB (Koleksi Harian - APPEND)
    collection_cid = db['CustomerData'] # CID (Data Pelanggan Statis - REPLACE)
    collection_sbrs = db['MeterReading'] # SBRS (Baca Meter Harian/Parsial - APPEND)
    collection_ardebt = db['AccountReceivable'] # ARDEBT (Tunggakan Detail - REPLACE)
    
    # ==========================================================
    # === START OPTIMASI: INDEXING KRITIS (SOLUSI KECEPATAN PERMANEN) ===
    # ==========================================================
    
    # CID (CustomerData): Paling KRITIS untuk JOIN
    collection_cid.create_index([('NOMEN', 1)], name='idx_cid_nomen', unique=True)
    collection_cid.create_index([('RAYON', 1), ('TIPEPLGGN', 1)], name='idx_cid_rayon_tipe')

    # MC (MasterCetak): Untuk Report Koleksi dan Grouping
    collection_mc.create_index([('NOMEN', 1)], name='idx_mc_nomen')
    collection_mc.create_index([('RAYON', 1), ('PCEZ', 1)], name='idx_mc_rayon_pcez') 
    collection_mc.create_index([('STATUS', 1)], name='idx_mc_status')
    collection_mc.create_index([('TARIF', 1), ('KUBIK', 1), ('NOMINAL', 1)], name='idx_mc_tarif_volume')

    # MB (MasterBayar): Untuk Detail Transaksi dan Undue Check
    collection_mb.create_index([('TGL_BAYAR', -1)], name='idx_mb_paydate_desc')
    collection_mb.create_index([('NOMEN', 1)], name='idx_mb_nomen')
    collection_mb.create_index([('RAYON', 1), ('PCEZ', 1), ('TGL_BAYAR', -1)], name='idx_mb_rayon_pcez_date')

    # SBRS (MeterReading): Untuk Anomaly Check
    collection_sbrs.create_index([('CMR_ACCOUNT', 1), ('CMR_RD_DATE', -1)], name='idx_sbrs_history')
    
    # ARDEBT (AccountReceivable): Untuk Cek Tunggakan
    collection_ardebt.create_index([('NOMEN', 1)], name='idx_ardebt_nomen')
    
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
        # 1. DATA STATIS (CID) - Master Data Pelanggan
        cleaned_nomen = query_nomen.strip().upper()
        cid_result = collection_cid.find_one({'NOMEN': cleaned_nomen})
        
        if not cid_result:
            return jsonify({
                "status": "not_found",
                "message": f"NOMEN {query_nomen} tidak ditemukan di Master Data Pelanggan (CID)."
            }), 404

        # 2. PIUTANG BERJALAN (MC) - Snapshot Bulan Ini
        mc_results = list(collection_mc.find({'NOMEN': cleaned_nomen}))
        piutang_nominal_total = sum(item.get('NOMINAL', 0) for item in mc_results)
        
        # 3. TUNGGAKAN DETAIL (ARDEBT)
        ardebt_results = list(collection_ardebt.find({'NOMEN': cleaned_nomen}))
        tunggakan_nominal_total = sum(item.get('JUMLAH', 0) for item in ardebt_results)
        
        # 4. RIWAYAT PEMBAYARAN TERAKHIR (MB)
        mb_last_payment_cursor = collection_mb.find({'NOMEN': cleaned_nomen}).sort('TGL_BAYAR', -1).limit(1)
        last_payment = list(mb_last_payment_cursor)[0] if list(mb_last_payment_cursor) else None
        
        # 5. RIWAYAT BACA METER (SBRS)
        sbrs_last_read_cursor = collection_sbrs.find({'CMR_ACCOUNT': cleaned_nomen}).sort('CMR_RD_DATE', -1).limit(2)
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

# --- FUNGSI BARU UNTUK REPORT KOLEKSI & PIUTANG ---
@app.route('/api/collection/report', methods=['GET'])
@login_required 
def collection_report_api():
    """Menghitung Nomen Bayar, Nominal Bayar, Total Nominal, dan Persentase per Rayon/PCEZ, termasuk KUBIKASI."""
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500

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
    
    # 1. MC (PIUTANG) METRICS - Billed
    pipeline_billed = [
        initial_project, 
        { '$group': {
            '_id': { 'rayon': '$RAYON', 'pcez': '$PCEZ' },
            'total_nomen_all': { '$addToSet': '$NOMEN' },
            'total_nominal': { '$sum': '$NOMINAL' },
            'total_kubik': { '$sum': '$KUBIK' } # Sum of Billed Kubik
        }}
    ]
    billed_data = list(collection_mc.aggregate(pipeline_billed))

    # 2. MC (KOLEKSI) METRICS - Collected (flagged in MC)
    pipeline_collected = [
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
    this_month_year_regex = pd.Timestamp.now().strftime('%m%Y') # Assuming format like 122025
    
    pipeline_mb_undue = [
        # Match payments for the current month's bill (approximation)
        { '$match': { 
            'BULAN_REK': { '$regex': this_month_year_regex }
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
        
        this_month_str = datetime.now().strftime('%m%Y')

        for doc in results:
            nominal_val = float(doc.get('NOMINAL', 0)) 
            kubik_val = float(doc.get('KUBIKBAYAR', 0)) # NEW: Include KUBIKBAYAR
            pay_dt = doc.get('TGL_BAYAR', '')
            bulan_rek = doc.get('BULAN_REK', '')
            
            is_undue = bulan_rek == doc.get('BULAN_REK', 'N/A')
            
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
    
    dimension_map = {'TARIF': '$TARIF_CID', 'MERK': '$MERK_CID', 'READ_METHOD': '$READ_METHOD'}
    
    # Base pipeline structure (Projection and CID Join for all necessary fields)
    pipeline = [
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

        response_data = {
            'status': 'success',
            'totals': totals,
            'breakdowns': breakdowns
        }
        
        return jsonify(response_data), 200

    except Exception as e:
        print(f"Error saat menganalisis custom grouping MC: {e}")
        return jsonify({"status": "error", "message": f"Gagal mengambil data grouping MC: {e}"}), 500

# =========================================================================
# === API GROUPING MB KUSTOM (HELPER FUNCTION) - FIXED FOR USER REQUEST ===
# =========================================================================

def _aggregate_mb_report_by_status(collection_mb, collection_cid, rayon_filter=None):
    """
    Mengagregasi data Master Bayar (MB) berdasarkan status: TOTAL, UNDUE, CURRENT, TUNGGAKAN.
    Logika Status:
    1. UNDUE: BULAN_REK sama dengan Bulan & Tahun TGL_BAYAR.
    2. CURRENT: BULAN_REK sama dengan Bulan & Tahun SEBELUM TGL_BAYAR.
    3. TUNGGAKAN: BULAN_REK lebih kecil dari Bulan & Tahun SEBELUM TGL_BAYAR.
    """
    
    rayon_keys = ['34', '35']
    
    # 1. Pipeline Base dan Normalisasi
    pipeline = [
        {'$match': {'NOMINAL': {'$gt': 0}}}, 
        {'$project': {
            'NOMEN': 1,
            'NOMINAL': {'$toDouble': {'$ifNull': ['$NOMINAL', 0]}},
            'TGL_BAYAR': 1, 
            'BULAN_REK': 1,
        }},
        {'$lookup': {'from': 'CustomerData', 'localField': 'NOMEN', 'foreignField': 'NOMEN', 'as': 'customer_info'}},
        {'$unwind': {'path': '$customer_info', 'preserveNullAndEmptyArrays': False}}, 
        {'$addFields': {
            'CLEAN_RAYON': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.RAYON', 'N/A']}}}}},
            'CLEAN_TIPEPLGGN': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.TIPEPLGGN', 'N/A']}}}}},
        }},
        {'$match': {'CLEAN_TIPEPLGGN': 'REG'}}
    ]
    
    # Apply Rayon filter
    if rayon_filter in rayon_keys:
        pipeline.append({'$match': {'CLEAN_RAYON': rayon_filter}})
    elif rayon_filter == 'TOTAL_34_35':
        pipeline.append({'$match': {'CLEAN_RAYON': {'$in': rayon_keys}}})
    else:
        return {} 

    # 2. Classification Stage
    pipeline.append({
        '$addFields': {
            'PAY_MONTH_YEAR': {
                '$concat': [
                    {'$substr': ['$TGL_BAYAR', 5, 2]},  # MM
                    {'$substr': ['$TGL_BAYAR', 0, 4]}   # YYYY
                ]
            },
            'BILL_MONTH_YEAR_NUM': {'$toInt': {'$ifNull': ['$BULAN_REK', '0']}}
        }
    })
    
    # Calculate previous month/year number (for CURRENT check)
    # This requires running some date logic in Python since complex date arithmetic in MongoDB aggregation is tricky.
    
    # For simplification and robustness against different MongoDB versions, we will compute the "Current" month's number logic here:
    # Get current date from one sample record, then calculate the previous month's MMYYYY number string.
    
    # We will use a standard MongoDB approach: compare BILL_MONTH_YEAR_NUM with PAY_MONTH_YEAR_NUM
    
    pipeline.append({
        '$addFields': {
            'PAY_MONTH_YEAR_NUM': {'$toInt': '$PAY_MONTH_YEAR'}
        }
    })
    
    # --- Logic for Current/Tunggakan vs Undue ---
    # Need to calculate MMYYYY-1 (Previous Month Number) based on PAY_MONTH_YEAR_NUM.
    # We can approximate by converting BULAN_REK to integer and comparing.
    
    # Example: Pay Date (112025). Previous Month (102025).
    # UNDUE: BULAN_REK == 112025
    # CURRENT: BULAN_REK == 102025
    # TUNGGAKAN: BULAN_REK < 102025
    
    # NOTE: Since the data type is string/int (MMYYYY), calculating the previous month number (MMYYYY-1) 
    # directly using integer arithmetic is complex (e.g., 012025 - 1 = 122024). 
    # For robust production code, external Python date logic is preferred, but for this exercise, 
    # we'll rely on classification based on current PAY_MONTH_YEAR_NUM vs BILL_MONTH_YEAR_NUM.

    pipeline.append({
        '$addFields': {
            'STATUS_BAYAR_GROUP': {
                '$let': {
                    'vars': {
                        'pay_month_year': '$PAY_MONTH_YEAR',
                        'bill_month_year': '$BULAN_REK'
                    },
                    'in': {
                        '$switch': {
                            'branches': [
                                # 1. UNDUE: BULAN_REK sama dengan Bulan & Tahun TGL_BAYAR
                                { 'case': {'$eq': ['$$bill_month_year', '$$pay_month_year']}, 'then': 'UNDUE' },
                                
                                # 2. CURRENT: BULAN_REK sama dengan Bulan & Tahun SEBELUM TGL_BAYAR.
                                # Calculate previous month/year number based on current PAY_MONTH_YEAR string (MMYYYY)
                                # Example: PAY_MONTH_YEAR = "112025" -> prev_MMYYYY_str = "102025"
                                # This approximation logic is applied in Python using datetime before running the query.
                                # Since we cannot execute Python code here, we must rely on pure Mongo logic.
                                
                                # Because simulating date arithmetic in Mongo is complex (Jan-1 = Dec Prev Year), 
                                # we simplify the classification using a Python pre-calculated value.
                                
                                # We must pass the calculated numeric representation of the Previous Month. 
                                # Let's assume we pass {current_month_num} and {previous_month_num} from Python:
                                
                                # Since we cannot dynamically inject Python variables easily here, we will define 
                                # the CURRENT and ARREARS based on the TGL_BAYAR (date field) and BULAN_REK (string field).
                                
                                # Simplified logic based on provided criteria and available fields:
                                # We treat the month number in the current TGL_BAYAR as the current billing month.
                                
                                # First, extract current and previous month MMYYYY numbers in Python before calling the API.
                                
                                # *** WARNING: The current setup relies on the assumption that TGL_BAYAR is consistent. ***
                                # We'll use the user's provided example logic inside the backend where possible, but API needs dynamic dates.
                                
                                # If BULAN_REK is less than the current month in TGL_BAYAR, it's either CURRENT or TUNGGAKAN.
                                
                                # To fix this without complex Mongo logic: We only differentiate based on 'current' billing month.
                                # If BULAN_REK == PrevMonth: CURRENT. If BULAN_REK < PrevMonth: TUNGGAKAN.
                                
                                # Since we cannot reliably calculate PrevMonth in this static pipeline, 
                                # we simplify: anything that is not UNDUE is ARREARS (Tunggakan + Current).
                                # To meet user's specific request for CURRENT, we MUST rely on the Python side to inject dynamic month comparison logic later, 
                                # but for the core aggregation, we use a single ARREARS bucket for safety, then split later if needed.
                                
                                # Reverting to the old simplified logic due to Mongo aggregation limitations on date arithmetic:
                                
                                # Let's try to define the CURRENT and TUNGGAKAN based on a single integer comparison point.
                                # Get TGL_BAYAR as YYYYMM (number)
                                'PAY_YEAR_MONTH_NUM': {'$toInt': {'$concat': [{'$substr': ['$TGL_BAYAR', 0, 4]}, {'$substr': ['$TGL_BAYAR', 5, 2}]}]},
                                'BILL_YEAR_MONTH_NUM': {'$toInt': {'$concat': [{'$substr': ['$BULAN_REK', 2, 4]}, {'$substr': ['$BULAN_REK', 0, 2}]}]} # Assuming BULAN_REK is MMYYYY
                            }
                        }
                    }
                }
            }
        }
    })

    # Since the full logic (UNDUE, CURRENT, TUNGGAKAN) is too complex for static Mongo pipeline 
    # without passing dynamic Python variables, we need to pass those variables via API calls, 
    # or rely on Python to calculate the exact month numbers. 
    
    # We will adjust the API endpoint to pass the needed month strings/numbers.
    # For now, let's redefine the classification in a new, separate function in Python/Flask 
    # which calculates the needed month numbers dynamically.
    
    return jsonify({"status": "error", "message": "Logic moved to dynamic Python calculation in the API endpoint."})

@app.route('/api/analyze/mb_grouping', methods=['GET'])
@login_required 
def analyze_mb_grouping_api():
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500

    # Dynamic date calculation based on current date (or a simulated query date)
    now = datetime.now()
    
    # Calculate CURRENT BILLING MONTH (e.g., if today is 2025-11-25, current month is 11, bill month for 'current' is 10)
    current_bill_month_dt = now.replace(day=1) - timedelta(days=1)
    
    # MMYYYY format for Mongo string comparison
    current_pay_month_str = now.strftime('%m%Y')      # e.g., 112025
    current_bill_month_str = current_bill_month_dt.strftime('%m%Y') # e.g., 102025 (Previous month)

    # Function to run the aggregation with dynamic matching, similar to _aggregate_custom_mc_report
    def aggregate_mb_by_status(status_type, rayon_filter, current_pay_month, current_bill_month):
        
        # Base Pipeline (as defined in _aggregate_mb_grouping base)
        pipeline = [
            {'$match': {'NOMINAL': {'$gt': 0}}}, 
            {'$project': {
                'NOMEN': 1,
                'NOMINAL': {'$toDouble': {'$ifNull': ['$NOMINAL', 0]}},
                'TGL_BAYAR': 1, 
                'BULAN_REK': 1,
            }},
            {'$lookup': {'from': 'CustomerData', 'localField': 'NOMEN', 'foreignField': 'NOMEN', 'as': 'customer_info'}},
            {'$unwind': {'path': '$customer_info', 'preserveNullAndEmptyArrays': False}}, 
            {'$addFields': {
                'CLEAN_RAYON': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.RAYON', 'N/A']}}}}},
                'CLEAN_TIPEPLGGN': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.TIPEPLGGN', 'N/A']}}}}},
                'PAY_MONTH_YEAR': {
                    '$concat': [
                        {'$substr': ['$TGL_BAYAR', 5, 2]},  # MM
                        {'$substr': ['$TGL_BAYAR', 0, 4]}   # YYYY
                    ]
                }
            }},
            {'$match': {'CLEAN_TIPEPLGGN': 'REG'}}
        ]
        
        # Apply Rayon filter
        rayon_keys = ['34', '35']
        if rayon_filter in rayon_keys:
            pipeline.append({'$match': {'CLEAN_RAYON': rayon_filter}})
        elif rayon_filter == 'TOTAL_34_35':
            pipeline.append({'$match': {'CLEAN_RAYON': {'$in': rayon_keys}}})
        else:
            return {'CountOfNOMEN': 0, 'SumOfNOMINAL': 0, 'details': []}

        # Apply Status Filter
        status_match = {}
        
        if status_type == 'TOTAL':
            # No specific match needed other than base pipeline filters
            pass
        elif status_type == 'UNDUE':
            # UNDUE: BULAN_REK == PAY_MONTH_YEAR (e.g., Bayar Nov untuk Rek Nov)
            status_match = {
                'BULAN_REK': current_pay_month
            }
        elif status_type == 'CURRENT':
            # CURRENT: BULAN_REK == Previous PAY_MONTH_YEAR (e.g., Bayar Nov untuk Rek Okt)
            status_match = {
                'BULAN_REK': current_bill_month
            }
        elif status_type == 'TUNGGAKAN':
            # TUNGGAKAN: BULAN_REK < Previous PAY_MONTH_YEAR (e.g., Bayar Nov untuk Rek Sept atau sebelumnya)
            # This requires converting MMYYYY to a sortable integer/string
            
            # Since the structure is MMYYYY, 112025 > 102025 works correctly for the same year.
            # To be robust, we need YYYYMM format for comparison.
            
            # Calculate the integer representation of the Previous Month Bill (e.g., 202510)
            current_bill_year_month_num = int(current_bill_month[2:6] + current_bill_month[0:2])

            pipeline.append({
                '$addFields': {
                    'BILL_YEAR_MONTH_NUM': {'$toInt': {
                        '$concat': [
                            {'$substr': ['$BULAN_REK', 2, 4]}, # YYYY
                            {'$substr': ['$BULAN_REK', 0, 2]}  # MM
                        ]
                    }}
                }
            })
            
            # Match where the bill date is numerically less than the 'Current' bill month date (Tunggakan)
            status_match = {
                'BILL_YEAR_MONTH_NUM': {'$lt': current_bill_year_month_num}
            }
        
        if status_match:
             pipeline.append({'$match': status_match})
             
        # Aggregation stage
        pipeline_agg = pipeline + [
            {'$group': {
                '_id': None,
                'CountOfNOMEN': {'$addToSet': '$NOMEN'},
                'SumOfNOMINAL': {'$sum': '$NOMINAL'},
            }},
            {'$project': {
                '_id': 0,
                'CountOfNOMEN': {'$size': '$CountOfNOMEN'},
                'SumOfNOMINAL': {'$round': ['$SumOfNOMINAL', 0]},
            }}
        ]
        
        # Detail Listing (Max 500 rows)
        pipeline_details = pipeline + [
            {'$project': {
                '_id': 0,
                'TGL_BAYAR': 1,
                'NOMEN': 1,
                'NOMINAL': {'$round': ['$NOMINAL', 0]},
                'STATUS_BAYAR': {'$literal': status_type} 
            }},
            {'$sort': {'TGL_BAYAR': -1}}, 
            {'$limit': 500}
        ]
        
        metrics = list(collection_mb.aggregate(pipeline_agg, allowDiskUse=True))
        details = list(collection_mb.aggregate(pipeline_details, allowDiskUse=True))
        
        metrics_result = metrics[0] if metrics else {'CountOfNOMEN': 0, 'SumOfNOMINAL': 0}
        
        return {
            'CountOfNOMEN': metrics_result['CountOfNOMEN'],
            'SumOfNOMINAL': metrics_result['SumOfNOMINAL'],
            'details': details
        }
    
    # Run all categories and rayons
    report = {}
    rayon_filters = ['34', '35', 'TOTAL_34_35']
    status_types = ['TOTAL', 'UNDUE', 'CURRENT', 'TUNGGAKAN']

    for status in status_types:
        report[status] = {}
        for rayon in rayon_filters:
            report[status][rayon] = aggregate_mb_by_status(
                status, rayon, current_pay_month_str, current_bill_month_str
            )

    # Special handling for Detail Listing (R34 and R35 only for the specific request format)
    # The new aggregate function already provides details, we just need to reformat the output structure.
    
    r34_details = report['TOTAL']['34']['details']
    r35_details = report['TOTAL']['35']['details']


    response_data = {
        'status': 'success',
        'report': report,
        'r34_details': r34_details,
        'r35_details': r35_details,
    }
    
    return jsonify(response_data), 200

# =========================================================================
# === END API GROUPING MB KUSTOM ===
# =========================================================================


# --- FUNGSI BARU: LAPORAN KOLEKSI HARIAN ---
@app.route('/api/collection/daily_summary', methods=['GET'])
@login_required 
def daily_collection_summary_api():
    """Menghitung total nomen dan nominal koleksi per tanggal."""
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500
        
    try:
        pipeline = [
            {'$match': {
                # Filter hanya data yang memiliki TGL_BAYAR dalam format YYYY-MM-DD
                'TGL_BAYAR': {'$type': 'string', '$regex': '^\d{4}-\d{2}-\d{2}'}, 
                'NOMINAL': {'$gt': 0}
            }},
            {'$project': {
                'TGL_BAYAR': 1,
                'NOMINAL': {'$toDouble': {'$ifNull': ['$NOMINAL', 0]}},
                'NOMEN': 1
            }},
            {'$group': {
                '_id': '$TGL_BAYAR',
                'CountOfNomen': {'$addToSet': '$NOMEN'},
                'SumOfNOMINAL': {'$sum': '$NOMINAL'}
            }},
            {'$project': {
                '_id': 0,
                'Tanggal': '$_id',
                'NomenCount': {'$size': '$CountOfNomen'},
                'NominalSum': {'$round': ['$SumOfNOMINAL', 0]}
            }},
            {'$sort': {'Tanggal': 1}}
        ]
        
        results = list(collection_mb.aggregate(pipeline))
        
        # Format tanggal dari YYYY-MM-DD ke DD/MM/YYYY
        for item in results:
            try:
                date_obj = datetime.strptime(item['Tanggal'], '%Y-%m-%d')
                item['Tanggal'] = date_obj.strftime('%d/%m/%Y')
            except ValueError:
                # Abaikan jika format tanggal salah
                pass
                
        return jsonify(results), 200
        
    except Exception as e:
        print(f"Error fetching daily collection summary: {e}")
        return jsonify({"message": f"Gagal mengambil ringkasan koleksi harian: {e}"}), 500

# =========================================================================
# === END FUNGSI BARU: LAPORAN KOLEKSI HARIAN ---
# =========================================================================


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
