import os
import pandas as pd
from flask import Flask, request, jsonify, render_template, redirect, url_for, flash, make_response
from pymongo import MongoClient, ReplaceOne # FIX: Import ReplaceOne untuk operasi UPSERT
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
    collection_mc = db['MasterCetak']   # MC (Piutang/Tagihan Bulanan - UPSERT Historis)
    collection_mb = db['MasterBayar']   # MB (Koleksi Harian - APPEND/UPSERT Transaksi)
    collection_cid = db['CustomerData'] # CID (Data Pelanggan Statis - REPLACE)
    collection_sbrs = db['MeterReading'] # SBRS (Baca Meter Harian/Parsial - APPEND/UPSERT Pembacaan)
    collection_ardebt = db['AccountReceivable'] # ARDEBT (Tunggakan Detail - UPSERT Historis)
    
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
    # INDEKS KRITIS BARU UNTUK UPSERT MC
    collection_mc.create_index([('NOMEN', 1), ('MASA', 1), ('TAHUN2', 1)], name='idx_mc_unique_period', unique=True)

    # MB (MasterBayar): Untuk Detail Transaksi dan Undue Check
    collection_mb.create_index([('TGL_BAYAR', -1)], name='idx_mb_paydate_desc')
    collection_mb.create_index([('NOMEN', 1)], name='idx_mb_nomen')
    collection_mb.create_index([('RAYON', 1), ('PCEZ', 1), ('TGL_BAYAR', -1)], name='idx_mb_rayon_pcez_date')

    # SBRS (MeterReading): Untuk Anomaly Check
    collection_sbrs.create_index([('CMR_ACCOUNT', 1), ('CMR_RD_DATE', -1)], name='idx_sbrs_history')
    
    # ARDEBT (AccountReceivable): Untuk Cek Tunggakan
    collection_ardebt.create_index([('NOMEN', 1)], name='idx_ardebt_nomen')
    # INDEKS KRITIS BARU UNTUK UPSERT ARDEBT
    collection_ardebt.create_index([('NOMEN', 1), ('CountOfPERIODE_BILL', 1)], name='idx_ardebt_unique_period', unique=True)
    
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

        # 2. PIUTANG BERJALAN (MC) - Snapshot Bulan Ini (Semua riwayat)
        # MC Nomen adalah Induk (master)
        mc_results = list(collection_mc.find({'NOMEN': cleaned_nomen}))
        piutang_nominal_total = sum(item.get('NOMINAL', 0) for item in mc_results)
        
        # 3. TUNGGAKAN DETAIL (ARDEBT)
        ardebt_results = list(collection_ardebt.find({'NOMEN': cleaned_nomen}))
        tunggakan_nominal_total = sum(item.get('JUMLAH', 0) for item in ardebt_results)
        
        # 4. RIWAYAT PEMBAYARAN TERAKHIR (MB)
        # FIX KRITIS: Menggunakan find_one dengan sort untuk menghindari list index out of range 
        last_payment = collection_mb.find_one(
            {'NOMEN': cleaned_nomen},
            sort=[('TGL_BAYAR', -1)]
        )
        
        # 5. RIWAYAT BACA METER (SBRS)
        sbrs_history = list(collection_sbrs.find({'CMR_ACCOUNT': cleaned_nomen}).sort('CMR_RD_DATE', -1).limit(2))
        
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
        # Kembalikan JSON error yang valid agar frontend bisa menampilkannya dengan benar
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
            'MB_UNDUE_NOMINAL', 'MB_UndueKubik',
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
        response.headers['Content-Disposition'] = 'attachment; filename=Laporan_Dashboard_Analytics.xlsx'
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

@app.route('/analyze/full_mb_report', methods=['GET'])
@login_required
def analyze_full_mb_report():
    """Rute untuk menampilkan halaman laporan Detail Master Bayar (MB) / Koleksi Lengkap. (FIX)"""
    return render_template('analyze_report_template.html', 
                           title="Laporan Detail Master Bayar (MB) Lengkap", 
                           description="Menampilkan semua data transaksi koleksi (MB) yang tersimpan di database.",
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

@app.route('/analyze/tarif_breakdown', methods=['GET'])
@login_required
def analyze_tarif_breakdown_page():
    """Rute untuk menampilkan halaman laporan Tarif Breakdown (menggunakan template yang ada). (FIX)"""
    return render_template('analyze_report_template.html', 
                           title="Distribusi Pelanggan Tarif Kustom", 
                           description="Menampilkan distribusi pelanggan (REG) berdasarkan tarif di Rayon 34/35.",
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
# === API GROUPING MB (Koleksi) LENGKAP ===
# =========================================================================

@app.route('/api/analyze/mb_grouping', methods=['GET'])
@login_required 
def analyze_mb_grouping_api():
    """Placeholder API untuk Laporan Detail Master Bayar (MB) Lengkap. (FIX)"""
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500
    
    # Placeholder: Mengambil 100 data transaksi MB terbaru
    try:
        results = list(collection_mb.find().sort('TGL_BAYAR', -1).limit(100))
        # Hapus _id untuk rendering
        for doc in results:
            doc.pop('_id', None)
        
        if not results:
             return jsonify([]), 200
             
        # FIX KRITIS: Pastikan nilai NaN (float) dikonversi menjadi null (None) untuk JSON valid
        df = pd.DataFrame(results)
        # Mengganti semua NaN (termasuk NaN yang muncul dari MongoDB atau konversi Pandas) dengan None
        # None dikonversi Flask ke 'null' yang valid JSON
        df_cleaned = df.where(pd.notnull(df), None) 
        
        return jsonify(df_cleaned.to_dict('records')), 200
        
    except Exception as e:
        print(f"Error fetching generic MB data: {e}")
        return jsonify({"message": f"Gagal mengambil data MB: {e}"}), 500


# =========================================================================
# === API UNTUK ANALISIS AKURAT (Fluktuasi Volume Naik/Turun) ===
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
        
# 2. API SUMMARY (Untuk KPI Cards di collection_unified.html)
@app.route('/api/analyze/mc_grouping/summary', methods=['GET'])
@login_required 
def analyze_mc_grouping_summary_api():
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500

    try:
        pipeline_summary = [
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
        pipeline_tarif_breakdown = [
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
# === END API GROUPING MC KUSTOM ===
# =========================================================================

# =========================================================================
# === NEW ANALYTIC ENDPOINTS (VIP, RISK, DQC) ===
# =========================================================================

@app.route('/analyze/vip_payers')
@login_required
def analyze_vip_payers_page():
    return render_template('analyze_report_template.html', 
                           title="Pelanggan VIP (Premium & Zero Debt)", 
                           description="Menampilkan pelanggan dengan kriteria Premium (Tarif/Meter Besar) yang TIDAK PERNAH memiliki catatan tunggakan di ARDEBT.",
                           is_admin=current_user.is_admin)

@app.route('/api/analyze/vip_payers', methods=['GET'])
@login_required 
def analyze_vip_payers_api():
    """Mencari pelanggan Premium yang TIDAK PERNAH menunggak (zero debt)."""
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500
        
    try:
        # 1. Dapatkan daftar NOMEN yang sedang menunggak (ARDEBT)
        debtors = collection_ardebt.distinct('NOMEN')
        
        # 2. Pipeline untuk mencari pelanggan Premium (CID) yang TIDAK menunggak (NOT IN ARDEBT)
        pipeline = [
            {'$match': {
                'NOMEN': {'$nin': debtors}, # Pelanggan TIDAK ADA di daftar penunggak
                '$or': [
                    {'TARIF': {'$in': ['4A', '3Q']}}, # Kriteria Tarif Premium (Pastikan nama kolom sudah TARIF)
                    {'UKURAN_METER': {'$gte': '0.70'}}, # Kriteria Ukuran Meter Besar (Asumsi tipe string)
                ],
                'TIPEPLGGN': 'REG' # Hanya pelanggan Reguler (asumsi)
            }},
            {'$project': {
                '_id': 0,
                'NOMEN': 1,
                'NAMA': 1,
                'RAYON': 1,
                'TARIF': 1,
                'UKURAN_METER': 1
            }},
            {'$limit': 100}
        ]
        
        vip_list = list(collection_cid.aggregate(pipeline))
        return jsonify(vip_list), 200

    except Exception as e:
        print(f"Error saat menganalisis VIP Payers: {e}")
        return jsonify({"message": f"Gagal mengambil data VIP Payers: {e}"}), 500

@app.route('/analyze/high_risk')
@login_required
def analyze_high_risk_page():
    return render_template('analyze_report_template.html', 
                           title="Pelanggan Berisiko Tinggi (Critical Risk)", 
                           description="Menampilkan pelanggan dengan Tunggakan Kritis (>= 5 bulan) atau Anomali Pemakaian Ekstrem (Zero/Sangat Tinggi).",
                           is_admin=current_user.is_admin)

@app.route('/api/analyze/high_risk', methods=['GET'])
@login_required 
def analyze_high_risk_api():
    """Mencari pelanggan dengan tunggakan kritis (>= 5 bulan) ATAU anomali ekstrem."""
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500
        
    try:
        # 1. Ambil data Pelanggan dengan Tunggakan Kritis (>= 5 bulan)
        pipeline_debt_risk = [
            {'$match': {'COUNTOFPERIODE_BILL': {'$gte': 5}}},
            {'$group': {
                '_id': '$NOMEN',
                'months': {'$first': '$COUNTOFPERIODE_BILL'},
                'amount': {'$first': '$JUMLAH'} # Menggunakan JUMLAH dari ARDEBT
            }},
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
                'STATUS_RISIKO': {'$concat': ["TUNGGAKAN KRITIS ", {'$toString': '$months'}, " BULAN"]},
                'JUMLAH_TUNGGAKAN': '$amount',
                'RAYON': {'$ifNull': ['$customer_info.RAYON', 'N/A']}
            }}
        ]
        debt_risk = list(collection_ardebt.aggregate(pipeline_debt_risk))
        
        # 2. Ambil data Anomali Ekstrem (dari SBRS)
        sbrs_anomalies = _get_sbrs_anomalies(collection_sbrs, collection_cid)
        extreme_risk = [
            {'NOMEN': a['NOMEN'], 'NAMA': a['NAMA'], 'STATUS_RISIKO': a['STATUS_PEMAKAIAN'], 
             'JUMLAH_TUNGGAKAN': 0, 'RAYON': a['RAYON']} # Default JUMLAH_TUNGGAKAN 0 atau N/A
            for a in sbrs_anomalies if 'EKSTRIM' in a['STATUS_PEMAKAIAN'] or 'ZERO' in a['STATUS_PEMAKAIAN']
        ]

        # 3. Gabungkan dan hapus duplikasi (NOMEN adalah kuncinya)
        combined_risk = {item['NOMEN']: item for item in (debt_risk + extreme_risk)}
        
        return jsonify(list(combined_risk.values())), 200

    except Exception as e:
        print(f"Error saat menganalisis High Risk: {e}")
        return jsonify({"message": f"Gagal mengambil data High Risk: {e}"}), 500

@app.route('/dqc/mc_missing_cid')
@login_required
@admin_required
def dqc_mc_missing_cid_page():
    return render_template('analyze_report_template.html', 
                           title="DQC: NOMEN di MC tanpa CID Master", 
                           description="Mencari pelanggan yang ada di Master Cetak (aktif) tetapi tidak memiliki data profil di Customer Data (CID). Laporan ini penting untuk memastikan validitas kunci utama (NOMEN).",
                           is_admin=current_user.is_admin)


@app.route('/api/dqc/mc_missing_cid', methods=['GET'])
@login_required 
@admin_required
def dqc_mc_missing_cid_api():
    """Cek NOMEN di MC yang tidak ada di CID (Master Data Hilang)."""
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500
        
    try:
        pipeline = [
            {'$lookup': {
                'from': 'CustomerData', 
                'localField': 'NOMEN',
                'foreignField': 'NOMEN',
                'as': 'cid_match'
            }},
            {'$match': {'cid_match': {'$eq': []}}}, # Cari yang tidak ada pasangannya di CID
            {'$group': {'_id': '$NOMEN', 'count': {'$sum': 1}}},
            {'$lookup': {
               'from': 'MasterCetak', 
               'localField': '_id',
               'foreignField': 'NOMEN',
               'as': 'mc_info'
            }},
            {'$project': {
                '_id': 0,
                'NOMEN': '$_id',
                'COUNT_MC_RECORDS': '$count',
                'RAYON_MC': {'$arrayElemAt': ['$mc_info.RAYON', 0]},
                'PCEZ_MC': {'$arrayElemAt': ['$mc_info.PCEZ', 0]},
                'STATUS_TAGIHAN_MC': {'$arrayElemAt': ['$mc_info.STATUS', 0]}
            }},
            {'$limit': 1000}
        ]
        
        inconsistencies = list(collection_mc.aggregate(pipeline, allowDiskUse=True))
        
        return jsonify(inconsistencies), 200

    except Exception as e:
        print(f"Error saat menjalankan DQC: {e}")
        return jsonify({"message": f"Gagal menjalankan DQC: {e}"}), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
