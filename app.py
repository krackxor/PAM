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
import time
from pymongo.errors import BulkWriteError, DuplicateKeyError, OperationFailure 
import traceback

load_dotenv() 

# --- PEMROSESAN DAFTAR PENGGUNA DARI .ENV (DIPINDAHKAN KE ATAS UNTUK MENGHINDARI NameError) ---
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
# --- END PEMROSESAN DAFTAR PENGGUNA ---

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
    try:
        collection_cid.create_index([('NOMEN', 1), ('TANGGAL_UPLOAD_CID', -1)], name='idx_cid_nomen_hist')
        collection_cid.create_index([('RAYON', 1), ('TIPEPLGGN', 1)], name='idx_cid_rayon_tipe')
    except: pass

    # MC (MasterCetak)
    try:
        collection_mc.create_index([('NOMEN', 1), ('BULAN_TAGIHAN', -1)], name='idx_mc_nomen_hist')
        collection_mc.create_index([('RAYON', 1), ('PCEZ', 1)], name='idx_mc_rayon_pcez') 
        collection_mc.create_index([('STATUS', 1)], name='idx_mc_status')
        collection_mc.create_index([('TARIF', 1), ('KUBIK', 1), ('NOMINAL', 1)], name='idx_mc_tarif_volume')
        collection_mc.create_index([('BULAN_TAGIHAN', -1)], name='idx_mc_bulan_tagihan_desc')
    except: pass

    # MB (MasterBayar)
    try:
        collection_mb.create_index([('NOTAGIHAN', 1), ('TGL_BAYAR', 1), ('NOMINAL', 1)], name='idx_mb_unique_transaction', unique=False)
        collection_mb.create_index([('TGL_BAYAR', -1)], name='idx_mb_paydate_desc')
        collection_mb.create_index([('NOMEN', 1)], name='idx_mb_nomen')
        collection_mb.create_index([('RAYON', 1), ('PCEZ', 1), ('TGL_BAYAR', -1)], name='idx_mb_rayon_pcez_date')
    except: pass

    # SBRS (MeterReading)
    try:
        collection_sbrs.create_index([('CMR_ACCOUNT', 1), ('CMR_RD_DATE', -1)], name='idx_sbrs_history')
    except: pass
    
    # ARDEBT (AccountReceivable)
    try:
        collection_ardebt.create_index([('NOMEN', 1), ('PERIODE_BILL', -1), ('JUMLAH', 1)], name='idx_ardebt_nomen_hist')
    except: pass
    
    # ==========================================================
    # === END OPTIMASI: INDEXING KRITIS ===
    # ==========================================================
    
    collection_data = collection_mc

    print("Koneksi MongoDB berhasil dan index dikonfigurasi!")
except Exception as e:
    print(f"Gagal terhubung ke MongoDB atau mengkonfigurasi index: {e}")
    client = None

# --- FUNGSI UTILITY INTERNAL: ZONA PARSER ---
def _parse_zona_novak(zona_str):
    """
    Mengekstrak Rayon, PC, EZ, PCEZ, dan Block dari ZONA_NOVAK string.
    """
    zona = str(zona_str).strip().upper()
    
    if not zona or zona == 'N/A' or len(zona) < 7:
        return {'RAYON_ZONA': 'N/A', 'PC_ZONA': 'N/A', 'EZ_ZONA': 'N/A', 'PCEZ_ZONA': 'N/A', 'BLOCK_ZONA': 'N/A'}
    
    try:
        rayon = zona[0:2]
        pc = zona[2:5]
        ez = zona[5:7]
        
        if len(zona) >= 9:
            block = zona[7:9]
        else:
            block = zona[7:] if len(zona) > 7 else 'N/A'
        
        pcez = pc + ez
        
        return {
            'RAYON_ZONA': rayon,
            'PC_ZONA': pc,
            'EZ_ZONA': ez,
            'PCEZ_ZONA': pcez,
            'BLOCK_ZONA': block
        }
    except Exception as e:
        print(f"Error parsing ZONA_NOVAK '{zona_str}': {e}")
        return {'RAYON_ZONA': 'N/A', 'PC_ZONA': 'N/A', 'EZ_ZONA': 'N/A', 'PCEZ_ZONA': 'N/A', 'BLOCK_ZONA': 'N/A'}

# --- FUNGSI UTILITY INTERNAL: ANALISIS SBRS ---
def _get_sbrs_anomalies(collection_sbrs, collection_cid):
    """
    Menjalankan pipeline agregasi untuk menemukan anomali pemakaian.
    PERBAIKAN: Menggunakan pipeline yang sangat konservatif (minimal agregasi string).
    """
    if collection_sbrs is None or collection_cid is None:
        return []
        
    try:
        pipeline_sbrs_history = [
            {'$sort': {'CMR_ACCOUNT': 1, 'CMR_RD_DATE': -1}},
            {'$group': {
                '_id': '$CMR_ACCOUNT',
                'history': {
                    '$push': {
                        'kubik': {'$ifNull': ['$CMR_KUBIK', 0]}, 
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
                'latest': {'$ne': None}
            }},
            {'$addFields': {
                'KUBIK_TERBARU': {'$toDouble': {'$ifNull': ['$latest.kubik', 0]}},
                'KUBIK_SEBELUMNYA': {'$toDouble': {'$ifNull': ['$previous.kubik', 0]}}
            }},
            {'$match': { # Filter setelah konversi
                'KUBIK_TERBARU': {'$ne': None},
                'KUBIK_SEBELUMNYA': {'$ne': None}
            }},
            {'$addFields': {
                'SELISIH_KUBIK': {'$subtract': ['$KUBIK_TERBARU', '$KUBIK_SEBELUMNYA']}
            }},
            {'$addFields': {
                'PERSEN_SELISIH': {
                    '$cond': {
                        'if': {'$gt': ['$KUBIK_SEBELUMNYA', 0]},
                        'then': {'$multiply': [{'$divide': ['$SELISIH_KUBIK', '$KUBIK_SEBELUMNYA']}, 100]},
                        'else': 0 
                    }
                }
            }},
            {'$project': {
                'NOMEN': 1,
                'KUBIK_TERBARU': {'$round': ['$KUBIK_TERBARU', 0]},
                'KUBIK_SEBELUMNYA': {'$round': ['$KUBIK_SEBELUMNYA', 0]},
                'SELISIH_KUBIK': {'$round': ['$SELISIH_KUBIK', 0]},
                'PERSEN_SELISIH': {'$round': ['$PERSEN_SELISIH', 2]},
                'STATUS_RAW': {
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
                'KUBIK_TERBARU': 1,
                'KUBIK_SEBELUMNYA': 1,
                'SELISIH_KUBIK': 1,
                'PERSEN_SELISIH': 1,
                'STATUS_PEMAKAIAN': '$STATUS_RAW'
            }},
            {'$match': { 
               'STATUS_PEMAKAIAN': {'$ne': 'STABIL / NORMAL'}
            }},
            {'$limit': 100}
        ]

        anomalies = list(collection_sbrs.aggregate(pipeline_sbrs_history, allowDiskUse=True))
        
        for doc in anomalies:
            doc.pop('_id', None)
            
        return anomalies
    except Exception as e:
        print(f"Error dalam _get_sbrs_anomalies: {e}")
        import traceback
        traceback.print_exc()
        return []

# --- FUNGSI UTILITY: DATE HELPERS ---
def _get_previous_month_year(bulan_tagihan):
    if not bulan_tagihan or len(bulan_tagihan) != 6: return None
    try:
        month = int(bulan_tagihan[:2])
        year = int(bulan_tagihan[2:])
        target_date = datetime(year, month, 1) - timedelta(days=1)
        return f"{target_date.month:02d}{target_date.year}"
    except ValueError: return None
        
def _get_month_date_range(bulan_tagihan):
    if not bulan_tagihan or len(bulan_tagihan) != 6: return None, None
    try:
        month = int(bulan_tagihan[:2])
        year = int(bulan_tagihan[2:])
        start_date = datetime(year, month, 1)
        next_month = (month % 12) + 1
        next_year = year + (1 if month == 12 else 0)
        end_date = datetime(next_year, next_month, 1)
        return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
    except ValueError: return None, None

def _mm_yyyy_to_datetime(mm_yyyy_str):
    try: return datetime.strptime(mm_yyyy_str, '%m%Y')
    except ValueError: return None

# --- KONFIGURASI FLASK-LOGIN ---
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login' 
login_manager.login_message_category = 'info'

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

@app.route('/')
@login_required 
def index():
    return render_template('index.html', is_admin=current_user.is_admin)

@app.route('/api/search', methods=['GET'])
@login_required 
def search_nomen():
    """
    PERBAIKAN: Safe list access dan error handling yang lebih baik.
    """
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500
        
    query_nomen = request.args.get('nomen', '').strip()

    if not query_nomen:
        return jsonify({"status": "fail", "message": "Masukkan NOMEN untuk memulai pencarian terintegrasi."}), 400

    try:
        cleaned_nomen = query_nomen.strip().upper()
        
        # 1. DATA STATIS (CID) - Safe access
        cid_result_cursor = collection_cid.find({'NOMEN': cleaned_nomen}).sort('TANGGAL_UPLOAD_CID', -1).limit(1)
        cid_results = list(cid_result_cursor)
        cid_result = cid_results[0] if cid_results else None
        
        if not cid_result:
            return jsonify({
                "status": "not_found",
                "message": f"NOMEN {query_nomen} tidak ditemukan di Master Data Pelanggan (CID)."
            }), 404

        # 2-5. Fetch data
        mc_results = list(collection_mc.find({'NOMEN': cleaned_nomen}).sort('BULAN_TAGIHAN', -1))
        piutang_nominal_total = sum(item.get('NOMINAL', 0) for item in mc_results)
        ardebt_results = list(collection_ardebt.find({'NOMEN': cleaned_nomen}).sort('PERIODE_BILL', -1))
        tunggakan_nominal_total = sum(item.get('JUMLAH', 0) for item in ardebt_results)
        mb_last_payment_cursor = collection_mb.find({'NOMEN': cleaned_nomen}).sort('TGL_BAYAR', -1).limit(1)
        mb_last_payments = list(mb_last_payment_cursor)
        last_payment = mb_last_payments[0] if mb_last_payments else None
        sbrs_last_read_cursor = collection_sbrs.find({'CMR_ACCOUNT': cleaned_nomen}).sort('CMR_RD_DATE', -1).limit(2)
        sbrs_history = list(sbrs_last_read_cursor)
        
        # --- LOGIKA KECERDASAN (INTEGRASI & DIAGNOSTIK) ---
        mc_latest = mc_results[0] if mc_results else None
        
        # Ekstraksi ZONA (di Python)
        zona_info = _parse_zona_novak(mc_latest.get('ZONA_NOVAK', 'N/A')) if mc_latest else _parse_zona_novak('N/A')

        # Status Finansial
        if tunggakan_nominal_total > 0:
            aktif_ardebt = [d for d in ardebt_results if d.get('STATUS', 'N/A') != 'LUNAS']
            status_financial = f"TUNGGAKAN AKTIF ({len(aktif_ardebt)} Periode)"
        elif mc_latest and mc_latest.get('STATUS') != 'PAYMENT' and mc_latest.get('NOMINAL', 0) > 0:
            status_financial = f"PIUTANG BULAN BERJALAN"
        else:
            status_financial = "LUNAS / TIDAK ADA TAGIHAN"
            
        last_payment_date = last_payment.get('TGL_BAYAR', 'N/A') if last_payment else 'BELUM ADA PEMBAYARAN MB'

        # Status Pemakaian
        status_pemakaian = "DATA SBRS KURANG"
        sbrs_latest = sbrs_history[0] if sbrs_history else {}
        if len(sbrs_history) >= 1:
            kubik_terakhir = sbrs_latest.get('CMR_KUBIK', 0)
            if kubik_terakhir > 100: status_pemakaian = f"EKSTRIM ({kubik_terakhir} m³)"
            elif kubik_terakhir <= 5 and kubik_terakhir > 0: status_pemakaian = f"TURUN DRASTIS / RENDAH ({kubik_terakhir} m³)"
            elif kubik_terakhir == 0: status_pemakaian = "ZERO (0 m³) / NON-AKTIF"
            else: status_pemakaian = f"NORMAL ({kubik_terakhir} m³)"

        # MENGUMPULKAN PROFILE PELANGGAN TERPADU
        cid_master = cid_result
        mc_latest_data = mc_latest if mc_latest else {}
        sbrs_latest_data = sbrs_history[0] if sbrs_history else {} # Ambil data SBRS terbaru untuk Pencatat
        
        profile_pelanggan = {
            "NOMEN": cleaned_nomen,
            "NAMA_PEL": mc_latest_data.get('NAMA_PEL', cid_master.get('NAMA', 'N/A')),
            "ALAMAT": cid_master.get('ALAMAT', 'N/A'),
            "ALM3_PEL": mc_latest_data.get('ALM3_PEL', 'N/A'),
            "STATUS_PELANGGAN": cid_master.get('STATUS_PELANGGAN', 'N/A'),
            "TIPEPLGGN": cid_master.get('TIPEPLGGN', 'N/A'),
            "TARIF": mc_latest_data.get('TARIF', cid_master.get('TARIF', 'N/A')),
            "RAYON": zona_info.get('RAYON_ZONA', cid_master.get('RAYON', 'N/A')),
            "PC": zona_info.get('PC_ZONA', 'N/A'),
            "PCEZ": zona_info.get('PCEZ_ZONA', cid_master.get('PCEZ', 'N/A')),
            "MERK": cid_master.get('MERK', 'N/A'),
            "SERIAL": cid_master.get('SERIAL', 'N/A'),
            "STAN_AWAL": mc_latest_data.get('STAN_AWAL', 'N/A'),
            "STAN_AKIR": mc_latest_data.get('STAN_AKIR', 'N/A'),
            "STAND": cid_master.get('STAND', 'N/A'),
            "KUBIK": mc_latest_data.get('KUBIK', 'N/A'),
            "NOMINAL_MC": mc_latest_data.get('NOMINAL', 0),
            "READ_METHOD": cid_master.get('READ_METHOD', 'N/A'),
            "HARI": cid_master.get('HARI', 'N/A'),
            "PENCATET": sbrs_latest_data.get('CMR_READER', 'N/A'),
            "TGL_BAYAR_TERAKHIR": last_payment_date,
        }

        health_summary = {
            "NOMEN": query_nomen,
            "NAMA": profile_pelanggan['NAMA_PEL'],
            "RAYON": profile_pelanggan['RAYON'],
            "STATUS_FINANSIAL": status_financial,
            "TOTAL_KEWAJIBAN_NOMINAL": piutang_nominal_total + tunggakan_nominal_total,
            "PEMBAYARAN_TERAKHIR": last_payment_date,
            "STATUS_PEMAKAIAN": status_pemakaian
        }
        
        def clean_mongo_id(doc):
            doc.pop('_id', None)
            return doc
        
        cid_data_clean = clean_mongo_id(cid_result)
        cid_data_clean.update(zona_info)

        return jsonify({
            "status": "success",
            "summary": health_summary,
            "profile_pelanggan": profile_pelanggan,
            "cid_data": cid_data_clean,
            "mc_data": [clean_mongo_id(doc) for doc in mc_results], 
            "ardebt_data": [clean_mongo_id(doc) for doc in ardebt_results],
            "sbrs_data": [clean_mongo_id(doc) for doc in sbrs_history]
        }), 200

    except Exception as e:
        print(f"Error saat mencari data terintegrasi: {e}")
        import traceback
        traceback.print_exc()
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

# --- HELPER BARU: HITUNG BULAN SEBELUMNYA ---
def _get_previous_month_year(bulan_tagihan):
    if not bulan_tagihan or len(bulan_tagihan) != 6: return None
    try:
        month = int(bulan_tagihan[:2])
        year = int(bulan_tagihan[2:])
        target_date = datetime(year, month, 1) - timedelta(days=1)
        return f"{target_date.month:02d}{target_date.year}"
    except ValueError: return None
        
def _get_month_date_range(bulan_tagihan):
    if not bulan_tagihan or len(bulan_tagihan) != 6: return None, None
    try:
        month = int(bulan_tagihan[:2])
        year = int(bulan_tagihan[2:])
        start_date = datetime(year, month, 1)
        next_month = (month % 12) + 1
        next_year = year + (1 if month == 12 else 0)
        end_date = datetime(next_year, next_month, 1)
        return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
    except ValueError: return None, None

def _mm_yyyy_to_datetime(mm_yyyy_str):
    try: return datetime.strptime(mm_yyyy_str, '%m%Y')
    except ValueError: return None

# --- FUNGSI DISTRIBUSI (DIPERBAIKI) ---
def _get_distribution_report(group_fields, collection_mc):
    """
    PERBAIKAN: Syntax MongoDB kompatibel dengan Atlas M0, ekstraksi ZONA_NOVAK yang benar
    """
    if collection_mc is None:
        return [], "N/A (Koneksi DB Gagal)"
        
    if isinstance(group_fields, str):
        group_fields = [group_fields]

    latest_mc_month_doc = collection_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
    latest_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None
    
    if not latest_month:
        return [], "N/A (Tidak Ada Data MC)"

    # PERBAIKAN: Menggunakan pipeline yang sangat konservatif
    pipeline = [
        {"$match": {"BULAN_TAGIHAN": latest_month}},
        # 1. Project/Konversi Awal
        {"$project": {
            "NOMEN": 1,
            "NOMINAL": {"$toDouble": {"$ifNull": ["$NOMINAL", 0]}}, 
            "KUBIK": {"$toDouble": {"$ifNull": ["$KUBIK", 0]}},
            "TARIF": {"$ifNull": ["$TARIF", "N/A"]},
            "JENIS_METER": {"$ifNull": ["$JENIS_METER", "N/A"]},
            "RAYON": {"$ifNull": ["$RAYON", "N/A"]},
            "PCEZ": {"$ifNull": ["$PCEZ", "N/A"]},
        }},
        # 2. Grouping
        {"$group": {
            "_id": {field: f"${field}" for field in group_fields},
            "total_nomen_set": {"$addToSet": "$NOMEN"},
            "total_piutang": {"$sum": "$NOMINAL"},
            "total_kubikasi": {"$sum": "$KUBIK"}
        }},
        # 3. Proyeksi
        {"$project": {
            **{field: f"$_id.{field}" for field in group_fields},
            "_id": 0,
            "total_nomen": {"$size": "$total_nomen_set"},
            "total_piutang": 1,
            "total_kubikasi": 1
        }},
        {"$sort": {"total_piutang": -1}}
    ]

    try:
        results = list(collection_mc.aggregate(pipeline, allowDiskUse=True))
    except Exception as e:
        print(f"Error during distribution aggregation: {e}")
        import traceback
        traceback.print_exc()
        return [], latest_month

    return results, latest_month

# --- FUNGSI KOLEKSI DAN MONITORING (DIPERBAIKI) ---
def _get_mc_piutang_denominator(latest_mc_month, collection_mc, collection_cid):
    """Helper untuk menghitung total Piutang MC kustom (REG Rayon 34/35) yang digunakan sebagai Denominator."""
    if not latest_mc_month: return 0.0, {'34': 0.0, '35': 0.0}

    # Pipeline Konservatif untuk Denominator Piutang
    pipeline = [
        {'$match': {'BULAN_TAGIHAN': latest_mc_month}},
        {'$project': {
            "NOMEN": 1, 
            "NOMINAL": {"$toDouble": {"$ifNull": ["$NOMINAL", 0]}},
            "CUST_TYPE_MC": {"$ifNull": ["$CUST_TYPE", "N/A"]},
            "RAYON_MC": {"$ifNull": ["$RAYON", "N/A"]}
        }},
        {'$lookup': {'from': 'CustomerData', 'localField': 'NOMEN', 'foreignField': 'NOMEN', 'as': 'customer_info'}},
        {'$unwind': {'path': '$customer_info', 'preserveNullAndEmptyArrays': True}},
        {'$project': {
            'NOMINAL': 1,
            'CLEAN_RAYON': {'$toUpper': {'$ifNull': ['$customer_info.RAYON', '$RAYON_MC']}}, 
            'CLEAN_TIPEPLGGN': {'$toUpper': {'$ifNull': ['$customer_info.TIPEPLGGN', '$CUST_TYPE_MC']}},
        }},
        {'$match': {'CLEAN_RAYON': {'$in': ['34', '35']}, 'CLEAN_TIPEPLGGN': 'REG', 'NOMINAL': {'$gt': 0}}},
        {'$group': {'_id': '$CLEAN_RAYON', 'TotalPiutang': {'$sum': '$NOMINAL'}}}
    ]

    try:
        mc_total_response = list(collection_mc.aggregate(pipeline, allowDiskUse=True))
        mc_totals = {doc['_id']: doc['TotalPiutang'] for doc in mc_total_response}
        total_mc_nominal_all = mc_totals.get('34', 0) + mc_totals.get('35', 0)
        return total_mc_nominal_all, mc_totals
    except Exception as e:
        print(f"Error Denominator MC: {e}")
        return 0.0, {'34': 0.0, '35': 0.0}

# --- AGGREGATE MB SUNTER DETAIL (DIPERBAIKI) ---
def _aggregate_mb_sunter_detail(collection_mb):
    """
    PERBAIKAN: Syntax kompatibel dengan MongoDB Atlas M0
    """
    if collection_mb is None:
        return {"status": "error", "message": "Database connection failed."}

    try:
        latest_mc_month_doc = collection_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
        latest_mc_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None

        if not latest_mc_month:
            return {"status": "error", "message": "Tidak ada data MC terbaru untuk menentukan periode koleksi."}

        M_MINUS_1_REK = _get_previous_month_year(latest_mc_month)
        COLLECTION_MONTH_START, COLLECTION_MONTH_END = _get_month_date_range(latest_mc_month)
        RAYON_KEYS = ['34', '35']

        def _get_mb_collection_metrics(rayon_filter, bulan_rek_filter_type):
            
            base_match = {
                'BILL_REASON': 'BIAYA PEMAKAIAN AIR',
                'TGL_BAYAR': {'$gte': COLLECTION_MONTH_START, '$lt': COLLECTION_MONTH_END},
            }

            if bulan_rek_filter_type == 'UNDUE':
                base_match['BULAN_REK'] = latest_mc_month
            elif bulan_rek_filter_type == 'CURRENT':
                base_match['BULAN_REK'] = M_MINUS_1_REK
            elif bulan_rek_filter_type == 'AGING':
                base_match['BULAN_REK'] = {'$lt': M_MINUS_1_REK} 
                
            pipeline = [
                {'$match': base_match},
                {'$project': {
                    'NOMINAL': {'$toDouble': {'$ifNull': ['$NOMINAL', 0]}},
                    'NOMEN': 1,
                    'RAYON': {'$toUpper': {'$ifNull': ['$RAYON', 'N/A']}}
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
            
            result = list(collection_mb.aggregate(pipeline, allowDiskUse=True))
            return {
                'nominal': result[0].get('TotalNominal', 0.0) if result else 0.0,
                'nomen_count': len(result[0].get('TotalNomen', [])) if result else 0
            }

        # AGGREGATE SUMMARY (Kini sangat bergantung pada data MB)
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
        
        # DETAIL HARIAN R34 dan R35
        def _get_mb_daily_detail(rayon_key):
            pipeline = [
                {'$match': {
                    'BILL_REASON': 'BIAYA PEMAKAIAN AIR',
                    'TGL_BAYAR': {'$gte': COLLECTION_MONTH_START, '$lt': COLLECTION_MONTH_END}
                }},
                {'$project': {
                    'TGL_BAYAR': 1,
                    'NOMINAL': {'$toDouble': {'$ifNull': ['$NOMINAL', 0]}},
                    'NOMEN': 1,
                    'RAYON': {'$toUpper': {'$ifNull': ['$RAYON', 'N/A']}}
                }},
                {'$match': {'RAYON': rayon_key}},
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
            return list(collection_mb.aggregate(pipeline, allowDiskUse=True))

        daily_detail = {
            '34': _get_mb_daily_detail('34'),
            '35': _get_mb_daily_detail('35'),
        }

        bayar_bulan_fmt = datetime.strptime(latest_mc_month, '%m%Y').strftime('%b %Y').upper()

        return {
            'status': 'success',
            'periods': {
                'bayar_bulan': bayar_bulan_fmt, 
                'undue_rek': latest_mc_month,
                'current_rek': M_MINUS_1_REK,
                'aging_rek_max': f"<{M_MINUS_1_REK}",
            },
            'summary': summary_data,
            'daily_detail': daily_detail
        }
    except Exception as e:
        print(f"Error dalam _aggregate_mb_sunter_detail: {e}")
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": f"Gagal mengambil data: {e}"}

# --- ENDPOINT KOLEKSI DAN MONITORING (Lanjutan) ---
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

# --- HELPER BARU: HITUNG BULAN SEBELUMNYA ---
def _get_previous_month_year(bulan_tagihan):
    if not bulan_tagihan or len(bulan_tagihan) != 6: return None
    try:
        month = int(bulan_tagihan[:2])
        year = int(bulan_tagihan[2:])
        target_date = datetime(year, month, 1) - timedelta(days=1)
        return f"{target_date.month:02d}{target_date.year}"
    except ValueError: return None
        
def _get_month_date_range(bulan_tagihan):
    if not bulan_tagihan or len(bulan_tagihan) != 6: return None, None
    try:
        month = int(bulan_tagihan[:2])
        year = int(bulan_tagihan[2:])
        start_date = datetime(year, month, 1)
        next_month = (month % 12) + 1
        next_year = year + (1 if month == 12 else 0)
        end_date = datetime(next_year, next_month, 1)
        return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
    except ValueError: return None, None

def _mm_yyyy_to_datetime(mm_yyyy_str):
    try: return datetime.strptime(mm_yyyy_str, '%m%Y')
    except ValueError: return None

# --- FUNGSI DISTRIBUSI (DIPERBAIKI) ---
def _get_distribution_report(group_fields, collection_mc):
    """
    PERBAIKAN: Syntax MongoDB kompatibel dengan Atlas M0, ekstraksi ZONA_NOVAK yang benar
    """
    if collection_mc is None:
        return [], "N/A (Koneksi DB Gagal)"
        
    if isinstance(group_fields, str):
        group_fields = [group_fields]

    latest_mc_month_doc = collection_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
    latest_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None
    
    if not latest_month:
        return [], "N/A (Tidak Ada Data MC)"

    # PERBAIKAN: Menggunakan pipeline yang sangat konservatif
    pipeline = [
        {"$match": {"BULAN_TAGIHAN": latest_month}},
        # 1. Project/Konversi Awal
        {"$project": {
            "NOMEN": 1,
            "NOMINAL": {"$toDouble": {"$ifNull": ["$NOMINAL", 0]}}, 
            "KUBIK": {"$toDouble": {"$ifNull": ["$KUBIK", 0]}},
            "TARIF": {"$ifNull": ["$TARIF", "N/A"]},
            "JENIS_METER": {"$ifNull": ["$JENIS_METER", "N/A"]},
            "RAYON": {"$ifNull": ["$RAYON", "N/A"]},
            "PCEZ": {"$ifNull": ["$PCEZ", "N/A"]},
        }},
        # 2. Grouping
        {"$group": {
            "_id": {field: f"${field}" for field in group_fields},
            "total_nomen_set": {"$addToSet": "$NOMEN"},
            "total_piutang": {"$sum": "$NOMINAL"},
            "total_kubikasi": {"$sum": "$KUBIK"}
        }},
        # 3. Proyeksi
        {"$project": {
            **{field: f"$_id.{field}" for field in group_fields},
            "_id": 0,
            "total_nomen": {"$size": "$total_nomen_set"},
            "total_piutang": 1,
            "total_kubikasi": 1
        }},
        {"$sort": {"total_piutang": -1}}
    ]

    try:
        results = list(collection_mc.aggregate(pipeline, allowDiskUse=True))
    except Exception as e:
        print(f"Error during distribution aggregation: {e}")
        import traceback
        traceback.print_exc()
        return [], latest_month

    return results, latest_month

# --- FUNGSI KOLEKSI DAN MONITORING (DIPERBAIKI) ---
def _get_mc_piutang_denominator(latest_mc_month, collection_mc, collection_cid):
    """Helper untuk menghitung total Piutang MC kustom (REG Rayon 34/35) yang digunakan sebagai Denominator."""
    if not latest_mc_month: return 0.0, {'34': 0.0, '35': 0.0}

    # Pipeline Konservatif untuk Denominator Piutang
    pipeline = [
        {'$match': {'BULAN_TAGIHAN': latest_mc_month}},
        {'$project': {
            "NOMEN": 1, 
            "NOMINAL": {"$toDouble": {"$ifNull": ["$NOMINAL", 0]}},
            "CUST_TYPE_MC": {"$ifNull": ["$CUST_TYPE", "N/A"]},
            "RAYON_MC": {"$ifNull": ["$RAYON", "N/A"]}
        }},
        {'$lookup': {'from': 'CustomerData', 'localField': 'NOMEN', 'foreignField': 'NOMEN', 'as': 'customer_info'}},
        {'$unwind': {'path': '$customer_info', 'preserveNullAndEmptyArrays': True}},
        {'$project': {
            'NOMINAL': 1,
            'CLEAN_RAYON': {'$toUpper': {'$ifNull': ['$customer_info.RAYON', '$RAYON_MC']}}, 
            'CLEAN_TIPEPLGGN': {'$toUpper': {'$ifNull': ['$customer_info.TIPEPLGGN', '$CUST_TYPE_MC']}},
        }},
        {'$match': {'CLEAN_RAYON': {'$in': ['34', '35']}, 'CLEAN_TIPEPLGGN': 'REG', 'NOMINAL': {'$gt': 0}}},
        {'$group': {'_id': '$CLEAN_RAYON', 'TotalPiutang': {'$sum': '$NOMINAL'}}}
    ]

    try:
        mc_total_response = list(collection_mc.aggregate(pipeline, allowDiskUse=True))
        mc_totals = {doc['_id']: doc['TotalPiutang'] for doc in mc_total_response}
        total_mc_nominal_all = mc_totals.get('34', 0) + mc_totals.get('35', 0)
        return total_mc_nominal_all, mc_totals
    except Exception as e:
        print(f"Error Denominator MC: {e}")
        return 0.0, {'34': 0.0, '35': 0.0}

# --- AGGREGATE MB SUNTER DETAIL (DIPERBAIKI) ---
def _aggregate_mb_sunter_detail(collection_mb):
    """
    PERBAIKAN: Syntax kompatibel dengan MongoDB Atlas M0
    """
    if collection_mb is None:
        return {"status": "error", "message": "Database connection failed."}

    try:
        latest_mc_month_doc = collection_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
        latest_mc_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None

        if not latest_mc_month:
            return {"status": "error", "message": "Tidak ada data MC terbaru untuk menentukan periode koleksi."}

        M_MINUS_1_REK = _get_previous_month_year(latest_mc_month)
        COLLECTION_MONTH_START, COLLECTION_MONTH_END = _get_month_date_range(latest_mc_month)
        RAYON_KEYS = ['34', '35']

        def _get_mb_collection_metrics(rayon_filter, bulan_rek_filter_type):
            
            base_match = {
                'BILL_REASON': 'BIAYA PEMAKAIAN AIR',
                'TGL_BAYAR': {'$gte': COLLECTION_MONTH_START, '$lt': COLLECTION_MONTH_END},
            }

            if bulan_rek_filter_type == 'UNDUE':
                base_match['BULAN_REK'] = latest_mc_month
            elif bulan_rek_filter_type == 'CURRENT':
                base_match['BULAN_REK'] = M_MINUS_1_REK
            elif bulan_rek_filter_type == 'AGING':
                base_match['BULAN_REK'] = {'$lt': M_MINUS_1_REK} 
                
            pipeline = [
                {'$match': base_match},
                {'$project': {
                    'NOMINAL': {'$toDouble': {'$ifNull': ['$NOMINAL', 0]}},
                    'NOMEN': 1,
                    'RAYON': {'$toUpper': {'$ifNull': ['$RAYON', 'N/A']}}
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
            
            result = list(collection_mb.aggregate(pipeline, allowDiskUse=True))
            return {
                'nominal': result[0].get('TotalNominal', 0.0) if result else 0.0,
                'nomen_count': len(result[0].get('TotalNomen', [])) if result else 0
            }

        # AGGREGATE SUMMARY (Kini sangat bergantung pada data MB)
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
        
        # DETAIL HARIAN R34 dan R35
        def _get_mb_daily_detail(rayon_key):
            pipeline = [
                {'$match': {
                    'BILL_REASON': 'BIAYA PEMAKAIAN AIR',
                    'TGL_BAYAR': {'$gte': COLLECTION_MONTH_START, '$lt': COLLECTION_MONTH_END}
                }},
                {'$project': {
                    'TGL_BAYAR': 1,
                    'NOMINAL': {'$toDouble': {'$ifNull': ['$NOMINAL', 0]}},
                    'NOMEN': 1,
                    'RAYON': {'$toUpper': {'$ifNull': ['$RAYON', 'N/A']}}
                }},
                {'$match': {'RAYON': rayon_key}},
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
            return list(collection_mb.aggregate(pipeline, allowDiskUse=True))

        daily_detail = {
            '34': _get_mb_daily_detail('34'),
            '35': _get_mb_daily_detail('35'),
        }

        bayar_bulan_fmt = datetime.strptime(latest_mc_month, '%m%Y').strftime('%b %Y').upper()

        return {
            'status': 'success',
            'periods': {
                'bayar_bulan': bayar_bulan_fmt, 
                'undue_rek': latest_mc_month,
                'current_rek': M_MINUS_1_REK,
                'aging_rek_max': f"<{M_MINUS_1_REK}",
            },
            'summary': summary_data,
            'daily_detail': daily_detail
        }
    except Exception as e:
        print(f"Error dalam _aggregate_mb_sunter_detail: {e}")
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": f"Gagal mengambil data: {e}"}

# --- ENDPOINT KOLEKSI DAN MONITORING (Lanjutan) ---
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

# --- FUNGSI BARU UNTUK REPORT KOLEKSI & PIUTANG ---

@app.route('/api/collection/report', methods=['GET'])
@login_required 
def collection_report_api():
    """Menghitung Nomen Bayar, Nominal Bayar, Total Nominal, dan Persentase per Rayon/PCEZ, termasuk KUBIKASI."""
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500

    try:
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

        # 3. MB (UNDUE BULAN INI)
        pipeline_mb_undue = [
            { '$match': { 
                'BULAN_REK': latest_mc_month, 
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

        # 4. MB (UNDUE BULAN SEBELUMNYA)
        pipeline_mb_undue_prev = [
            { '$match': { 
                'BULAN_REK': previous_mc_month, 
                'BILL_REASON': 'BIAYA PEMAKAIAN AIR'
            }},
            { '$group': {
                '_id': None,
                'mb_undue_prev_nominal': { '$sum': {'$toDouble': {'$ifNull': ['$NOMINAL', 0]}} },
            }}
        ]
        mb_undue_prev_result = list(collection_mb.aggregate(pipeline_mb_undue_prev))
        total_undue_prev_nominal = mb_undue_prev_result[0]['mb_undue_prev_nominal'] if mb_undue_prev_result else 0.0
        
        report_map = {}
        
        # Merge data
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

        for item in collected_data:
            key = (item['_id']['rayon'], item['_id']['pcez'])
            if key in report_map:
                report_map[key]['MC_CollectedNominal'] = float(item['collected_nominal'])
                report_map[key]['MC_CollectedKubik'] = float(item['collected_kubik'])
                report_map[key]['MC_CollectedNomen'] = len(item['collected_nomen'])
                
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


        # Final calculations
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
        
        # Simpan total MC (denominator untuk Monitoring)
        total_mc_nominal_all, mc_totals = _get_mc_piutang_denominator(latest_mc_month, collection_mc, collection_cid)

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
            'total_mc_nominal_all': total_mc_nominal_all 
        }), 200
    except Exception as e:
        print(f"Error dalam collection_report_api: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"message": f"Gagal mengambil data laporan koleksi: {e}"}), 500

# --- API MONITORING KOLEKSI HARIAN (DIPERBAIKI) ---
@app.route('/api/collection/monitoring', methods=['GET'])
@login_required
def collection_monitoring_api():
    """
    PERBAIKAN: Syntax MongoDB kompatibel dengan Atlas M0
    """
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500
        
    try:
        latest_mc_month_doc = collection_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
        latest_mc_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None
        
        if not latest_mc_month:
             empty_summary = {
                 'R34': {'MC1125': 0, 'CURRENT': 0}, 
                 'R35': {'MC1125': 0, 'CURRENT': 0}, 
                 'GLOBAL': {'TotalPiutangMC': 0, 'TotalUnduePrev': 0, 'CurrentKoleksiTotal': 0, 'TotalKoleksiPersen': 0}
             }
             return jsonify({'monitoring_data': {'R34': [], 'R35': []}, 'summary_top': empty_summary}), 200

        previous_mc_month = _get_previous_month_year(latest_mc_month)
        
        total_mc_nominal_all, mc_totals = _get_mc_piutang_denominator(latest_mc_month, collection_mc, collection_cid)
        total_mc_34 = mc_totals.get('34', 0)
        total_mc_35 = mc_totals.get('35', 0)

        total_undue_prev_nominal = 0.0
        
        if previous_mc_month:
            prev_month_start_date, prev_month_end_date = _get_month_date_range(previous_mc_month)

            pipeline_undue_prev = [
                { '$match': { 
                    'BULAN_REK': previous_mc_month, 
                    'BILL_REASON': 'BIAYA PEMAKAIAN AIR',
                    'TGL_BAYAR': {'$gte': prev_month_start_date, '$lt': prev_month_end_date} 
                }},
                { '$group': {
                    '_id': None,
                    'TotalUnduePrev': { '$sum': {'$toDouble': {'$ifNull': ['$NOMINAL', 0]}} },
                }}
            ]
            undue_prev_result = list(collection_mb.aggregate(pipeline_undue_prev))
            total_undue_prev_nominal = undue_prev_result[0]['TotalUnduePrev'] if undue_prev_result else 0.0

        # 3. Ambil Data Transaksi MB (Koleksi) Harian (Rp1 - Current Aging 1)
        now = datetime.now()
        this_month_start = now.strftime('%Y-%m-01')
        next_month_start = (now.replace(day=1) + timedelta(days=32)).replace(day=1).strftime('%Y-%m-%d')
        
        # Pipeline Konservatif
        pipeline_mb_daily = [
            {'$match': {
                'TGL_BAYAR': {'$gte': this_month_start, '$lt': next_month_start}, 
                'BULAN_REK': previous_mc_month, 
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
                 'CLEAN_RAYON': {'$toUpper': {'$ifNull': ['$customer_info.RAYON', '$RAYON_MB']}},
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
             empty_summary = {'R34': {'MC1125': total_mc_34, 'CURRENT': 0}, 'R35': {'MC1125': total_mc_35, 'CURRENT': 0}, 'GLOBAL': {'TotalPiutangMC': total_mc_nominal_all, 'TotalUnduePrev': total_undue_prev_nominal, 'CurrentKoleksiTotal': 0, 'TotalKoleksiPersen': 0}}
             return jsonify({'monitoring_data': {'R34': [], 'R35': []}, 'summary_top': empty_summary}), 200

        df_monitoring['TGL'] = pd.to_datetime(df_monitoring['TGL'])
        df_monitoring = df_monitoring.sort_values(by='TGL')
        
        df_monitoring['Rp1_Kumulatif'] = df_monitoring.groupby('RAYON')['COLL_NOMINAL'].cumsum()
        df_monitoring['CUST_Kumulatif'] = df_monitoring.groupby('RAYON')['CUST_COUNT'].cumsum()

        df_monitoring_global = df_monitoring.groupby('TGL').agg({'COLL_NOMINAL': 'sum'}).reset_index()
        df_monitoring_global['Rp1_Kumulatif_Global'] = df_monitoring_global['COLL_NOMINAL'].cumsum()
        
        df_monitoring = pd.merge(df_monitoring, df_monitoring_global[['TGL', 'Rp1_Kumulatif_Global']], on='TGL', how='left')

        if total_mc_nominal_all > 0:
            df_monitoring['COLL_Kumulatif_Persen'] = ((df_monitoring['Rp1_Kumulatif_Global'] + total_undue_prev_nominal) / total_mc_nominal_all) * 100
            df_monitoring['COLL_Kumulatif_Persen'] = df_monitoring['COLL_Kumulatif_Persen'].fillna(0)
            
            df_monitoring['COLL_VAR'] = df_monitoring.groupby('RAYON')['COLL_Kumulatif_Persen'].diff()
            for rayon in df_monitoring['RAYON'].unique():
                is_first = (df_monitoring['RAYON'] == rayon) & (df_monitoring['COLL_VAR'].isna())
                if is_first.any():
                    df_monitoring.loc[is_first, 'COLL_VAR'] = df_monitoring.loc[is_first, 'COLL_Kumulatif_Persen']
            df_monitoring['COLL_VAR'] = df_monitoring['COLL_VAR'].fillna(0)
        else:
            df_monitoring['COLL_Kumulatif_Persen'] = 0
            df_monitoring['COLL_VAR'] = 0

        df_monitoring = df_monitoring.drop(columns=['Rp1_Kumulatif_Global'], errors='ignore')
        df_monitoring['TGL'] = df_monitoring['TGL'].dt.strftime('%d/%m/%Y')
        df_monitoring['RAYON_OUTPUT'] = 'R' + df_monitoring['RAYON']

        df_r34 = df_monitoring[df_monitoring['RAYON_OUTPUT'] == 'R34'].drop(columns=['RAYON', 'RAYON_OUTPUT']).reset_index(drop=True)
        df_r35 = df_monitoring[df_monitoring['RAYON_OUTPUT'] == 'R35'].drop(columns=['RAYON', 'RAYON_OUTPUT']).reset_index(drop=True)

        summary_r34 = {'MC1125': total_mc_34, 'CURRENT': df_r34['Rp1_Kumulatif'].iloc[-1] if not df_r34.empty else 0}
        summary_r35 = {'MC1125': total_mc_35, 'CURRENT': df_r35['Rp1_Kumulatif'].iloc[-1] if not df_r35.empty else 0}
        
        current_koleksi_total = df_monitoring_global['COLL_NOMINAL'].sum() if not df_monitoring_global.empty else 0
        total_koleksi_persen = df_monitoring['COLL_Kumulatif_Persen'].iloc[-1] if not df_monitoring.empty else 0
        
        grand_total_summary = {
            'TotalPiutangMC': total_mc_nominal_all,
            'TotalUnduePrev': total_undue_prev_nominal,
            'CurrentKoleksiTotal': current_koleksi_total,
            'TotalKoleksiPersen': total_koleksi_persen
        }

        return jsonify({
            'monitoring_data': {'R34': df_r34.to_dict('records'), 'R35': df_r35.to_dict('records')},
            'summary_top': {'R34': summary_r34, 'R35': summary_r35, 'GLOBAL': grand_total_summary}
        }), 200

    except Exception as e:
        print(f"Error collection monitoring: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"message": f"Gagal membuat data monitoring koleksi: {e}"}), 500

# =========================================================================
# === API PERBANDINGAN KOLEKSI MoM (Month-over-Month) ===
# =========================================================================

@app.route('/api/collection/mom_report', methods=['GET'])
@login_required
def mom_report_api():
    """
    Menghitung perbandingan koleksi (Nominal dan Pelanggan) bulan ini vs bulan lalu.
    """
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500

    try:
        now = datetime.now()
        day_of_month = now.day
        this_month_str = now.strftime('%Y-%m')
        last_month = now.replace(day=1) - timedelta(days=1)
        last_month_str = last_month.strftime('%Y-%m')

        date_pattern = []
        for i in range(1, day_of_month + 1):
            day_str = f'{i:02d}'
            date_pattern.append(f"{this_month_str}-{day_str}")
            try:
                datetime.strptime(f"{last_month_str}-{day_str}", '%Y-%m-%d') 
                date_pattern.append(f"{last_month_str}-{day_str}")
            except ValueError:
                pass 
        
        regex_pattern = "|".join(date_pattern)

        pipeline = [
            {'$match': {
                'TGL_BAYAR': {'$regex': f"^({regex_pattern})$"},
                'BILL_REASON': 'BIAYA PEMAKAIAN AIR' 
            }},
            {'$project': {
                'NOMINAL': {'$toDouble': {'$ifNull': ['$NOMINAL', 0]}},
                'NOMEN': 1,
                'Periode': {'$substr': ['$TGL_BAYAR', 0, 7]} 
            }},
            {'$group': {
                '_id': '$Periode',
                'TotalNominal': {'$sum': '$NOMINAL'},
                'TotalNomen': {'$addToSet': '$NOMEN'}
            }},
        ]
        
        raw_data = list(collection_mb.aggregate(pipeline))

        report_map = {
            this_month_str: {'nominal': 0, 'nomen': 0},
            last_month_str: {'nominal': 0, 'nomen': 0},
        }
        
        for item in raw_data:
            period = item['_id']
            if period in report_map:
                report_map[period]['nominal'] = item.get('TotalNominal', 0)
                report_map[period]['nomen'] = len(item.get('TotalNomen', []))

        def calculate_change(current, previous):
            if previous == 0:
                return 100.0 if current > 0 else 0.0
            return ((current - previous) / previous) * 100

        current_nom = report_map.get(this_month_str, {}).get('nominal', 0)
        last_nom = report_map.get(last_month_str, {}).get('nominal', 0)
        current_nomen = report_map.get(this_month_str, {}).get('nomen', 0)
        last_nomen = report_map.get(last_month_str, {}).get('nomen', 0)

        final_report = {
            'period_current': this_month_str,
            'period_last': last_month_str,
            'current_nominal': current_nom,
            'last_nominal': last_nom,
            'current_nomen': current_nomen,
            'last_nomen': last_nomen,
            'change_nominal': calculate_change(current_nom, last_nom),
            'change_nomen': calculate_change(current_nomen, last_nomen)
        }
        
        if not raw_data and day_of_month == 1:
             return jsonify({'status': 'success', 'data': final_report}), 200

        return jsonify({'status': 'success', 'data': final_report}), 200

    except Exception as e:
        print(f"Error saat membuat laporan MoM: {e}")
        return jsonify({"status": 'error', "message": f"Gagal mengambil laporan MoM: {e}"}), 500

# --- API PERBANDINGAN KOLEKSI DOH (Day-of-the-Month) ---
@app.route('/api/collection/doh_comparison_report', methods=['GET'])
@login_required
def doh_comparison_report_api():
    """Menghitung perbandingan koleksi harian (Nominal) Bulan Ini vs Bulan Lalu, per Rayon."""
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500

    try:
        now = datetime.now()
        day_of_month = now.day
        
        this_month_str = now.strftime('%Y-%m')
        last_month = now.replace(day=1) - timedelta(days=1)
        last_month_str = last_month.strftime('%Y-%m')

        date_prefixes = []
        for i in range(1, day_of_month + 1):
            day_str = f'{i:02d}'
            date_prefixes.append(f"{this_month_str}-{day_str}")
            try:
                datetime.strptime(f"{last_month_str}-{day_str}", '%Y-%m-%d') 
                date_prefixes.append(f"{last_month_str}-{day_str}")
            except ValueError:
                pass 
        
        regex_pattern = "|".join(date_prefixes)

        pipeline = [
            {'$match': {
                'TGL_BAYAR': {'$regex': f"^({regex_pattern})$"},
                'BILL_REASON': 'BIAYA PEMAKAIAN AIR' 
            }},
            {'$project': {
                'NOMINAL': {'$toDouble': {'$ifNull': ['$NOMINAL', 0]}},
                'RAYON': 1,
                'Day': {'$substr': ['$TGL_BAYAR', 8, 2]},
                'Periode': {'$substr': ['$TGL_BAYAR', 0, 7]}
            }},
            # FIX: Normalisasi RAYON dari MB yang mungkin tidak seragam
            {'$project': { 
                'NOMINAL': 1,
                'Day': 1,
                'Periode': 1,
                'CLEAN_RAYON': {'$toUpper': {'$ifNull': ['$RAYON', 'N/A']}},
            }},
            {'$match': {'CLEAN_RAYON': {'$in': ['34', '35']}}},
            
            {'$group': {
                '_id': {'periode': '$Periode', 'day': '$Day', 'rayon': '$CLEAN_RAYON'},
                'DailyNominal': {'$sum': '$NOMINAL'},
            }},
            {'$sort': {'_id.date': 1}}
        ]
        
        raw_data = list(collection_mb.aggregate(pipeline))
        
        days = [i for i in range(1, day_of_month + 1)]
        
        report_structure = {
            'days': days,
            'R34': {last_month_str: [0] * day_of_month, this_month_str: [0] * day_of_month},
            'R35': {last_month_str: [0] * day_of_month, this_month_str: [0] * day_of_month},
            'TOTAL_AB': {last_month_str: [0] * day_of_month, this_month_str: [0] * day_of_month},
        }
        
        RAYON_MAP = {'34': 'R34', '35': 'R35'}
        
        for item in raw_data:
            day_index = int(item['_id']['day']) - 1
            rayon_raw = item['_id']['rayon'] 
            periode = item['_id']['periode']
            nominal = item['DailyNominal']
            
            areaKey = RAYON_MAP.get(rayon_raw)

            if areaKey:
                if periode in report_structure[areaKey]:
                    report_structure[areaKey][periode][day_index] = nominal
                
                if periode in report_structure['TOTAL_AB']:
                    report_structure['TOTAL_AB'][periode][day_index] += nominal
        
        return jsonify({
            'status': 'success',
            'data': report_structure,
            'periods': {'current': this_month_str, 'last': last_month_str}
        }), 200

    except Exception as e:
        print(f"Error saat membuat laporan DOH comparison: {e}")
        return jsonify({"status": 'error', "message": f"Gagal mengambil laporan DOH comparison: {e}"}), 500

if __name__ == '__main__':
    # Memastikan STATIC_USERS didefinisikan sebelum init_app (untuk flask_login)
    from flask_login import LoginManager 
    from werkzeug.security import generate_password_hash 
    
    login_manager = LoginManager() 
    login_manager.init_app(app)

    # Re-import class User dan load_user jika diperlukan, atau pastikan mereka 
    # berada di level modul di atas (sudah diperbaiki di v9)
    
    app.run(debug=True, host='0.0.0.0', port=5000)
