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
db = None # Deklarasi db di scope global

try:
    # PERBAIKAN KRITIS UNTUK BULK WRITE/SBRS: Meningkatkan batas waktu koneksi dan socket.
    # Peningkatan timeout membantu mencegah hang pada query besar
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=60000, socketTimeoutMS=300000)
    client.admin.command('ping')
    db = client[DB_NAME] # Tetapkan objek db
    
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
    collection_cid.create_index([('NOMEN', 1), ('TANGGAL_UPLOAD_CID', -1)], name='idx_cid_nomen_hist')
    collection_cid.create_index([('RAYON', 1), ('TIPEPLGGN', 1)], name='idx_cid_rayon_tipe')
    # Tambah index untuk PCEZ yang didekode
    collection_cid.create_index([('PCEZ', 1)], name='idx_cid_pcez')

    # MC (MasterCetak)
    collection_mc.create_index([('NOMEN', 1), ('BULAN_TAGIHAN', -1)], name='idx_mc_nomen_hist')
    collection_mc.create_index([('RAYON', 1), ('PCEZ', 1)], name='idx_mc_rayon_pcez')
    collection_mc.create_index([('STATUS', 1)], name='idx_mc_status')
    collection_mc.create_index([('TARIF', 1), ('KUBIK', 1), ('NOMINAL', 1)], name='idx_mc_tarif_volume')

    # MB (MasterBayar): PERBAIKAN KRITIS DUPLIKASI STARTUP
    try:
        # Coba drop index lama yang mungkin unik dan rusak
        collection_mb.drop_index('idx_mb_unique_transaction')
    except Exception:
        pass
        
    # Buat index TANPA unique=True agar startup tidak gagal
    collection_mb.create_index([('NOTAGIHAN', 1), ('TGL_BAYAR', 1), ('NOMINAL', 1)], name='idx_mb_unique_transaction', unique=False)
    collection_mb.create_index([('TGL_BAYAR', -1)], name='idx_mb_paydate_desc')
    collection_mb.create_index([('NOMEN', 1)], name='idx_mb_nomen')
    collection_mb.create_index([('RAYON', 1), ('PCEZ', 1), ('TGL_BAYAR', -1)], name='idx_mb_rayon_pcez_date')
    collection_mb.create_index([('BULAN_REK', 1)], name='idx_mb_bulan_rek')


    # SBRS (MeterReading): Untuk Anomaly Check
    try:
        collection_sbrs.create_index([('CMR_ACCOUNT', 1), ('CMR_RD_DATE', 1)], name='idx_sbrs_unique_read', unique=True)
    except OperationFailure:
        collection_sbrs.drop_index('idx_sbrs_unique_read')
        collection_sbrs.create_index([('CMR_ACCOUNT', 1), ('CMR_RD_DATE', 1)], name='idx_sbrs_unique_read', unique=False)
        
    collection_sbrs.create_index([('CMR_ACCOUNT', 1), ('CMR_RD_DATE', -1)], name='idx_sbrs_history')
    
    # ARDEBT (AccountReceivable)
    collection_ardebt.create_index([('NOMEN', 1), ('PERIODE_BILL', -1), ('JUMLAH', 1)], name='idx_ardebt_nomen_hist')
    
    # ==========================================================
    # === END OPTIMASI: INDEXING KRITIS ===
    # ==========================================================
    
    collection_data = collection_mc

    print("Koneksi MongoDB berhasil dan index dikonfigurasi!")
except Exception as e:
    print(f"Gagal terhubung ke MongoDB atau mengkonfigurasi index: {e}")
    client = None
    db = None

# --- FUNGSI UTILITY INTERNAL: DEKODE ZONA_NOVAK (DIPERKUAT) ---
def decode_zona_novak(df):
    """
    Mendekode ZONANOvaK (string) menjadi Rayon, PC, EZ, PCEZ, dan Block.
    Fungsi ini harus dipanggil pada DataFrame CID sebelum di-insert ke MongoDB.
    """
    
    # Asumsi nama kolom di file CID adalah ZONANOvaK
    target_col = 'ZONANOvaK'
    if target_col not in df.columns:
        if 'ZONA_NOVAK' in df.columns:
             df = df.rename(columns={'ZONA_NOVAK': target_col})
        else:
            # Jika kolom kunci ZONA tidak ada, set kolom-kolom dekode ke 'N/A'
            for col in ['RAYON', 'PC', 'EZ', 'PCEZ', 'BLOCK']:
                if col not in df.columns:
                    df[col] = 'N/A'
            return df 

    df[target_col] = df[target_col].astype(str).str.strip()
    
    def parse_zona(zona):
        # Asumsi minimum 9 digit (misal: 350960217)
        zona = zona.strip().upper()
        if not zona or len(zona) < 9 or zona in ['N/A', 'NAN', 'NONE']: 
            return 'N/A', 'N/A', 'N/A', 'N/A', 'N/A'

        # Dekode berdasarkan posisi string:
        rayon = zona[0:2].strip()             
        pc = zona[2:5].strip()                
        ez = zona[5:7].strip()                
        block = zona[7:].strip()             
        pcez = f"{pc}/{ez}".strip()          
        
        # Jika hasil dekode kosong, ganti dengan 'N/A'
        rayon = rayon if rayon else 'N/A'
        pcez = pcez if pcez else 'N/A'
        pc = pc if pc else 'N/A'
        ez = ez if ez else 'N/A'
        block = block if block else 'N/A'

        return rayon, pc, ez, pcez, block

    # Terapkan fungsi parsing ke kolom ZONANOvaK
    try:
        # Gunakan 'object' sebagai tipe data sementara untuk mencegah error Pandas
        df[['RAYON', 'PC', 'EZ', 'PCEZ', 'BLOCK']] = df[target_col].apply(
            lambda x: pd.Series(parse_zona(x), dtype='object')
        )
    except Exception as e:
        print(f"Peringatan: Gagal menerapkan parse_zona. Error: {e}")
        # Jika gagal, tambahkan kolom dengan nilai default 'N/A'
        for col in ['RAYON', 'PC', 'EZ', 'PCEZ', 'BLOCK']:
            if col not in df.columns:
                df[col] = 'N/A'
    
    return df

# --- FUNGSI UTILITY INTERNAL: AGREGASI MC vs MB UNDUE (DIPERKUAT FALLBACK) ---
def get_mc_mb_comparison_by_pcez(bulan_tagihan_target):
    """
    Menghitung perbandingan Piutang Master Cetak (MC) bulan berjalan
    terhadap Koleksi Belum Jatuh Tempo (MB Undue) berdasarkan Rayon dan PCEZ.
    
    Args:
        bulan_tagihan_target (str): Bulan tagihan target (format MMYYYY, cth: '112025').
        
    Returns:
        list: Daftar hasil perbandingan per Rayon/PCEZ.
    """
    if db is None:
        return []
    
    # Filter dasar untuk Rayon yang dianalisis
    RAYON_KEYS = ['34', '35']
    
    # 1. Pipeline untuk Master Cetak (MC) - Total Piutang Bulan Berjalan
    mc_pipeline = [
        # Filter data MC untuk bulan tagihan target
        {'$match': {'BULAN_TAGIHAN': bulan_tagihan_target}},
        
        # Ekstraksi Rayon dan PCEZ dari ZONA_NOVAK MC
        {"$project": {
            "NOMEN": 1,
            "JML_TAGIHAN": {"$toDouble": {"$ifNull": ["$NOMINAL", 0]}}, # Menggunakan NOMINAL dari MC untuk Piutang
            "CUST_TYPE_MC": "$CUST_TYPE",
            "CLEAN_ZONA": {"$trim": {"input": {"$ifNull": ["$ZONA_NOVAK", ""]}}},
            # Ekstrak Rayon/PCEZ dari ZONA_NOVAK MC sebagai fallback:
            "RAYON_ZONA": {"$substrCP": [{"$trim": {"input": {"$ifNull": ["$ZONA_NOVAK", ""]}}}, 0, 2]},
            "PCEZ_ZONA": {"$concat": [{"$substrCP": [{"$trim": {"input": {"$ifNull": ["$ZONA_NOVAK", ""]}}}, 2, 3]}, {"$literal": "/"}, {"$substrCP": [{"$trim": {"input": {"$ifNull": ["$ZONA_NOVAK", ""]}}}, 5, 2]}]}
        }},
        
        # Join dengan CustomerData (CID) untuk mendapatkan data yang paling akurat
        {
            '$lookup': {
                'from': 'CustomerData',
                'localField': 'NOMEN',
                'foreignField': 'NOMEN',
                'pipeline': [
                    {'$sort': {'TANGGAL_UPLOAD_CID': -1}}, 
                    {'$limit': 1},
                    # Kita fetch field yang sudah di-decode: PCEZ, RAYON, TIPEPLGGN
                    {'$project': {'_id': 0, 'PCEZ': 1, 'RAYON': 1, 'TIPEPLGGN': 1}},
                ],
                'as': 'customer_info'
            }
        },
        
        # Unwind, dengan preservasi agar data MC tidak hilang
        {'$unwind': {'path': '$customer_info', 'preserveNullAndEmptyArrays': True}},
        
        # Finalisasi Rayon/PCEZ dan filter TIPEPLGGN REG
        {'$addFields': {
            # FALLBACK: Prioritas ke CID, lalu ke field MC raw (dari ZONA_NOVAK)
            'CLEAN_RAYON': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.RAYON', '$RAYON_ZONA']}}}}},
            'CLEAN_PCEZ': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.PCEZ', '$PCEZ_ZONA']}}}}},
            'CLEAN_TIPEPLGGN': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.TIPEPLGGN', '$CUST_TYPE_MC']}}}}},
        }},
        
        # Filter: Hanya Rayon AB Sunter dan TIPE REG, dan PCEZ bukan N/A
        {'$match': {'CLEAN_RAYON': {'$in': RAYON_KEYS}, 'CLEAN_TIPEPLGGN': 'REG', 'CLEAN_PCEZ': {'$ne': 'N/A'}}},

        # Grouping berdasarkan Rayon dan PCEZ
        {
            '$group': {
                '_id': {
                    'rayon': '$CLEAN_RAYON',
                    'pcez': '$CLEAN_PCEZ'
                },
                'total_mc_nominal': {'$sum': '$JML_TAGIHAN'},
                'total_mc_nomen': {'$addToSet': '$NOMEN'}
            }
        },
        
        # Format output
        {
            '$project': {
                '_id': 0,
                'rayon': '$_id.rayon',
                'pcez': '$_id.pcez',
                'total_mc_nominal': 1,
                'total_mc_nomen': {'$size': '$total_mc_nomen'}
            }
        },
    ]
    
    mc_results = list(collection_mc.aggregate(mc_pipeline, allowDiskUse=True))

    # 2. Pipeline untuk Master Bayar (MB) - Total Koleksi Undue Bulan Berjalan
    mb_pipeline = [
        # Filter MB untuk BULAN_REK target (Undue)
        {'$match': {'BULAN_REK': bulan_tagihan_target, 'BILL_REASON': 'BIAYA PEMAKAIAN AIR'}},
        
        # Ekstraksi Rayon/PCEZ dari MB (Fallback jika CID join gagal)
        {"$project": {
            "NOMEN": 1,
            "NOMINAL": {"$toDouble": {"$ifNull": ["$NOMINAL", 0]}},
            "NOTAGIHAN": 1,
            "RAYON_MB": "$RAYON",
            "PCEZ_MB": "$PCEZ",
        }},
        
        # Join dengan CustomerData (CID) untuk mendapatkan Rayon dan PCEZ
        {
            '$lookup': {
                'from': 'CustomerData',
                'localField': 'NOMEN',
                'foreignField': 'NOMEN',
                'pipeline': [
                    {'$sort': {'TANGGAL_UPLOAD_CID': -1}}, 
                    {'$limit': 1},
                    {'$project': {'_id': 0, 'PCEZ': 1, 'RAYON': 1, 'TIPEPLGGN': 1}},
                ],
                'as': 'customer_info'
            }
        },
        {'$unwind': {'path': '$customer_info', 'preserveNullAndEmptyArrays': True}},
        
        # Finalisasi Rayon/PCEZ dan filter TIPEPLGGN REG
        {'$addFields': {
            # Prioritaskan CID, fallback ke MB data
            'CLEAN_RAYON': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.RAYON', '$RAYON_MB']}}}}},
            'CLEAN_PCEZ': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.PCEZ', '$PCEZ_MB']}}}}},
            'CLEAN_TIPEPLGGN': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.TIPEPLGGN', 'REG']}}}},}
        }},
        
        # Filter: Hanya Rayon AB Sunter dan TIPE REG, dan PCEZ bukan N/A
        {'$match': {'CLEAN_RAYON': {'$in': RAYON_KEYS}, 'CLEAN_TIPEPLGGN': 'REG', 'CLEAN_PCEZ': {'$ne': 'N/A'}}},

        # Grouping berdasarkan Rayon dan PCEZ
        {
            '$group': {
                '_id': {
                    'rayon': '$CLEAN_RAYON',
                    'pcez': '$CLEAN_PCEZ'
                },
                'total_mb_undue_nominal': {'$sum': '$NOMINAL'},
                'total_mb_transaksi': {'$addToSet': '$NOTAGIHAN'} # Hitung jumlah transaksi unik
            }
        },
        
        # Rename field hasil grouping
        {
            '$project': {
                '_id': 0,
                'rayon': '$_id.rayon',
                'pcez': '$_id.pcez',
                'total_mb_undue_nominal': 1,
                'total_mb_transaksi': {'$size': '$total_mb_transaksi'}
            }
        },
    ]
    
    mb_results = list(collection_mb.aggregate(mb_pipeline, allowDiskUse=True))

    # 3. Gabungkan Hasil (Merge) di Python
    mc_map = {
        (res['rayon'], res['pcez']): {
            'mc_nominal': res['total_mc_nominal'],
            'mc_nomen': res['total_mc_nomen']
        }
        for res in mc_results
    }
    
    final_report = []
    
    # Gabungkan data MB ke MC map
    for mb_res in mb_results:
        key = (mb_res['rayon'], mb_res['pcez'])
        # Pop untuk menandai bahwa item MC telah diproses
        mc_data = mc_map.pop(key, {'mc_nominal': 0.0, 'mc_nomen': 0}) 
        
        mc_nominal = mc_data['mc_nominal']
        mb_nominal = mb_res['total_mb_undue_nominal']
        
        persentase_koleksi = 0.00
        if mc_nominal > 0:
            persentase_koleksi = (mb_nominal / mc_nominal) * 100
            
        final_report.append({
            'rayon': mb_res['rayon'],
            'pcez': mb_res['pcez'],
            'mc_nominal': mc_nominal,
            'mc_nomen': mc_data['mc_nomen'],
            'mb_undue_nominal': mb_nominal,
            'mb_transaksi': mb_res['total_mb_transaksi'],
            'persentase_koleksi': round(persentase_koleksi, 2)
        })

    # Tambahkan sisa data MC yang belum ada koleksi
    for key, mc_data in mc_map.items():
        mc_nominal = mc_data['mc_nominal']
        
        final_report.append({
            'rayon': key[0],
            'pcez': key[1],
            'mc_nominal': mc_nominal,
            'mc_nomen': mc_data['mc_nomen'],
            'mb_undue_nominal': 0.00,
            'mb_transaksi': 0,
            'persentase_koleksi': 0.00
        })

    # Urutkan berdasarkan Rayon dan PCEZ
    final_report.sort(key=lambda x: (x['rayon'], x['pcez']))
    
    return final_report

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

# --- ENDPOINT KOLEKSI DAN ANALISIS LAINNYA (NAVIGASI BARU) ---

# Rute Hub Koleksi (Menggantikan collection_unified.html)
@app.route('/collection', methods=['GET'])
@login_required 
def collection_landing_page():
    return render_template('collection_landing.html', is_admin=current_user.is_admin)

# Sub-Rute Koleksi (Untuk Halaman Ringkasan/Monitoring/Analisis)
@app.route('/collection/summary', methods=['GET'])
@login_required 
def collection_summary():
    return render_template('collection_summary.html', is_admin=current_user.is_admin)

@app.route('/collection/monitoring', methods=['GET'])
@login_required 
def collection_monitoring():
    return render_template('collection_monitoring.html', is_admin=current_user.is_admin)

# Rute Hub Analisis (Menggantikan analyze_reports_landing)
@app.route('/collection/analysis', methods=['GET'])
@login_required 
def collection_analysis():
    # collection_analysis.html kini adalah hub untuk sub-laporan di bawah ini
    return render_template('collection_analysis.html', is_admin=current_user.is_admin)

# Rute-Rute Baru untuk Halaman Laporan Analisis Spesifik (Dipanggil dari collection_analysis.html)
@app.route('/analysis/tarif', methods=['GET'])
@login_required 
def analysis_tarif_breakdown():
    return render_template('analysis_report_template.html', 
                            title="Distribusi Tarif Pelanggan (R34/R35)",
                            description="Laporan detail Distribusi Tarif Nomen, Piutang, dan Kubikasi per Rayon/Tarif. (Memuat chart dan tabel)",
                            report_type="TARIF_BREAKDOWN", # <--- KUNCI PENTING
                            is_admin=current_user.is_admin)

@app.route('/analysis/grouping', methods=['GET'])
@login_required 
def analysis_grouping_sunter():
    return render_template('analysis_report_template.html', 
                            title="Grouping MC: AB Sunter Detail",
                            description="Laporan agregasi Nomen, Nominal, dan Kubikasi berdasarkan Tarif, Merek, dan Metode Baca untuk R34/R35.",
                            report_type="MC_GROUPING_AB_SUNTER", # <--- KUNCI PENTING
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
                            report_type="TOP_LISTS", # <--- KUNCI PENTING
                            is_admin=current_user.is_admin)

@app.route('/analysis/volume', methods=['GET'])
@login_required 
def analysis_volume_dasar():
    return render_template('analysis_report_template.html', 
                            title="Laporan Volume Dasar Historis",
                            description="Riwayat volume KUBIK bulanan agregat berdasarkan Rayon dari seluruh data Master Cetak (MC).",
                            report_type="BASIC_VOLUME",
                            is_admin=current_user.is_admin)

# --- RUTE BARU UNTUK PERBANDINGAN MC vs MB (Sesuai Permintaan) ---
@app.route('/api/collection/comparison/rayon_pcez', methods=['GET'])
@login_required
def collection_comparison_rayon_pcez_api():
    """Endpoint API untuk data perbandingan MC vs MB Undue per Rayon/PCEZ."""
    
    if client is None:
        return jsonify({'error': 'Server tidak terhubung ke Database.'}), 500

    # Tentukan Bulan Tagihan Target (Bulan Terbaru)
    try:
        latest_mc = collection_mc.find_one({}, sort=[('BULAN_TAGIHAN', -1)])
        bulan_target = latest_mc['BULAN_TAGIHAN'] if latest_mc else None
    except Exception as e:
        print(f"Error fetching latest BULAN_TAGIHAN: {e}")
        return jsonify({'error': 'Gagal mengambil bulan tagihan terbaru.'}), 500
        
    if not bulan_target:
        return jsonify({'error': 'Tidak ada data Master Cetak (MC) ditemukan.'}), 404

    try:
        comparison_data = get_mc_mb_comparison_by_pcez(bulan_target)
        
        # Mengubah format bulan untuk tampilan
        formatted_month = datetime.strptime(bulan_target, '%m%Y').strftime('%b %Y')
        
        return jsonify({
            'bulan_tagihan': formatted_month,
            'data': comparison_data
        })
    except Exception as e:
        print(f"Error generating comparison report: {e}")
        return jsonify({'error': f'Gagal memproses data perbandingan: {e}'}), 500

@app.route('/collection/report/comparison_rayon_pcez')
@login_required
def collection_comparison_rayon_pcez_view():
    """Rute view untuk halaman laporan perbandingan MC vs MB per Rayon/PCEZ."""
    # Menggunakan template analysis_report_template.html yang bersifat universal
    return render_template(
        'analysis_report_template.html',
        title="Perbandingan MC vs MB (Undue) per Rayon/PCEZ",
        description="Detail Piutang (MC) Bulan Berjalan vs Koleksi (MB) Belum Jatuh Tempo, dikelompokkan per Rayon dan PCEZ.",
        report_type="MC_MB_COMPARE_PCEZ", # Kunci untuk JavaScript memanggil API yang benar
        is_admin=current_user.is_admin
    )
# --- END RUTE BARU ---

# --- HELPER BARU: HITUNG BULAN SEBELUMNYA ---
def _get_previous_month_year(bulan_tagihan):
    """Mengubah format 'MMYYYY' menjadi 'MMYYYY' bulan sebelumnya."""
    if not bulan_tagihan or len(bulan_tagihan) != 6:
        return None
    try:
        # Asumsi format input adalah MMYYYY
        month = int(bulan_tagihan[:2])
        year = int(bulan_tagihan[2:])
        
        # Go back one month
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
        
        # Calculate next month to get the end date (exclusive)
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

# --- NEW HELPER: HITUNG BULAN N BULAN SEBELUMNYA (Robust) ---
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
    # Calculate target month and year by iterating n times
    for _ in range(n):
        # Subtract one month (by subtracting one day from the 1st of the month)
        target_dt = target_dt.replace(day=1) - timedelta(days=1)
        
    return target_dt.strftime('%m%Y')
# --- END HELPER BARU ---

# =========================================================================
# === START FUNGSI BARU UNTUK LAPORAN DISTRIBUSI (Lanjutan) ===
# =========================================================================

def _get_distribution_report(group_fields, collection_mc):
    """
    Menghitung distribusi Nomen (Count Distinct), Piutang (NOMINAL), dan Kubikasi (KUBIK) 
    berdasarkan field yang diberikan untuk BULAN_TAGIHAN terbaru.
    """
    if collection_mc is None:
        return [], "N/A (Koneksi DB Gagal)"
        
    if isinstance(group_fields, str):
        group_fields = [group_fields]

    # Mencari BULAN_TAGIHAN terbaru
    latest_mc_month_doc = collection_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
    latest_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None
    
    if not latest_month:
        return [], "N/A (Tidak Ada Data MC)"


    # Pipeline MongoDB Aggregation
    pipeline = [
        # 1. Filter data untuk bulan tagihan terbaru saja
        {"$match": {"BULAN_TAGIHAN": latest_month}},
        # 2. Project dan konversi tipe data yang diperlukan, serta ekstraksi ZONA_NOVAK
        {"$project": {
            **{field: f"${field}" for field in group_fields},
            "NOMEN": 1,
            "NOMINAL": {"$toDouble": {"$ifNull": ["$NOMINAL", 0]}},
            "KUBIK": {"$toDouble": {"$ifNull": ["$KUBIK", 0]}},
            # --- EKSTRAKSI ZONA_NOVAK BARU ---
            "CLEAN_ZONA": {"$trim": {"input": {"$ifNull": ["$ZONA_NOVAK", ""]}}},
        }},
        {"$addFields": {
            "RAYON": {"$substrCP": ["$CLEAN_ZONA", 0, 2]}, # Index 0, Length 2
            "PC": {"$substrCP": ["$CLEAN_ZONA", 2, 3]},    # Index 2, Length 3
            "EZ": {"$substrCP": ["$CLEAN_ZONA", 5, 2]},    # Index 5, Length 2
            "BLOCK": {"$substrCP": ["$CLEAN_ZONA", 7, 2]},  # Index 7, Length 2
            # Perbaikan: Menggunakan '/' di PCEZ sesuai format kebutuhan
            "PCEZ": {"$concat": [{"$substrCP": ["$CLEAN_ZONA", 2, 3]}, {"$literal": "/"}, {"$substrCP": ["$CLEAN_ZONA", 5, 2]}]}
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
            # Memisahkan field grouping dari _id
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

# --- 1. API Distribusi Rayon ---
@app.route("/api/distribution/rayon_report")
@login_required
def rayon_distribution_report():
    """Laporan Distribusi Nomen, Piutang, dan Kubikasi per Rayon."""
    # NOTE: Menggunakan field 'RAYON' yang diekstrak dari ZONA_NOVAK
    results, latest_month = _get_distribution_report(group_fields="RAYON", collection_mc=collection_mc)
    
    data_for_display = []
    for item in results:
        data_for_display.append({
            "RAYON": item.get("RAYON", "N/A"),
            "Jumlah Nomen": f"{item['total_nomen']:,.0f}",
            "Total Piutang (Rp)": f"Rp {item['total_piutang']:,.0f}",
            "Total Kubikasi (m³)" : f"{item['total_kubikasi']:,.0f}",
            # Data Mentah untuk Chart (agar mudah diproses JS)
            "chart_label": item.get("RAYON", "N/A"),
            "chart_data_nomen": item['total_nomen'],
            "chart_data_piutang": round(item['total_piutang'], 2),
        })

    return jsonify({
        "data": data_for_display,
        "title": f"Distribusi Pelanggan per Rayon (dari ZONA_NOVAK)",
        "subtitle": f"Data Piutang & Kubikasi per Bulan Tagihan Terbaru: {latest_month}",
    })

# --- 2. API Distribusi PCEZ ---
@app.route("/api/distribution/pcez_report")
@login_required
def pcez_distribution_report():
    """Laporan Distribusi Nomen, Piutang, dan Kubikasi per PCEZ."""
    # NOTE: Menggunakan field 'PCEZ' yang diekstrak dari ZONA_NOVAK
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

# --- 3. API Distribusi Rayon/Tarif ---
@app.route("/api/distribution/rayon_tarif_report")
@login_required
def rayon_tarif_distribution_report():
    """Laporan Distribusi Nomen, Piutang, dan Kubikasi per Rayon dan Tarif."""
    # NOTE: Menggunakan field 'RAYON' yang diekstrak dari ZONA_NOVAK dan 'TARIF' dari CID/MC
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

# --- 4. API Distribusi Rayon/Jenis Meter ---
@app.route("/api/distribution/rayon_meter_report")
@login_required
def rayon_meter_distribution_report():
    """Laporan Distribusi Nomen, Piutang, dan Kubikasi per Rayon dan Jenis Meter."""
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


# --- RUTE HTML VIEW BARU UNTUK LAPORAN DISTRIBUSI ---
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

# =========================================================================
# === END FUNGSI BARU UNTUK LAPORAN DISTRIBUSI ===
# =========================================================================

# =========================================================================
# === HELPER AGGREGATE MB SUNTER DETAIL (MODIFIED) ===
# =========================================================================
def _aggregate_mb_sunter_detail(collection_mb):
    """
    Menghitung agregasi koleksi (Undue, Current, Tunggakan) berdasarkan
    definisi Aging yang baru, menggunakan BULAN_TAGIHAN MC terbaru (M) sebagai patokan.
    """
    if collection_mb is None:
        return {"status": "error", "message": "Database connection failed."}

    # 1. TENTUKAN PERIODE DINAMIS
    # Ambil Bulan Tagihan MC terbaru (M)
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
            # Memastikan TGL_BAYAR berada dalam bulan yang sama dengan Bulan Tagihan MC terbaru (latest_mc_month)
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
        pipeline = [
            {'$match': {
                'BILL_REASON': 'BIAYA PEMAKAIAN AIR',
                # Filter TGL_BAYAR: Bulan M (sesuai COLLECTION_MONTH_START/END)
                'TGL_BAYAR': {'$gte': COLLECTION_MONTH_START, '$lt': COLLECTION_MONTH_END}, 
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
    # Bayar bulan adalah bulan MC terbaru (M)
    bayar_bulan_fmt = datetime.strptime(latest_mc_month, '%m%Y').strftime('%b %Y').upper()

    return {
        'status': 'success',
        'periods': {
            'bayar_bulan': bayar_bulan_fmt, 
            'undue_rek': latest_mc_month,
            'current_rek': M_MINUS_1_REK,
            'aging_rek_max': f"<{M_MINUS_1_REK}", # Tunggakan: < M-1 (semua sebelum current)
        },
        'summary': summary_data,
        'daily_detail': daily_detail
    }
# =========================================================================
# === END HELPER AGGREGATE MB SUNTER DETAIL ===
# =========================================================================
    
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
                            report_type="MB_SUNTER_DETAIL", # <--- KUNCI PENTING BARU
                            is_admin=current_user.is_admin)

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

    previous_mc_month = _get_previous_month_year(latest_mc_month)
    
    # Filter MC hanya untuk bulan terbaru
    mc_filter = {'BULAN_TAGIHAN': latest_mc_month}
    
    # Kunci untuk Proyeksi Awal (Digunakan di Pipeline Billed dan Collected)
    initial_project = {
        '$project': {
            # PERBAIKAN KRITIS: Memastikan RAYON dan PCEZ dinormalisasi di awal
            'RAYON': {'$toUpper': {'$trim': {'input': { '$ifNull': [ '$RAYON', 'N/A' ] }}}},
            'PCEZ': {'$toUpper': {'$trim': {'input': { '$ifNull': [ '$PCEZ', 'N/A' ] }}}},
            'NOMEN': 1,
            'NOMINAL': {'$toDouble': {'$ifNull': ['$NOMINAL', 0]}}, 
            'KUBIK': {'$toDouble': {'$ifNull': ['$KUBIK', 0]}},
            # PERBAIKAN KRITIS: Normalisasi STATUS di awal proyeksi
            'STATUS_CLEAN': {'$toUpper': {'$trim': {'input': {'$ifNull': ['$STATUS', 'N/A']}}}}, 
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
        { '$match': { 'STATUS_CLEAN': 'PAYMENT' } }, # Menggunakan STATUS_CLEAN
        { '$group': {
            '_id': { 'rayon': '$RAYON', 'pcez': '$PCEZ' },
            'collected_nomen': { '$addToSet': '$NOMEN' }, 
            'collected_nominal': { '$sum': '$NOMINAL' },
            'collected_kubik': { '$sum': '$KUBIK' } # Sum of Collected Kubik
        }}
    ]
    collected_data = list(collection_mc.aggregate(pipeline_collected))

    # 3. MB (UNDUE BULAN INI) - MB yang BULAN_REK sama dengan bulan tagihan MC terbaru
    pipeline_mb_undue = [
        { '$match': { 
            'BULAN_REK': latest_mc_month, # Filter bulan tagihan (BILL_PERIOD)
            'BILL_REASON': 'BIAYA PEMAKAIAN AIR'
        }},
        { '$project': {
            'NOMEN': 1,
            'NOMINAL': {'$toDouble': {'$ifNull': ['$NOMINAL', 0]}}, 
            'KUBIKBAYAR': {'$toDouble': {'$ifNull': ['$KUBIKBAYAR', 0]}}, 
            'RAYON_MB': { '$toUpper': { '$trim': { 'input': { '$ifNull': [ '$RAYON', 'N/A' ] } } } },
            'PCEZ_MB': { '$toUpper': { '$trim': { 'input': { '$ifNull': [ '$PCEZ', 'N/A' ] } } } },
        }},
        # Lookup ke CID untuk memastikan Rayon/PCEZ yang digunakan konsisten dengan CID/MC
        {'$lookup': {
           'from': 'CustomerData', 
           'localField': 'NOMEN',
           'foreignField': 'NOMEN',
           'as': 'customer_info'
        }},
        {'$unwind': {'path': '$customer_info', 'preserveNullAndEmptyArrays': True}},
        { '$project': {
             # Prioritaskan CID, fallback ke MB data
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

    # 4. MB (UNDUE BULAN SEBELUMNYA) - Transaksi MB dari bulan lalu untuk Koleksi %
    pipeline_mb_undue_prev = [
        { '$match': { 
            'BULAN_REK': previous_mc_month, # Filter bulan tagihan bulan sebelumnya
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
             # Ciptakan entry baru jika Rayon/PCEZ hanya ada di MB Undue
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
        # Nilai baru
        'TotalUnduePrevNominal': total_undue_prev_nominal
    }
    
    # Simpan total MC (denominator untuk Monitoring)
    total_mc_nominal_all = grand_total['MC_TotalNominal']

    for key, data in report_map.items():
        data.setdefault('MB_UndueNominal', 0.0)
        data.setdefault('MB_UndueKubik', 0.0)
        data.setdefault('MB_UndueNomen', 0)
        
        # Persentase Koleksi MC vs Piutang MC Bulan Ini (lama)
        data['PercentNominal'] = (data['MC_CollectedNominal'] / data['MC_TotalNominal']) * 100 if data['MC_TotalNominal'] > 0 else 0
        # Persentase MB Undue Bulan Ini vs Piutang MC Bulan Ini (lama)
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
        'grand_total': grand_total,
        'total_mc_nominal_all': total_mc_nominal_all 
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
                {'BILL_REASON': {'$regex': safe_query_str}}, # Tambahkan filter BILL_REASON
                {'BULAN_REK': {'$regex': safe_query_str}} # Tambahkan BULAN_REK ke pencarian
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
            bulan_rek = doc.get('BULAN_REK', 'N/A')
            
            # Perbandingan IS_UNDUE menggunakan BULAN_REK MB dan BULAN_TAGIHAN MC terbaru
            is_undue = bulan_rek == latest_mc_month
            
            cleaned_results.append({
                'NOMEN': doc.get('NOMEN', 'N/A'),
                'RAYON': doc.get('RAYON', doc.get('ZONA_NOREK', 'N/A')), 
                'PCEZ': doc.get('PCEZ', doc.get('LKS_BAYAR', 'N/A')),
                'NOMINAL': nominal_val,
                'KUBIKBAYAR': kubik_val, # NEW
                'PAY_DT': pay_dt,
                'BULAN_REK': bulan_rek, # Tambahkan BULAN_REK untuk debugging
                'BILL_REASON': doc.get('BILL_REASON', 'N/A'), # Tambahkan BILL_REASON
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
        df_grand_total = df_grand_total.drop(columns=['MC_TotalNomen', 'MC_CollectedNomen', 'MB_UndueNomen', 'TotalPelanggan', 'TotalUnduePrevNominal'], errors='ignore')
        
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


# --- ENDPOINT ANALISIS DATA LANJUTAN (MENU NAVIGASI ANALISIS LAMA) ---

# Rute Peringatan Anomali (LAMA) - Mengarah ke template umum
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
                             description="Menampilkan pelanggan dengan konsumsi air di atas ambang batas (memerlukan join MC, CID, dan SBRS) dan fluktuasi signifikan.",
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

@app.route('/analyze/zero', methods=['GET'])
@login_required
def analyze_zero_usage():
    return render_template('analyze_report_template.html', 
                             title="Tidak Ada Pemakaian (Zero)", 
                             description="Menampilkan pelanggan dengan konsumsi air nol (Zero) di periode tagihan terakhir.",
                             report_type="ZERO_USAGE",
                             is_admin=current_user.is_admin)

@app.route('/analyze/standby', methods=['GET'])
@login_required
def analyze_stand_tungggu():
    return render_template('analysis_report_template.html', 
                             title="Stand Tunggu", 
                             description="Menampilkan pelanggan yang berstatus Stand Tunggu (Freeze/Blokir).",
                             report_type="STANDBY_STATUS",
                             is_admin=current_user.is_admin)

# =========================================================================
# === API GROUPING MC KUSTOM (HELPER FUNCTION) ===
# =========================================================================

def _aggregate_custom_mc_report(collection_mc, collection_cid, dimension=None, rayon_filter=None):
    """
    Menghitung Piutang Kustom (Rayon 34/35 REG) untuk laporan grup (mis. Grouping MC).
    Diperbaiki: Menggunakan preserveNullAndEmptyArrays=True dan fallback CUST_TYPE MC.
    """
    
    # FIX: Ambil BULAN_TAGIHAN terbaru dari MC Historis
    latest_mc_month_doc = collection_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
    latest_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None
    
    if not latest_month:
        return {'CountOfNOMEN': 0, 'SumOfKUBIK': 0, 'SumOfNOMINAL': 0}

    dimension_map = {'TARIF': '$TARIF_CID', 'MERK': '$MERK_CID', 'READ_METHOD': '$READ_METHOD'}
    
    # Base pipeline structure (Projection and CID Join for all necessary fields)
    pipeline = [
        {'$match': {'BULAN_TAGIHAN': latest_month}}, # HANYA AMBIL MC BULAN TERBARU
        {"$project": {
            # Kolom Piutang/Kubik
            "NOMEN": "$NOMEN",
            "NOMINAL": {"$toDouble": {"$ifNull": ["$NOMINAL", 0]}},
            "KUBIK": {"$toDouble": {"$ifNull": ["$KUBIK", 0]}},
            "CUST_TYPE_MC": "$CUST_TYPE", # <-- DITAMBAHKAN: Field CUST_TYPE dari MC
            # --- EKSTRAKSI ZONA_NOVAK BARU ---
            "CLEAN_ZONA": {"$trim": {"input": {"$ifNull": ["$ZONA_NOVAK", ""]}}},
        }},
        {"$addFields": {
            "RAYON_ZONA": {"$substrCP": ["$CLEAN_ZONA", 0, 2]}, # Index 0, Length 2
            "PC_ZONA": {"$substrCP": ["$CLEAN_ZONA", 2, 3]},    # Index 2, Length 3
            "EZ_ZONA": {"$substrCP": ["$CLEAN_ZONA", 5, 2]},    # Index 5, Length 2
            "BLOCK_ZONA": {"$substrCP": ["$CLEAN_ZONA", 7, 2]},  # Index 7, Length 2
            "PCEZ_ZONA": {"$concat": ["$PC_ZONA", "$EZ_ZONA"]} # PC + EZ
        }},
        {'$lookup': {'from': 'CustomerData', 'localField': 'NOMEN', 'foreignField': 'NOMEN', 'as': 'customer_info'}},
        # PERBAIKAN KRITIS: preserveNullAndEmptyArrays=True agar data MC tidak hilang jika CID tidak match
        {'$unwind': {'path': '$customer_info', 'preserveNullAndEmptyArrays': True}}, 
        {'$addFields': {
            'CLEAN_TIPEPLGGN': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.TIPEPLGGN', '$CUST_TYPE_MC']}}}}}, # Fallback ke MC CUST_TYPE
            'CLEAN_RAYON': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.RAYON', '$RAYON_ZONA']}}}}}, # Menggunakan Rayon dari ZONA jika CID hilang
            'TARIF_CID': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.TARIF', 'N/A']}}}}}, 
            'MERK_CID': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.MERK', 'N/A']}}}}},
            'READ_METHOD': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.READ_METHOD', 'N/A']}}}}},
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
        
        # NOTE: Agregasi ini SANGAT berat. Waktu respons mungkin lambat (bisa >30 detik) tergantung ukuran data.
        return jsonify(response_data), 200

    except Exception as e:
        print(f"Error saat menganalisis custom grouping MC: {e}")
        return jsonify({"status": "error", "message": f"Gagal mengambil data grouping MC: {e}"}), 500

# 2. API SUMMARY (Untuk KPI Cards di collection_summary.html)
@app.route('/api/analyze/mc_grouping/summary', methods=['GET'])
@login_required 
def analyze_mc_grouping_summary_api():
    """
    Menghitung Piutang Kustom (Rayon 34/35 REG) untuk kartu KPI.
    Diperbaiki: Menggunakan preserveNullAndEmptyArrays=True dan fallback CUST_TYPE MC.
    """
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
             {"$project": {
                # Kolom Piutang/Kubik
                "NOMEN": "$NOMEN",
                "NOMINAL": {"$toDouble": {"$ifNull": ["$NOMINAL", 0]}},
                "KUBIK": {"$toDouble": {"$ifNull": ["$KUBIK", 0]}},
                "CUST_TYPE_MC": "$CUST_TYPE", # <-- DITAMBAHKAN
                # --- EKSTRAKSI ZONA_NOVAK BARU ---
                "CLEAN_ZONA": {"$trim": {"input": {"$ifNull": ["$ZONA_NOVAK", ""]}}},
            }},
            {"$addFields": {
                "RAYON_ZONA": {"$substrCP": ["$CLEAN_ZONA", 0, 2]}, # Index 0, Length 2
            }},
            {'$lookup': {
               'from': 'CustomerData', 
               'localField': 'NOMEN',
               'foreignField': 'NOMEN',
               'as': 'customer_info'
            }},
            # PERBAIKAN KRITIS: preserveNullAndEmptyArrays=True
            {'$unwind': {'path': '$customer_info', 'preserveNullAndEmptyArrays': True}},
            
            # --- NORMALISASI DATA UNTUK FILTER ---
            {'$addFields': {
                'CLEAN_TIPEPLGGN': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.TIPEPLGGN', '$CUST_TYPE_MC']}}}}}, # Fallback ke MC CUST_TYPE
                'CLEAN_RAYON': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.RAYON', '$RAYON_ZONA']}}}}}, # Menggunakan Rayon dari ZONA jika CID hilang
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

# 3. API BREAKDOWN TARIF (Untuk Tabel Distribusi di collection_analysis.html)
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
            # 1. Project dan Ekstraksi ZONA_NOVAK
            {"$project": {
                "NOMEN": 1, "RAYON": 1, "TARIF": 1,
                "CLEAN_ZONA": {"$trim": {"input": {"$ifNull": ["$ZONA_NOVAK", ""]}}},
                "CUST_TYPE_MC": "$CUST_TYPE", # <-- DITAMBAHKAN
            }},
            {"$addFields": {
                "RAYON_ZONA": {"$substrCP": ["$CLEAN_ZONA", 0, 2]}, # Index 0, Length 2
            }},
            # 2. Join MC ke CID
            {'$lookup': {
               'from': 'CustomerData', 
               'localField': 'NOMEN',
               'foreignField': 'NOMEN',
               'as': 'customer_info'
            }},
            {'$unwind': {'path': '$customer_info', 'preserveNullAndEmptyArrays': True}}, # Diperbaiki
            
            # --- NORMALISASI DATA UNTUK FILTER ---
            {'$addFields': {
                'CLEAN_TIPEPLGGN': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.TIPEPLGGN', '$CUST_TYPE_MC']}}}}}, # Fallback ke MC CUST_TYPE
                'CLEAN_RAYON': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.RAYON', '$RAYON_ZONA']}}}}}, # Menggunakan Rayon dari ZONA jika CID hilang
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
# === API MONITORING KOLEKSI HARIAN (PERBAIKAN PANDAS) ===
# =========================================================================

@app.route('/api/collection/monitoring', methods=['GET'])
@login_required
def collection_monitoring_api():
    """
    Menghasilkan data harian, kumulatif, dan persentase koleksi berdasarkan Rayon (34 & 35).
    Metrik Koleksi (Rp1): Total Nominal Per Hari dengan filter BULAN_REK bulan lalu.
    Persentase: (Rp1 Kumulatif + Total Undue Bulan Sebelumnya) / Total Piutang MC.
    """
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500
        
    try:
        # Tentukan Bulan Tagihan MC Terbaru (Bulan Piutang)
        latest_mc_month_doc = collection_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
        latest_mc_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None
        
        if not latest_mc_month:
             # Default fallback jika tidak ada data MC sama sekali
            empty_summary = {'R34': {'MC1125': 0, 'CURRENT': 0}, 'R35': {'MC1125': 0, 'CURRENT': 0}, 'GLOBAL': {'TotalPiutangMC': 0, 'TotalUnduePrev': 0, 'CurrentKoleksiTotal': 0, 'TotalKoleksiPersen': 0}}
            return jsonify({'monitoring_data': {'R34': [], 'R35': []}, 'summary_top': empty_summary}), 200

        # Tentukan Bulan Tagihan MC Sebelumnya (Untuk Koleksi CURRENT dan Total UNDUE Bulan Lalu)
        previous_mc_month = _get_previous_month_year(latest_mc_month)
        
        if not previous_mc_month:
            # Jika tidak bisa menghitung bulan lalu, kembali menggunakan bulan ini
            previous_mc_month = latest_mc_month

        # 1. Hitung Total Piutang MC (Denominator) dari bulan tagihan terbaru
        # PERBAIKAN: Memastikan filter 'REG' dan Rayon '34'/'35' diterapkan pada denominator Piutang MC
        mc_total_response = collection_mc.aggregate([
            {'$match': {'BULAN_TAGIHAN': latest_mc_month}},
            {"$project": {
                "NOMEN": 1, "NOMINAL": {"$toDouble": {"$ifNull": ["$NOMINAL", 0]}},
                "CLEAN_ZONA": {"$trim": {"input": {"$ifNull": ["$ZONA_NOVAK", ""]}}},
                "CUST_TYPE_MC": "$CUST_TYPE", # <-- DITAMBAHKAN
            }},
            {"$addFields": {
                "RAYON_ZONA": {"$substrCP": ["$CLEAN_ZONA", 0, 2]}, # Index 0, Length 2
            }},
            {'$lookup': {'from': 'CustomerData', 'localField': 'NOMEN', 'foreignField': 'NOMEN', 'as': 'customer_info'}},
            {'$unwind': {'path': '$customer_info', 'preserveNullAndEmptyArrays': False}},
            {'$addFields': {'CLEAN_RAYON': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.RAYON', '$RAYON_ZONA']}}}}},
             'CLEAN_TIPEPLGGN': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$customer_info.TIPEPLGGN', '$CUST_TYPE_MC']}}}}},}},
            
            # FILTER KRITIS: Hanya Piutang AB Sunter (R34/R35) dan tipe REG
            {'$match': {'CLEAN_RAYON': {'$in': ['34', '35']}, 'CLEAN_TIPEPLGGN': 'REG'}},
            
            {'$group': {'_id': '$CLEAN_RAYON', 'TotalPiutang': {'$sum': {'$toDouble': {'$ifNull': ['$NOMINAL', 0]}}}}}
        ], allowDiskUse=True)
        
        mc_totals = {doc['_id']: doc['TotalPiutang'] for doc in mc_total_response}
        total_mc_34 = mc_totals.get('34', 0)
        total_mc_35 = mc_totals.get('35', 0)
        
        # Total Piutang MC Keseluruhan (Denominator Persentase Koleksi)
        total_mc_nominal_all = total_mc_34 + total_mc_35
        
        # 2. Hitung Total UNDUE Bulan Sebelumnya (Baseline Collection)
        # Definition: MB collected IN month M-1 (TGL_BAYAR) where BILL_REK is month M-1 (UNDUE)
        prev_month_start_date, prev_month_end_date = _get_month_date_range(previous_mc_month)

        pipeline_undue_prev = [
            { '$match': { 
                'BULAN_REK': previous_mc_month, 
                'BILL_REASON': 'BIAYA PEMAKAIAN AIR',
                # 🚨 PERBAIKAN KRITIS: Filter TGL_BAYAR harus dalam rentang bulan M-1
                'TGL_BAYAR': {'$gte': prev_month_start_date, '$lt': prev_month_end_date} 
            }},
            { '$group': {
                '_id': None,
                'TotalUnduePrev': { '$sum': {'$toDouble': {'$ifNull': ['$NOMINAL', 0]}} },
            }}
        ]
        undue_prev_result = list(collection_mb.aggregate(pipeline_undue_prev))
        total_undue_prev_nominal = undue_prev_result[0]['TotalUnduePrev'] if undue_prev_result else 0.0

        # 3. Ambil Data Transaksi MB (Koleksi) Harian (Rp1)
        
        # Koleksi Rp1 (CURRENT) Dihitung dari transaksi MB yang TGL_BAYAR nya bulan ini, 
        # TAPI BULAN_REK-nya adalah bulan lalu (Piutang Lama)
        now = datetime.now()
        this_month_start = now.strftime('%Y-%m-01')
        # Hitung tanggal satu hari setelah bulan ini
        next_month_start = (now.replace(day=1) + timedelta(days=32)).replace(day=1).strftime('%Y-%m-%d')
        
        pipeline_mb_daily = [
            {'$match': {
                'TGL_BAYAR': {'$gte': this_month_start, '$lt': next_month_start}, # Filter A: TGL_BAYAR di bulan ini
                'BULAN_REK': previous_mc_month, # Filter B: BULAN_REK bulan lalu
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
            # Pastikan output konsisten bahkan saat kosong
            empty_summary = {'R34': {'MC1125': total_mc_34, 'CURRENT': 0}, 'R35': {'MC1125': total_mc_35, 'CURRENT': 0}, 'GLOBAL': {'TotalPiutangMC': total_mc_nominal_all, 'TotalUnduePrev': total_undue_prev_nominal, 'CurrentKoleksiTotal': 0, 'TotalKoleksiPersen': 0}}
            return jsonify({'monitoring_data': {'R34': [], 'R35': []}, 'summary_top': empty_summary}), 200

        # Pastikan kolom TGL adalah datetime
        df_monitoring['TGL'] = pd.to_datetime(df_monitoring['TGL'])
        df_monitoring = df_monitoring.sort_values(by='TGL')
        
        # Hitung Kumulatif Koleksi Harian (Rp1 Kumulatif)
        df_monitoring['Rp1_Kumulatif'] = df_monitoring.groupby('RAYON')['COLL_NOMINAL'].cumsum()
        df_monitoring['CUST_Kumulatif'] = df_monitoring.groupby('RAYON')['CUST_COUNT'].cumsum()

        # 5. Hitung Persentase Koleksi BARU: (Rp1 Kumulatif Global + Total Undue Bulan Sebelumnya) / Total Piutang MC
        
        # Hitung Kumulatif Harian GLOBAL untuk Rp1
        df_monitoring_global = df_monitoring.groupby('TGL').agg({
            'COLL_NOMINAL': 'sum', 
        }).reset_index()
        df_monitoring_global['Rp1_Kumulatif_Global'] = df_monitoring_global['COLL_NOMINAL'].cumsum()
        
        # Gabungkan Kumulatif Global ke dataframe monitoring utama
        df_monitoring = pd.merge(df_monitoring, 
                                 df_monitoring_global[['TGL', 'Rp1_Kumulatif_Global']], 
                                 on='TGL', 
                                 how='left')

        # Persentase Kumulatif Harian Global (Sesuai Rumus Bisnis)
        # Denominator: TotalPiutangMC (total_mc_nominal_all)
        # Numerator: Rp1 Kumulatif Global + Total Undue Bulan Lalu
        df_monitoring['COLL_Kumulatif_Persen'] = (
            (df_monitoring['Rp1_Kumulatif_Global'] + total_undue_prev_nominal) / total_mc_nominal_all
        ) * 100
        df_monitoring['COLL_Kumulatif_Persen'] = df_monitoring['COLL_Kumulatif_Persen'].fillna(0)

        # Hitung COLL_VAR (Daily Change in Percentage)
        df_monitoring['COLL_VAR'] = df_monitoring.groupby('RAYON')['COLL_Kumulatif_Persen'].diff().fillna(df_monitoring['COLL_Kumulatif_Persen'])
        
        # Bersihkan kolom sementara
        df_monitoring = df_monitoring.drop(columns=['Rp1_Kumulatif_Global'], errors='ignore')
        
        # Format Output
        df_monitoring['TGL'] = df_monitoring['TGL'].dt.strftime('%d/%m/%Y')
        
        # Mapping Rayon 34 -> R34 dan 35 -> R35 untuk output JSON
        df_monitoring['RAYON_OUTPUT'] = 'R' + df_monitoring['RAYON']

        df_r34 = df_monitoring[df_monitoring['RAYON_OUTPUT'] == 'R34'].drop(columns=['RAYON', 'RAYON_OUTPUT']).reset_index(drop=True)
        df_r35 = df_monitoring[df_monitoring['RAYON_OUTPUT'] == 'R35'].drop(columns=['RAYON', 'RAYON_OUTPUT']).reset_index(drop=True)

        summary_r34 = {
            'MC1125': total_mc_34,
            'CURRENT': df_r34['Rp1_Kumulatif'].iloc[-1] if not df_r34.empty else 0
        }
        summary_r35 = {
            'MC1125': total_mc_35,
            'CURRENT': df_r35['Rp1_Kumulatif'].iloc[-1] if not df_r35.empty else 0
        }
        
        # Ringkasan Global
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
# === API PERBANDINGAN KOLEKSI MoM (Month-over-Month) ===
# =========================================================================

@app.route('/api/collection/mom_report', methods=['GET'])
@login_required
def mom_report_api():
    """
    Menghitung perbandingan koleksi (Nominal dan Pelanggan) bulan ini vs bulan lalu.
    (Menggunakan perbandingan DAY-TO-DATE (D-T-D) untuk akurasi di tengah bulan).
    Logika DTD MoM TIDAK diubah, menggunakan seluruh transaksi MB.
    """
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500

    try:
        now = datetime.now()
        
        # Tentukan hari dan tanggal hari ini
        day_of_month = now.day # Misal, 14
        this_month_str = now.strftime('%Y-%m') # Misal, 2025-12
        
        # Hitung bulan lalu
        last_month = now.replace(day=1) - timedelta(days=1)
        last_month_str = last_month.strftime('%Y-%m') # Misal, 2025-11

        # Buat daftar tanggal yang akan dicocokkan (1 sampai hari ini) untuk filtering yang adil (DTD)
        date_pattern = []
        for i in range(1, day_of_month + 1):
            day_str = f'{i:02d}' # 1 menjadi 01, 14 tetap 14
            date_pattern.append(f"{this_month_str}-{day_str}")
            # Pastikan bulan lalu juga memiliki tanggal tersebut (misal: Feb tidak punya tgl 30)
            try:
                datetime.strptime(f"{last_month_str}-{day_str}", '%Y-%m-%d') 
                date_pattern.append(f"{last_month_str}-{day_str}")
            except ValueError:
                pass 
        
        regex_pattern = "|".join(date_pattern)


        # Pipeline untuk mengambil koleksi dari dua bulan terakhir HANYA sampai hari ke-N
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
                report_map[period]['nominal'] = item['TotalNominal']
                report_map[period]['nomen'] = len(item['TotalNomen'])

        # Hitung Persentase Perubahan (MoM)
        def calculate_change(current, previous):
            if previous == 0:
                # Jika bulan lalu 0, perubahan 100% jika bulan ini ada data, atau 0% jika nol
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

        return jsonify({'status': 'success', 'data': final_report}), 200

    except Exception as e:
        print(f"Error saat membuat laporan MoM: {e}")
        return jsonify({"status": 'error', "message": f"Gagal mengambil laporan MoM: {e}"}), 500

# =========================================================================
# === API PERBANDINGAN KOLEKSI DOH (Day-of-the-Month) ===
# =========================================================================

@app.route('/api/collection/doh_comparison_report', methods=['GET'])
@login_required
def doh_comparison_report_api():
    """Menghitung perbandingan koleksi harian (Nominal) Bulan Ini vs Bulan Lalu, per Rayon."""
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500

    try:
        now = datetime.now()
        day_of_month = now.day # Misal: 14
        
        this_month_str = now.strftime('%Y-%m') # Misal, 2025-12
        last_month = now.replace(day=1) - timedelta(days=1)
        last_month_str = last_month.strftime('%Y-%m') # Misal, 2025-11
        
        # 1. Tentukan tanggal perbandingan (DTD)
        date_prefixes = []
        for i in range(1, day_of_month + 1):
            day_str = f'{i:02d}' # 1 menjadi 01, 14 tetap 14
            date_prefixes.append(f"{this_month_str}-{day_str}")
            # Pastikan bulan lalu juga memiliki tanggal tersebut (misal: Feb tidak punya tgl 30)
            try:
                datetime.strptime(f"{last_month_str}-{day_str}", '%Y-%m-%d') 
                date_prefixes.append(f"{last_month_str}-{day_str}")
            except ValueError:
                pass 
        
        regex_pattern = "|".join(date_prefixes)

        # 2. Pipeline Agregasi MoM DtD per Hari dan per Rayon
        pipeline = [
            {'$match': {
                'TGL_BAYAR': {'$regex': f"^({regex_pattern})$"},
                'BILL_REASON': 'BIAYA PEMAKAIAN AIR' 
            }},
            {'$project': {
                'NOMINAL': {'$toDouble': {'$ifNull': ['$NOMINAL', 0]}},
                'RAYON': 1,
                # Ekstrak Hari (DD) dan Periode (YYYY-MM)
                'Day': {'$substr': ['$TGL_BAYAR', 8, 2]},
                'Periode': {'$substr': ['$TGL_BAYAR', 0, 7]}
            }},
            # FIX: Normalisasi RAYON dari MB yang mungkin tidak seragam
            {'$addFields': {
                'CLEAN_RAYON': {'$toUpper': {'$trim': {'input': {'$toString': {'$ifNull': ['$RAYON', 'N/A']}}}}},
            }},
            # Filter hanya Rayon 34 dan 35
            {'$match': {'CLEAN_RAYON': {'$in': ['34', '35']}}},
            
            {'$group': {
                '_id': {'periode': '$Periode', 'day': '$Day', 'rayon': '$CLEAN_RAYON'},
                'DailyNominal': {'$sum': '$NOMINAL'},
            }},
            {'$sort': {'_id.date': 1}}
        ]
        
        raw_data = list(collection_mb.aggregate(pipeline))
        
        # 3. Strukturisasi Data untuk Frontend
        
        # Buat daftar hari (01 hingga day_of_month)
        days = [i for i in range(1, day_of_month + 1)]
        
        # Inisialisasi struktur hasil
        report_structure = {
            'days': days,
            'R34': {last_month_str: [0] * day_of_month, this_month_str: [0] * day_of_month},
            'R35': {last_month_str: [0] * day_of_month, this_month_str: [0] * day_of_month},
            'TOTAL_AB': {last_month_str: [0] * day_of_month, this_month_str: [0] * day_of_month},
        }
        
        # PETA UNTUK NORMALISASI DARI API KE FRONTEND (34 -> R34, 35 -> R35)
        RAYON_MAP = {'34': 'R34', '35': 'R35'}
        
        # Isi data ke dalam struktur
        for item in raw_data:
            day_index = int(item['_id']['day']) - 1
            rayon_raw = item['_id']['rayon'] 
            periode = item['_id']['periode']
            nominal = item['DailyNominal']
            
            # Mapping dari Rayon API ('34', '35') ke kunci Frontend ('R34', 'R35')
            areaKey = RAYON_MAP.get(rayon_raw)

            if areaKey:
                # Isi data ke Rayon spesifik (R34 atau R35)
                if periode in report_structure[areaKey]:
                    report_structure[areaKey][periode][day_index] = nominal
                
                # Agregasi ke TOTAL_AB (gabungan R34 dan R35)
                if periode in report_structure['TOTAL_AB']:
                    report_structure['TOTAL_AB'][periode][day_index] += nominal
        
        # Mengembalikan struktur yang sudah terisi dengan kunci 'R34', 'R35', dan 'TOTAL_AB'
        return jsonify({
            'status': 'success',
            'data': report_structure,
            'periods': {'current': this_month_str, 'last': last_month_str}
        }), 200

    except Exception as e:
        print(f"Error saat membuat laporan DOH comparison: {e}")
        return jsonify({"status": 'error', "message": f"Gagal mengambil laporan DOH comparison: {e}"}), 500

# =========================================================================
# === API PERUBAHAN TARIF PELANGGAN (LANJUTAN) ===
# =========================================================================

def _aggregate_tariff_changes(collection_cid):
    """
    Menjalankan pipeline agregasi untuk menemukan pelanggan yang tarifnya berubah
    dengan membandingkan riwayat TARIF dari data CID yang di-APPEND.
    """
    if collection_cid.count_documents({}) == 0:
        return []

    pipeline_tariff_history = [
        {'$group': {
            '_id': '$NOMEN',
            'history': {
                '$push': {
                    'tanggal': {'$ifNull': ['$TANGGAL_UPLOAD_CID', '1900-01-01 00:00:00']},
                    'tarif': '$TARIF'
                }
            }
        }},
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
        {'$addFields': {
            'latest_tarif': {'$arrayElemAt': ['$sorted_history.tarif', -1]}, 
            'previous_tarif': {
                '$arrayElemAt': [
                    '$sorted_history.tarif',
                    {'$subtract': [{'$size': '$sorted_history.tarif'}, 2]}
                ]
            }
        }},
        {'$match': {
            '$expr': {'$ne': ['$latest_tarif', '$previous_tarif']},
            'previous_tarif': {'$ne': None} 
        }},
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
        return jsonify({"message": f"Gagal mengambil data perubahan tarif: {e}"}), 500

# =========================================================================
# === API LAPORAN TOP LISTS (LANJUTAN) ===
# =========================================================================

# 🚨 PERBAIKAN KRITIS UNTUK MENGHINDARI STUCK LOADING
def _aggregate_top_debt(collection_mc, collection_ardebt, collection_cid):
    """
    Menghitung total kewajiban (Piutang MC + Tunggakan ARDEBT) dan mengembalikan 500 teratas.
    OPTIMASI: Pre-fetch semua data CID untuk menghindari query di dalam loop.
    """
    # 1. Ambil BULAN_TAGIHAN terbaru dari MC Historis
    latest_mc_month_doc = collection_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
    latest_mc_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None
    
    # Pre-fetch semua data CID terbaru (untuk JOIN cepat)
    cid_data_map = {doc['NOMEN']: doc for doc in collection_cid.aggregate([
        {'$sort': {'NOMEN': 1, 'TANGGAL_UPLOAD_CID': -1}},
        {'$group': {
            '_id': '$NOMEN',
            'NAMA': {'$first': '$NAMA'},
            'RAYON': {'$first': '$RAYON'},
            'TARIF': {'$first': '$TARIF'},
        }},
        {'$project': {'_id': 0, 'NOMEN': '$_id', 'NAMA': 1, 'RAYON': 1, 'TARIF': 1}}
    ], allowDiskUse=True)}


    # 2. Agregasi Tunggakan (ARDEBT)
    pipeline_ardebt_total = [
        {'$group': {
            '_id': '$NOMEN',
            'TotalARDEBT': {'$sum': {'$toDouble': {'$ifNull': ['$JUMLAH', 0]}}},
            'TotalPeriodeTunggakan': {'$sum': 1}
        }}
    ]
    ardebt_totals = {doc['_id']: doc for doc in collection_ardebt.aggregate(pipeline_ardebt_total, allowDiskUse=True)}

    # 3. Agregasi Piutang Bulan Ini (MC) - Filter ke bulan tagihan terbaru
    pipeline_mc_piutang = [
        {'$match': {'BULAN_TAGIHAN': latest_mc_month}} if latest_mc_month else {'$match': {'NOMEN': {'$exists': True}}}, 
        {'$group': {
            '_id': '$NOMEN',
            'MC_Piutang': {'$sum': {'$toDouble': {'$ifNull': ['$NOMINAL', 0]}}}
        }}
    ]
    mc_piutang = {doc['_id']: doc['MC_Piutang'] for doc in collection_mc.aggregate(pipeline_mc_piutang, allowDiskUse=True)}

    # 4. Gabungkan, Hitung Total Debt, dan Gunakan CID yang sudah di-fetch
    debt_list = []
    all_nomens = set(mc_piutang.keys()) | set(ardebt_totals.keys())

    for nomen in all_nomens:
        ardebt = ardebt_totals.get(nomen, {})
        mc_val = mc_piutang.get(nomen, 0)
        
        total_ardebt = ardebt.get('TotalARDEBT', 0)
        total_debt = mc_val + total_ardebt

        if total_debt > 0:
            # Menggunakan CID data yang sudah di-fetch (O(1) lookup)
            cid_info = cid_data_map.get(nomen, {}) 
            
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
        # 🚨 Memanggil fungsi yang sudah di-optimize
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
        paid_list = list(collection_mc.aggregate(paid_pipeline, allowDiskUse=True)) # Tambah allowDiskUse
        
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
        unpaid_list = list(collection_mc.aggregate(unpaid_pipeline, allowDiskUse=True)) # Tambah allowDiskUse

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
# === API LAPORAN VOLUME DASAR BULANAN (LANJUTAN) ===
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
# === API LAPORAN AGING PIUTANG (BARU) ===
# =========================================================================

@app.route('/api/report/aging_report', methods=['GET'])
@login_required 
def aging_report_api():
    """Menghasilkan laporan aging piutang (MC) berdasarkan BULAN_TAGIHAN relatif terhadap bulan terbaru."""
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500

    try:
        # Tentukan bulan tagihan terbaru
        latest_mc_month_doc = collection_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
        latest_mc_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None
        
        if not latest_mc_month:
            return jsonify({'status': 'error', 'message': 'Tidak ada data MC historis ditemukan.'}), 404

        # Pipeline untuk mencari semua tagihan yang BUKAN bulan terbaru, BERSTATUS belum BAYAR, dan NOMINAL > 0
        pipeline = [
            {'$match': {
                'BULAN_TAGIHAN': {'$ne': latest_mc_month},
                'STATUS': {'$ne': 'PAYMENT'}, # Filter status yang belum bayar
                'NOMINAL': {'$gt': 0}
            }},
            {'$project': {
                'NOMEN': 1,
                'RAYON': 1,
                'NOMINAL': {'$toDouble': {'$ifNull': ['$NOMINAL', 0]}},
                'BULAN_TAGIHAN': 1
            }},
            # Grouping berdasarkan NOMEN, menjumlahkan total nominal piutang lama
            {'$group': {
                '_id': '$NOMEN',
                'TotalPiutangLama': {'$sum': '$NOMINAL'},
                'Bulan_Tagihan_Terlama': {'$min': '$BULAN_TAGIHAN'},
                'RayonMC': {'$first': '$RAYON'}
            }},
            {'$match': {'TotalPiutangLama': {'$gt': 0}}},
            {'$lookup': { # Join ke CID untuk Nama dan proper Rayon cleanup
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
                # Hitung Aging dalam Bulan (asumsi MMYYYY)
                'Aging_Bulan': {
                    '$subtract': [
                        {'$add': [
                            {'$multiply': [{'$toInt': {'$substr': [latest_mc_month, 2, 4]}}, 12]},
                            {'$toInt': {'$substr': [latest_mc_month, 0, 2]}}
                        ]},
                        {'$add': [
                            {'$multiply': [{'$toInt': {'$substr': ['$Bulan_Tagihan_Terlama', 2, 4]}}, 12]},
                            {'$toInt': {'$substr': ['$Bulan_Tagihan_Terlama', 0, 2]}}
                        ]}
                    ]
                }
            }},
            {'$sort': {'PiutangLama': -1}},
            {'$limit': 500} # Batasi 500 pelanggan dengan piutang lama terbesar
        ]

        aging_data = list(collection_mc.aggregate(pipeline, allowDiskUse=True))
        
        if not aging_data:
            return jsonify({'status': 'success', 'message': 'Tidak ada piutang lama ditemukan (semua lunas atau bulan berjalan).', 'data': []}), 200

        return jsonify({'status': 'success', 'data': aging_data}), 200

    except Exception as e:
        print(f"Error saat membuat laporan aging: {e}")
        return jsonify({"status": 'error', "message": f"Gagal mengambil laporan aging: {e}"}), 500

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
        columns_to_normalize_mc = ['PC', 'EMUH', 'NOMEN', 'STATUS', 'TARIF', 'BULAN_TAGIHAN', 'ZONA_NOVAK', 'CUST_TYPE'] # DITAMBAH CUST_TYPE
        
        for col in df.columns:
            if df[col].dtype == 'object' or col in columns_to_normalize_mc:
                df[col] = df[col].astype(str).str.strip().str.upper()
                df[col] = df[col].replace(['NAN', 'NONE', '', ' '], 'N/A')
            
            if col in ['NOMINAL', 'NOMINAL_AKHIR', 'KUBIK', 'SUBNOMINAL', 'ANG_BP', 'DENDA', 'PPN']: 
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        if 'PC' in df.columns:
            df = df.rename(columns={'PC': 'RAYON'})
        
        # --- Sarankan: Pastikan STATUS ada dan dinormalisasi ---
        if 'STATUS' not in df.columns:
            df['STATUS'] = 'N/A' # Default status jika tidak ada

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
        
        # >>> START PERBAIKAN: MAPPING HEADER KRITIS UNTUK MB <<<
        # 1. NOTAG/NOTAGIHAN (Nomor Tagihan)
        # MC & MB menggunakan NOTAGIHAN. Jika file upload menggunakan NOTAG, kita map.
        # MC tidak ada field TGL_BAYAR, MB menggunakan TGL_BAYAR, jika file upload menggunakan PAY_DT, kita map.
        rename_map = {
            'NOTAG': 'NOTAGIHAN', # Jawaban #1: Map NOTAG (Daily) ke NOTAGIHAN (MB)
            'PAY_DT': 'TGL_BAYAR', # Jawaban #2: Map PAY_DT (Daily) ke TGL_BAYAR (MB)
            'BILL_PERIOD': 'BULAN_REK',
            'MC VOL OKT 25_NOMEN': 'NOMEN' 
        }
        # Menggunakan errors='ignore' memastikan hanya kolom yang ada di file yang diubah namanya
        df = df.rename(columns=lambda x: rename_map.get(x, x), errors='ignore')
        # >>> END PERBAIKAN: MAPPING HEADER KRITIS UNTUK MB <<<
        
        # >>> START PERBAIKAN: INJECT MISSING KRITICAL COLUMNS <<<
        
        # Pastikan BILL_REASON ada. Jika hilang, set ke 'UNKNOWN' (agar tidak termasuk 'BIAYA PEMAKAIAN AIR' di laporan)
        if 'BILL_REASON' not in df.columns:
            df['BILL_REASON'] = 'UNKNOWN'
            print("Peringatan: Kolom BILL_REASON hilang, diisi dengan UNKNOWN.")
        
        # Jika BULAN_REK masih hilang setelah mapping BILL_PERIOD, set ke 'N/A'
        if 'BULAN_REK' not in df.columns:
            df['BULAN_REK'] = 'N/A' 
            print("Peringatan: Kolom BULAN_REK/BILL_PERIOD hilang, diisi dengan N/A.")
            
        # >>> END PERBAIKAN: INJECT MISSING KRITICAL COLUMNS <<<

        # >>> START PERBAIKAN KRITIS: NORMALISASI FORMAT TANGGAL TGL_BAYAR <<<
        if 'TGL_BAYAR' in df.columns:
            
            # 1. Coba parse sebagai tanggal Pandas
            df['TGL_BAYAR_OBJ'] = pd.to_datetime(
                df['TGL_BAYAR'].astype(str).str.strip(), 
                format='%d-%m-%Y', 
                errors='coerce'
            )
            
            # 2. Coba parse dari format float/int (Excel serial date number)
            # Origin '1899-12-30' adalah standar untuk konversi Excel date number
            numeric_dates = pd.to_numeric(df['TGL_BAYAR'].replace({'N/A': float('nan')}), errors='coerce')
            
            # 3. Mengisi nilai NaN dari parsing string dengan hasil konversi Excel serial date
            df['TGL_BAYAR_OBJ'] = df['TGL_BAYAR_OBJ'].fillna(
                pd.to_datetime(numeric_dates, unit='D', origin='1899-12-30', errors='coerce')
            )
            
            # 4. Konversi objek datetime ke string YYYY-MM-DD yang seragam
            df['TGL_BAYAR'] = df['TGL_BAYAR_OBJ'].dt.strftime('%Y-%m-%d').fillna('N/A')
            df = df.drop(columns=['TGL_BAYAR_OBJ'], errors='ignore')
            
            if (df['TGL_BAYAR'] == 'N/A').any():
                print("Peringatan: Beberapa nilai TGL_BAYAR gagal dikonversi ke format YYYY-MM-DD yang seragam.")

        # >>> END PERBAIKAN KRITIS: NORMALISASI FORMAT TANGGAL TGL_BAYAR <<<
        
        # >>> PERBAIKAN KRITIS MB: NORMALISASI DATA PANDAS <<<

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
        columns_to_normalize = ['MERK', 'READ_METHOD', 'TIPEPLGGN', 'RAYON', 'NOMEN', 'TARIFF', 'ZONANOvaK'] # Tambah ZONANOvaK
        
        for col in df.columns:
            if df[col].dtype == 'object' or col in columns_to_normalize:
                df[col] = df[col].astype(str).str.strip().str.upper()
                df[col] = df[col].replace(['NAN', 'NONE', '', ' '], 'N/A')
        
        if 'TARIFF' in df.columns:
            df = df.rename(columns={'TARIFF': 'TARIF'})

        # >>> START PERUBAHAN KRITIS: DEKODE ZONA_NOVAK <<<
        # Panggil fungsi decode_zona_novak untuk memastikan RAYON, PCEZ, dll. terbuat.
        df = decode_zona_novak(df)
        print("Peringatan: Field RAYON, PCEZ, PC, EZ, BLOCK didekode dari ZONANOvaK.")
        
        # >>> END PERUBAHAN KRITIS: DEKODE ZONA_NOVAK <<<
        
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
        
        # === ANALISIS ANOMALI INSTAN SETELAN INSERT ===
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
            "anomaly_list": []
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
        
        # Tentukan Bulan Tagihan MC Terbaru (Denominator Koleksi)
        latest_mc_month_doc = collection_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
        latest_mc_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None
        
        if not latest_mc_month:
              return jsonify({
                 "message": "Tidak ada data Master Cetak (MC) ditemukan.",
                 "summary": {"TotalPelanggan": 0, "TotalPiutangNominal": 0, "TotalPiutangKubik": 0},
                 "ardebt_summary": {"TotalTunggakanPelanggan": 0, "TotalTunggakanNominal": 0, "TotalTunggakanKubik": 0}
            }), 200

        previous_mc_month = _get_previous_month_year(latest_mc_month)
        today_date = datetime.now().strftime('%Y-%m-%d')
        this_month_str = datetime.now().strftime('%Y-%m')

        # --- A. MC METRICS (Piutang Bulan Berjalan) ---
        mc_summary_pipeline = [
            {'$match': {'BULAN_TAGIHAN': latest_mc_month}},
            {"$project": {
                "NOMEN": 1,
                "NOMINAL": {"$toDouble": {"$ifNull": ["$NOMINAL", 0]}},
                "KUBIK": {"$toDouble": {"$ifNull": ["$KUBIK", 0]}},
                "STATUS_CLEAN": {'$toUpper': {'$trim': {'input': {'$ifNull': ['$STATUS', 'N/A']}}}}, 
                "CLEAN_ZONA": {"$trim": {"input": {"$ifNull": ["$ZONA_NOVAK", ""]}}},
                "CUST_TYPE_MC": "$CUST_TYPE",
            }},
            {"$addFields": {
                "RAYON_ZONA": {"$substrCP": ["$CLEAN_ZONA", 0, 2]},
            }},
            {'$lookup': {'from': 'CustomerData', 'localField': 'NOMEN', 'foreignField': 'NOMEN', 'as': 'customer_info'}},
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
                'TotalPiutangNominal': {'$sum': '$NOMINAL'},
                'TotalPiutangKubik': {'$sum': '$KUBIK'},
                'TotalNomen': {'$addToSet': '$NOMEN'},
                'TotalCollectedNominal': {'$sum': {'$cond': [{'$eq': ['$STATUS_CLEAN', 'PAYMENT']}, '$NOMINAL', 0]}},
            }},
            {'$project': {
                '_id': 0, 'TotalPiutangNominal': 1, 'TotalPiutangKubik': 1,
                'TotalNomen': {'$size': '$TotalNomen'},
                'TotalCollectedNominal': 1
            }}
        ]
        mc_data = list(collection_mc.aggregate(mc_summary_pipeline))
        
        mc_summary = mc_data[0] if mc_data else {'TotalPiutangNominal': 0, 'TotalPiutangKubik': 0, 'TotalNomen': 0, 'TotalCollectedNominal': 0}
        
        summary_data['mc'] = {
            "TotalPelanggan": mc_summary['TotalNomen'],
            "TotalPiutangNominal": mc_summary['TotalPiutangNominal'],
            "TotalPiutangKubik": mc_summary['TotalPiutangKubik'],
            "TotalKoleksiMC": mc_summary['TotalCollectedNominal'],
        }

        # --- B. ARDEBT METRICS (Tunggakan Historis) ---
        ardebt_summary_pipeline = [
            {"$project": {
                "NOMEN": 1,
                "JUMLAH": {"$toDouble": {"$ifNull": ["$JUMLAH", 0]}},
                "VOLUME": {"$toDouble": {"$ifNull": ["$VOLUME", 0]}},
            }},
            {'$group': {
                '_id': None,
                'TotalTunggakanNominal': {'$sum': '$JUMLAH'},
                'TotalTunggakanKubik': {'$sum': '$VOLUME'},
                'TotalTunggakanPelanggan': {'$addToSet': '$NOMEN'},
            }},
            {'$project': {
                '_id': 0, 'TotalTunggakanNominal': 1, 'TotalTunggakanKubik': 1,
                'TotalTunggakanPelanggan': {'$size': '$TotalTunggakanPelanggan'},
            }}
        ]
        ardebt_data = list(collection_ardebt.aggregate(ardebt_summary_pipeline))
        ardebt_summary = ardebt_data[0] if ardebt_data else {'TotalTunggakanNominal': 0, 'TotalTunggakanKubik': 0, 'TotalTunggakanPelanggan': 0}

        summary_data['ardebt'] = {
            "TotalTunggakanPelanggan": ardebt_summary['TotalTunggakanPelanggan'],
            "TotalTunggakanNominal": ardebt_summary['TotalTunggakanNominal'],
            "TotalTunggakanKubik": ardebt_summary['TotalTunggakanKubik'],
        }

        # --- C. KOLEKSI MB (MB Undue Koleksi) ---
        # 1. MB Undue Koleksi (MB yang dibayar BERSAMAAN dengan BULAN REK MC terbaru)
        mb_undue_pipeline = [
             { '$match': { 'BULAN_REK': latest_mc_month, 'BILL_REASON': 'BIAYA PEMAKAIAN AIR' }},
             { '$group': {
                 '_id': None,
                 'TotalUndueNominal': {'$sum': {'$toDouble': {'$ifNull': ['$NOMINAL', 0]}}}
             }}
        ]
        mb_undue_result = list(collection_mb.aggregate(mb_undue_pipeline))
        mb_undue_nominal = mb_undue_result[0]['TotalUndueNominal'] if mb_undue_result else 0.0

        # 2. Koleksi Hari Ini
        koleksi_today_pipeline = [
            {'$match': {'TGL_BAYAR': today_date, 'BILL_REASON': 'BIAYA PEMAKAIAN AIR'}},
            {'$group': {'_id': None, 'koleksi_hari_ini': {'$sum': '$NOMINAL'}}}
        ]
        koleksi_today_result = list(collection_mb.aggregate(koleksi_today_pipeline))
        koleksi_hari_ini = koleksi_today_result[0]['koleksi_hari_ini'] if koleksi_today_result else 0.0

        # 3. Koleksi Bulan Ini (YTD)
        koleksi_month_pipeline = [
            {'$match': {'TGL_BAYAR': {'$regex': f'^{this_month_str}'}, 'BILL_REASON': 'BIAYA PEMAKAIAN AIR'}},
            {'$group': {'_id': None, 'koleksi_bulan_ini': {'$sum': '$NOMINAL'}}}
        ]
        koleksi_month_result = list(collection_mb.aggregate(koleksi_month_pipeline))
        koleksi_bulan_ini = koleksi_month_result[0]['koleksi_bulan_ini'] if koleksi_month_result else 0.0
        
        summary_data['koleksi'] = {
            "KoleksiHariIni": koleksi_hari_ini,
            "KoleksiBulanIni": koleksi_bulan_ini,
            "MBUndueNominal": mb_undue_nominal,
        }
        
        # Persentase Koleksi
        total_piutang_mc = summary_data['mc']['TotalPiutangNominal']
        summary_data['koleksi']['PercentKoleksi'] = (summary_data['koleksi']['KoleksiBulanIni'] / total_piutang_mc) * 100 if total_piutang_mc > 0 else 0.0

        # --- D. TOP 5 PCEZ ---
        # Agregasi untuk Piutang Nominal MC Bulan Terbaru, dikelompokkan per PCEZ
        top_pcez_pipeline = [
            {'$match': {'BULAN_TAGIHAN': latest_mc_month}},
            {"$project": {
                "NOMINAL": {"$toDouble": {"$ifNull": ["$NOMINAL", 0]}},
                "CLEAN_ZONA": {"$trim": {"input": {"$ifNull": ["$ZONA_NOVAK", ""]}}},
            }},
            {"$addFields": {"PCEZ_CLEAN": {"$concat": [{"$substrCP": ["$CLEAN_ZONA", 2, 3]}, {"$substrCP": ["$CLEAN_ZONA", 5, 2]}]}}},
            {'$group': {
                '_id': '$PCEZ_CLEAN',
                'total_piutang': {'$sum': '$NOMINAL'},
            }},
            {'$sort': {'total_piutang': -1}},
            {'$limit': 5}
        ]
        top_pcez = list(collection_mc.aggregate(top_pcez_pipeline))
        summary_data['top_pcez_piutang'] = [{'PCEZ': item['_id'], 'nominal': item['total_piutang']} for item in top_pcez]

        # --- E. TREN KOLEKSI 7 HARI TERAKHIR ---
        trend_data = []
        for i in range(7):
            date_obj = datetime.now() - timedelta(days=i)
            date = date_obj.strftime('%Y-%m-%d')
            
            pipeline = [
                {'$match': {'TGL_BAYAR': date, 'BILL_REASON': 'BIAYA PEMAKAIAN AIR'}}, 
                {'$group': {'_id': None, 'total': {'$sum': '$NOMINAL'}}}
            ]
            result = list(collection_mb.aggregate(pipeline))
            total = result[0]['total'] if result else 0.0
            
            trend_data.append({
                'tanggal': date,
                'total_nominal': total
            })
        summary_data['tren_koleksi_7_hari'] = sorted(trend_data, key=lambda x: x['tanggal'])
        
        return jsonify(summary_data), 200
        
    except Exception as e:
        print(f"Error fetching dashboard summary: {e}")
        # Jika terjadi error, kirimkan pesan yang lebih informatif
        return jsonify({"message": f"Gagal mengambil data dashboard: {e}. Pastikan data MC, CID, dan MB sudah ter-upload."}), 500


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
                             # Filter untuk Koleksi Rutin
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
                summary_data['mc']['TotalNomen'],
                summary_data['mc']['TotalPiutangNominal'],
                summary_data['ardebt']['TotalTunggakanNominal'],
                summary_data['koleksi']['KoleksiBulanIni'],
                f"{summary_data['koleksi']['PercentKoleksi']:.2f}%"
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
