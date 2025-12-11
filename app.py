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
    
    # KOLEKSI DIPISAH BERDASARKAN SUMBER DATA
    collection_mc = db['MasterCetak']   # MC (Piutang/Tagihan Bulanan - REPLACE)
    collection_mb = db['MasterBayar']   # MB (Koleksi Harian - APPEND, BULK INSERT)
    collection_cid = db['CustomerData'] # CID (Data Pelanggan Statis - REPLACE)
    collection_sbrs = db['MeterReading'] # SBRS (Baca Meter Harian/Parsial - APPEND, BULK INSERT)
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

# --- KELAS DAN DEKORATOR ---
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
        
    # SINKRONISASI PENCARIAN: Konversi input NOMEN ke Huruf Besar
    query_nomen = request.args.get('nomen', '').strip().upper()

    if not query_nomen:
        return jsonify({"status": "fail", "message": "Masukkan NOMEN untuk memulai pencarian terintegrasi."}), 400

    try:
        print(f"DEBUG: Mencari NOMEN dari input: '{query_nomen}'")
        
        # 1. DATA STATIS (CID) - Master Data Pelanggan (Wajib ada)
        cid_result = collection_cid.find_one({'NOMEN': query_nomen})
        
        if not cid_result:
            print(f"DEBUG: GAGAL! NOMEN '{query_nomen}' TIDAK DITEMUKAN di koleksi CID.")
            return jsonify({
                "status": "not_found",
                "message": f"NOMEN {query_nomen} tidak ditemukan di Master Data Pelanggan (CID)."
            }), 404

        print(f"DEBUG: SUKSES! CID ditemukan untuk NOMEN: '{query_nomen}'. Melanjutkan pencarian data terintegrasi.")

        # 2. PIUTANG BERJALAN (MC) - Snapshot Bulan Ini
        mc_results = list(collection_mc.find({'NOMEN': query_nomen}))
        piutang_nominal_total = sum(item.get('NOMINAL', 0) for item in mc_results)
        
        # 3. TUNGGAKAN DETAIL (ARDEBT)
        ardebt_results = list(collection_ardebt.find({'NOMEN': query_nomen}))
        tunggakan_nominal_total = sum(item.get('JUMLAH', 0) for item in ardebt_results)
        
        # 4. RIWAYAT PEMBAYARAN TERAKHIR (MB)
        mb_last_payment_cursor = collection_mb.find({'NOMEN': query_nomen}).sort('TGL_BAYAR', -1).limit(1)
        mb_payments = list(mb_last_payment_cursor)
        last_payment = mb_payments[0] if mb_payments else None
        
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
    """PEROMBAKAN: Menghitung total tagihan (MC), total koleksi (MB), dan total tunggakan (ARDEBT)."""
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500
    
    # --- 1. DATA TAGIHAN (BILLED) dari MC ---
    # Sumber untuk Total Tagihan (Nominal) dan Total Pelanggan (Nomen)
    pipeline_mc = [
        { '$match': { 'NOMEN': { '$exists': True } } }, 
        { '$group': {
            '_id': { 'rayon': '$RAYON', 'pcez': '$PCEZ' },
            'total_nomen_mc': { '$addToSet': '$NOMEN' },
            'total_nominal_mc': { '$sum': '$NOMINAL' } 
        }}
    ]
    mc_data = list(collection_mc.aggregate(pipeline_mc))

    # --- 2. DATA KOLEKSI (PAID) dari MB ---
    # Sumber untuk Total Nominal Bayar dan Total Sudah Bayar (Nomen)
    pipeline_mb = [
        { '$match': { 'NOMEN': { '$exists': True } } }, 
        { '$group': {
            '_id': { 'rayon': '$RAYON', 'pcez': '$PCEZ' },
            'collected_nomen_mb': { '$addToSet': '$NOMEN' }, 
            'collected_nominal_mb': { '$sum': '$NOMINAL' } 
        }}
    ]
    mb_data = list(collection_mb.aggregate(pipeline_mb)) 

    # --- 3. DATA TUNGGAKAN (DEBT) - GRAND TOTAL ---
    # Total Tunggakan Keseluruhan dari ARDEBT (menggunakan kolom 'JUMLAH')
    pipeline_ardebt_total = [
        { '$group': {
            '_id': None,
            'grand_total_debt': { '$sum': '$JUMLAH' }
        }}
    ]
    ardebt_total = list(collection_ardebt.aggregate(pipeline_ardebt_total))
    grand_total_debt = ardebt_total[0]['grand_total_debt'] if ardeb_total and ardeb_total[0].get('grand_total_debt') is not None else 0.0

    # --- 4. GABUNGKAN DATA DAN HITUNG TOTAL PER RAYON ---
    report_map = {}
    
    # Inisialisasi dari MC (Data Tagihan Dasar)
    for item in mc_data:
        key = (item['_id'].get('rayon', 'N/A'), item['_id'].get('pcez', 'N/A'))
        report_map[key] = {
            'RAYON': key[0],
            'PCEZ': key[1],
            'TotalNominalMC': float(item['total_nominal_mc']),
            'TotalNomenMC': len(item['total_nomen_mc']),
            # Inisialisasi koleksi
            'CollectedNominalMB': 0.0, 'CollectedNomenMB': 0, 
            'PercentNominal': 0.0, 'PercentNomenCount': 0.0
        }

    # Tambahkan data koleksi dari MB
    for item in mb_data:
        key = (item['_id'].get('rayon', 'N/A'), item['_id'].get('pcez', 'N/A'))
        
        # Jika rayon/pcez ini ada di MC, kita update datanya. Jika tidak, kita inisialisasi dengan Total Tagihan 0
        if key not in report_map:
             report_map[key] = {
                'RAYON': key[0],
                'PCEZ': key[1],
                'TotalNominalMC': 0.0, 'TotalNomenMC': 0, 
                'CollectedNominalMB': 0.0, 'CollectedNomenMB': 0, 
                'PercentNominal': 0.0, 'PercentNomenCount': 0.0
            }
            
        report_map[key]['CollectedNominalMB'] = float(item['collected_nominal_mb'])
        report_map[key]['CollectedNomenMB'] = len(item['collected_nomen_mb'])
        
        # Hitung Persentase
        if report_map[key]['TotalNominalMC'] > 0:
            report_map[key]['PercentNominal'] = (report_map[key]['CollectedNominalMB'] / report_map[key]['TotalNominalMC']) * 100
        
        if report_map[key]['TotalNomenMC'] > 0:
            report_map[key]['PercentNomenCount'] = (report_map[key]['CollectedNomenMB'] / report_map[key]['TotalNomenMC']) * 100

    # Hitung Grand Total (Konsolidasi)
    grand_total = {
        'TotalNominalBilled': sum(d['TotalNominalMC'] for d in report_map.values()),
        'CollectedNominal': sum(d['CollectedNominalMB'] for d in report_map.values()),
        'TotalNomenBilled': sum(d['TotalNomenMC'] for d in report_map.values()),
        'CollectedNomen': sum(d['CollectedNomenMB'] for d in report_map.values()),
        'TotalDebtNominal': grand_total_debt # Tambahkan data tunggakan
    }
    
    # Hitung Persentase Grand Total
    grand_total['PercentNominal'] = (grand_total['CollectedNominal'] / grand_total['TotalNominalBilled']) * 100 if grand_total['TotalNominalBilled'] > 0 else 0
    grand_total['PercentNomenCount'] = (grand_total['CollectedNomen'] / grand_total['TotalNomenBilled']) * 100 if grand_total['TotalNomenBilled'] > 0 else 0


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
                           title="Pemakaian Air Turun", 
                           description="Menampilkan pelanggan dengan penurunan konsumsi air signifikan (memerlukan data MC dan SBRS historis).",
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
                 # SINKRONISASI: Pastikan data NOMEN di-uppercase saat bergabung
                 df[JOIN_KEY] = df[JOIN_KEY].astype(str).str.strip().str.upper() 
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
                # SINKRONISASI: Pastikan NOMEN selalu di-uppercase
                if col == 'NOMEN':
                     df[col] = df[col].str.upper()
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
    """Mode APPEND: Untuk Master Bayar (MB) / Koleksi Harian. Dipercepat dengan Bulk Insert."""
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
                # SINKRONISASI: Pastikan NOMEN selalu di-uppercase
                if col == 'NOMEN':
                     df[col] = df[col].str.upper()
            if col in ['NOMINAL', 'SUBNOMINAL', 'BEATETAP', 'BEA_SEWA']: # Kolom finansial MB
                 df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        data_to_insert = df.to_dict('records')
        
        if not data_to_insert:
            return jsonify({"message": "File kosong, tidak ada data yang dimasukkan."}), 200
        
        # Kunci unik: NOTAGIHAN (dari MB) + TGL_BAYAR + NOMINAL
        UNIQUE_KEYS = ['NOTAGIHAN', 'TGL_BAYAR', 'NOMINAL'] 
        
        if not all(key in df.columns for key in UNIQUE_KEYS):
            return jsonify({"message": f"Gagal Append: File MB harus memiliki kolom kunci unik: {', '.join(UNIQUE_KEYS)}. Cek file Anda."}), 400
            
        # 1. Pastikan index unik ada untuk mencegah duplikasi secara efisien
        collection_mb.create_index(
            [("NOTAGIHAN", 1), ("TGL_BAYAR", 1), ("NOMINAL", 1)],
            unique=True,
            name='unique_mb_entry'
        )
        
        # OPERASI KRITIS: APPEND DATA BARU MENGGUNAKAN BULK INSERT
        try:
            # ordered=False: Lanjutkan menyisipkan meskipun ada duplikat (DuplicateKeyError)
            result = collection_mb.insert_many(data_to_insert, ordered=False)
            inserted_count = len(result.inserted_ids)
            skipped_count = len(data_to_insert) - inserted_count
            
            return jsonify({"message": f"Sukses Append! {inserted_count} baris Master Bayar (MB) baru ditambahkan. ({skipped_count} duplikat diabaikan)."}), 200

        except Exception as e:
            # Tangani BulkWriteError (yang terjadi saat ada duplikat)
            inserted_count = 0
            skipped_count = 0
            if hasattr(e, 'details') and 'writeErrors' in e.details:
                # Menghitung sisipan yang berhasil/gagal dari error
                skipped_count = len(e.details['writeErrors'])
                inserted_count = len(data_to_insert) - skipped_count
                
                if inserted_count > 0 or skipped_count > 0:
                     return jsonify({"message": f"Sukses Append! {inserted_count} baris Master Bayar (MB) baru ditambahkan. ({skipped_count} duplikat diabaikan oleh DB)."}), 200
            
            # Jika error lain
            print(f"Error saat memproses file MB: {e}")
            return jsonify({"message": f"Gagal memproses file MB: {e}. Pastikan format data dan koneksi DB benar."}), 500


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
        
        # SINKRONISASI KRITIS: Pastikan data NOMEN di-uppercase
        if 'NOMEN' in df.columns:
            df['NOMEN'] = df['NOMEN'].astype(str).str.upper() 

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
    """Mode APPEND: Untuk data Baca Meter (SBRS) / Riwayat Stand Meter. Dipercepat dengan Bulk Insert."""
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
        
        # SINKRONISASI KRITIS: Pastikan data CMR_ACCOUNT (NOMEN) di-uppercase
        if 'CMR_ACCOUNT' in df.columns:
            df['CMR_ACCOUNT'] = df['CMR_ACCOUNT'].astype(str).str.upper() 

        data_to_insert = df.to_dict('records')
        
        if not data_to_insert:
            return jsonify({"message": "File kosong, tidak ada data yang dimasukkan."}), 200
        
        # Kunci unik: cmr_account (NOMEN) + cmr_rd_date (Tanggal Baca)
        UNIQUE_KEYS = ['CMR_ACCOUNT', 'CMR_RD_DATE'] 
        
        if not all(key in df.columns for key in UNIQUE_KEYS):
            return jsonify({"message": f"Gagal Append: File SBRS harus memiliki kolom kunci unik: {', '.join(UNIQUE_KEYS)}. Cek file Anda."}), 400
        
        # 1. Pastikan index unik ada untuk mencegah duplikasi secara efisien
        collection_sbrs.create_index(
            [("CMR_ACCOUNT", 1), ("CMR_RD_DATE", 1)],
            unique=True,
            name='unique_sbrs_entry'
        )

        # OPERASI KRITIS: APPEND DATA BARU MENGGUNAKAN BULK INSERT
        try:
            # ordered=False: Lanjutkan menyisipkan meskipun ada duplikat (DuplicateKeyError)
            result = collection_sbrs.insert_many(data_to_insert, ordered=False)
            inserted_count = len(result.inserted_ids)
            skipped_count = len(data_to_insert) - inserted_count
            
            return jsonify({"message": f"Sukses Append! {inserted_count} baris Riwayat Baca Meter (SBRS) baru ditambahkan. ({skipped_count} duplikat diabaikan)."}), 200

        except Exception as e:
            # Tangani BulkWriteError (yang terjadi saat ada duplikat)
            inserted_count = 0
            skipped_count = 0
            if hasattr(e, 'details') and 'writeErrors' in e.details:
                # Menghitung sisipan yang berhasil/gagal dari error
                skipped_count = len(e.details['writeErrors'])
                inserted_count = len(data_to_insert) - skipped_count
                
                if inserted_count > 0 or skipped_count > 0:
                     return jsonify({"message": f"Sukses Append! {inserted_count} baris Riwayat Baca Meter (SBRS) baru ditambahkan. ({skipped_count} duplikat diabaikan oleh DB)."}), 200

            # Jika error lain
            print(f"Error saat memproses file SBRS: {e}")
            return jsonify({"message": f"Gagal memproses file SBRS: {e}. Pastikan format data dan koneksi DB benar."}), 500


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
                # SINKRONISASI: Pastikan NOMEN selalu di-uppercase
                if col == 'NOMEN':
                     df[col] = df[col].str.upper()
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


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
