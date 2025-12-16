import os
import pandas as pd
from flask import Flask, request, jsonify, render_template, redirect, url_for, flash, make_response
from dotenv import load_dotenv
from werkzeug.utils import secure_filename
from werkzeug.security import check_password_hash, generate_password_hash
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from flask_wtf import FlaskForm 
from wtforms import StringField, PasswordField, SubmitField 
from wtforms.validators import DataRequired 
from functools import wraps
import io 
from datetime import datetime, timedelta
from pymongo.errors import BulkWriteError

# Import modul yang sudah dipecah
from utils import init_db, get_db_status, _parse_zona_novak, _get_sbrs_anomalies, _get_day_n_ago, _get_previous_month_year
from routes_collection import bp_collection
from routes_meter_reading import bp_meter_reading

load_dotenv() 

# --- KONFIGURASI UTAMA APLIKASI ---
app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv("SECRET_KEY", "dev_key_rahasia")

# Inisialisasi Database (Dilakukan di luar permintaan)
with app.app_context():
    init_db(app)

ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls'} 

# --- KONFIGURASI FLASK-LOGIN ---
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

# --- PENDAFTARAN BLUEPRINTS ---
app.register_blueprint(bp_collection)
app.register_blueprint(bp_meter_reading)


# --- ROUTE CORE (LOGIN/INDEX/DASHBOARD/UPLOAD) ---

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

@app.route('/dashboard', methods=['GET'])
@login_required
def analytics_dashboard():
    return render_template('dashboard_analytics.html', is_admin=current_user.is_admin)

# --- ROUTE UPLOAD (TETAP DI CORE KARENA PERLU MENGAKSES FS) ---

@app.route('/upload/mc', methods=['POST'])
@login_required 
@admin_required 
def upload_mc_data():
    db_status = get_db_status()
    if db_status['status'] == 'error': return jsonify({"message": db_status['message']}), 500
    collection_mc = db_status['collections']['mc']
    
    if 'file' not in request.files: return jsonify({"message": "Tidak ada file di permintaan"}), 400
    file = request.files['file']
    if file.filename == '' or not allowed_file(file.filename): return jsonify({"message": "Format file tidak valid. Harap unggah CSV, XLSX, atau XLS."}), 400

    filename = secure_filename(file.filename)
    file_extension = filename.rsplit('.', 1)[1].lower()

    try:
        upload_month = request.form.get('month')
        upload_year = request.form.get('year')
        
        if not upload_month or not upload_year: return jsonify({"message": "Gagal: Bulan dan Tahun Tagihan harus diisi."}), 400
        bulan_tagihan_value = f"{upload_month}{upload_year}"

        if file_extension == 'csv': df = pd.read_csv(file)
        elif file_extension in ['xlsx', 'xls']: df = pd.read_excel(file, sheet_name=0) 
        
        df.columns = [col.strip().upper() for col in df.columns]
        rename_map = {'PC': 'RAYON'}
        df = df.rename(columns=lambda x: rename_map.get(x, x), errors='ignore')

        if 'NOMEN' not in df.columns: return jsonify({"message": "Gagal Append: File MC harus memiliki kolom kunci 'NOMEN'."}), 400
        df['BULAN_TAGIHAN'] = bulan_tagihan_value

        columns_to_normalize_mc = ['RAYON', 'EMUH', 'NOMEN', 'STATUS', 'TARIF', 'BULAN_TAGIHAN', 'ZONA_NOVAK', 'CUST_TYPE'] 
        
        for col in df.columns:
            if df[col].dtype == 'object' or col in columns_to_normalize_mc:
                df[col] = df[col].astype(str).str.strip().str.upper().replace(['NAN', 'NONE', '', ' '], 'N/A')
            
            if col in ['NOMINAL', 'NOMINAL_AKHIR', 'KUBIK', 'SUBNOMINAL', 'ANG_BP', 'DENDA', 'PPN']: 
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        if 'STATUS' not in df.columns: df['STATUS'] = 'N/A'
        data_to_insert = df.to_dict('records')
        
        if not data_to_insert: return jsonify({"message": "File kosong, tidak ada data yang dimasukkan."}), 200
        
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
        
        return jsonify({"status": "success", "message": f"Sukses Historis! {inserted_count} baris Master Cetak (MC) baru ditambahkan. ({skipped_count} duplikat diabaikan).", "summary_report": {"total_rows": total_rows, "type": "APPEND", "inserted_count": inserted_count, "skipped_count": skipped_count}, "anomaly_list": []}), 200

    except Exception as e:
        print(f"Error saat memproses file MC: {e}")
        return jsonify({"message": f"Gagal memproses file MC: {e}. Pastikan format data benar."}), 500

@app.route('/upload/mb', methods=['POST'])
@login_required 
@admin_required 
def upload_mb_data():
    db_status = get_db_status()
    if db_status['status'] == 'error': return jsonify({"message": db_status['message']}), 500
    collection_mb = db_status['collections']['mb']
        
    if 'file' not in request.files: return jsonify({"message": "Tidak ada file di permintaan"}), 400
    file = request.files['file']
    if file.filename == '' or not allowed_file(file.filename): return jsonify({"message": "Format file tidak valid. Harap unggah CSV, XLSX, atau XLS."}), 400

    filename = secure_filename(file.filename)
    file_extension = filename.rsplit('.', 1)[1].lower()

    try:
        if file_extension == 'csv': df = pd.read_csv(file)
        elif file_extension in ['xlsx', 'xls']: df = pd.read_excel(file, sheet_name=0) 
        
        df.columns = [col.strip().upper() for col in df.columns]
        
        rename_map = {'NOTAG': 'NOTAGIHAN', 'PAY_DT': 'TGL_BAYAR', 'BILL_PERIOD': 'BULAN_REK', 'MC VOL OKT 25_NOMEN': 'NOMEN', 'VOL_COLLECT': 'KUBIKBAYAR', 'TIPE_PLGGN': 'TIPEPLGGN'}
        df = df.rename(columns=lambda x: rename_map.get(x, x), errors='ignore')
        
        if 'NOMEN' not in df.columns or 'NOTAGIHAN' not in df.columns or 'TGL_BAYAR' not in df.columns:
            return jsonify({"message": "Gagal Append: File MB harus memiliki kolom kunci 'NOMEN', 'NOTAGIHAN', dan 'TGL_BAYAR'."}), 400

        if 'BILL_REASON' not in df.columns: df['BILL_REASON'] = 'UNKNOWN'
        if 'BULAN_REK' not in df.columns: df['BULAN_REK'] = 'N/A' 
            
        if 'TGL_BAYAR' in df.columns:
            df['TGL_BAYAR_OBJ'] = pd.to_datetime(df['TGL_BAYAR'].astype(str).str.strip(), errors='coerce')
            if df['TGL_BAYAR_OBJ'].isna().sum() > 0:
                 df['TGL_BAYAR_OBJ'] = df['TGL_BAYAR_OBJ'].fillna(pd.to_datetime(
                     df['TGL_BAYAR'].astype(str).str.strip(), format='%d-%m-%Y', errors='coerce'
                 ))
            numeric_dates = pd.to_numeric(df['TGL_BAYAR'].replace({'N/A': float('nan')}), errors='coerce')
            df['TGL_BAYAR_OBJ'] = df['TGL_BAYAR_OBJ'].fillna(
                pd.to_datetime(numeric_dates, unit='D', origin='1899-12-30', errors='coerce')
            )
            df['TGL_BAYAR'] = df['TGL_BAYAR_OBJ'].dt.strftime('%Y-%m-%d').fillna('N/A')
            df = df.drop(columns=['TGL_BAYAR_OBJ'], errors='ignore')

        columns_to_normalize_mb = ['NOMEN', 'RAYON', 'PCEZ', 'ZONA_NOREK', 'LKS_BAYAR', 'BULAN_REK', 'NOTAGIHAN', 'BILL_REASON', 'TIPEPLGGN'] 
        
        for col in df.columns:
            if df[col].dtype == 'object' or col in columns_to_normalize_mb:
                df[col] = df[col].astype(str).str.strip().str.upper().replace(['NAN', 'NONE', '', ' '], 'N/A')

            if col in ['NOMINAL', 'SUBNOMINAL', 'BEATETAP', 'BEA_SEWA', 'KUBIKBAYAR']: 
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        data_to_insert = df.to_dict('records')
        
        if not data_to_insert: return jsonify({"message": "File kosong, tidak ada data yang dimasukkan."}), 200
        
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
        
        return jsonify({"status": "success", "message": f"Sukses Append! {inserted_count} baris Master Bayar (MB) baru ditambahkan. ({skipped_count} duplikat diabaikan).", "summary_report": {"total_rows": total_rows, "type": "APPEND", "inserted_count": inserted_count, "skipped_count": skipped_count}, "anomaly_list": []}), 200

    except Exception as e:
        print(f"Error saat memproses file MB: {e}")
        return jsonify({"message": f"Gagal memproses file MB: {e}. Pastikan format data benar."}), 500

@app.route('/upload/cid', methods=['POST'])
@login_required 
@admin_required 
def upload_cid_data():
    db_status = get_db_status()
    if db_status['status'] == 'error': return jsonify({"message": db_status['message']}), 500
    collection_cid = db_status['collections']['cid']
        
    if 'file' not in request.files: return jsonify({"message": "Tidak ada file di permintaan"}), 400
    file = request.files['file']
    if file.filename == '' or not allowed_file(file.filename): return jsonify({"message": "Format file tidak valid. Harap unggah CSV, XLSX, atau XLS."}), 400

    filename = secure_filename(file.filename)
    file_extension = filename.rsplit('.', 1)[1].lower()

    try:
        if file_extension == 'csv': df = pd.read_csv(file)
        elif file_extension in ['xlsx', 'xls']: df = pd.read_excel(file, sheet_name=0) 
        
        df.columns = [col.strip().upper() for col in df.columns]

        rename_map = {'TARIFF': 'TARIF'}
        df = df.rename(columns=lambda x: rename_map.get(x, x), errors='ignore')

        if 'NOMEN' not in df.columns or 'TARIF' not in df.columns or 'RAYON' not in df.columns:
            return jsonify({"message": "Gagal Append: File CID harus memiliki kolom kunci 'NOMEN', 'TARIF', dan 'RAYON'."}), 400
        
        columns_to_normalize = ['MERK', 'READ_METHOD', 'TIPEPLGGN', 'RAYON', 'NOMEN', 'TARIF', 'PCEZ', 'STATUS_PELANGGAN'] 
        
        for col in df.columns:
            if df[col].dtype == 'object' or col in columns_to_normalize:
                df[col] = df[col].astype(str).str.strip().str.upper().replace(['NAN', 'NONE', '', ' '], 'N/A')
        
        data_to_insert = df.to_dict('records')
        
        if not data_to_insert: return jsonify({"message": "File kosong, tidak ada data yang dimasukkan."}), 200
        
        upload_date_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for record in data_to_insert: record['TANGGAL_UPLOAD_CID'] = upload_date_str

        inserted_count = 0
        total_rows = len(data_to_insert)

        try:
            result = collections['cid'].insert_many(data_to_insert, ordered=False)
            inserted_count = len(result.inserted_ids)
        except BulkWriteError as bwe:
            inserted_count = bwe.details.get('nInserted', 0)
        except Exception as e:
            return jsonify({"message": f"Gagal menyimpan data secara massal: {e}"}), 500
            
        return jsonify({"status": "success", "message": f"Sukses Historis! {inserted_count} baris Customer Data (CID) baru ditambahkan.", "summary_report": {"total_rows": total_rows, "type": "APPEND", "inserted_count": inserted_count, "skipped_count": total_rows - inserted_count}, "anomaly_list": []}), 200

    except Exception as e:
        print(f"Error saat memproses file CID: {e}")
        return jsonify({"message": f"Gagal memproses file CID: {e}. Pastikan format data benar."}), 500

@app.route('/upload/sbrs', methods=['POST'])
@login_required 
@admin_required 
def upload_sbrs_data():
    db_status = get_db_status()
    if db_status['status'] == 'error': return jsonify({"message": db_status['message']}), 500
    collections = db_status['collections']
        
    if 'file' not in request.files: return jsonify({"message": "Tidak ada file di permintaan"}), 400
    file = request.files['file']
    if file.filename == '' or not allowed_file(file.filename): return jsonify({"message": "Format file tidak valid. Harap unggah CSV, XLSX, atau XLS."}), 400

    filename = secure_filename(file.filename)
    file_extension = filename.rsplit('.', 1)[1].lower()

    try:
        if file_extension == 'csv': df = pd.read_csv(file)
        elif file_extension in ['xlsx', 'xls']: df = pd.read_excel(file, sheet_name=0) 
        
        df.columns = [col.strip().upper() for col in df.columns]

        rename_map = {'CMR_DIAL_DIFFERENCE': 'CMR_KUBIK'}
        df = df.rename(columns=lambda x: rename_map.get(x, x), errors='ignore')

        columns_to_normalize_sbrs = ['CMR_ACCOUNT', 'CMR_RD_DATE', 'CMR_READER', 'PC', 'EZ', 'RAYON'] 

        for col in df.columns:
            if df[col].dtype == 'object' or col in columns_to_normalize_sbrs:
                df[col] = df[col].astype(str).str.strip().str.upper().replace(['NAN', 'NONE', '', ' '], 'N/A')

            if col in ['CMR_PREV_READ', 'CMR_READING', 'CMR_KUBIK']: 
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        data_to_insert = df.to_dict('records')
        
        if not data_to_insert: return jsonify({"message": "File kosong, tidak ada data yang dimasukkan."}), 200
        
        UNIQUE_KEYS = ['CMR_ACCOUNT', 'CMR_RD_DATE'] 
        if not all(key in df.columns for key in UNIQUE_KEYS):
            return jsonify({"message": f"Gagal Append: File SBRS harus memiliki kolom kunci unik: {', '.join(UNIQUE_KEYS)}. Cek file Anda."}), 400

        inserted_count = 0
        skipped_count = 0
        total_rows = len(data_to_insert)

        try:
            result = collections['sbrs'].insert_many(data_to_insert, ordered=False)
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
                anomaly_list = _get_sbrs_anomalies(collections['sbrs'], collections['cid'])
        except Exception as e:
            print(f"Peringatan: Gagal menjalankan analisis anomali instan: {e}")

        return jsonify({"status": "success", "message": f"Sukses Append! {inserted_count} baris Riwayat Baca Meter (SBRS) baru ditambahkan. ({skipped_count} duplikat diabaikan).", "summary_report": {"total_rows": total_rows, "type": "APPEND", "inserted_count": inserted_count, "skipped_count": skipped_count}, "anomaly_list": []}), 200

    except Exception as e:
        print(f"Error saat memproses file SBRS: {e}")
        return jsonify({"message": f"Gagal memproses file SBRS: {e}. Pastikan format data benar."}), 500

@app.route('/upload/ardebt', methods=['POST'])
@login_required 
@admin_required 
def upload_ardebt_data():
    db_status = get_db_status()
    if db_status['status'] == 'error': return jsonify({"message": db_status['message']}), 500
    collection_ardebt = db_status['collections']['ardebt']
        
    if 'file' not in request.files: return jsonify({"message": "Tidak ada file di permintaan"}), 400
    file = request.files['file']
    if file.filename == '' or not allowed_file(file.filename): return jsonify({"message": "Format file tidak valid. Harap unggah CSV, XLSX, atau XLS."}), 400

    filename = secure_filename(file.filename)
    file_extension = filename.rsplit('.', 1)[1].lower()

    try:
        if file_extension == 'csv': df = pd.read_csv(file)
        elif file_extension in ['xlsx', 'xls']: df = pd.read_excel(file, sheet_name=0) 
        
        df.columns = [col.strip().upper() for col in df.columns]

        if 'NOMEN' not in df.columns: return jsonify({"message": "Gagal Append: File ARDEBT harus memiliki kolom kunci 'NOMEN' untuk penyimpanan historis."}), 400
            
        monetary_keys = ['JUMLAH', 'AMOUNT', 'TOTAL', 'NOMINAL']
        found_monetary_key = next((key for key in monetary_keys if key in df.columns), None)

        if found_monetary_key and found_monetary_key != 'JUMLAH':
             df = df.rename(columns={found_monetary_key: 'JUMLAH'})
        elif 'JUMLAH' not in df.columns:
            return jsonify({"message": "Gagal Append: Kolom kunci JUMLAH (atau AMOUNT/TOTAL/NOMINAL) untuk nominal tunggakan tidak ditemukan di file Anda."}), 400

        upload_month = request.form.get('month')
        upload_year = request.form.get('year')
        
        if not upload_month or not upload_year: return jsonify({"message": "Gagal: Bulan dan Tahun Tunggakan harus diisi."}), 400
        periode_bill_value = f"{upload_month}{upload_year}"

        df['PERIODE_BILL'] = periode_bill_value 

        columns_to_normalize_ardebt = ['NOMEN', 'RAYON', 'TIPEPLGGN', 'PERIODE_BILL', 'STATUS_PELANGGAN', 'READ_METHOD', 'PETUGAS', 'MERK', 'SERIAL'] 
        
        for col in df.columns:
            if df[col].dtype == 'object' or col in columns_to_normalize_ardebt:
                df[col] = df[col].astype(str).str.strip().str.upper().replace(['NAN', 'NONE', '', ' '], 'N/A')
            
            if col in ['JUMLAH', 'VOLUME']: 
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        data_to_insert = df.to_dict('records')
        
        if not data_to_insert: return jsonify({"message": "File kosong, tidak ada data yang dimasukkan."}), 200
        
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
        
        return jsonify({"status": "success", "message": f"Sukses Historis! {inserted_count} baris Detail Tunggakan (ARDEBT) baru ditambahkan. ({skipped_count} duplikat diabaikan).", "summary_report": {"total_rows": total_rows, "type": "APPEND", "inserted_count": inserted_count, "skipped_count": skipped_count}, "anomaly_list": []}), 200

    except Exception as e:
        print(f"Error saat memproses file ARDEBT: {e}")
        return jsonify({"message": f"Gagal memproses file ARDEBT: {e}. Pastikan format data benar."}), 500


# --- API CORE (PENCARIAN DAN RINGKASAN DASHBOARD) ---

@app.route('/api/search', methods=['GET'])
@login_required 
def search_nomen():
    db_status = get_db_status()
    if db_status['status'] == 'error':
        return jsonify({"message": db_status['message']}), 500
        
    collections = db_status['collections']
    query_nomen = request.args.get('nomen', '').strip()

    if not query_nomen:
        return jsonify({"status": "fail", "message": "Masukkan NOMEN untuk memulai pencarian terintegrasi."}), 400

    try:
        cleaned_nomen = query_nomen.strip().upper()
        
        # 1. DATA STATIS (CID) - Ambil data CID TERBARU
        cid_result = collections['cid'].find({'NOMEN': cleaned_nomen}).sort('TANGGAL_UPLOAD_CID', -1).limit(1)
        cid_result = list(cid_result)[0] if list(cid_result) else None
        
        if not cid_result:
            return jsonify({
                "status": "not_found",
                "message": f"NOMEN {query_nomen} tidak ditemukan di Master Data Pelanggan (CID)."
            }), 404

        # 2. RIWAYAT PIUTANG (MC)
        mc_results = list(collections['mc'].find({'NOMEN': cleaned_nomen}).sort('BULAN_TAGIHAN', -1))
        piutang_nominal_total = sum(item.get('NOMINAL', 0) for item in mc_results)
        
        # 3. RIWAYAT TUNGGAKAN DETAIL (ARDEBT)
        ardebt_results = list(collections['ardebt'].find({'NOMEN': cleaned_nomen}).sort('PERIODE_BILL', -1))
        tunggakan_nominal_total = sum(item.get('JUMLAH', 0) for item in ardebt_results)
        
        # 4. RIWAYAT PEMBAYARAN TERAKHIR (MB)
        mb_last_payment_cursor = collections['mb'].find({'NOMEN': cleaned_nomen}).sort('TGL_BAYAR', -1).limit(1)
        last_payment = list(mb_last_payment_cursor)[0] if list(mb_last_payment_cursor) else None
        
        # 5. RIWAYAT BACA METER (SBRS) - 2 Riwayat terakhir untuk Anomaly Check
        sbrs_last_read_cursor = collections['sbrs'].find({'CMR_ACCOUNT': cleaned_nomen}).sort('CMR_RD_DATE', -1).limit(2)
        sbrs_history = list(sbrs_last_read_cursor)
        
        # --- LOGIKA KECERDASAN (INTEGRASI & DIAGNOSTIK) ---
        
        mc_latest = mc_results[0] if mc_results else None
        
        # Ekstraksi ZONA
        zona_info = {}
        if mc_latest and mc_latest.get('ZONA_NOVAK'):
             zona_info = _parse_zona_novak(mc_latest['ZONA_NOVAK'])

        # A. Status Tunggakan/Piutang
        if tunggakan_nominal_total > 0:
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
        sbrs_latest = sbrs_history[0] if sbrs_history else {}
        
        if len(sbrs_history) >= 1:
            kubik_terakhir = sbrs_latest.get('CMR_KUBIK', 0)
            
            if kubik_terakhir > 100: status_pemakaian = f"EKSTRIM ({kubik_terakhir} m³)"
            elif kubik_terakhir <= 5 and kubik_terakhir > 0: status_pemakaian = f"TURUN DRASITS / RENDAH ({kubik_terakhir} m³)"
            elif kubik_terakhir == 0: status_pemakaian = "ZERO (0 m³) / NON-AKTIF"
            else: status_pemakaian = f"NORMAL ({kubik_terakhir} m³)"

        # Profil Pelanggan Terpadu
        cid_master = cid_result
        mc_latest_data = mc_latest if mc_latest else {}
        sbrs_latest_data = sbrs_history[0] if sbrs_history else {}
        
        profile_pelanggan = {
            "NOMEN": cleaned_nomen,
            "NAMA_PEL": mc_latest_data.get('NAMA_PEL', cid_master.get('NAMA', 'N/A')),
            "ALAMAT": cid_master.get('ALAMAT', 'N/A'),
            "RAYON": zona_info.get('RAYON_ZONA', cid_master.get('RAYON', 'N/A')),
            "TARIF": mc_latest_data.get('TARIF', cid_master.get('TARIF', 'N/A')),
            "MERK": cid_master.get('MERK', 'N/A'), 
            "SERIAL": cid_master.get('SERIAL', 'N/A'),
            "KUBIK": mc_latest_data.get('KUBIK', 'N/A'),
            "READ_METHOD": cid_master.get('READ_METHOD', 'N/A'),
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
        return jsonify({"message": f"Gagal mengambil data terintegrasi: {e}"}), 500


@app.route('/api/dashboard/summary', methods=['GET'])
@login_required
def dashboard_summary_api():
    db_status = get_db_status()
    if db_status['status'] == 'error': return jsonify({"message": db_status['message']}), 500
    collections = db_status['collections']
    
    try:
        summary_data = {}
        
        latest_mc_month_doc = collections['mc'].find_one(sort=[('BULAN_TAGIHAN', -1)])
        latest_mc_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None
        
        summary_data['total_pelanggan'] = len(collections['cid'].distinct('NOMEN'))
        
        pipeline_piutang = [
            {'$match': {'BULAN_TAGIHAN': latest_mc_month, 'NOMINAL': {'$gt': 0}}} if latest_mc_month else {'$match': {'NOMINAL': {'$gt': 0}}},
            {'$group': {
                '_id': None,
                'total_piutang': {'$sum': {"$toDouble": {'$cond': [{'$ne': ['$NOMINAL', None]}, '$NOMINAL', 0]}}},
                'jumlah_tagihan': {'$sum': 1}
            }}
        ]
        piutang_result = list(collections['mc'].aggregate(pipeline_piutang))
        summary_data['total_piutang'] = piutang_result[0]['total_piutang'] if piutang_result else 0
        summary_data['jumlah_tagihan'] = piutang_result[0]['jumlah_tagihan'] if piutang_result else 0
        
        pipeline_tunggakan = [
            {'$match': {'JUMLAH': {'$gt': 0}}},
            {'$group': {
                '_id': None,
                'total_tunggakan': {'$sum': {"$toDouble": {'$cond': [{'$ne': ['$JUMLAH', None]}, '$JUMLAH', 0]}}},
                'jumlah_tunggakan': {'$sum': 1}
            }}
        ]
        tunggakan_result = list(collections['ardebt'].aggregate(pipeline_tunggakan))
        summary_data['total_tunggakan'] = tunggakan_result[0]['total_tunggakan'] if tunggakan_result else 0
        summary_data['jumlah_tunggakan'] = tunggakan_result[0]['jumlah_tunggakan'] if tunggakan_result else 0
        
        today_date = datetime.now().strftime('%Y-%m-%d')
        pipeline_koleksi_today = [
            {'$match': {'TGL_BAYAR': today_date,
                             'BILL_REASON': 'BIAYA PEMAKAIAN AIR'
            }}, 
            {'$group': {
                '_id': None,
                'koleksi_hari_ini': {'$sum': {"$toDouble": {'$cond': [{'$ne': ['$NOMINAL', None]}, '$NOMINAL', 0]}}},
                'transaksi_hari_ini': {'$sum': 1}
            }}
        ]
        koleksi_result = list(collections['mb'].aggregate(pipeline_koleksi_today))
        summary_data['koleksi_hari_ini'] = koleksi_result[0]['koleksi_hari_ini'] if koleksi_result else 0
        summary_data['transaksi_hari_ini'] = koleksi_result[0]['transaksi_hari_ini'] if koleksi_result else 0
        
        this_month = datetime.now().strftime('%Y-%m')
        pipeline_koleksi_month = [
            {'$match': {'TGL_BAYAR': {'$regex': this_month},
                             'BILL_REASON': 'BIAYA PEMAKAIAN AIR'
            }},
            {'$group': {
                '_id': None,
                'koleksi_bulan_ini': {'$sum': {"$toDouble": {'$cond': [{'$ne': ['$NOMINAL', None]}, '$NOMINAL', 0]}}},
                'transaksi_bulan_ini': {'$sum': 1}
            }}
        ]
        koleksi_month_result = list(collections['mb'].aggregate(pipeline_koleksi_month))
        summary_data['koleksi_bulan_ini'] = koleksi_month_result[0]['koleksi_bulan_ini'] if koleksi_month_result else 0
        summary_data['transaksi_bulan_ini'] = koleksi_month_result[0]['transaksi_bulan_ini'] if koleksi_month_result else 0
        
        # Menggunakan helper function dari utils
        anomalies = _get_sbrs_anomalies(collections['sbrs'], collections['cid'])
        summary_data['total_anomali'] = len(anomalies)
        
        anomali_breakdown = {'kategori': {}}
        for item in anomalies:
            status = item.get('STATUS_PEMAKAIAN', 'UNKNOWN')
            if 'EKSTRIM' in status or 'NAIK' in status: key = 'KENAIKAN_SIGNIFIKAN'
            elif 'TURUN' in status: key = 'PENURUNAN_SIGNIFIKAN'
            elif 'ZERO' in status: key = 'ZERO_USAGE'
            else: key = 'LAINNYA'
            
            if key not in anomali_breakdown['kategori']: anomali_breakdown['kategori'][key] = {'jumlah': 0, 'data': []}
                
            anomali_breakdown['kategori'][key]['jumlah'] += 1
            if len(anomali_breakdown['kategori'][key]['data']) < 10: anomali_breakdown['kategori'][key]['data'].append(item)
            
        summary_data['anomali_breakdown'] = anomali_breakdown
        
        pelanggan_tunggakan = collections['ardebt'].distinct('NOMEN')
        summary_data['pelanggan_dengan_tunggakan'] = len(pelanggan_tunggakan)
        
        pipeline_top_rayon = [
            {'$match': {'BULAN_TAGIHAN': latest_mc_month, 'NOMINAL': {'$gt': 0}}} if latest_mc_month else {'$match': {'NOMINAL': {'$gt': 0}}},
            {'$group': {
                '_id': '$RAYON',
                'total_piutang': {'$sum': {"$toDouble": {'$cond': [{'$ne': ['$NOMINAL', None]}, '$NOMINAL', 0]}}},
                'total_pelanggan': {'$addToSet': '$NOMEN'}
            }},
            {'$project': {'_id': 0, 'RAYON': '$_id', 'total_piutang': 1, 'total_pelanggan': {'$size': '$total_pelanggan'}}},
            {'$sort': {'total_piutang': -1}},
            {'$limit': 5}
        ]
        top_rayon = list(collections['mc'].aggregate(pipeline_top_rayon))
        summary_data['top_rayon_piutang'] = [{'rayon': item['RAYON'], 'total': item['total_piutang']} for item in top_rayon]
        
        trend_data = []
        for i in range(7):
            date_obj = datetime.now() - timedelta(days=i)
            date = date_obj.strftime('%Y-%m-%d')
            
            pipeline = [
                {'$match': {'TGL_BAYAR': date, 'BILL_REASON': 'BIAYA PEMAKAIAN AIR'}}, 
                {'$group': {
                    '_id': None,
                    'total': {'$sum': {"$toDouble": {'$cond': [{'$ne': ['$NOMINAL', None]}, '$NOMINAL', 0]}}},
                    'count': {'$sum': 1}
                }}
            ]
            result = list(collections['mb'].aggregate(pipeline))
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
    db_status = get_db_status()
    if db_status['status'] == 'error': return jsonify({"message": db_status['message']}), 500
    collections = db_status['collections']
    
    latest_mc_month_doc = collections['mc'].find_one(sort=[('BULAN_TAGIHAN', -1)])
    latest_mc_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None
    
    try:
        pipeline_piutang_rayon = [
            {'$match': {'BULAN_TAGIHAN': latest_mc_month, 'NOMINAL': {'$gt': 0}}} if latest_mc_month else {'$match': {'NOMINAL': {'$gt': 0}}}, 
            {'$group': {
                '_id': '$RAYON',
                'total_piutang': {'$sum': {"$toDouble": {'$cond': [{'$ne': ['$NOMINAL', None]}, '$NOMINAL', 0]}}},
                'total_pelanggan': {'$addToSet': '$NOMEN'}
            }},
            {'$project': {'_id': 0, 'RAYON': '$_id', 'total_piutang': 1, 'total_pelanggan': {'$size': '$total_pelanggan'}}},
            {'$sort': {'total_piutang': -1}}
        ]
        rayon_piutang_data = list(collections['mc'].aggregate(pipeline_piutang_rayon))
        
        rayon_map = {item['RAYON']: item for item in rayon_piutang_data}
        
        this_month = datetime.now().strftime('%Y-%m')
        pipeline_koleksi_rayon = [
            {'$match': {'TGL_BAYAR': {'$regex': this_month}, 'BILL_REASON': 'BIAYA PEMAKAIAN AIR'}},
            {'$group': {'_id': '$RAYON', 'total_koleksi': {'$sum': {"$toDouble": {'$cond': [{'$ne': ['$NOMINAL', None]}, '$NOMINAL', 0]}}},}},
        ]
        koleksi_result = list(collections['mb'].aggregate(pipeline_koleksi_rayon))
        
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
    db_status = get_db_status()
    if db_status['status'] == 'error': return jsonify({"message": db_status['message']}), 500
    collections = db_status['collections']
    
    try:
        all_anomalies = _get_sbrs_anomalies(collections['sbrs'], collections['cid'])
        
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
    db_status = get_db_status()
    if db_status['status'] == 'error': return jsonify({"message": db_status['message']}), 500
    collections = db_status['collections']

    try:
        alerts = []
        anomalies = _get_sbrs_anomalies(collections['sbrs'], collections['cid'])
        ekstrim_alerts = [
            {'nomen': a['NOMEN'], 'status': a['STATUS_PEMAKAIAN'], 'ray': a['RAYON'], 'category': 'VOLUME_EKSTRIM'}
            for a in anomalies if 'EKSTRIM' in a['STATUS_PEMAKAIAN'] or 'ZERO' in a['STATUS_PEMAKAIAN']
        ]
        alerts.extend(ekstrim_alerts[:20])

        pipeline_critical_debt = [
            {'$match': {'JUMLAH': {'$gt': 0}}},
            {'$group': {
                '_id': '$NOMEN',
                'months': {'$sum': 1},
                'amount': {'$sum': {"$toDouble": {'$cond': [{'$ne': ['$JUMLAH', None]}, '$JUMLAH', 0]}}}
            }},
            {'$match': {'months': {'$gte': 5}}},
            {'$limit': 20}
        ]
        
        critical_debt_result = list(collections['ardebt'].aggregate(pipeline_critical_debt))
        
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
    db_status = get_db_status()
    if db_status['status'] == 'error': return jsonify({"message": db_status['message']}), 500
        
    try:
        summary_response = dashboard_summary_api()
        summary_data = summary_response.get_json()
        
        rayon_response = rayon_analysis_api()
        rayon_data = rayon_response.get_json()
        
        df_rayon = pd.DataFrame(rayon_data)
        df_summary = pd.DataFrame({
            'Metrik': ['Total Pelanggan', 'Total Piutang (MC)', 'Total Tunggakan (ARDEBT)', 'Koleksi Bulan Ini', 'Persentase Koleksi'],
            'Nilai': [summary_data['total_pelanggan'], summary_data['total_piutang'], summary_data['total_tunggakan'], summary_data['koleksi_bulan_ini'], f"{summary_data['persentase_koleksi']:.2f}%"]
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
    db_status = get_db_status()
    if db_status['status'] == 'error': return jsonify({"message": db_status['message']}), 500
    collections = db_status['collections']
        
    try:
        all_anomalies = _get_sbrs_anomalies(collections['sbrs'], collections['cid'])
        
        if not all_anomalies: return jsonify({"message": "Tidak ada data anomali untuk diekspor."}), 404
            
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
