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
from bson.objectid import ObjectId

load_dotenv() 

# --- KONFIGURASI APLIKASI & DATABASE ---
app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv("SECRET_KEY")

# Konfigurasi MongoDB
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB_NAME")
COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME")

NOME_COLUMN_NAME = 'NOMEN' 
ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls'} 

# Koneksi ke MongoDB (Hanya untuk Data Tagihan)
client = None
collection_data = None
try:
    client = MongoClient(MONGO_URI)
    client.admin.command('ping') 
    db = client[DB_NAME]
    collection_data = db[COLLECTION_NAME] 
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
                'id': username, 
                'password_hash': hashed_password,
                'is_admin': is_admin,
                'username': username
            }
        except ValueError as e:
            print(f"Peringatan: Format USER_LIST salah pada entry '{user_entry}'. Error: {e}")


# --- KONFIGURASI FLASK-LOGIN ---
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login' 
login_manager.login_message_category = 'info'

# --- MODEL PENGGUNA (User Model) ---
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

# --- FORMULIR LOGIN (Flask-WTF) ---
class LoginForm(FlaskForm):
    username = StringField('Username', validators=[DataRequired()])
    password = PasswordField('Password', validators=[DataRequired()])
    submit = SubmitField('Masuk')

# --- DEKORATOR OTORISASI ADMIN ---
def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.is_admin:
            flash('Anda tidak memiliki izin (Admin) untuk mengakses halaman ini.', 'danger')
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function

# --- Fungsi Utility ---
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

# --- ENDPOINT KOLEKSI TERPADU (UNIFIED COLLECTION PAGE) ---
@app.route('/daily_collection', methods=['GET'])
@login_required 
def daily_collection_unified_page():
    """Menampilkan halaman tunggal yang menggabungkan Report dan Detail Koleksi."""
    return render_template('collection_unified.html', is_admin=current_user.is_admin)

@app.route('/api/collection/report', methods=['GET'])
@login_required 
def collection_report_api():
    """Menghitung Nomen Bayar, Nominal Bayar, Total Nominal, dan Persentase per Rayon/PCEZ."""
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500

    # 1. Agregasi Total Piutang (ASUMSI: Semua record adalah piutang)
    pipeline_billed = [
        { '$group': {
            '_id': { 'rayon': '$RAYON', 'pcez': '$PCEZ' },
            'total_nomen_all': { '$addToSet': '$NOMEN' },
            'total_nominal': { '$sum': '$NOMINAL' } 
        }}
    ]
    billed_data = list(collection_data.aggregate(pipeline_billed))

    # 2. Agregasi Total Koleksi (Hanya STATUS='Payment')
    pipeline_collected = [
        { '$match': { 'STATUS': 'Payment' } }, 
        { '$group': {
            '_id': { 'rayon': '$RAYON', 'pcez': '$PCEZ' },
            'collected_nomen': { '$addToSet': '$NOMEN' }, 
            'collected_nominal': { '$sum': '$NOMINAL' } 
        }}
    ]
    collected_data = list(collection_data.aggregate(pipeline_collected))

    # 3. Gabungkan hasil Billed dan Collected
    report_map = {}
    
    for item in billed_data:
        key = (item['_id']['rayon'], item['_id']['pcez'])
        report_map[key] = {
            'RAYON': item['_id']['rayon'],
            'PCEZ': item['_id']['pcez'],
            'TotalNominal': float(item['total_nominal']),
            'TotalNomen': len(item['total_nomen_all']),
            'CollectedNominal': 0.0, 
            'CollectedNomen': 0, 
            'PercentNominal': 0.0,
            'PercentNomenCount': 0.0
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

    # 4. Hitung Total Keseluruhan (Grand Total)
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
    
    mongo_query = {'STATUS': 'Payment'}
    
    if query_str:
        search_filter = {
            '$or': [
                {'RAYON': {'$regex': query_str, '$options': 'i'}},
                {'PCEZ': {'$regex': query_str, '$options': 'i'}},
                {'NOMEN': {'$regex': query_str, '$options': 'i'}}
            ]
        }
        mongo_query.update(search_filter)

    sort_order = [('PAY_DT', -1)] 

    try:
        results = list(collection_data.find(mongo_query)
                                      .sort(sort_order)
                                      .limit(1000))

        cleaned_results = []
        for doc in results:
            # Pastikan NOMINAL dikonversi ke float untuk total di frontend
            nominal_val = float(doc.get('NOMINAL', 0)) 
            
            cleaned_results.append({
                'NOMEN': doc.get('NOMEN'),
                'RAYON': doc.get('RAYON'),
                'PCEZ': doc.get('PCEZ'),
                'NOMINAL': nominal_val,
                'PAY_DT': doc.get('PAY_DT')
            })
            
        return jsonify(cleaned_results), 200

    except Exception as e:
        print(f"Error fetching detailed collection data: {e}")
        return jsonify({"message": f"Gagal mengambil data detail koleksi: {e}"}), 500


# --- ENDPOINT ANALISIS DATA DINAMIS (SEMUA PENGGUNA) ---
@app.route('/analyze_data', methods=['GET'])
@login_required 
def analyze_data_page():
    return render_template('analyze.html', is_admin=current_user.is_admin)

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

# --- ENDPOINT ADMIN & UTAMA ---
@app.route('/admin/upload', methods=['GET'])
@login_required 
@admin_required 
def admin_upload_page():
    return render_template('upload_admin.html', is_admin=current_user.is_admin)

@app.route('/upload', methods=['POST'])
@login_required 
@admin_required 
def upload_data():
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
        
        if NOME_COLUMN_NAME in df.columns:
            df[NOME_COLUMN_NAME] = df[NOME_COLUMN_NAME].astype(str).str.strip() 

        df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x) 
        
        data_to_insert = df.to_dict('records')
        
        collection_data.delete_many({})
        
        if data_to_insert:
            collection_data.insert_many(data_to_insert)
            count = len(data_to_insert)
            return jsonify({"message": f"Sukses! {count} baris data dari {file_extension.upper()} berhasil diperbarui ke MongoDB."}), 200
        else:
            return jsonify({"message": "File kosong, tidak ada data yang dimasukkan."}), 200

    except Exception as e:
        print(f"Error saat memproses file: {e}")
        return jsonify({"message": f"Gagal memproses file: {e}. Pastikan format data benar dan kolom tersedia."}), 500

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
        return jsonify([])

    try:
        mongo_query = { NOME_COLUMN_NAME: query_nomen }
        
        results = list(collection_data.find(mongo_query).limit(50)) 
        
        for result in results:
            result.pop('_id', None) 

        return jsonify(results), 200

    except Exception as e:
        print(f"Error saat mencari data: {e}")
        return jsonify({"message": "Gagal mengambil data dari database."}), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
