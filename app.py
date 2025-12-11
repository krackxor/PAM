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

# --- PEMROSESAN DAFTAR PENGGUNA DARI .ENV ---
STATIC_USERS = {}
user_list_str = os.getenv("USER_LIST", "")

# Membuat hash password statis saat aplikasi dimulai
if user_list_str:
    for user_entry in user_list_str.split(','):
        try:
            username, plain_password, is_admin_str = user_entry.strip().split(':')
            
            # Hash password untuk keamanan
            hashed_password = generate_password_hash(plain_password)
            is_admin = is_admin_str.lower() == 'true'
            
            # Gunakan username sebagai ID unik (untuk Flask-Login)
            STATIC_USERS[username] = {
                'id': username, 
                'password_hash': hashed_password,
                'is_admin': is_admin,
                'username': username
            }
        except ValueError as e:
            # Jika format di .env salah, cetak peringatan
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
    """Callback yang digunakan Flask-Login untuk memuat pengguna dari ID sesi."""
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
    """Dekorator untuk membatasi akses hanya kepada pengguna yang memiliki is_admin=True."""
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
            # Login berhasil: buat objek User dari data statis
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


# --- ENDPOINT ADMIN (Hanya Upload) ---
@app.route('/admin/upload', methods=['GET'])
@login_required 
@admin_required # HANYA ADMIN YANG BISA
def admin_upload_page():
    """Menampilkan halaman admin untuk upload data."""
    return render_template('upload_admin.html')


# --- ENDPOINT APLIKASI UTAMA (DILINDUNGI) ---
@app.route('/')
@login_required 
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
@login_required 
@admin_required # HANYA ADMIN YANG BISA
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

@app.route('/api/search', methods=['GET'])
@login_required 
def search_nomen():
    # API pencarian ini bisa diakses oleh siapa saja yang sudah login (Admin atau Pengguna Biasa)
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database."}), 500
        
    query_nomen = request.args.get('nomen', '').strip()

    if not query_nomen:
        return jsonify([])

    try:
        mongo_query = { NOME_COLUMN_NAME: query_nomen }
        
        results = list(collection_data.find(mongo_query)) 
        
        for result in results:
            result.pop('_id', None) 

        return jsonify(results), 200

    except Exception as e:
        print(f"Error saat mencari data: {e}")
        return jsonify({"message": "Gagal mengambil data dari database."}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
