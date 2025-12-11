import os
import pandas as pd
from flask import Flask, request, jsonify, render_template, redirect, url_for, flash
from pymongo import MongoClient
from dotenv import load_dotenv
from werkzeug.utils import secure_filename, check_password_hash, generate_password_hash
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required
from flask_wtf import FlaskForm 
from wtforms import StringField, PasswordField, SubmitField 
from wtforms.validators import DataRequired 

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

# Koneksi ke MongoDB
client = None
try:
    client = MongoClient(MONGO_URI)
    client.admin.command('ping') 
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    print("Koneksi MongoDB berhasil!")
except Exception as e:
    print(f"Gagal terhubung ke MongoDB: {e}")
    client = None

# --- KONFIGURASI FLASK-LOGIN ---
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login' 
login_manager.login_message_category = 'info'

# --- MODEL PENGGUNA (User Model) ---
class User(UserMixin):
    def __init__(self, id, username, password_hash):
        self.id = id
        self.username = username
        self.password_hash = password_hash

# Membuat User data (untuk testing)
TEST_USERNAME = os.getenv("WEB_USERNAME")
TEST_PASSWORD = os.getenv("WEB_PASSWORD")
# Menghasilkan ID unik dari username
TEST_USER_ID = int(TEST_USERNAME.encode('utf-8').hex(), 16) 
# Menghasilkan hash password dari nilai di .env
PASSWORD_HASH = generate_password_hash(TEST_PASSWORD)

USER_DATA = {
    TEST_USER_ID: User(
        id=TEST_USER_ID,
        username=TEST_USERNAME,
        password_hash=PASSWORD_HASH 
    )
}

@login_manager.user_loader
def load_user(user_id):
    """Callback yang digunakan Flask-Login untuk memuat pengguna dari ID sesi."""
    return USER_DATA.get(int(user_id))

# --- FORMULIR LOGIN (Flask-WTF) ---
class LoginForm(FlaskForm):
    username = StringField('Username', validators=[DataRequired()])
    password = PasswordField('Password', validators=[DataRequired()])
    submit = SubmitField('Masuk')

# --- Fungsi Utility ---
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# --- ENDPOINT AUTENTIKASI ---
@app.route('/login', methods=['GET', 'POST'])
def login():
    form = LoginForm()
    if form.validate_on_submit():
        user = next((u for u in USER_DATA.values() if u.username == form.username.data), None)
        
        # Memverifikasi password
        if user and check_password_hash(user.password_hash, form.password.data):
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

# --- ENDPOINT APLIKASI UTAMA (DILINDUNGI) ---
@app.route('/')
@login_required 
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
@login_required 
def upload_data():
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database. Cek MONGO_URI Anda."}), 500
    
    if 'file' not in request.files:
        return jsonify({"message": "Tidak ada file di permintaan"}), 400

    file = request.files['file']
    if file.filename == '' or not allowed_file(file.filename):
        return jsonify({"message": "Format file tidak valid. Harap unggah CSV, XLSX, atau XLS."}), 400

    filename = secure_filename(file.filename)
    file_extension = filename.rsplit('.', 1)[1].lower()

    try:
        # 1. Membaca file menggunakan Pandas
        if file_extension == 'csv':
            df = pd.read_csv(file)
        elif file_extension in ['xlsx', 'xls']:
            df = pd.read_excel(file, sheet_name=0) 
        
        # 2. Pembersihan dan Konversi Data
        df.columns = [col.strip().upper() for col in df.columns]
        
        # SOLUSI TIPE DATA: Paksa kolom NOME_COLUMN_NAME menjadi string sebelum dimasukkan ke Mongo
        if NOME_COLUMN_NAME in df.columns:
            df[NOME_COLUMN_NAME] = df[NOME_COLUMN_NAME].astype(str).str.strip() 

        df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x) 
        
        data_to_insert = df.to_dict('records')
        
        collection.delete_many({})
        
        if data_to_insert:
            collection.insert_many(data_to_insert)
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
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database. Cek MONGO_URI Anda."}), 500
        
    query_nomen = request.args.get('nomen', '').strip()

    if not query_nomen:
        return jsonify([])

    try:
        mongo_query = { NOME_COLUMN_NAME: query_nomen }
        
        results = list(collection.find(mongo_query))
        
        for result in results:
            result.pop('_id', None) 

        return jsonify(results), 200

    except Exception as e:
        print(f"Error saat mencari data: {e}")
        return jsonify({"message": "Gagal mengambil data dari database."}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
