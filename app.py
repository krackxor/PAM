import os
import pandas as pd
from flask import Flask, request, jsonify, render_template
from pymongo import MongoClient
from dotenv import load_dotenv
from werkzeug.utils import secure_filename
from flask_httpauth import HTTPBasicAuth # <-- PUSTAKA AUTENTIKASI

# Muat variabel dari file .env
load_dotenv() 

# --- Konfigurasi Awal & Autentikasi ---
app = Flask(__name__)
auth = HTTPBasicAuth()

# Kredensial Pengguna dari .env
USERS = {
    os.getenv("WEB_USERNAME"): os.getenv("WEB_PASSWORD")
}

@auth.verify_password
def verify_password(username, password):
    """Fungsi verifikasi kredensial pengguna."""
    if username in USERS and USERS[username] == password:
        return username
    return None

# Konfigurasi Database dari .env
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB_NAME")
COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME")

# Kolom pencarian: Harus sesuai dengan nama kolom di CSV Anda (setelah dikonversi ke huruf besar)
NOME_COLUMN_NAME = 'NOMEN' 

# Ekstensi file yang diizinkan untuk diupload
ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls'} 

# Koneksi ke MongoDB
client = None
try:
    client = MongoClient(MONGO_URI)
    client.admin.command('ping') # Tes koneksi
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    print("Koneksi MongoDB berhasil!")
except Exception as e:
    print(f"Gagal terhubung ke MongoDB: {e}")

# --- Fungsi Utility ---
def allowed_file(filename):
    """Mengecek apakah ekstensi file diizinkan."""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# --- Endpoint Routing ---

@app.route('/')
@auth.login_required # <-- Wajib Login
def index():
    """Tampilkan halaman utama (index.html)."""
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
@auth.login_required # <-- Wajib Login
def upload_data():
    """Endpoint untuk mengunggah file (CSV atau Excel) dan memperbarui MongoDB."""
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
        
        # Konversi nama kolom menjadi huruf besar dan bersihkan spasi
        df.columns = [col.strip().upper() for col in df.columns]
        
        # SOLUSI TIPE DATA: Paksa kolom NOME_COLUMN_NAME menjadi string sebelum dimasukkan ke Mongo
        if NOME_COLUMN_NAME in df.columns:
            # Mengonversi ke string dan membersihkan spasi awal/akhir
            df[NOME_COLUMN_NAME] = df[NOME_COLUMN_NAME].astype(str).str.strip() 

        # Bersihkan spasi di nilai data lainnya (hanya untuk kolom string/object)
        df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x) 
        
        # Konversi ke format list of dictionaries
        data_to_insert = df.to_dict('records')
        
        # 3. Hapus data lama dan masukkan data baru
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
@auth.login_required # <-- Wajib Login
def search_nomen():
    """Endpoint API untuk mencari data di MongoDB berdasarkan NOMEN."""
    if client is None:
        return jsonify({"message": "Server tidak terhubung ke Database. Cek MONGO_URI Anda."}), 500
        
    query_nomen = request.args.get('nomen', '').strip()

    if not query_nomen:
        return jsonify([])

    try:
        # Query MongoDB: Mencari NOMEN yang cocok secara eksak
        # NOMEN di database dan query_nomen dari web sudah distringkan
        mongo_query = { NOME_COLUMN_NAME: query_nomen }
        
        results = list(collection.find(mongo_query))
        
        # Bersihkan ID Mongo sebelum dikirim ke klien
        for result in results:
            result.pop('_id', None) 

        return jsonify(results), 200

    except Exception as e:
        print(f"Error saat mencari data: {e}")
        return jsonify({"message": "Gagal mengambil data dari database."}), 500


if __name__ == '__main__':
    # Jalankan Flask
    # Ganti debug=False dan host='0.0.0.0' jika deploy di VPS/Production
    app.run(debug=True, host='0.0.0.0', port=5000)
