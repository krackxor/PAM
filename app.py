import os
import pandas as pd
from flask import Flask, request, jsonify, render_template
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv() # Muat variabel dari file .env

app = Flask(__name__)

# Konfigurasi Database dari .env
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB_NAME")
COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME")
NOME_COLUMN_NAME = 'NOMEN' # Nama kolom untuk pencarian (sesuai data Anda)

# Koneksi ke MongoDB
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

@app.route('/')
def index():
    """Tampilkan halaman utama (tempat upload dan pencarian)."""
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_csv():
    """Endpoint untuk mengunggah file CSV, menghapus data lama, dan memasukkan data baru ke MongoDB."""
    if 'file' not in request.files:
        return jsonify({"message": "Tidak ada file di permintaan"}), 400

    file = request.files['file']
    if file.filename == '' or not file.filename.endswith('.csv'):
        return jsonify({"message": "Format file tidak valid. Harap unggah CSV."}), 400

    try:
        # Baca file CSV menggunakan pandas
        df = pd.read_csv(file)
        
        # Konversi nama kolom menjadi huruf besar dan bersihkan spasi agar konsisten dengan pencarian
        df.columns = [col.strip().upper() for col in df.columns]
        
        # Konversi DataFrame ke format list of dictionaries (yang diterima MongoDB)
        data_to_insert = df.to_dict('records')
        
        # 1. Hapus data lama
        collection.delete_many({})
        
        # 2. Masukkan data baru
        if data_to_insert:
            collection.insert_many(data_to_insert)
            count = len(data_to_insert)
            return jsonify({"message": f"Sukses! {count} baris data berhasil diperbarui ke MongoDB."}), 200
        else:
            return jsonify({"message": "File kosong, tidak ada data yang dimasukkan."}), 200

    except Exception as e:
        print(f"Error saat memproses CSV: {e}")
        return jsonify({"message": f"Gagal memproses file: {e}"}), 500

@app.route('/api/search', methods=['GET'])
def search_nomen():
    """Endpoint API untuk mencari data di MongoDB berdasarkan NOMEN."""
    query_nomen = request.args.get('nomen', '').strip()

    if not query_nomen:
        return jsonify([]) # Kembalikan array kosong jika input kosong

    try:
        # Query MongoDB: Mencari NOMEN yang cocok secara eksak
        # $regex dan $options 'i' bisa digunakan untuk pencarian case-insensitive, tapi untuk ID lebih baik eksak.
        mongo_query = { NOME_COLUMN_NAME: query_nomen }
        
        # Ambil hasil dari MongoDB
        results = list(collection.find(mongo_query))
        
        # Bersihkan ID Mongo sebelum dikirim ke klien
        for result in results:
            result.pop('_id', None) 

        return jsonify(results), 200

    except Exception as e:
        print(f"Error saat mencari data: {e}")
        return jsonify({"message": "Gagal mengambil data dari database."}), 500


if __name__ == '__main__':
    # Pastikan server Flask berjalan di port yang bisa diakses
    app.run(debug=True, host='0.0.0.0', port=5000)
