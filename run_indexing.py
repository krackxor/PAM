import os
import pymongo
from pymongo import MongoClient
from dotenv import load_dotenv
from urllib.parse import urlparse

# Memuat konfigurasi dari file .env
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

def create_indexes():
    if not MONGO_URI:
        print("❌ Kesalahan: MONGO_URI tidak ditemukan di file .env")
        return

    try:
        client = MongoClient(MONGO_URI)
        
        # Mengambil nama database dari URI (berdasarkan teks yang dipilih di Canvas)
        parsed_uri = urlparse(MONGO_URI)
        db_name = parsed_uri.path.strip('/')
        
        if not db_name:
            # Fallback ke variabel env jika nama database tidak ada di URI path
            db_name = os.getenv("MONGO_DB_NAME", "TagihanDB")

        db = client[db_name]
        print(f"--- Memulai Proses Indexing pada DB: {db_name} ---")

        # Daftar koleksi dan field yang perlu di-index untuk performa laporan
        # Menggunakan nama index spesifik untuk menghindari konflik nama default
        indexes = [
            ("MasterCetak", [("BULAN_TAGIHAN", 1), ("NOMEN", 1)], "idx_mc_report"),
            ("CustomerData", [("NOMEN", 1)], "idx_cust_nomen"),
            ("MasterBayar", [("TGL_BAYAR", 1), ("NOMEN", 1)], "idx_mb_report"),
            ("MeterReading", [("BULAN_BACA", 1), ("NOMEN", 1)], "idx_mr_report")
        ]

        for coll_name, keys, idx_name in indexes:
            try:
                print(f"Memproses indeks untuk koleksi: {coll_name}...")
                db[coll_name].create_index(keys, name=idx_name)
            except pymongo.errors.OperationFailure as e:
                # Jika error karena indeks sudah ada dengan nama berbeda (Code 85), abaikan saja
                if e.code == 85: 
                    print(f"ℹ️  Indeks pada {coll_name} sudah ada dengan konfigurasi yang sama. Melanjutkan...")
                else:
                    raise e

        print("--- ✅ SEMUA INDEX BERHASIL DIPROSES ---")
        print("Optimasi database selesai. Anda sekarang bisa menjalankan: python3 app.py")

    except Exception as e:
        print(f"❌ Terjadi kesalahan: {e}")

if __name__ == "__main__":
    create_indexes()
