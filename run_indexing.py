import os
from pymongo import MongoClient
from dotenv import load_dotenv

# Load konfigurasi dari .env
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

def create_indexes():
    try:
        client = MongoClient(MONGO_URI)
        # Ambil nama database dari URI
        db_name = MONGO_URI.split("/")[-1].split("?")[0]
        db = client[db_name]
        
        print(f"--- Memulai Proses Indexing pada DB: {db_name} ---")

        # 1. MasterCetak
        print("Indexing MasterCetak...")
        db.MasterCetak.create_index([("BULAN_TAGIHAN", 1), ("NOMEN", 1)])
        
        # 2. CustomerData
        print("Indexing CustomerData...")
        db.CustomerData.create_index([("NOMEN", 1)])
        
        # 3. MasterBayar
        print("Indexing MasterBayar...")
        db.MasterBayar.create_index([("TGL_BAYAR", 1), ("NOMEN", 1)])
        
        # 4. MeterReading
        print("Indexing MeterReading...")
        db.MeterReading.create_index([("BULAN_BACA", 1), ("NOMEN", 1)])

        print("--- ✅ SEMUA INDEX BERHASIL DIBUAT ---")
        print("Sekarang coba reload halaman laporan Anda.")

    except Exception as e:
        print(f"❌ Terjadi kesalahan: {e}")

if __name__ == "__main__":
    create_indexes()
