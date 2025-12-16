import os
import re
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime
import sys

# Load Environment Variables
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB_NAME")

def print_header(title):
    print(f"\n{'='*60}")
    print(f" {title}")
    print(f"{'='*60}")

def is_valid_date_format(date_str):
    """Cek apakah string sesuai format YYYY-MM-DD"""
    if not isinstance(date_str, str): return False
    # Regex untuk YYYY-MM-DD
    return bool(re.match(r'^\d{4}-\d{2}-\d{2}$', date_str))

def analyze_collection(db, coll_name, numeric_fields=[], date_fields=[], id_field='NOMEN'):
    """Analisa mendalam satu koleksi"""
    coll = db[coll_name]
    total = coll.count_documents({})
    
    print(f"\n📂 ANALISA KOLEKSI: {coll_name}")
    print(f"   📊 Total Dokumen: {total}")
    
    if total == 0:
        print(f"   ❌ KOSONG: Koleksi ini tidak ada isinya. Upload belum berhasil.")
        return {'status': 'EMPTY', 'total': 0}

    issues = []
    
    # 1. Cek Field Numerik (Uang/Angka)
    for field in numeric_fields:
        # Hitung berapa yang string (seharusnya angka)
        bad_count = coll.count_documents({field: {'$type': 'string'}})
        # Hitung berapa yang null/missing
        missing_count = coll.count_documents({field: {'$exists': False}})
        
        if bad_count > 0:
            msg = f"CRITICAL: {bad_count} data di kolom '{field}' bertipe TEXT/STRING. (Harus Angka/Float)"
            print(f"   ⚠️  {msg}")
            issues.append(msg)
        elif missing_count == total:
             msg = f"CRITICAL: Kolom '{field}' TIDAK DITEMUKAN sama sekali."
             print(f"   ❌ {msg}")
             issues.append(msg)
        else:
            print(f"   ✅ Kolom '{field}' aman (Numerik).")

    # 2. Cek Field Tanggal
    for field in date_fields:
        sample = coll.find_one({field: {'$exists': True}})
        if sample:
            val = sample.get(field)
            if not is_valid_date_format(str(val)):
                msg = f"WARNING: Format '{field}' salah. Contoh data: '{val}' (Harusnya YYYY-MM-DD)"
                print(f"   ⚠️  {msg}")
                issues.append(msg)
            else:
                print(f"   ✅ Kolom '{field}' formatnya benar (ISO Date).")

    # 3. Cek ID (Leading Zero)
    sample_id = coll.find_one()
    id_val = sample_id.get(id_field)
    print(f"   ℹ️  Sampel ID ({id_field}): '{id_val}' (Tipe: {type(id_val).__name__})")
    
    if isinstance(id_val, (int, float)):
        msg = f"WARNING: ID '{id_field}' tersimpan sebagai ANGKA. Nol di depan (0123) akan hilang."
        print(f"   ⚠️  {msg}")
        issues.append(msg)

    return {'status': 'OK' if not issues else 'ISSUES', 'issues': issues, 'total': total}

def cek_kondisi_data():
    if not MONGO_URI or not DB_NAME:
        print("❌ ERROR: .env tidak terbaca atau variabel kosong.")
        return

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping') # Cek koneksi nyata
        db = client[DB_NAME]
        
        print_header(f"DIAGNOSA KESEHATAN DATABASE: {DB_NAME}")

        # --- DATA PEMBANDING (CID) ---
        cid_stats = analyze_collection(db, 'CustomerData', 
                                     numeric_fields=[], 
                                     id_field='NOMEN')
        
        cid_nomen_set = set()
        if cid_stats['total'] > 0:
            # Ambil semua Nomen untuk cek relasi
            cursor = db['CustomerData'].find({}, {'NOMEN': 1})
            cid_nomen_set = {str(doc.get('NOMEN')) for doc in cursor}
            print(f"   ℹ️  Index Pelanggan dimuat: {len(cid_nomen_set)} ID unik.")

        # --- ANALISA MC (TAGIHAN) ---
        mc_stats = analyze_collection(db, 'MasterCetak', 
                                    numeric_fields=['NOMINAL', 'KUBIK'], 
                                    date_fields=[], # MC biasanya pakai BULAN_TAGIHAN string
                                    id_field='NOMEN')
        
        # Cek Relasi MC -> CID
        if mc_stats['total'] > 0 and cid_stats['total'] > 0:
            orphan_mc = 0
            # Cek sampel 100 data
            for doc in db['MasterCetak'].find().limit(100):
                if str(doc.get('NOMEN')) not in cid_nomen_set:
                    orphan_mc += 1
            if orphan_mc > 0:
                print(f"   ⚠️  INTEGRITY CHECK: Dari 100 sampel Tagihan, {orphan_mc} ID tidak ada di CustomerData.")
                print("       -> Artinya: Ada tagihan untuk pelanggan yang tidak terdaftar.")

        # --- ANALISA MB (PEMBAYARAN) ---
        mb_stats = analyze_collection(db, 'MasterBayar', 
                                    numeric_fields=['NOMINAL', 'DENDA'], 
                                    date_fields=['TGL_BAYAR'], 
                                    id_field='NOMEN')

        # --- ANALISA ARDEBT (TUNGGAKAN) ---
        ardebt_stats = analyze_collection(db, 'AccountReceivable', 
                                        numeric_fields=['TOTAL_TUNGGAKAN'], 
                                        id_field='NOMEN')

        # --- KESIMPULAN AKHIR ---
        print_header("KESIMPULAN DOKTER DATABASE")
        
        critical_errors = []
        
        # 1. Cek Kosong
        if mc_stats['total'] == 0: critical_errors.append("Tabel Tagihan (MC) KOSONG.")
        if cid_stats['total'] == 0: critical_errors.append("Tabel Pelanggan (CID) KOSONG.")
        
        # 2. Cek Isu Nominal
        for msg in mc_stats.get('issues', []) + mb_stats.get('issues', []):
            if "CRITICAL" in msg: critical_errors.append(msg)
            
        # 3. Cek Isu Tanggal
        for msg in mb_stats.get('issues', []):
            if "Format" in msg: critical_errors.append("Format Tanggal Salah di MasterBayar.")

        if not critical_errors:
            print("✅ DATABASE SEHAT SECARA STRUKTUR.")
            print("   Jika dashboard masih 0, kemungkinan masalah ada di:")
            print("   1. Filter Bulan di Codingan Dashboard tidak cocok dengan data 'BULAN_TAGIHAN'.")
            print("   2. Cache Browser (Coba Ctrl+F5).")
        else:
            print("❌ DITEMUKAN PENYAKIT KRITIS:")
            for err in critical_errors:
                print(f"   - {err}")
            
            print("\n💊 RESEP PERBAIKAN:")
            print("   1. Jalankan script 'reset_db.py' (Hapus Database).")
            print("   2. Restart aplikasi Flask (app.py).")
            print("   3. Upload ulang file dengan urutan: CID -> MC -> ARDEBT -> MB.")
            print("   4. Pastikan menggunakan 'app.py' versi TERBARU yang sudah saya perbaiki.")

    except Exception as e:
        print(f"❌ TERJADI ERROR SAAT DIAGNOSA: {e}")
        print("Pastikan MongoDB server berjalan dan .env benar.")

if __name__ == "__main__":
    cek_kondisi_data()
