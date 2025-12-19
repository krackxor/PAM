import os
import pandas as pd
import numpy as np
from pymongo import MongoClient
from datetime import datetime

# --- KONFIGURASI DATABASE ---
# URI diambil dari .env (pastikan app.py memanggil load_dotenv())
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB_NAME") or "pam_analytics"

client = None
db = None
collections = {}

# --- KAMUS REFERENSI KODE ---
# Digunakan untuk menerjemahkan kode teknis lapangan menjadi informasi yang mudah dipahami
SKIP_MAP = {
    "1A": "Meter Buram (Ganti Meter)", "1B": "Meter Berembun (Ilegal)", "1C": "Meter Rusak (Ilegal)",
    "2A": "Meter Tidak Ada - Air Tidak Dipakai", "2B": "Meter Tidak Ada - Air Dipakai", "3A": "Rumah Kosong",
    "4A": "Rumah Dibongkar", "4B": "Meter Terendam", "4C": "Alamat Tidak Ketemu",
    "5A": "Tutup Bak Meter Berat", "5B": "Meter Tertimbun", "5C": "Meter Terhalang Barang Berat", 
    "5D": "Meter Dicor", "5E": "Bak Meter Dikunci", "5F": "Pagar Dikunci", "5G": "Tidak Diizinkan Baca"
}

TROUBLE_MAP = {
    "1A": "Meter Berembun", "1B": "Meter Mati", "1C": "Meter Buram", "1D": "Segel Pabrik Putus",
    "2A": "Meter Terbalik", "2B": "Meter Dipindah", "2C": "Meter Lepas", "2D": "By Pass Meter",
    "2E": "Meter Dicolok", "2F": "Meter Tidak Normal", "2G": "Kaca Meter Pecah",
    "3A": "Air Kecil/Mati", "4A": "Pipa Dinas Sebelum Meter Bocor", "4B": "Pipa Lama Keluar Air",
    "5A": "Stand Tempel", "5B": "No Seri Beda"
}

READ_METHOD_MAP = {
    "30/PE": "System Estimate", "35/PS": "SP Estimate", "40/PE": "Office Estimate",
    "60/SE": "Regular (Actual)", "80/PE": "Force Billing"
}

def init_db(app=None):
    """Menginisialisasi koneksi MongoDB dan membuat index otomatis untuk performa."""
    global client, db, collections
    
    # Ambil ulang URI dari environment untuk memastikan sinkronisasi dengan .env
    uri = os.getenv("MONGO_URI") or MONGO_URI
    db_name = os.getenv("MONGO_DB_NAME") or DB_NAME
    
    if not uri:
        print("⚠️ Warning: MONGO_URI tidak ditemukan. Menggunakan localhost default.")
        uri = "mongodb://localhost:27017"

    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=12000)
        # Verifikasi koneksi dengan ping
        client.admin.command('ping')
        
        db = client[db_name]
        collections = {
            'mc': db['MasterCetak'],
            'mb': db['MasterBayar'],
            'cid': db['CustomerData'],
            'ardebt': db['AccountReceivable'],
            'sbrs': db['MeterReading'],
            'coll': db['DailyCollection'],
            'mainbill': db['MainBill'],
            'audit': db['ManualAudit']
        }
        
        # Membuat Index untuk optimasi pencarian NOMEN (ID Pelanggan) dan RAYON
        for name, coll in collections.items():
            coll.create_index([('NOMEN', 1)])
            coll.create_index([('RAYON', 1)])
            # Index tambahan untuk format data SBRS
            if name == 'sbrs':
                coll.create_index([('cmr_account', 1)])
            
        print(f"✅ Database '{db_name}' & Kamus Analitik PAM Siap.")
    except Exception as e:
        print(f"❌ Koneksi Database Gagal: {e}")

# --- 1. MESIN PEMBERSIH DATA (DATA CLEANSING) ---

def clean_dataframe(df):
    """Membersihkan header dan menormalisasi tipe data agar perhitungan statistik akurat."""
    # Normalisasi Header: Huruf Besar, Tanpa Spasi
    df.columns = [str(col).strip().upper() for col in df.columns]
    
    # Bersihkan whitespace pada data teks
    for col in df.select_dtypes(['object']).columns:
        df[col] = df[col].astype(str).str.strip().str.upper()
    
    # Daftar kolom yang harus berupa angka untuk perhitungan
    numeric_keys = [
        'NOMINAL', 'JUMLAH', 'VOLUME', 'KUBIK', 'READING', 'PREV', 
        'AMT_COLLECT', 'VOL_COLLECT', 'KONSUMSI', 'CMR_READING', 
        'CMR_PREV_READ', 'CMR_HI1_RDG', 'STAN_AWAL', 'STAN_AKIR'
    ]
    for col in df.columns:
        if any(key in col for key in numeric_keys):
            # Hilangkan karakter pemisah ribuan (koma) dan konversi ke float/int
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
    
    return df.to_dict('records')

# --- 2. SUMMARIZING ENGINE (MULTIDIMENSI) ---

def get_summarized_report(coll_name, dimension='RAYON'):
    """Ringkasan Data: Mengonversi baris mentah menjadi agregat per wilayah/tarif."""
    if coll_name not in collections or not db: return []
    
    dim_field = f"${dimension.upper()}"
    
    # Tentukan field mana yang dijumlahkan berdasarkan koleksi
    val_field = "$NOMINAL" if coll_name in ['mc', 'mb'] else ("$AMT_COLLECT" if coll_name == 'coll' else "$JUMLAH")
    vol_field = "$KUBIK" if coll_name in ['mc', 'mb', 'coll'] else "$VOLUME"

    pipeline = [
        {"$group": {
            "_id": dim_field,
            "total_nominal": {"$sum": val_field},
            "total_volume": {"$sum": vol_field},
            "count": {"$sum": 1}
        }},
        {"$sort": {"_id": 1}}
    ]
    return list(collections[coll_name].aggregate(pipeline))

# --- 3. LOGIKA COLLECTION & STATUS PIUTANG ---

def get_collection_analysis():
    """Analisa Arus Kas: Mendeteksi Pembayaran Undue (Awal), Current (Tepat), Arrears (Tunggakan)."""
    if not collections.get('coll'): return []
    
    pipeline = [
        {"$project": {
            "category": {
                "$cond": {
                    "if": {"$gt": ["$BILL_PERIOD", "$PAY_DT"]}, "then": "UNDUE",
                    "else": {"$cond": {"if": {"$eq": ["$BILL_PERIOD", "$PAY_DT"]}, "then": "CURRENT", "else": "ARREARS"}}
                }
            },
            "AMT_COLLECT": 1, "VOL_COLLECT": 1
        }},
        {"$group": {
            "_id": "$category",
            "total_money": {"$sum": "$AMT_COLLECT"},
            "total_vol": {"$sum": "$VOL_COLLECT"},
            "count": {"$sum": 1}
        }}
    ]
    return list(collections['coll'].aggregate(pipeline))

# --- 4. ENGINE ANALISA METER READING (DETEKTIF) ---

def analyze_meter_anomalies(df_sbrs):
    """Sistem Deteksi Anomali Meter: Stand Negatif, Ekstrim, Zero, Salah Catat, dll."""
    anomalies = []
    EXTREME_LIMIT = 150 # m3

    for _, row in df_sbrs.iterrows():
        prev = float(row.get('CMR_PREV_READ', 0))
        curr = float(row.get('CMR_READING', 0))
        usage = curr - prev
        
        skip = str(row.get('CMR_SKIP_CODE', '')).upper()
        trbl = str(row.get('CMR_TRBL1_CODE', '')).upper()
        method = str(row.get('CMR_READ_CODE', '')).upper()
        msg = str(row.get('CMR_CHG_SPCL_MSG', '')).upper()
        
        status_tags = []
        
        if curr < prev: status_tags.append("STAND NEGATIF")
        if usage > EXTREME_LIMIT: status_tags.append("EKSTRIM")
        
        hi_rdg = float(row.get('CMR_HI1_RDG', 0))
        if hi_rdg > 0 and usage < (hi_rdg * 0.3) and usage > 0: status_tags.append("PEMAKAIAN TURUN")
        if usage == 0 and prev > 0: status_tags.append("PEMAKAIAN ZERO")
        if skip == "3A" and usage > 5: status_tags.append("SALAH CATAT")
        if "REBILL" in msg: status_tags.append("REBILL")
        if any(e in method for e in ["30/PE", "35/PS", "40/PE"]): status_tags.append("ESTIMASI")

        if status_tags:
            anomalies.append({
                'nomen': row.get('CMR_ACCOUNT'),
                'mrid': row.get('CMR_MRID'),
                'name': row.get('CMR_NAME'),
                'usage': int(usage),
                'status': status_tags,
                'details': {
                    'prev': int(prev), 'curr': int(curr),
                    'skip': f"{skip} - {SKIP_MAP.get(skip, 'Normal')}",
                    'trouble': f"{trbl} - {TROUBLE_MAP.get(trbl, 'Normal')}",
                    'method': READ_METHOD_MAP.get(method, method),
                    'special_msg': msg if msg else "-"
                }
            })
    return anomalies

# --- 5. TOP 100 ANALYTICS ---

def get_top_100_data(category, rayon_id):
    """Ranking 100 teratas per wilayah (Rayon) untuk kategori tertentu."""
    if not db: return []
    query = {"RAYON": str(rayon_id)}
    
    # Memilih koleksi berdasarkan kategori pencarian
    coll_target = 'mb' if category == 'PREMIUM' else ('ardebt' if category == 'TUNGGAKAN' else 'mainbill')
    sort_field = 'NOMINAL' if coll_target == 'mb' else ('JUMLAH' if coll_target == 'ardebt' else 'TOTAL_TAGIHAN')
    
    return list(collections[coll_target].find(query).sort(sort_field, -1).limit(100))

# --- 6. HISTORY DETEKTIF ---

def get_audit_detective_data(nomen):
    """Mengambil profil lengkap & history 12 bulan pelanggan untuk audit tim."""
    if not db: return {}
    
    # Riwayat multi-koleksi (SBRS, Bayar, Cetak)
    history_sbrs = list(collections['sbrs'].find({"cmr_account": nomen}).sort("cmr_rd_date", -1).limit(12))
    history_bayar = list(collections['mb'].find({"NOMEN": nomen}).sort("TGL_BAYAR", -1).limit(12))
    history_mc = list(collections['mc'].find({"NOMEN": nomen}).sort([("TAHUN2", -1), ("NAMA_BLN2", -1)]).limit(12))
    
    # Pembersihan field internal MongoDB sebelum dikirim ke Frontend
    for dataset in [history_sbrs, history_bayar, history_mc]:
        for doc in dataset: doc.pop('_id', None)

    return {
        'reading_history': history_sbrs,
        'payment_history': history_bayar,
        'billing_history': history_mc,
        'customer': collections['cid'].find_one({"NOMEN": nomen}, {"_id": 0}),
        'audit_logs': list(collections['audit'].find({"NOMEN": nomen}).sort("date", -1))
    }

def save_manual_audit(nomen, remark, user, status):
    """Menyimpan catatan audit manual dari tim ke dalam database."""
    if not collections.get('audit'): return False
    
    audit_entry = {
        "NOMEN": nomen,
        "remark": remark,
        "status_final": status,
        "user": user,
        "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    collections['audit'].insert_one(audit_entry)
    return True
