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
    
    # Ambil ulang URI jika belum terdefinisi (antisipasi urutan load_dotenv)
    uri = os.getenv("MONGO_URI") or MONGO_URI
    
    if not uri:
        print("⚠️ Warning: MONGO_URI tidak ditemukan. Menggunakan localhost default.")
        uri = "mongodb://localhost:27017"

    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=12000)
        # Ping database untuk memastikan koneksi aktif
        client.admin.command('ping')
        
        db = client[DB_NAME]
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
        
        # Membuat Index agar pencarian NOMEN dan RAYON secepat kilat
        # Indexing sangat penting untuk collection besar agar tidak lemot
        for name, coll in collections.items():
            coll.create_index([('NOMEN', 1)])
            coll.create_index([('RAYON', 1)])
            # Khusus data SBRS sering menggunakan lowercase cmr_account
            if name == 'sbrs':
                coll.create_index([('cmr_account', 1)])
            
        print(f"✅ Database '{DB_NAME}' & Kamus Analitik PAM Siap.")
    except Exception as e:
        print(f"❌ Koneksi Database Gagal: {e}")

# --- 1. MESIN PEMBERSIH DATA (DATA CLEANSING) ---

def clean_dataframe(df):
    """Membersihkan header dan menormalisasi tipe data agar perhitungan statistik akurat."""
    # Normalisasi Header ke Huruf Besar dan Hapus Spasi
    df.columns = [str(col).strip().upper() for col in df.columns]
    
    # Bersihkan whitespace pada data teks
    for col in df.select_dtypes(['object']).columns:
        df[col] = df[col].astype(str).str.strip().str.upper()
    
    # Konversi otomatis kolom numerik kritis (Menghindari error saat agregasi)
    numeric_keys = [
        'NOMINAL', 'JUMLAH', 'VOLUME', 'KUBIK', 'READING', 'PREV', 
        'AMT_COLLECT', 'VOL_COLLECT', 'KONSUMSI', 'CMR_READING', 
        'CMR_PREV_READ', 'CMR_HI1_RDG', 'STAN_AWAL', 'STAN_AKIR'
    ]
    for col in df.columns:
        if any(key in col for key in numeric_keys):
            # Membersihkan koma ribuan dan mengonversi ke angka
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
    
    return df.to_dict('records')

# --- 2. SUMMARIZING ENGINE (MULTIDIMENSI) ---

def get_summarized_report(coll_name, dimension='RAYON'):
    """
    Summarizing Data: Mengubah ribuan baris menjadi ringkasan yang mudah dipahami.
    """
    if coll_name not in collections or not db: return []
    
    dim_map = {
        'RAYON': '$RAYON',
        'PC': '$PC',
        'PCEZ': '$PCEZ',
        'TARIF': '$TARIF',
        'METER': '$UKURAN_METER'
    }
    
    # Penentuan Field Nilai berdasarkan jenis data
    val_field = "$NOMINAL" if coll_name in ['mc', 'mb'] else ("$AMT_COLLECT" if coll_name == 'coll' else "$JUMLAH")
    vol_field = "$KUBIK" if coll_name in ['mc', 'mb', 'coll'] else "$VOLUME"

    pipeline = [
        {"$group": {
            "_id": dim_map.get(dimension, '$RAYON'),
            "total_nominal": {"$sum": val_field},
            "total_volume": {"$sum": vol_field},
            "count": {"$sum": 1}
        }},
        {"$sort": {"_id": 1}}
    ]
    return list(collections[coll_name].aggregate(pipeline))

# --- 3. LOGIKA COLLECTION & STATUS PIUTANG ---

def get_collection_analysis():
    """Analisa Arus Kas Mendalam: Paid Undue, Current, Arrears."""
    if not collections.get('coll'): return []
    
    pipeline = [
        {"$project": {
            "category": {
                "$cond": {
                    "if": {"$gt": ["$BILL_PERIOD", "$PAY_DT"]}, "then": "UNDUE",
                    "else": {"$cond": {"if": {"$eq": ["$BILL_PERIOD", "$PAY_DT"]}, "then": "CURRENT", "else": "ARREARS"}}
                }
            },
            "AMT_COLLECT": 1, "VOL_COLLECT": 1, "STATUS": 1
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
    """
    Sistem Deteksi 7 Jenis Anomali Meter Reading.
    """
    anomalies = []
    EXTREME_THRESHOLD = 150 

    for _, row in df_sbrs.iterrows():
        prev = row.get('CMR_PREV_READ', 0)
        curr = row.get('CMR_READING', 0)
        usage = curr - prev
        skip = str(row.get('CMR_SKIP_CODE', '')).upper()
        trbl = str(row.get('CMR_TRBL1_CODE', '')).upper()
        method = str(row.get('CMR_READ_CODE', '')).upper()
        special_msg = str(row.get('CMR_CHG_SPCL_MSG', '')).upper()
        
        status_tags = []
        
        # A. Stand Negatif
        if curr < prev: status_tags.append("STAND NEGATIF")
        
        # B. Pemakaian Extreme
        if usage > EXTREME_THRESHOLD: status_tags.append("EKSTRIM")
        
        # C. Pemakaian Turun Drastis
        hi_rdg = row.get('CMR_HI1_RDG', 0)
        if hi_rdg > 0 and usage < (hi_rdg * 0.3) and usage > 0: status_tags.append("PEMAKAIAN TURUN")
        
        # D. Pemakaian Zero
        if usage == 0 and prev > 0: status_tags.append("PEMAKAIAN ZERO")
        
        # E. Indikasi Salah Catat (Audit Lapangan)
        if skip == "3A" and usage > 5: status_tags.append("SALAH CATAT")
        
        # F. Rebill & Estimasi
        if "REBILL" in special_msg: status_tags.append("REBILL")
        if any(e in method for e in ["30/PE", "35/PS", "40/PE"]): status_tags.append("ESTIMASI")

        if status_tags:
            anomalies.append({
                'nomen': row.get('CMR_ACCOUNT'),
                'mrid': row.get('CMR_MRID'),
                'name': row.get('CMR_NAME'),
                'usage': int(usage),
                'status': status_tags,
                'raw': {
                    'prev': int(prev), 
                    'curr': int(curr), 
                    'date': row.get('CMR_RD_DATE'),
                    'skip': f"{skip} - {SKIP_MAP.get(skip, 'Normal')}",
                    'trbl': f"{trbl} - {TROUBLE_MAP.get(trbl, 'Normal')}",
                    'method': READ_METHOD_MAP.get(method, method),
                    'msg': row.get('CMR_CHG_SPCL_MSG', '-')
                }
            })
    return anomalies

# --- 5. TOP 100 ANALYTICS ---

def get_top_100_data(category, rayon_id):
    """Ranking Top 100 berdasarkan kategori per Rayon."""
    if not db: return []
    query = {"RAYON": str(rayon_id)}
    
    if category == 'PREMIUM':
        return list(collections['mb'].find(query).sort("NOMINAL", -1).limit(100))
    elif category == 'TUNGGAKAN':
        return list(collections['ardebt'].find(query).sort("JUMLAH", -1).limit(100))
    elif category == 'UNPAID_CURRENT':
        return list(collections['mainbill'].find(query).sort("TOTAL_TAGIHAN", -1).limit(100))
        
    return []

# --- 6. HISTORY DETEKTIF ---

def get_audit_detective_data(nomen):
    """Mengambil sejarah lengkap 12 bulan pelanggan untuk audit manual."""
    if not db: return {}
    
    history_sbrs = list(collections['sbrs'].find({"cmr_account": nomen}).sort("cmr_rd_date", -1).limit(12))
    history_bayar = list(collections['mb'].find({"NOMEN": nomen}).sort("TGL_BAYAR", -1).limit(12))
    history_mc = list(collections['mc'].find({"NOMEN": nomen}).sort([("TAHUN2", -1), ("NAMA_BLN2", -1)]).limit(12))
    
    # Hapus _id agar tidak error saat dikirim ke frontend JSON
    for items in [history_sbrs, history_bayar, history_mc]:
        for item in items: item.pop('_id', None)

    return {
        'reading_history': history_sbrs,
        'payment_history': history_bayar,
        'billing_history': history_mc,
        'customer': collections['cid'].find_one({"NOMEN": nomen}, {"_id": 0}),
        'audit_logs': list(collections['audit'].find({"NOMEN": nomen}).sort("date", -1))
    }

def save_manual_audit(nomen, remark, user, status):
    """Menyimpan keterangan audit manual dari tim ke database."""
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
