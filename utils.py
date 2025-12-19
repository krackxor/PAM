import os
import pandas as pd
import numpy as np
from pymongo import MongoClient, ASCENDING, DESCENDING
from datetime import datetime
from dotenv import load_dotenv

# --- KONFIGURASI GLOBAL ---
client = None
db = None
collections = {}

# --- KAMUS REFERENSI KODE (Berdasarkan Standar Operasional Lapangan) ---
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
    """
    Menginisialisasi koneksi MongoDB Atlas. 
    Memastikan variabel lingkungan dimuat dan membuat index untuk performa maksimal.
    """
    global client, db, collections
    
    # Memuat variabel lingkungan dari file .env
    load_dotenv()
    
    uri = os.getenv("MONGO_URI")
    db_name = os.getenv("MONGO_DB_NAME") or "TagihanDB"
    
    if not uri:
        print("❌ CRITICAL: MONGO_URI tidak ditemukan di .env. Pastikan konfigurasi benar.")
        return

    try:
        # Koneksi ke Atlas dengan timeout 5 detik
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping') # Verifikasi koneksi aktif
        
        db = client[db_name]
        
        # Inisialisasi Koleksi Utama
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
        
        # AUTOMATIC INDEXING: Memastikan pencarian NOMEN & RAYON tetap responsif
        for name, coll in collections.items():
            coll.create_index([('NOMEN', ASCENDING)])
            coll.create_index([('RAYON', ASCENDING)])
            if name == 'sbrs':
                coll.create_index([('cmr_account', ASCENDING)])
                coll.create_index([('cmr_rd_date', DESCENDING)])
            
        print(f"✅ Database '{db_name}' Terhubung & Indexing Berhasil.")
    except Exception as e:
        print(f"❌ Koneksi Database Gagal: {str(e)}")

# --- 1. ENGINE PEMBERSIH DATA ---

def clean_dataframe(df):
    """
    Menormalisasi data mentah agar siap diolah oleh sistem.
    - Standardisasi header ke Huruf Besar.
    - Pembersihan whitespace pada teks.
    - Konversi string angka (berkoma) ke numerik murni.
    """
    # Header Standar
    df.columns = [str(col).strip().upper() for col in df.columns]
    
    # Pembersihan teks di seluruh kolom objek
    for col in df.select_dtypes(['object']).columns:
        df[col] = df[col].astype(str).str.strip().str.upper()
    
    # Daftar kolom numerik yang sering muncul di file PAM
    numeric_keys = [
        'NOMINAL', 'JUMLAH', 'VOLUME', 'KUBIK', 'READING', 'PREV', 
        'AMT_COLLECT', 'VOL_COLLECT', 'KONSUMSI', 'CMR_READING', 
        'CMR_PREV_READ', 'CMR_HI1_RDG', 'STAN_AWAL', 'STAN_AKIR'
    ]
    
    for col in df.columns:
        if any(key in col for key in numeric_keys):
            # Menghapus koma ribuan sebelum konversi ke angka
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
    
    return df.to_dict('records')

# --- 2. SUMMARIZING ENGINE ---

def get_summarized_report(coll_name, dimension='RAYON'):
    """
    Menghasilkan ringkasan data berdasarkan dimensi tertentu (RAYON, TARIF, dll).
    Menggunakan agregasi MongoDB untuk kecepatan tinggi pada dataset besar.
    """
    if coll_name not in collections or not db: return []
    
    # Penentuan field nominal & volume berdasarkan target data
    val_field = "$NOMINAL" if coll_name in ['mc', 'mb'] else ("$AMT_COLLECT" if coll_name == 'coll' else "$JUMLAH")
    vol_field = "$KUBIK" if coll_name in ['mc', 'mb', 'coll'] else "$VOLUME"

    pipeline = [
        {"$group": {
            "_id": f"${dimension.upper()}",
            "total_money": {"$sum": val_field},
            "total_usage": {"$sum": vol_field},
            "count": {"$sum": 1}
        }},
        {"$sort": {"_id": 1}}
    ]
    return list(collections[coll_name].aggregate(pipeline))

# --- 3. CASH FLOW ANALYSIS ---

def get_collection_analysis():
    """
    Analisa Kualitas Penagihan: Membagi pembayaran ke kategori Undue, Current, atau Arrears.
    """
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
            "revenue": {"$sum": "$AMT_COLLECT"},
            "volume": {"$sum": "$VOL_COLLECT"},
            "count": {"$sum": 1}
        }}
    ]
    return list(collections['coll'].aggregate(pipeline))

# --- 4. ANALISA DETEKTIF METER ---

def analyze_meter_anomalies(df_sbrs):
    """
    Logic Utama Deteksi 7 Jenis Anomali Meter Reading untuk audit NRW.
    Mendeteksi: Negatif, Lonjakan Ekstrim, Penurunan Drastis, Salah Catat, dll.
    """
    anomalies = []
    THRESHOLD_EKSTRIM = 150 # Batas kubikasi ekstrim

    for _, row in df_sbrs.iterrows():
        prev = float(row.get('CMR_PREV_READ', 0))
        curr = float(row.get('CMR_READING', 0))
        usage = curr - prev
        
        # Pengambilan metadata lapangan
        skip = str(row.get('CMR_SKIP_CODE', '')).upper()
        trbl = str(row.get('CMR_TRBL1_CODE', '')).upper()
        meth = str(row.get('CMR_READ_CODE', '')).upper()
        msg  = str(row.get('CMR_CHG_SPCL_MSG', '')).upper()
        
        tags = []
        
        # 1. Stand Negatif
        if curr < prev: tags.append("STAND NEGATIF")
        
        # 2. Lonjakan Ekstrim
        if usage > THRESHOLD_EKSTRIM: tags.append("EKSTRIM")
        
        # 3. Analisa Penurunan vs History
        hi_rdg = float(row.get('CMR_HI1_RDG', 0))
        if hi_rdg > 0 and usage < (hi_rdg * 0.3) and usage > 0: tags.append("PEMAKAIAN TURUN")
        
        # 4. Pemakaian Nol (Padahal sebelumnya ada)
        if usage == 0 and prev > 0: tags.append("PEMAKAIAN ZERO")
        
        # 5. Salah Catat (Audit Lapangan)
        if skip == "3A" and usage > 5: tags.append("SALAH CATAT")
        
        # 6. Catatan Khusus Rebill
        if "REBILL" in msg: tags.append("INDIKASI REBILL")
        
        # 7. Estimasi Sistem
        if any(code in meth for code in ["30/PE", "35/PS", "40/PE"]): tags.append("ESTIMASI")

        if tags:
            anomalies.append({
                'nomen': row.get('CMR_ACCOUNT'),
                'name': row.get('CMR_NAME'),
                'usage': int(usage),
                'status': tags,
                'details': {
                    'prev': int(prev), 'curr': int(curr),
                    'skip': f"{skip}: {SKIP_MAP.get(skip, 'Normal')}",
                    'trouble': f"{trbl}: {TROUBLE_MAP.get(trbl, 'Normal')}",
                    'method': READ_METHOD_MAP.get(meth, meth),
                    'note': msg if msg else "-"
                }
            })
    return anomalies

# --- 5. TOP 100 RANKING ENGINE ---

def get_top_100_data(category, rayon_id):
    """
    Mengambil 100 data teratas per Rayon untuk keperluan penagihan prioritas.
    Kategori: PREMIUM, TUNGGAKAN, atau CURRENT.
    """
    if not db: return []
    query = {"RAYON": str(rayon_id)}
    
    # Routing koleksi berdasarkan kategori
    if category == 'PREMIUM':
        coll = collections['mb']
        sort_f = "NOMINAL"
    elif category == 'TUNGGAKAN':
        coll = collections['ardebt']
        sort_f = "JUMLAH"
    else:
        coll = collections['mainbill']
        sort_f = "TOTAL_TAGIHAN"
        
    cursor = coll.find(query).sort(sort_f, DESCENDING).limit(100)
    results = list(cursor)
    for r in results: r.pop('_id', None) # Pembersihan ID MongoDB
    return results

# --- 6. AUDIT & HISTORY 360 ---

def get_audit_detective_data(nomen):
    """
    Mengumpulkan seluruh riwayat pelanggan (12 bulan) dalam satu tampilan.
    Riwayat Baca Meter, Riwayat Bayar, dan Riwayat Billing.
    """
    if not db: return {}
    
    # Ambil data dari 3 sumber utama
    read_hist = list(collections['sbrs'].find({"cmr_account": nomen}).sort("cmr_rd_date", DESCENDING).limit(12))
    pay_hist  = list(collections['mb'].find({"NOMEN": nomen}).sort("TGL_BAYAR", DESCENDING).limit(12))
    bill_hist = list(collections['mc'].find({"NOMEN": nomen}).sort("TAHUN2", DESCENDING).limit(12))
    
    # Hapus _id dari semua list agar tidak error di JSON frontend
    for dataset in [read_hist, pay_hist, bill_hist]:
        for doc in dataset: doc.pop('_id', None)

    return {
        'info': collections['cid'].find_one({"NOMEN": nomen}, {"_id": 0}),
        'reading': read_hist,
        'payment': pay_hist,
        'billing': bill_hist,
        'audit_logs': list(collections['audit'].find({"NOMEN": nomen}).sort("date", DESCENDING))
    }

def save_manual_audit(nomen, remark, user, status):
    """
    Menyimpan hasil audit manual yang dilakukan oleh tim detektif ke database.
    """
    if not collections.get('audit'): return False
    
    entry = {
        "NOMEN": nomen,
        "remark": remark,
        "status": status,
        "inspector": user,
        "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    try:
        collections['audit'].insert_one(entry)
        return True
    except:
        return False
