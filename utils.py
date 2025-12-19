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
    
    # Muat ulang .env untuk memastikan MONGO_URI tersedia sebelum koneksi
    load_dotenv()
    
    uri = os.getenv("MONGO_URI")
    db_name = os.getenv("MONGO_DB_NAME") or "pam_analytics"
    
    if not uri:
        print("❌ CRITICAL: MONGO_URI tidak ditemukan di .env. Gunakan format mongodb+srv://...")
        return

    try:
        # Koneksi dengan timeout agar tidak menggantung jika jaringan bermasalah
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping') # Tes koneksi nyata
        
        db = client[db_name]
        
        # Mapping Koleksi Utama
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
        
        # AUTOMATIC INDEXING: Memastikan pencarian NOMEN & RAYON secepat kilat
        for name, coll in collections.items():
            coll.create_index([('NOMEN', ASCENDING)])
            coll.create_index([('RAYON', ASCENDING)])
            if name == 'sbrs':
                coll.create_index([('cmr_account', ASCENDING)])
                coll.create_index([('cmr_rd_date', DESCENDING)])
            
        print(f"✅ Database '{db_name}' Terhubung ke Cloud Atlas & Indexing Siap.")
    except Exception as e:
        print(f"❌ Koneksi Database Gagal: {str(e)}")

# --- 1. ENGINE PEMBERSIH DATA ---

def clean_dataframe(df):
    """
    Menormalisasi data mentah dari Excel/CSV agar siap dianalisa.
    - Menghapus spasi pada header & data.
    - Mengubah string angka (dengan koma) menjadi numerik murni.
    """
    # Header Standar (Upper Case)
    df.columns = [str(col).strip().upper() for col in df.columns]
    
    # Trim Whitespace pada seluruh kolom teks
    for col in df.select_dtypes(['object']).columns:
        df[col] = df[col].astype(str).str.strip().str.upper()
    
    # Konversi Numerik Aman (Menangani ribuan dengan koma)
    numeric_columns = [
        'NOMINAL', 'JUMLAH', 'VOLUME', 'KUBIK', 'READING', 'PREV', 
        'AMT_COLLECT', 'VOL_COLLECT', 'KONSUMSI', 'CMR_READING', 
        'CMR_PREV_READ', 'CMR_HI1_RDG', 'STAN_AWAL', 'STAN_AKIR'
    ]
    
    for col in df.columns:
        if any(key in col for key in numeric_columns):
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
    
    return df.to_dict('records')

# --- 2. MULTIDIMENSIONAL SUMMARIZING ---

def get_summarized_report(coll_name, dimension='RAYON'):
    """
    Menghasilkan ringkasan eksekutif per dimensi (Wilayah/Tarif/Ukuran Meter).
    Menggunakan Aggregation Pipeline MongoDB untuk efisiensi RAM.
    """
    if coll_name not in collections or not db: return []
    
    # Penentuan field berdasarkan target koleksi
    val_field = "$NOMINAL" if coll_name in ['mc', 'mb'] else ("$AMT_COLLECT" if coll_name == 'coll' else "$JUMLAH")
    vol_field = "$KUBIK" if coll_name in ['mc', 'mb', 'coll'] else "$VOLUME"

    pipeline = [
        {"$group": {
            "_id": f"${dimension.upper()}",
            "total_money": {"$sum": val_field},
            "total_usage": {"$sum": vol_field},
            "customer_count": {"$sum": 1}
        }},
        {"$sort": {"_id": 1}}
    ]
    return list(collections[coll_name].aggregate(pipeline))

# --- 3. CASH FLOW & AR ANALYSIS ---

def get_collection_analysis():
    """
    Analisa Kualitas Penagihan:
    - UNDUE: Bayar sebelum jatuh tempo.
    - CURRENT: Bayar tepat di bulan berjalan.
    - ARREARS: Bayar tunggakan bulan lalu.
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

# --- 4. ENGINE DETEKSI ANOMALI METER (DETEKTIF) ---

def analyze_meter_anomalies(df_sbrs):
    """
    Logic Deteksi 7 Anomali Utama untuk mencegah kerugian air (NRW).
    Mendeteksi: Negatif, Ekstrim, Zero, Salah Catat, Estimasi, dll.
    """
    anomalies = []
    THRESHOLD_EKSTRIM = 150 # m3

    for _, row in df_sbrs.iterrows():
        prev = float(row.get('CMR_PREV_READ', 0))
        curr = float(row.get('CMR_READING', 0))
        usage = curr - prev
        
        # Metadata Petugas Lapangan
        skip = str(row.get('CMR_SKIP_CODE', '')).upper()
        trbl = str(row.get('CMR_TRBL1_CODE', '')).upper()
        meth = str(row.get('CMR_READ_CODE', '')).upper()
        msg  = str(row.get('CMR_CHG_SPCL_MSG', '')).upper()
        
        tags = []
        
        if curr < prev: tags.append("STAND NEGATIF")
        if usage > THRESHOLD_EKSTRIM: tags.append("EKSTRIM")
        
        # Bandingkan dengan High Reading (Rata-rata history)
        hi_rdg = float(row.get('CMR_HI1_RDG', 0))
        if hi_rdg > 0 and usage < (hi_rdg * 0.3) and usage > 0: tags.append("PEMAKAIAN TURUN")
        
        if usage == 0 and prev > 0: tags.append("PEMAKAIAN ZERO")
        if skip == "3A" and usage > 5: tags.append("SALAH CATAT (Audit Lapangan)")
        if "REBILL" in msg: tags.append("INDIKASI REBILL")
        if any(code in meth for code in ["30/PE", "35/PS", "40/PE"]): tags.append("ESTIMASI")

        if tags:
            anomalies.append({
                'nomen': row.get('CMR_ACCOUNT'),
                'mrid': row.get('CMR_MRID'),
                'name': row.get('CMR_NAME'),
                'usage': int(usage),
                'status': tags,
                'audit_details': {
                    'prev': int(prev), 'curr': int(curr),
                    'skip_info': f"{skip}: {SKIP_MAP.get(skip, 'Normal')}",
                    'trouble_info': f"{trbl}: {TROUBLE_MAP.get(trbl, 'Normal')}",
                    'method': READ_METHOD_MAP.get(meth, meth),
                    'field_note': msg if msg else "Tidak ada catatan"
                }
            })
    return anomalies

# --- 5. TOP 100 RANKING ENGINE ---

def get_top_100_data(category, rayon_id):
    """
    Peringkat 100 teratas per Rayon.
    Digunakan untuk apresiasi pelanggan premium atau penagihan intensif tunggakan.
    """
    if not db: return []
    query = {"RAYON": str(rayon_id)}
    
    # Pemilihan sumber data
    if category == 'PREMIUM':
        coll = collections['mb']
        sort_field = "NOMINAL"
    elif category == 'TUNGGAKAN':
        coll = collections['ardebt']
        sort_field = "JUMLAH"
    else:
        coll = collections['mainbill']
        sort_field = "TOTAL_TAGIHAN"
        
    results = list(coll.find(query).sort(sort_field, DESCENDING).limit(100))
    for r in results: r.pop('_id', None) # Clean for JSON
    return results

# --- 6. AUDIT HISTORY (TIM DETEKTIF) ---

def get_audit_detective_data(nomen):
    """
    Profil 360 Derajat Pelanggan:
    Menggabungkan history baca meter, pembayaran, dan cetakan tagihan 12 bulan terakhir.
    """
    if not db: return {}
    
    # Ambil 12 Periode Terakhir
    sbrs_hist = list(collections['sbrs'].find({"cmr_account": nomen}).sort("cmr_rd_date", DESCENDING).limit(12))
    pay_hist  = list(collections['mb'].find({"NOMEN": nomen}).sort("TGL_BAYAR", DESCENDING).limit(12))
    bill_hist = list(collections['mc'].find({"NOMEN": nomen}).sort("TAHUN2", DESCENDING).limit(12))
    
    # Hapus _id MongoDB agar tidak error JSON
    for dataset in [sbrs_hist, pay_hist, bill_hist]:
        for doc in dataset: doc.pop('_id', None)

    return {
        'customer_info': collections['cid'].find_one({"NOMEN": nomen}, {"_id": 0}),
        'reading_history': sbrs_hist,
        'payment_history': pay_hist,
        'billing_history': bill_hist,
        'manual_audit_logs': list(collections['audit'].find({"NOMEN": nomen}).sort("date", DESCENDING))
    }

def save_manual_audit(nomen, remark, user, status):
    """
    Menyimpan hasil kunjungan atau analisa manual tim detektif ke database.
    """
    if not collections.get('audit'): return False
    
    audit_entry = {
        "NOMEN": nomen,
        "remark": remark,
        "status_final": status,
        "inspector": user,
        "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    try:
        collections['audit'].insert_one(audit_entry)
        return True
    except:
        return False
