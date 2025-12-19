import os
import pandas as pd
import numpy as np
from pymongo import MongoClient
from datetime import datetime

# --- KONFIGURASI DATABASE ---
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB_NAME") or "pam_analytics"

client = None
db = None
collections = {}

# --- KAMUS KODE REFERENSI (Berdasarkan File yang Anda Berikan) ---
SKIP_MAP = {
    "1A": "Meter Buram (Ganti Meter)", "1B": "Meter Berembun (Ilegal)", "1C": "Meter Rusak (Ilegal)",
    "2A": "Meter Tidak Ada - Air Tdk Pakai", "2B": "Meter Tidak Ada - Air Pakai", "3A": "Rumah Kosong",
    "4A": "Rumah Dibongkar", "4B": "Meter Terendam", "4C": "Alamat Tidak Ketemu",
    "5A": "Tutup Bak Meter Berat", "5B": "Meter Tertimbun", "5C": "Meter Terhalang Barang",
    "5D": "Meter Dicor", "5E": "Bak Meter Dikunci", "5F": "Pagar Dikunci", "5G": "Tdk Diizinkan Baca"
}

TROUBLE_MAP = {
    "1A": "Meter Berembun", "1B": "Meter Mati", "1C": "Meter Buram", "1D": "Segel Pabrik Putus",
    "2A": "Meter Terbalik", "2B": "Meter Dipindah", "2C": "Meter Lepas", "2D": "By Pass Meter",
    "2E": "Meter Dicolok", "2F": "Meter Tidak Normal", "2G": "Kaca Meter Pecah",
    "3A": "Air Kecil/Mati", "4A": "Pipa Dinas Bocor", "4B": "Pipa Lama Keluar Air",
    "5A": "Stand Tempel", "5B": "No Seri Beda"
}

READ_METHOD_MAP = {
    "30/PE": "System Estimate", "35/PS": "SP Estimate", "40/PE": "Office Estimate",
    "60/SE": "Regular (Actual)", "80/PE": "Billing Force"
}

def init_db(app=None):
    global client, db, collections
    if client: return
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=12000)
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
        # Indexing Nomen dan Rayon untuk performa DSS
        for coll in collections.values():
            coll.create_index([('NOMEN', 1)])
            if 'cmr_account' in coll.name: coll.create_index([('cmr_account', 1)])
            coll.create_index([('RAYON', 1)])
            
        print("✅ Database & Kamus Kode Terkoneksi.")
    except Exception as e:
        print(f"❌ Koneksi gagal: {e}")

# --- 1. SUMMARIZING ENGINE (MC, MB, AR, MAINBILL, COLL) ---
def get_summarized_report(coll_name, dimension='RAYON'):
    """
    Menyederhanakan kumpulan data besar (MC, MB, AR, dll) 
    berdasarkan dimensi: RAYON, PC, PCEZ, TARIF, METER.
    """
    if coll_name not in collections: return []
    
    dim_map = {
        'RAYON': '$RAYON',
        'PC': '$PC',
        'PCEZ': '$PCEZ',
        'TARIF': '$TARIFF',
        'METER': '$UKURAN_METER'
    }
    
    # Pilih field nilai berdasarkan jenis koleksi
    val_f = "$NOMINAL" if coll_name in ['mc', 'mb'] else ("$AMT_COLLECT" if coll_name == 'coll' else "$JUMLAH")
    vol_f = "$KUBIK" if coll_name in ['mc', 'mb'] else ("$VOL_COLLECT" if coll_name == 'coll' else "$VOLUME")

    pipeline = [
        {"$group": {
            "_id": dim_map.get(dimension, '$RAYON'),
            "nominal": {"$sum": val_f},
            "volume": {"$sum": vol_f},
            "count": {"$sum": 1}
        }},
        {"$sort": {"_id": 1}}
    ]
    return list(collections[coll_name].aggregate(pipeline))

# --- 2. COLLECTION CATEGORIZATION (UNDUE & CURRENT) ---
def get_collection_analysis():
    """Analisis arus kas: Pembayaran Undue, Current, dan Arrears."""
    pipeline = [
        {
            "$project": {
                "category": {
                    "$cond": {
                        "if": {"$gt": ["$BILL_PERIOD", "$PAY_DT"]}, "then": "UNDUE",
                        "else": {"$cond": {"if": {"$eq": ["$BILL_PERIOD", "$PAY_DT"]}, "then": "CURRENT", "else": "ARREARS"}}
                    }
                },
                "AMT_COLLECT": 1,
                "VOL_COLLECT": 1
            }
        },
        {"$group": {
            "_id": "$category",
            "amount": {"$sum": "$AMT_COLLECT"},
            "volume": {"$sum": "$VOL_COLLECT"},
            "count": {"$sum": 1}
        }}
    ]
    return list(collections['coll'].aggregate(pipeline))

# --- 3. METER READING ANALYSIS ENGINE ---
def analyze_meter_anomalies(df_sbrs):
    """
    Mendeteksi anomali: Extreme, Turun, Zero, Negatif, Salah Catat, Estimasi, Rebill.
    Menerjemahkan kode skip/trouble ke bahasa manusia.
    """
    anomalies = []
    for _, row in df_sbrs.iterrows():
        prev = row.get('cmr_prev_read', 0)
        curr = row.get('cmr_reading', 0)
        usage = curr - prev
        skip = str(row.get('cmr_skip_code', '')).upper()
        trbl = str(row.get('cmr_trbl1_code', '')).upper()
        method = str(row.get('cmr_read_code', '')).upper()
        
        flags = []
        if curr < prev: flags.append("STAND NEGATIF")
        if usage > 150: flags.append("PEMAKAIAN EXTREME")
        if usage == 0 and prev > 0: flags.append("PEMAKAIAN ZERO")
        
        # Logika Salah Catat: Rumah Kosong (3A) tapi ada pemakaian > 5m3
        if skip == "3A" and usage > 5: flags.append("INDIKASI SALAH CATAT")
        if "EST" in method: flags.append("ESTIMASI")
        if "REBILL" in str(row.get('cmr_chg_spcl_msg', '')).upper(): flags.append("INDIKASI REBILL")

        if flags:
            anomalies.append({
                'nomen': row.get('cmr_account'),
                'name': row.get('cmr_name'),
                'usage': usage,
                'status': flags,
                'raw': {
                    'prev': prev, 'curr': curr, 
                    'skip': f"{skip} - {SKIP_MAP.get(skip, '')}",
                    'trbl': f"{trbl} - {TROUBLE_MAP.get(trbl, '')}",
                    'method': READ_METHOD_MAP.get(method, method),
                    'msg': row.get('cmr_chg_spcl_msg', '-')
                }
            })
    return anomalies

# --- 4. TOP 100 ENGINE ---
def get_top_100(category, rayon_id):
    """Query Top 100: Premium, Belum Bayar Current, Tunggakan, Belum Bayar Tunggakan."""
    query = {"RAYON": str(rayon_id)}
    
    if category == 'PREMIUM':
        # Pelanggan yang bayar tepat waktu terbanyak
        return list(collections['mb'].find(query).sort("NOMINAL", -1).limit(100))
    elif category == 'TUNGGAKAN':
        # Nominal Ardebt terbesar
        return list(collections['ardebt'].find(query).sort("JUMLAH", -1).limit(100))
    # Tambahan logika lainnya sesuai kebutuhan query top
    return []

# --- 5. HISTORY DETECTIVE ENGINE ---
def get_audit_detective_data(nomen):
    """Fetch history lengkap 12 bulan (Stand, Skip, Trouble, Method) untuk analisa manual."""
    history = list(collections['sbrs'].find({"cmr_account": nomen}).sort("cmr_rd_date", -1).limit(12))
    
    formatted = []
    for h in history:
        sk = str(h.get('cmr_skip_code', '')).upper()
        tr = str(h.get('cmr_trbl1_code', '')).upper()
        mt = str(h.get('cmr_read_code', '')).upper()
        
        formatted.append({
            "tanggal": h.get('cmr_rd_date'),
            "awal": h.get('cmr_prev_read', 0),
            "akhir": h.get('cmr_reading', 0),
            "kubik": h.get('cmr_reading', 0) - h.get('cmr_prev_read', 0),
            "skip": f"{sk} - {SKIP_MAP.get(sk, sk)}",
            "trbl": f"{tr} - {TROUBLE_MAP.get(tr, tr)}",
            "method": READ_METHOD_MAP.get(mt, mt),
            "petugas": h.get('cmr_read_code', '-'),
            "memo": h.get('cmr_chg_spcl_msg', '-')
        })
    return {
        'history': formatted,
        'customer': collections['cid'].find_one({"NOMEN": nomen}, {"_id": 0}),
        'audit_logs': list(collections['audit'].find({"NOMEN": nomen}).sort("date", -1))
    }

def save_manual_audit(nomen, remark, user, status):
    collections['audit'].insert_one({
        "NOMEN": nomen,
        "remark": remark,
        "status_final": status,
        "user": user,
        "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })
    return True
