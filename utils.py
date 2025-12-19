import os
import pandas as pd
import numpy as np
from pymongo import MongoClient, ASCENDING, DESCENDING
from datetime import datetime, timedelta
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
    
    load_dotenv()
    
    uri = os.getenv("MONGO_URI")
    db_name = os.getenv("MONGO_DB_NAME") or "TagihanDB"
    
    if not uri:
        print("❌ CRITICAL: MONGO_URI tidak ditemukan di .env. Pastikan konfigurasi benar.")
        return

    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        
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
            'audit': db['ManualAudit'],
            'payment_history': db['PaymentHistory']  # NEW: untuk tracking pembayaran
        }
        
        # AUTOMATIC INDEXING
        for name, coll in collections.items():
            coll.create_index([('NOMEN', ASCENDING)])
            coll.create_index([('RAYON', ASCENDING)])
            if 'PC' in [f['name'] for f in coll.index_information().values() if 'name' in f]:
                coll.create_index([('PC', ASCENDING)])
            if name == 'sbrs':
                coll.create_index([('cmr_account', ASCENDING)])
                coll.create_index([('cmr_rd_date', DESCENDING)])
            if name in ['mb', 'coll', 'payment_history']:
                coll.create_index([('TGL_BAYAR', DESCENDING)])
                coll.create_index([('PAYMENT_TYPE', ASCENDING)])
            
        print(f"✅ Database '{db_name}' Terhubung & Indexing Berhasil.")
    except Exception as e:
        print(f"❌ Koneksi Database Gagal: {str(e)}")

# --- 1. ENGINE PEMBERSIH DATA ---

def clean_dataframe(df):
    """
    Menormalisasi data mentah agar siap diolah oleh sistem.
    """
    df.columns = [str(col).strip().upper() for col in df.columns]
    
    for col in df.select_dtypes(['object']).columns:
        df[col] = df[col].astype(str).str.strip().str.upper()
    
    numeric_keys = [
        'NOMINAL', 'JUMLAH', 'VOLUME', 'KUBIK', 'READING', 'PREV', 
        'AMT_COLLECT', 'VOL_COLLECT', 'KONSUMSI', 'CMR_READING', 
        'CMR_PREV_READ', 'CMR_HI1_RDG', 'STAN_AWAL', 'STAN_AKIR'
    ]
    
    for col in df.columns:
        if any(key in col for key in numeric_keys):
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
    
    return df.to_dict('records')

# --- 2. SUMMARIZING ENGINE (IMPROVED) ---

def get_summarized_report(coll_name, dimension='RAYON', rayon_filter=None):
    """
    ✅ IMPROVED: Menghasilkan ringkasan data berdasarkan dimensi tertentu
    Support: RAYON, PC, PCEZ, TARIF, METER
    """
    if coll_name not in collections or not db: 
        return []
    
    # Mapping kolom berdasarkan koleksi
    field_mapping = {
        'mc': {'value': '$NOMINAL', 'volume': '$KUBIK'},
        'mb': {'value': '$NOMINAL', 'volume': '$KUBIK'},
        'ardebt': {'value': '$JUMLAH', 'volume': '$KUBIK'},
        'mainbill': {'value': '$TOTAL_TAGIHAN', 'volume': '$TOTAL_VOLUME'},
        'coll': {'value': '$AMT_COLLECT', 'volume': '$VOL_COLLECT'}
    }
    
    fields = field_mapping.get(coll_name, {'value': '$NOMINAL', 'volume': '$KUBIK'})
    
    # Build match stage jika ada filter rayon
    match_stage = {}
    if rayon_filter:
        match_stage = {"RAYON": str(rayon_filter)}
    
    pipeline = []
    if match_stage:
        pipeline.append({"$match": match_stage})
    
    pipeline.extend([
        {"$group": {
            "_id": f"${dimension.upper()}",
            "total_nominal": {"$sum": fields['value']},
            "total_volume": {"$sum": fields['volume']},
            "count": {"$sum": 1},
            "avg_nominal": {"$avg": fields['value']}
        }},
        {"$sort": {"_id": 1}}
    ])
    
    results = list(collections[coll_name].aggregate(pipeline))
    
    # Format hasil untuk frontend
    formatted = []
    for r in results:
        formatted.append({
            'group': r['_id'] if r['_id'] else 'UNKNOWN',
            'nominal': round(r['total_nominal'] / 1000000, 2),  # Dalam juta
            'volume': round(r['total_volume'], 2),
            'count': r['count'],
            'avg': round(r['avg_nominal'], 2),
            'realization_pct': round((r['total_nominal'] / (r['avg_nominal'] * r['count'])) * 100, 1) if r['avg_nominal'] > 0 else 0
        })
    
    return formatted

# --- 3. COLLECTION ANALYSIS (IMPROVED) ---

def get_collection_detailed_analysis(rayon_filter=None):
    """
    ✅ IMPROVED: Analisa Collection dengan kategori:
    - UNDUE (Bayar sebelum jatuh tempo)
    - CURRENT (Bayar tepat waktu)
    - ARREARS (Bayar terlambat/tunggakan)
    """
    if not collections.get('coll'): 
        return {}
    
    match_stage = {}
    if rayon_filter:
        match_stage = {"RAYON": str(rayon_filter)}
    
    pipeline = []
    if match_stage:
        pipeline.append({"$match": match_stage})
    
    pipeline.extend([
        {"$addFields": {
            "payment_category": {
                "$switch": {
                    "branches": [
                        {"case": {"$lt": ["$TGL_BAYAR", "$TGL_JATUH_TEMPO"]}, "then": "UNDUE"},
                        {"case": {"$eq": ["$TGL_BAYAR", "$TGL_JATUH_TEMPO"]}, "then": "CURRENT"},
                        {"case": {"$gt": ["$TGL_BAYAR", "$TGL_JATUH_TEMPO"]}, "then": "ARREARS"}
                    ],
                    "default": "UNKNOWN"
                }
            }
        }},
        {"$group": {
            "_id": "$payment_category",
            "total_revenue": {"$sum": "$AMT_COLLECT"},
            "total_volume": {"$sum": "$VOL_COLLECT"},
            "count": {"$sum": 1}
        }}
    ])
    
    results = list(collections['coll'].aggregate(pipeline))
    
    # Format hasil
    analysis = {
        'undue': {'revenue': 0, 'volume': 0, 'count': 0},
        'current': {'revenue': 0, 'volume': 0, 'count': 0},
        'arrears': {'revenue': 0, 'volume': 0, 'count': 0}
    }
    
    for r in results:
        category = r['_id'].lower() if r['_id'] else 'unknown'
        if category in analysis:
            analysis[category] = {
                'revenue': round(r['total_revenue'] / 1000000, 2),  # Dalam juta
                'volume': round(r['total_volume'], 2),
                'count': r['count']
            }
    
    # Hitung ratio
    total_revenue = sum(v['revenue'] for v in analysis.values())
    for key in analysis:
        if total_revenue > 0:
            analysis[key]['percentage'] = round((analysis[key]['revenue'] / total_revenue) * 100, 1)
        else:
            analysis[key]['percentage'] = 0
    
    return analysis

def get_customer_payment_status(rayon_filter=None):
    """
    ✅ NEW: Mendapatkan status pembayaran pelanggan:
    - Pelanggan belum bayar piutang (tanpa tunggakan)
    - Pelanggan tunggakan
    - Pelanggan sudah bayar tunggakan
    """
    if not collections.get('mb') or not collections.get('ardebt'):
        return {}
    
    match_stage = {}
    if rayon_filter:
        match_stage = {"RAYON": str(rayon_filter)}
    
    # 1. Pelanggan dengan tunggakan
    pipeline_debt = []
    if match_stage:
        pipeline_debt.append({"$match": match_stage})
    
    pipeline_debt.extend([
        {"$match": {"JUMLAH": {"$gt": 0}}},
        {"$group": {
            "_id": None,
            "count": {"$sum": 1},
            "total_debt": {"$sum": "$JUMLAH"}
        }}
    ])
    
    debt_result = list(collections['ardebt'].aggregate(pipeline_debt))
    
    # 2. Pelanggan sudah bayar tunggakan (di MB ada flag BAYAR_TUNGGAKAN)
    pipeline_paid_debt = []
    if match_stage:
        pipeline_paid_debt.append({"$match": match_stage})
    
    pipeline_paid_debt.extend([
        {"$match": {"JENIS_BAYAR": "TUNGGAKAN"}},
        {"$group": {
            "_id": None,
            "count": {"$sum": 1},
            "total_paid": {"$sum": "$NOMINAL"}
        }}
    ])
    
    paid_debt_result = list(collections['mb'].aggregate(pipeline_paid_debt))
    
    # 3. Pelanggan belum bayar piutang (tidak ada di ARDEBT tapi belum bayar current)
    pipeline_unpaid = []
    if match_stage:
        pipeline_unpaid.append({"$match": match_stage})
    
    pipeline_unpaid.extend([
        {"$lookup": {
            "from": "AccountReceivable",
            "localField": "NOMEN",
            "foreignField": "NOMEN",
            "as": "debt"
        }},
        {"$match": {"debt": {"$size": 0}}},  # Tidak ada di ARDEBT
        {"$match": {"STATUS_BAYAR": {"$ne": "LUNAS"}}},
        {"$group": {
            "_id": None,
            "count": {"$sum": 1},
            "total_outstanding": {"$sum": "$NOMINAL"}
        }}
    ])
    
    unpaid_result = list(collections['mainbill'].aggregate(pipeline_unpaid))
    
    return {
        'with_debt': {
            'count': debt_result[0]['count'] if debt_result else 0,
            'total': round(debt_result[0]['total_debt'] / 1000000, 2) if debt_result else 0
        },
        'paid_debt': {
            'count': paid_debt_result[0]['count'] if paid_debt_result else 0,
            'total': round(paid_debt_result[0]['total_paid'] / 1000000, 2) if paid_debt_result else 0
        },
        'unpaid_receivable': {
            'count': unpaid_result[0]['count'] if unpaid_result else 0,
            'total': round(unpaid_result[0]['total_outstanding'] / 1000000, 2) if unpaid_result else 0
        }
    }

# --- 4. HISTORY ANALYSIS ---

def get_usage_history(dimension='CUSTOMER', identifier=None, rayon_filter=None, months=12):
    """
    ✅ NEW: History kubikasi berdasarkan:
    - CUSTOMER (by NOMEN)
    - RAYON
    - PC
    - PCEZ
    - TARIF
    - METER
    """
    if not collections.get('sbrs'):
        return []
    
    match_stage = {}
    
    if dimension == 'CUSTOMER' and identifier:
        match_stage['cmr_account'] = identifier
    elif rayon_filter:
        match_stage['RAYON'] = str(rayon_filter)
    
    # Ambil data X bulan terakhir
    pipeline = []
    if match_stage:
        pipeline.append({"$match": match_stage})
    
    pipeline.extend([
        {"$sort": {"cmr_rd_date": -1}},
        {"$limit": months * 1000},  # Batasi data
        {"$addFields": {
            "usage": {"$subtract": ["$cmr_reading", "$cmr_prev_read"]}
        }},
        {"$group": {
            "_id": {
                "period": "$cmr_rd_date",
                "dimension": f"${dimension.upper()}" if dimension != 'CUSTOMER' else "$cmr_account"
            },
            "total_usage": {"$sum": "$usage"},
            "avg_usage": {"$avg": "$usage"},
            "count": {"$sum": 1}
        }},
        {"$sort": {"_id.period": -1}},
        {"$limit": months}
    ])
    
    results = list(collections['sbrs'].aggregate(pipeline))
    
    formatted = []
    for r in results:
        formatted.append({
            'period': r['_id']['period'],
            'dimension_value': r['_id']['dimension'],
            'total_usage': round(r['total_usage'], 2),
            'avg_usage': round(r['avg_usage'], 2),
            'count': r['count']
        })
    
    return formatted

def get_payment_history(nomen=None, payment_type='ALL', rayon_filter=None, months=12):
    """
    ✅ NEW: History pembayaran pelanggan:
    - ALL: Semua pembayaran
    - UNDUE: Hanya pembayaran dimuka
    - CURRENT: Hanya pembayaran tepat waktu
    """
    if not collections.get('mb'):
        return []
    
    match_stage = {}
    
    if nomen:
        match_stage['NOMEN'] = nomen
    elif rayon_filter:
        match_stage['RAYON'] = str(rayon_filter)
    
    if payment_type != 'ALL':
        match_stage['PAYMENT_TYPE'] = payment_type
    
    pipeline = []
    if match_stage:
        pipeline.append({"$match": match_stage})
    
    pipeline.extend([
        {"$sort": {"TGL_BAYAR": -1}},
        {"$limit": months},
        {"$project": {
            "NOMEN": 1,
            "TGL_BAYAR": 1,
            "NOMINAL": 1,
            "KUBIK": 1,
            "PAYMENT_TYPE": 1,
            "JENIS_BAYAR": 1,
            "RAYON": 1
        }}
    ])
    
    results = list(collections['mb'].aggregate(pipeline))
    
    # Bersihkan _id
    for r in results:
        r.pop('_id', None)
    
    return results

# --- 5. METER READING ANALYSIS (IMPROVED) ---

def analyze_meter_anomalies(df_sbrs):
    """
    ✅ IMPROVED: Logic deteksi anomali meter reading yang lebih komprehensif
    """
    anomalies = []
    THRESHOLD_EKSTRIM = 150
    THRESHOLD_TURUN = 0.3  # Turun 70% dari history
    
    for idx, row in df_sbrs.iterrows():
        try:
            prev = float(row.get('CMR_PREV_READ', 0))
            curr = float(row.get('CMR_READING', 0))
            usage = curr - prev
            
            # Ambil history reading
            hi_rdg = float(row.get('CMR_HI1_RDG', 0))
            hi2_rdg = float(row.get('CMR_HI2_RDG', 0))
            avg_history = (hi_rdg + hi2_rdg) / 2 if hi2_rdg > 0 else hi_rdg
            
            # Metadata lapangan
            skip = str(row.get('CMR_SKIP_CODE', '')).upper()
            trbl = str(row.get('CMR_TRBL1_CODE', '')).upper()
            meth = str(row.get('CMR_READ_CODE', '')).upper()
            msg = str(row.get('CMR_CHG_SPCL_MSG', '')).upper()
            mrid = str(row.get('CMR_MRID', ''))
            rd_date = str(row.get('CMR_RD_DATE', ''))
            
            tags = []
            details = {
                'mrid': mrid,
                'rd_date': rd_date,
                'prev_read': int(prev),
                'curr_read': int(curr),
                'usage': int(usage),
                'skip_code': skip,
                'skip_desc': SKIP_MAP.get(skip, 'Normal'),
                'trouble_code': trbl,
                'trouble_desc': TROUBLE_MAP.get(trbl, 'Normal'),
                'read_method': READ_METHOD_MAP.get(meth, meth),
                'special_msg': msg if msg and msg != 'NAN' else '-',
                'history_avg': round(avg_history, 2)
            }
            
            # 1. STAND NEGATIF
            if usage < 0:
                tags.append("STAND NEGATIF")
                details['anomaly_reason'] = f"Stand mundur {abs(int(usage))} m³"
            
            # 2. PEMAKAIAN EKSTRIM (>150 m³ atau >200% dari rata-rata)
            if usage > THRESHOLD_EKSTRIM or (avg_history > 0 and usage > avg_history * 2):
                tags.append("EKSTRIM")
                if avg_history > 0:
                    details['anomaly_reason'] = f"Lonjakan {int((usage/avg_history)*100)}% dari rata-rata"
                else:
                    details['anomaly_reason'] = f"Pemakaian {int(usage)} m³ sangat tinggi"
            
            # 3. PEMAKAIAN TURUN DRASTIS
            if avg_history > 0 and usage > 0 and usage < (avg_history * THRESHOLD_TURUN):
                tags.append("PEMAKAIAN TURUN")
                details['anomaly_reason'] = f"Turun {int(100-(usage/avg_history)*100)}% dari rata-rata {int(avg_history)} m³"
            
            # 4. PEMAKAIAN ZERO (Padahal sebelumnya ada)
            if usage == 0 and prev > 0 and avg_history > 5:
                tags.append("PEMAKAIAN ZERO")
                details['anomaly_reason'] = f"Tidak ada pemakaian, padahal rata-rata {int(avg_history)} m³"
            
            # 5. SALAH CATAT (Skip 3A tapi ada pemakaian signifikan)
            if skip == "3A" and usage > 5:
                tags.append("SALAH CATAT")
                details['anomaly_reason'] = f"Dicatat Rumah Kosong tapi ada pemakaian {int(usage)} m³"
            
            # 6. INDIKASI REBILL
            if "REBILL" in msg or "KOREKSI" in msg:
                tags.append("INDIKASI REBILL")
                details['anomaly_reason'] = "Terdeteksi koreksi atau rebill di sistem"
            
            # 7. ESTIMASI (Bukan bacaan actual)
            if any(code in meth for code in ["30/PE", "35/PS", "40/PE"]):
                tags.append("ESTIMASI")
                details['anomaly_reason'] = f"Metode: {READ_METHOD_MAP.get(meth, meth)}"
            
            # 8. TROUBLE CODE SIGNIFIKAN
            if trbl in ['1B', '1C', '2D', '2E', '2F']:  # Meter bermasalah
                if "METER ISSUE" not in tags:
                    tags.append("METER ISSUE")
                details['anomaly_reason'] = details.get('anomaly_reason', '') + f" | {TROUBLE_MAP.get(trbl, trbl)}"
            
            # Hanya simpan jika ada anomali
            if tags:
                anomalies.append({
                    'nomen': row.get('CMR_ACCOUNT'),
                    'name': row.get('CMR_NAME', 'UNKNOWN'),
                    'usage': int(usage),
                    'status': tags,
                    'details': details
                })
        
        except Exception as e:
            print(f"Error analyzing row {idx}: {str(e)}")
            continue
    
    return anomalies

# --- 6. TOP 100 RANKINGS (IMPROVED) ---

def get_top_100_premium(rayon_id):
    """
    ✅ IMPROVED: Top 100 pelanggan premium (selalu bayar tepat waktu)
    """
    if not collections.get('mb'):
        return []
    
    # Hitung frekuensi bayar tepat waktu dalam 12 bulan terakhir
    pipeline = [
        {"$match": {"RAYON": str(rayon_id), "PAYMENT_TYPE": "CURRENT"}},
        {"$group": {
            "_id": "$NOMEN",
            "count_ontime": {"$sum": 1},
            "total_paid": {"$sum": "$NOMINAL"},
            "avg_usage": {"$avg": "$KUBIK"},
            "name": {"$first": "$NAMA"}
        }},
        {"$match": {"count_ontime": {"$gte": 10}}},  # Min 10x bayar tepat waktu
        {"$sort": {"count_ontime": -1, "total_paid": -1}},
        {"$limit": 100}
    ]
    
    results = list(collections['mb'].aggregate(pipeline))
    
    formatted = []
    for r in results:
        formatted.append({
            'nomen': r['_id'],
            'name': r.get('name', 'UNKNOWN'),
            'ontime_count': r['count_ontime'],
            'total_paid': round(r['total_paid'] / 1000000, 2),
            'avg_usage': round(r['avg_usage'], 2),
            'status': 'PREMIUM'
        })
    
    return formatted

def get_top_100_unpaid_current(rayon_id):
    """
    ✅ NEW: Top 100 belum bayar current (tagihan bulan ini)
    """
    if not collections.get('mainbill'):
        return []
    
    pipeline = [
        {"$match": {
            "RAYON": str(rayon_id),
            "STATUS_BAYAR": {"$ne": "LUNAS"},
            "PERIODE": {"$gte": datetime.now().strftime("%Y%m")}  # Periode current
        }},
        {"$sort": {"TOTAL_TAGIHAN": -1}},
        {"$limit": 100},
        {"$project": {
            "NOMEN": 1,
            "NAMA": 1,
            "TOTAL_TAGIHAN": 1,
            "KUBIK": 1,
            "TGL_JATUH_TEMPO": 1
        }}
    ]
    
    results = list(collections['mainbill'].aggregate(pipeline))
    
    for r in results:
        r.pop('_id', None)
        r['outstanding'] = round(r.get('TOTAL_TAGIHAN', 0) / 1000000, 2)
    
    return results

def get_top_100_debt(rayon_id):
    """
    ✅ IMPROVED: Top 100 pelanggan tunggakan
    """
    if not collections.get('ardebt'):
        return []
    
    pipeline = [
        {"$match": {"RAYON": str(rayon_id), "JUMLAH": {"$gt": 0}}},
        {"$sort": {"JUMLAH": -1}},
        {"$limit": 100},
        {"$lookup": {
            "from": "CustomerData",
            "localField": "NOMEN",
            "foreignField": "NOMEN",
            "as": "customer"
        }},
        {"$unwind": {"path": "$customer", "preserveNullAndEmptyArrays": True}},
        {"$project": {
            "NOMEN": 1,
            "NAMA": {"$ifNull": ["$customer.NAMA", "$NAMA"]},
            "JUMLAH": 1,
            "UMUR_TUNGGAKAN": 1,
            "TAGIHAN_TERLAMA": 1
        }}
    ]
    
    results = list(collections['ardebt'].aggregate(pipeline))
    
    for r in results:
        r.pop('_id', None)
        r['debt_amount'] = round(r.get('JUMLAH', 0) / 1000000, 2)
    
    return results

def get_top_100_unpaid_debt(rayon_id):
    """
    ✅ NEW: Top 100 belum bayar tunggakan
    """
    if not collections.get('ardebt') or not collections.get('mb'):
        return []
    
    # Ambil NOMEN yang ada di ARDEBT tapi tidak ada pembayaran tunggakan
    pipeline = [
        {"$match": {"RAYON": str(rayon_id), "JUMLAH": {"$gt": 0}}},
        {"$lookup": {
            "from": "MasterBayar",
            "let": {"nomen": "$NOMEN"},
            "pipeline": [
                {"$match": {
                    "$expr": {"$eq": ["$NOMEN", "$$nomen"]},
                    "JENIS_BAYAR": "TUNGGAKAN"
                }}
            ],
            "as": "payments"
        }},
        {"$match": {"payments": {"$size": 0}}},  # Tidak ada pembayaran tunggakan
        {"$sort": {"JUMLAH": -1}},
        {"$limit": 100},
        {"$project": {
            "NOMEN": 1,
            "NAMA": 1,
            "JUMLAH": 1,
            "UMUR_TUNGGAKAN": 1
        }}
    ]
    
    results = list(collections['ardebt'].aggregate(pipeline))
    
    for r in results:
        r.pop('_id', None)
        r['unpaid_debt'] = round(r.get('JUMLAH', 0) / 1000000, 2)
    
    return results

# --- 7. DETECTIVE MODE ---

def get_audit_detective_data(nomen):
    """
    ✅ IMPROVED: Data lengkap untuk detective mode
    """
    if not db: 
        return {}
    
    # 1. Reading History (12 periode terakhir)
    reading_history = list(collections['sbrs'].find(
        {"cmr_account": nomen}
    ).sort("cmr_rd_date", DESCENDING).limit(12))
    
    # 2. Payment History (12 periode terakhir)
    payment_history = list(collections['mb'].find(
        {"NOMEN": nomen}
    ).sort("TGL_BAYAR", DESCENDING).limit(12))
    
    # 3. Customer Info
    customer_info = collections['cid'].find_one({"NOMEN": nomen}, {"_id": 0})
    
    # 4. Audit Logs
    audit_logs = list(collections['audit'].find(
        {"NOMEN": nomen}
    ).sort("date", DESCENDING))
    
    # Bersihkan _id
    for item in reading_history:
        item.pop('_id', None)
    for item in payment_history:
        item.pop('_id', None)
    for item in audit_logs:
        item.pop('_id', None)
    
    return {
        'customer': customer_info,
        'reading_history': reading_history,
        'payment_history': payment_history,
        'audit_logs': audit_logs
    }

def save_manual_audit(nomen, remark, user, status):
    """
    Menyimpan hasil audit manual
    """
    if not collections.get('audit'): 
        return False
    
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
    except Exception as e:
        print(f"Error saving audit: {str(e)}")
        return False
