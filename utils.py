import os
import pandas as pd
from pymongo import MongoClient, DESCENDING, ASCENDING
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# --- DATABASE CONNECTION ---
db = None

def init_db():
    global db
    mongo_uri = os.getenv("MONGO_URI")
    if mongo_uri:
        try:
            client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            db = client.get_database("PAM_DSS_DB")
            print(f"✅ Database Connected: {db.name}")
            # Buat index untuk mempercepat pencarian
            db.meter_history.create_index([("nomen", ASCENDING), ("period", DESCENDING)])
            db.billing_history.create_index([("nomen", ASCENDING)])
            db.customers.create_index([("nomen", ASCENDING)], unique=True)
        except Exception as e:
            print(f"⚠️ Database Connection Failed: {e}")
    else:
        print("⚠️ Warning: MONGO_URI not found in .env. Running in Stateless Mode (Upload Only).")

# --- DATA CLEANING ---
def clean_dataframe(df):
    """Membersihkan DataFrame: Uppercase header, handle NaN, string trimming"""
    # Standarisasi Header
    df.columns = [str(col).strip().upper() for col in df.columns]
    
    # Handle NaN
    df = df.fillna(0)
    
    # Trim String Columns
    for col in df.select_dtypes(include=['object']):
        df[col] = df[col].astype(str).str.strip()
        
    return df.to_dict('records')

# --- 1. METER READING ANALYSIS ---
def analyze_meter_anomalies(df_records):
    """
    Menganalisa data SBRS untuk menemukan:
    - Ekstrim (Lonjakan > 100% atau Threshold)
    - Stand Negatif (Kini < Lalu)
    - Zero Usage (0 m3)
    - Estimasi / Salah Catat
    """
    anomalies = []
    
    for row in df_records:
        status_list = []
        
        # Mapping nama kolom yang umum
        nomen = str(row.get('NOMEN', row.get('CMR_ACCOUNT', 'Unknown')))
        name = row.get('NAMA', row.get('CMR_NAME', 'Pelanggan'))
        
        try:
            prev = float(row.get('CMR_PREV_READ', 0))
            curr = float(row.get('CMR_READING', 0))
            usage = curr - prev
            # Asumsi rata-rata pemakaian dari data historis (jika ada di row) atau default
            avg_usage = float(row.get('AVG_USAGE', 20)) 
        except:
            continue # Skip jika data numerik rusak

        # A. STAND NEGATIF
        if usage < 0:
            status_list.append('STAND NEGATIF')
            
        # B. ZERO USAGE
        if usage == 0:
            status_list.append('PEMAKAIAN ZERO')
            
        # C. PEMAKAIAN EKSTRIM (Contoh: > 2x Rata-rata atau > 50m3 mendadak)
        if usage > 0 and (usage > (avg_usage * 2) and usage > 50):
            status_list.append('EKSTRIM')
            
        # D. ANALISA KODE (Skip/Trouble)
        skip_code = str(row.get('CMR_SKIP_CODE', '0')).strip()
        if skip_code not in ['0', 'nan', '']:
            status_list.append(f'SKIP: {skip_code}')
            if skip_code in ['EST']: status_list.append('ESTIMASI')
            
        # E. PESAN KHUSUS
        msg = str(row.get('CMR_CHG_SPCL_MSG', '')).upper()
        if 'REBILL' in msg: status_list.append('INDIKASI REBILL')
        if 'SALAH' in msg: status_list.append('SALAH CATAT')

        # Jika ada anomali, masukkan ke list
        if status_list:
            anomalies.append({
                'nomen': nomen,
                'name': name,
                'usage': usage,
                'status': status_list,
                'details': {
                    'anomaly_reason': ', '.join(status_list),
                    'skip_desc': skip_code,
                    'prev_read': prev,
                    'curr_read': curr
                }
            })
            
    # Opsi: Simpan data ini ke DB untuk history
    if db is not None and df_records:
        try:
            # Gunakan bulk write untuk performa jika data banyak
            # Disini kita simpan contoh simpel one-by-one untuk keamanan
            # db.meter_history.insert_many(df_records) (Hati-hati duplikat)
            pass
        except: pass
            
    return anomalies

# --- 2. SUMMARIZING REPORT ---
def get_summarized_report(target, dimension, rayon_filter=None):
    """
    Summarize data berdasarkan Target (MC, MB, dll) dan Dimensi (RAYON, TARIF, dll).
    Menggunakan Aggregation Pipeline MongoDB.
    """
    if db is None: return []
    
    # Mapping Target ke Collection Name
    col_map = {
        'mc': 'master_cetak',
        'mb': 'master_bayar',
        'ardebt': 'arrears',
        'mainbill': 'main_bill',
        'collection': 'collections'
    }
    collection = db[col_map.get(target, 'master_cetak')]
    
    pipeline = []
    
    # 1. Filter Rayon (Optional)
    if rayon_filter:
        pipeline.append({'$match': {'RAYON': str(rayon_filter)}})
        
    # 2. Grouping
    # Tentukan nama field nilai uang (NOMINAL vs AMT_COLLECT)
    val_field = '$AMT_COLLECT' if target == 'collection' else ('$JUMLAH' if target == 'ardebt' else '$NOMINAL')
    
    pipeline.append({
        '$group': {
            '_id': f'${dimension}', # e.g., $RAYON, $TARIF
            'nominal': {'$sum': val_field},
            'volume': {'$sum': '$KUBIK'},
            'count': {'$sum': 1}
        }
    })
    
    # 3. Sorting
    pipeline.append({'$sort': {'_id': 1}})
    
    results = list(collection.aggregate(pipeline))
    
    # Formatting Output
    formatted = []
    for r in results:
        formatted.append({
            'group': r['_id'] if r['_id'] else 'UNDEFINED',
            'nominal': r.get('nominal', 0),
            'volume': r.get('volume', 0),
            'count': r['count'],
            'realization_pct': 0 # Placeholder untuk perhitungan lanjutan
        })
    return formatted

# --- 3. COLLECTION DETAILED ANALYSIS ---
def get_customer_payment_status(rayon=None):
    """
    Mengelompokkan pelanggan berdasarkan perilaku bayar:
    - Undue (Bayar Dimuka)
    - Current (Bayar Bulan Ini)
    - Arrears (Bayar Tunggakan)
    - Unpaid Receivable (Punya Piutang tapi lancar)
    - Outstanding Arrears (Masih Menunggak)
    """
    if db is None: return {}
    
    match_stage = {'RAYON': str(rayon)} if rayon else {}
    
    # Helper untuk hitung agregat
    def count_sum(coll_name, query):
        pipeline = [
            {'$match': {**match_stage, **query}},
            {'$group': {'_id': None, 'total': {'$sum': '$AMT_COLLECT'}, 'count': {'$sum': 1}}}
        ]
        res = list(db[coll_name].aggregate(pipeline))
        return {'revenue': res[0]['total'], 'count': res[0]['count']} if res else {'revenue': 0, 'count': 0}

    # A. Collection Stats (Uang Masuk)
    stats = {
        'undue': count_sum('collections', {'TYPE': 'UNDUE'}),
        'current': count_sum('collections', {'TYPE': 'CURRENT'}),
        'paid_arrears': count_sum('collections', {'TYPE': 'ARREARS'}),
        'total_cash': 0
    }
    stats['total_cash'] = stats['undue']['revenue'] + stats['current']['revenue'] + stats['paid_arrears']['revenue']
    
    # B. Outstanding Stats (Belum Bayar)
    # 1. Menunggak (Ada di tabel Arrears)
    arrears_res = list(db.arrears.aggregate([
        {'$match': match_stage},
        {'$group': {'_id': None, 'total': {'$sum': '$JUMLAH'}, 'count': {'$sum': 1}}} # Asumsi kolom JUMLAH di Arrears
    ]))
    stats['outstanding_arrears'] = {'revenue': arrears_res[0]['total'], 'count': arrears_res[0]['count']} if arrears_res else {'revenue':0, 'count':0}
    
    # 2. Belum Bayar Piutang (No Tunggakan)
    # Logika: Ada di Master Cetak, Tidak ada di Master Bayar, Tidak ada di Arrears
    # Ini query kompleks, disederhanakan: Ambil count dari MC yang flag 'LUNAS' belum set
    mc_unpaid = list(db.master_cetak.aggregate([
        {'$match': {**match_stage, 'STATUS_LUNAS': {'$ne': True}}}, # Asumsi ada flag
        {'$group': {'_id': None, 'total': {'$sum': '$NOMINAL'}, 'count': {'$sum': 1}}}
    ]))
    stats['unpaid_receivable_no_arrears'] = {'revenue': mc_unpaid[0]['total'], 'count': mc_unpaid[0]['count']} if mc_unpaid else {'revenue':0, 'count':0}
    
    return stats

# --- 4. HISTORY ---
def get_usage_history(dimension, value):
    """Ambil riwayat pemakaian (Kubikasi)"""
    if db is None: return []
    
    query = {}
    if dimension == 'CUSTOMER': query['nomen'] = value
    elif dimension == 'RAYON': query['RAYON'] = value
    # ... tambahkan dimensi lain jika perlu
    
    # Ambil dari meter_history
    history = list(db.meter_history.find(query).sort('period', -1).limit(12))
    
    return [{
        'period': h.get('period', h.get('cmr_rd_date', '-')),
        'value': h.get('usage', h.get('CMR_READING', 0) - h.get('CMR_PREV_READ', 0)),
        'desc': f"Stand: {h.get('CMR_READING',0)}"
    } for h in history]

def get_payment_history(nomen):
    """Ambil riwayat pembayaran"""
    if db is None: return []
    
    history = list(db.collections.find({'NOMEN': nomen}).sort('TGL_BAYAR', -1).limit(12))
    return [{
        'date': h.get('TGL_BAYAR'),
        'value': h.get('AMT_COLLECT'),
        'desc': h.get('TYPE', 'PAYMENT')
    } for h in history]

# --- 5. TOP 100 RANKINGS ---
def get_top_100_debt(rayon):
    """Top 100 Penunggak Terbesar"""
    if db is None: return []
    
    pipeline = [
        {'$match': {'RAYON': str(rayon)}},
        {'$sort': {'JUMLAH': -1}}, # Sort Descending by Jumlah Tunggakan
        {'$limit': 100},
        {'$project': {'_id': 0, 'NAMA': 1, 'NOMEN': 1, 'debt_amount': '$JUMLAH', 'UMUR_TUNGGAKAN': '$LEMBAR'}}
    ]
    return list(db.arrears.aggregate(pipeline))

def get_top_100_premium(rayon):
    """Top 100 Pelanggan Premium (Selalu Tepat Waktu)"""
    # Logika: Ambil dari Master Bayar dengan tgl bayar < tgl jatuh tempo terbanyak
    if db is None: return []
    
    # Simplified: Top Revenue dari Collection Current
    pipeline = [
        {'$match': {'RAYON': str(rayon), 'TYPE': 'CURRENT'}},
        {'$group': {'_id': '$NOMEN', 'total_paid': {'$sum': '$AMT_COLLECT'}, 'NAMA': {'$first': '$NAMA'}}},
        {'$sort': {'total_paid': -1}},
        {'$limit': 100}
    ]
    return list(db.collections.aggregate(pipeline))

def get_top_100_unpaid_current(rayon):
    """Top 100 Belum Bayar Tagihan Bulan Ini (Current)"""
    if db is None: return []
    
    # Ambil MC, filter yang belum ada di MB
    pipeline = [
        {'$match': {'RAYON': str(rayon), 'STATUS_LUNAS': {'$ne': True}}},
        {'$sort': {'NOMINAL': -1}},
        {'$limit': 100},
        {'$project': {'_id': 0, 'NAMA': 1, 'NOMEN': 1, 'outstanding': '$NOMINAL'}}
    ]
    return list(db.master_cetak.aggregate(pipeline))

def get_top_100_unpaid_debt(rayon):
    """Top 100 Belum Bayar Tunggakan (Sama dengan Top Debt tapi khusus status belum lunas)"""
    # Asumsi tabel arrears hanya berisi yang belum lunas
    return get_top_100_debt(rayon)

# --- 6. DETECTIVE & AUDIT ---
def get_audit_detective_data(nomen):
    if db is None: return {}
    
    customer = db.customers.find_one({'nomen': nomen}, {'_id': 0})
    reading_hist = list(db.meter_history.find({'nomen': nomen}, {'_id': 0}).sort('period', -1).limit(12))
    
    return {
        'customer': customer or {'NAMA': 'Unknown', 'NOMEN': nomen},
        'reading_history': reading_hist
    }

def save_manual_audit(nomen, remark, user, status):
    if db is None: return False
    
    db.audit_logs.insert_one({
        'nomen': nomen,
        'remark': remark,
        'user': user,
        'status': status,
        'timestamp': datetime.now()
    })
    return True
