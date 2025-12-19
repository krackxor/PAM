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
            
            # Indexing untuk performa query cepat
            db.meter_history.create_index([("nomen", ASCENDING), ("period", DESCENDING)])
            db.collections.create_index([("RAYON", ASCENDING), ("TYPE", ASCENDING)])
            db.arrears.create_index([("RAYON", ASCENDING), ("JUMLAH", DESCENDING)])
            db.master_cetak.create_index([("RAYON", ASCENDING)])
            
        except Exception as e:
            print(f"⚠️ Database Connection Failed: {e}")
    else:
        print("⚠️ Warning: MONGO_URI not found in .env. Running in Stateless Mode.")

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
    Menganalisa data SBRS/Meter Reading.
    Menyimpan history ke database secara otomatis jika terkoneksi.
    """
    anomalies = []
    
    # Opsi: Simpan data mentah ke DB untuk keperluan History
    if db is not None and df_records:
        try:
            # Menggunakan insert_many dengan ordered=False agar data duplikat tidak menghentikan proses
            # Pastikan logic aplikasi menangani duplikasi jika perlu (misal menggunakan upsert di masa depan)
            # Untuk versi simpel, kita append saja.
            # db.meter_history.insert_many(df_records, ordered=False) 
            pass # Diaktifkan jika ingin auto-save history setiap upload
        except:
            pass

    for row in df_records:
        status_list = []
        
        # Mapping nama kolom yang umum
        nomen = str(row.get('NOMEN', row.get('CMR_ACCOUNT', 'Unknown')))
        name = row.get('NAMA', row.get('CMR_NAME', 'Pelanggan'))
        
        # Pastikan angka valid
        try:
            prev = float(row.get('CMR_PREV_READ', 0))
            curr = float(row.get('CMR_READING', 0))
            usage = curr - prev
            # Asumsi rata-rata pemakaian (bisa diambil dari DB jika ada, disini pakai default/kolom)
            avg_usage = float(row.get('AVG_USAGE', 20)) 
        except:
            continue 

        # A. STAND NEGATIF
        if usage < 0:
            status_list.append('STAND NEGATIF')
            
        # B. ZERO USAGE
        if usage == 0:
            status_list.append('PEMAKAIAN ZERO')
            
        # C. PEMAKAIAN EKSTRIM (Contoh: > 2x Rata-rata dan > 50m3)
        if usage > 0 and (usage > (avg_usage * 2) and usage > 50):
            status_list.append('EKSTRIM')
            
        # D. ANALISA KODE (Skip/Trouble)
        skip_code = str(row.get('CMR_SKIP_CODE', '0')).strip()
        if skip_code not in ['0', 'nan', '', 'None']:
            status_list.append(f'SKIP: {skip_code}')
            if skip_code in ['EST', 'E']: status_list.append('ESTIMASI')
            
        # E. PESAN KHUSUS
        msg = str(row.get('CMR_CHG_SPCL_MSG', '')).upper()
        if 'REBILL' in msg: status_list.append('INDIKASI REBILL')
        if 'SALAH' in msg: status_list.append('SALAH CATAT')

        # Jika ada anomali, masukkan ke list result
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
            
    return anomalies

# --- 2. SUMMARIZING REPORT ---
def get_summarized_report(target, dimension, rayon_filter=None):
    """
    Summarize data berdasarkan Target (MC, MB, dll) dan Dimensi (RAYON, TARIF, dll).
    Mengambil data real dari MongoDB.
    """
    if db is None: return []
    
    # Mapping Target ke Collection Name di MongoDB
    col_map = {
        'mc': 'master_cetak',
        'mb': 'master_bayar',
        'ardebt': 'arrears',
        'mainbill': 'main_bill',
        'collection': 'collections'
    }
    coll_name = col_map.get(target, 'master_cetak')
    collection = db[coll_name]
    
    pipeline = []
    
    # 1. Filter Rayon (Optional)
    if rayon_filter:
        pipeline.append({'$match': {'RAYON': str(rayon_filter)}})
        
    # 2. Grouping
    # Tentukan nama field nilai uang (NOMINAL vs AMT_COLLECT vs JUMLAH)
    if target == 'collection': val_field = '$AMT_COLLECT'
    elif target == 'ardebt': val_field = '$JUMLAH'
    else: val_field = '$NOMINAL'
    
    # Dimensi grouping (perlu $ di depan nama field)
    group_id = f'${dimension}'
    
    pipeline.append({
        '$group': {
            '_id': group_id, 
            'nominal': {'$sum': val_field},
            'volume': {'$sum': '$KUBIK'}, # Asumsi kolom KUBIK ada
            'count': {'$sum': 1}
        }
    })
    
    # 3. Sorting
    pipeline.append({'$sort': {'_id': 1}})
    
    try:
        results = list(collection.aggregate(pipeline))
    except Exception as e:
        print(f"Error Aggregation: {e}")
        return []
    
    formatted = []
    for r in results:
        formatted.append({
            'group': r['_id'] if r['_id'] else 'LAINNYA',
            'nominal': r.get('nominal', 0),
            'volume': r.get('volume', 0),
            'count': r['count'],
            'realization_pct': 0 # Bisa ditambahkan logic persentase jika ada target
        })
    return formatted

# --- 3. COLLECTION DETAILED ANALYSIS ---
# FUNGSI INI MENGGANTIKAN get_collection_detailed_analysis
def get_customer_payment_status(rayon=None):
    """
    Mengelompokkan status pembayaran pelanggan:
    - Undue (Dimuka)
    - Current (Bulan Ini)
    - Arrears (Tunggakan)
    - Unpaid Receivable (Piutang Lancar tapi belum bayar)
    - Outstanding Arrears (Masih Menunggak)
    """
    if db is None: return {}
    
    match_stage = {'RAYON': str(rayon)} if rayon else {}
    
    # Helper query
    def agg_sum(coll, query):
        pipeline = [
            {'$match': {**match_stage, **query}},
            {'$group': {'_id': None, 'total': {'$sum': '$AMT_COLLECT'}, 'count': {'$sum': 1}}}
        ]
        try:
            res = list(db[coll].aggregate(pipeline))
            return {'revenue': res[0]['total'], 'count': res[0]['count']} if res else {'revenue': 0, 'count': 0}
        except: return {'revenue': 0, 'count': 0}

    # A. Realisasi Pembayaran (Dari tabel collections)
    # Asumsi kolom TYPE ada di tabel collections (UNDUE, CURRENT, ARREARS)
    stats = {
        'undue': agg_sum('collections', {'TYPE': 'UNDUE'}),
        'current': agg_sum('collections', {'TYPE': 'CURRENT'}),
        'paid_arrears': agg_sum('collections', {'TYPE': 'ARREARS'}),
        'total_cash': 0
    }
    stats['total_cash'] = stats['undue']['revenue'] + stats['current']['revenue'] + stats['paid_arrears']['revenue']
    
    # B. Posisi Saldo Piutang (Outstanding)
    # 1. Menunggak (Ada di tabel Arrears)
    arrears_res = list(db.arrears.aggregate([
        {'$match': match_stage},
        {'$group': {'_id': None, 'total': {'$sum': '$JUMLAH'}, 'count': {'$sum': 1}}} 
    ]))
    stats['outstanding_arrears'] = {'revenue': arrears_res[0]['total'], 'count': arrears_res[0]['count']} if arrears_res else {'revenue':0, 'count':0}
    
    # 2. Belum Bayar Current (Ada di MC tapi belum Lunas)
    # Asumsi MC punya flag LUNAS atau kita cek yang tidak ada di MB
    # Disini kita pakai query simpel: MC dengan STATUS_LUNAS != True
    mc_unpaid = list(db.master_cetak.aggregate([
        {'$match': {**match_stage, 'STATUS_LUNAS': {'$ne': True}}}, 
        {'$group': {'_id': None, 'total': {'$sum': '$NOMINAL'}, 'count': {'$sum': 1}}}
    ]))
    stats['unpaid_receivable_no_arrears'] = {'revenue': mc_unpaid[0]['total'], 'count': mc_unpaid[0]['count']} if mc_unpaid else {'revenue':0, 'count':0}
    
    return stats

# --- 4. HISTORY ---
def get_usage_history(dimension, value):
    """Ambil riwayat pemakaian (Kubikasi) dari database"""
    if db is None: return []
    
    query = {}
    # Mapping filter field
    if dimension == 'CUSTOMER': query['nomen'] = value # Asumsi field di db lower case
    elif dimension == 'RAYON': query['RAYON'] = value
    elif dimension == 'PC': query['PC'] = value
    
    # Ambil dari meter_history
    history = list(db.meter_history.find(query).sort('period', -1).limit(12))
    
    return [{
        'period': h.get('period', h.get('cmr_rd_date', '-')),
        'value': h.get('usage', h.get('CMR_READING', 0) - h.get('CMR_PREV_READ', 0)),
        'desc': f"Stand: {h.get('CMR_READING',0)}"
    } for h in history]

def get_payment_history(nomen):
    """Ambil riwayat pembayaran per Nomen"""
    if db is None: return []
    
    history = list(db.collections.find({'NOMEN': str(nomen)}).sort('TGL_BAYAR', -1).limit(12))
    return [{
        'date': h.get('TGL_BAYAR'),
        'value': h.get('AMT_COLLECT'),
        'desc': h.get('TYPE', 'PAYMENT')
    } for h in history]

# --- 5. TOP 100 RANKINGS ---
def get_top_100_debt(rayon):
    """Top 100 Penunggak Terbesar (Dari Arrears)"""
    if db is None: return []
    
    pipeline = [
        {'$match': {'RAYON': str(rayon)}},
        {'$sort': {'JUMLAH': -1}}, 
        {'$limit': 100},
        {'$project': {'_id': 0, 'NAMA': 1, 'NOMEN': 1, 'debt_amount': '$JUMLAH', 'UMUR_TUNGGAKAN': '$LEMBAR'}}
    ]
    return list(db.arrears.aggregate(pipeline))

def get_top_100_premium(rayon):
    """Top 100 Pelanggan Premium (Selalu Tepat Waktu / Revenue Tertinggi)"""
    if db is None: return []
    
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
    
    pipeline = [
        {'$match': {'RAYON': str(rayon), 'STATUS_LUNAS': {'$ne': True}}},
        {'$sort': {'NOMINAL': -1}},
        {'$limit': 100},
        {'$project': {'_id': 0, 'NAMA': 1, 'NOMEN': 1, 'outstanding': '$NOMINAL'}}
    ]
    return list(db.master_cetak.aggregate(pipeline))

def get_top_100_unpaid_debt(rayon):
    """Top 100 Belum Bayar Tunggakan"""
    # Mengembalikan data yang sama dengan top debt, karena arrears asumsinya belum lunas
    return get_top_100_debt(rayon)

# --- 6. DETECTIVE & AUDIT ---
def get_audit_detective_data(nomen):
    if db is None: return {}
    
    # Ambil info customer (bisa dari MC atau tabel customers)
    customer = db.customers.find_one({'nomen': str(nomen)}, {'_id': 0})
    if not customer:
        # Fallback cari di master cetak jika tabel customers belum sync
        customer = db.master_cetak.find_one({'NOMEN': str(nomen)}, {'_id': 0})
        
    reading_hist = list(db.meter_history.find({'nomen': str(nomen)}, {'_id': 0}).sort('period', -1).limit(12))
    
    return {
        'customer': customer or {'NAMA': 'Tidak Ditemukan', 'NOMEN': nomen},
        'reading_history': reading_hist
    }

def save_manual_audit(nomen, remark, user, status):
    if db is None: return False
    
    try:
        db.audit_logs.insert_one({
            'nomen': nomen,
            'remark': remark,
            'user': user,
            'status': status,
            'timestamp': datetime.now()
        })
        return True
    except:
        return False
