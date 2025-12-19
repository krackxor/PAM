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
            
            # Indexing
            db.meter_history.create_index([("nomen", ASCENDING), ("period", DESCENDING)])
            db.collections.create_index([("RAYON", ASCENDING), ("TYPE", ASCENDING)])
            db.arrears.create_index([("RAYON", ASCENDING), ("JUMLAH", DESCENDING)])
            db.master_cetak.create_index([("RAYON", ASCENDING)])
        except Exception as e:
            print(f"⚠️ Database Connection Failed: {e}")
    else:
        print("⚠️ Warning: MONGO_URI not found. Running in Stateless Mode.")

# --- SMART COLUMN MAPPING ---
def standardize_row_keys(row):
    """
    Fungsi cerdas untuk menstandarisasi nama kolom dari berbagai format file
    agar sesuai dengan logika sistem (NOMEN, CMR_READING, NOMINAL).
    """
    new_row = row.copy()
    
    # Mapping Dictionary (Variasi Header -> Header Standar)
    mappings = {
        'NOMEN': ['CMR_ACCOUNT', 'Nomen', 'NOMEN', 'NO_SAMBUNGAN'],
        'NAMA': ['CMR_NAME', 'NAMA_PEL', 'Nama'],
        'RAYON': ['Rayon', 'KODERAYON'],
        
        # Meter Reading
        'CMR_READING': ['Curr_Read_1', 'STAN_AKIR', 'cmr_reading', 'KINI'],
        'CMR_PREV_READ': ['Prev_Read_1', 'STAN_AWAL', 'cmr_prev_read', 'LALU'],
        'CMR_SKIP_CODE': ['Force_reason', 'cmr_skip_code', 'KODE_BACA'],
        'CMR_CHG_SPCL_MSG': ['REMARK', 'cmr_trbl_msg'],
        
        # Billing / Collection
        'NOMINAL': ['JUMLAH', 'TAGIHAN_AIR', 'AMT_COLLECT', 'TOTAL_TAGIHAN'],
        'KUBIK': ['KONSUMSI', 'VOL_COLLECT', 'KUBIKASI', 'PAKAI']
    }

    # Lakukan mapping
    for standard_key, variations in mappings.items():
        if standard_key not in new_row: # Jika key standar belum ada
            for v in variations:
                v_upper = v.upper() # Cek case insensitive
                # Cari di row keys (row keys sudah di upper di clean_dataframe)
                if v_upper in new_row:
                    new_row[standard_key] = new_row[v_upper]
                    break
    
    return new_row

# --- DATA CLEANING ---
def clean_dataframe(df):
    """Standarisasi Header & Isi Data"""
    # 1. Header Uppercase & Strip
    df.columns = [str(col).strip().upper() for col in df.columns]
    
    # 2. Handle NaN
    df = df.fillna(0)
    
    # 3. String Trimming
    for col in df.select_dtypes(include=['object']):
        df[col] = df[col].astype(str).str.strip()
        
    # 4. Apply Smart Mapping per Row
    records = df.to_dict('records')
    standardized_records = [standardize_row_keys(row) for row in records]
    
    return standardized_records

# --- 1. METER READING ANALYSIS ---
def analyze_meter_anomalies(df_records):
    anomalies = []
    
    # Auto-Save History (Optional)
    if db is not None and df_records:
        try:
            # Siapkan data untuk history
            history_data = []
            for r in df_records:
                history_data.append({
                    'nomen': str(r.get('NOMEN')),
                    'period': datetime.now().strftime('%Y-%m'), # Atau ambil dari BILL_PERIOD
                    'usage': float(r.get('CMR_READING', 0)) - float(r.get('CMR_PREV_READ', 0)),
                    'cmr_rd_date': r.get('READ_DATE_1', datetime.now().strftime('%Y-%m-%d')),
                    'CMR_READING': float(r.get('CMR_READING', 0)),
                    'CMR_PREV_READ': float(r.get('CMR_PREV_READ', 0))
                })
            # db.meter_history.insert_many(history_data, ordered=False) 
        except: pass

    for row in df_records:
        status_list = []
        
        nomen = str(row.get('NOMEN', 'Unknown'))
        name = row.get('NAMA', 'Pelanggan')
        
        try:
            prev = float(row.get('CMR_PREV_READ', 0))
            curr = float(row.get('CMR_READING', 0))
            usage = curr - prev
            avg_usage = float(row.get('AVG_USAGE', 20)) 
        except:
            continue 

        # Logika Deteksi
        if usage < 0: status_list.append('STAND NEGATIF')
        if usage == 0: status_list.append('PEMAKAIAN ZERO')
        if usage > 0 and (usage > (avg_usage * 2) and usage > 50): status_list.append('EKSTRIM')
        
        # Deteksi Kode Masalah
        skip_code = str(row.get('CMR_SKIP_CODE', '0')).strip()
        if skip_code not in ['0', 'nan', '', 'None', 'NULL']:
            status_list.append(f'KODE: {skip_code}')
            if skip_code in ['EST', 'E', 'FORCE']: status_list.append('ESTIMASI')
        
        # Pesan Khusus
        msg = str(row.get('CMR_CHG_SPCL_MSG', '')).upper()
        if 'REBILL' in msg: status_list.append('INDIKASI REBILL')

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
    if db is None: return []
    
    col_map = {
        'mc': 'master_cetak', 'mb': 'master_bayar',
        'ardebt': 'arrears', 'mainbill': 'main_bill',
        'collection': 'collections'
    }
    coll_name = col_map.get(target, 'master_cetak')
    
    # Tentukan field nilai berdasarkan target
    val_field = '$AMT_COLLECT' if target == 'collection' else '$NOMINAL'
    if target == 'ardebt': val_field = '$JUMLAH' # File Arrears pakai kolom JUMLAH
    if target == 'mainbill': val_field = '$TOTAL_TAGIHAN' # File MainBill pakai TOTAL_TAGIHAN

    pipeline = []
    if rayon_filter: pipeline.append({'$match': {'RAYON': str(rayon_filter)}})
    
    pipeline.append({
        '$group': {
            '_id': f'${dimension}', 
            'nominal': {'$sum': val_field},
            'volume': {'$sum': '$KUBIK'},
            'count': {'$sum': 1}
        }
    })
    pipeline.append({'$sort': {'_id': 1}})
    
    try:
        results = list(db[coll_name].aggregate(pipeline))
        return [{
            'group': r['_id'] or 'LAINNYA',
            'nominal': r.get('nominal', 0),
            'volume': r.get('volume', 0),
            'count': r['count'],
            'realization_pct': 0
        } for r in results]
    except: return []

# --- 3. COLLECTION ANALYSIS ---
def get_customer_payment_status(rayon=None):
    if db is None: return {}
    match = {'RAYON': str(rayon)} if rayon else {}
    
    def calc(coll, q):
        try:
            res = list(db[coll].aggregate([{'$match': {**match, **q}}, {'$group': {'_id': None, 'tot': {'$sum': '$NOMINAL'}, 'cnt': {'$sum': 1}}}]))
            return {'revenue': res[0]['tot'], 'count': res[0]['cnt']} if res else {'revenue': 0, 'count': 0}
        except: return {'revenue': 0, 'count': 0}

    # Karena file collection Anda tidak punya kolom TYPE (Undue/Current), 
    # kita gunakan logika sederhana atau default.
    # Disini kita asumsikan semua di collections adalah 'CURRENT' jika TYPE kosong.
    
    stats = {
        'undue': calc('collections', {'TYPE': 'UNDUE'}),
        'current': calc('collections', {'TYPE': {'$in': ['CURRENT', None]}}), # Default ke Current
        'paid_arrears': calc('collections', {'TYPE': 'ARREARS'}),
        'outstanding_arrears': calc('arrears', {}),
        # Unpaid MC (Current Bill belum bayar)
        'unpaid_receivable_no_arrears': calc('master_cetak', {'STATUS_LUNAS': {'$ne': True}})
    }
    
    stats['total_cash'] = stats['undue']['revenue'] + stats['current']['revenue'] + stats['paid_arrears']['revenue']
    return stats

# --- 4. HISTORY & TOP 100 ---
# Fungsi-fungsi ini tetap sama, hanya memastikan field sesuai mapping baru
def get_usage_history(dimension, value):
    if db is None: return []
    q = {}
    if dimension == 'CUSTOMER': q['nomen'] = value
    elif dimension == 'RAYON': q['RAYON'] = value
    
    hist = list(db.meter_history.find(q).sort('period', -1).limit(12))
    return [{'period': h.get('period'), 'value': h.get('usage'), 'desc': 'Usage'} for h in hist]

def get_payment_history(nomen):
    if db is None: return []
    hist = list(db.collections.find({'NOMEN': str(nomen)}).sort('PAY_DT', -1).limit(12))
    return [{'date': h.get('PAY_DT'), 'value': h.get('AMT_COLLECT'), 'desc': 'Payment'} for h in hist]

def get_top_100_debt(rayon):
    if db is None: return []
    return list(db.arrears.aggregate([
        {'$match': {'RAYON': str(rayon)}},
        {'$sort': {'JUMLAH': -1}}, {'$limit': 100},
        {'$project': {'_id':0, 'NAMA':1, 'NOMEN':1, 'debt_amount':'$JUMLAH', 'UMUR_TUNGGAKAN':1}}
    ]))

def get_top_100_premium(rayon):
    if db is None: return []
    # Mengambil dari collection (file Collection-2025...)
    return list(db.collections.aggregate([
        {'$match': {'RAYON': str(rayon)}},
        {'$group': {'_id': '$NOMEN', 'total_paid': {'$sum': '$AMT_COLLECT'}, 'NAMA': {'$first': 'PELANGGAN'}}},
        {'$sort': {'total_paid': -1}}, {'$limit': 100}
    ]))

def get_top_100_unpaid_current(rayon):
    if db is None: return []
    return list(db.master_cetak.aggregate([
        {'$match': {'RAYON': str(rayon), 'STATUS_LUNAS': {'$ne': True}}},
        {'$sort': {'NOMINAL': -1}}, {'$limit': 100},
        {'$project': {'_id':0, 'NAMA':'$NAMA_PEL', 'NOMEN':1, 'outstanding':'$NOMINAL'}}
    ]))

def get_top_100_unpaid_debt(rayon):
    return get_top_100_debt(rayon)

def get_audit_detective_data(nomen):
    if db is None: return {}
    return {
        'customer': db.customers.find_one({'NOMEN': str(nomen)}, {'_id':0}) or {'NAMA': 'Unknown'},
        'reading_history': list(db.meter_history.find({'nomen': str(nomen)}, {'_id':0}).limit(12))
    }

def save_manual_audit(nomen, remark, user, status):
    if db: db.audit_logs.insert_one({'nomen': nomen, 'remark': remark, 'user': user, 'status': status, 'ts': datetime.now()})
