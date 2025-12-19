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
            
            # Indexing untuk performa
            db.meter_history.create_index([("nomen", ASCENDING), ("period", DESCENDING)])
            db.collections.create_index([("RAYON", ASCENDING), ("PAY_DT", DESCENDING)])
            db.arrears.create_index([("RAYON", ASCENDING), ("JUMLAH", DESCENDING)])
            db.master_cetak.create_index([("RAYON", ASCENDING), ("NOMEN", ASCENDING)])
            db.master_bayar.create_index([("RAYON", ASCENDING), ("NOMEN", ASCENDING)])
        except Exception as e:
            print(f"⚠️ Database Connection Failed: {e}")
    else:
        print("⚠️ Warning: MONGO_URI not found. Running in Stateless Mode.")

# --- SMART COLUMN MAPPING YANG AKURAT ---
def standardize_row_keys(row):
    """
    Fungsi mapping kolom yang akurat untuk semua jenis file PAM DSS:
    - SBRS/Cycle (Meter Reading)
    - Master Cetak (MC)
    - Master Bayar (MB)
    - Arrears (ARDEBT)
    - Main Bill
    - Collection
    """
    new_row = row.copy()
    
    # COMPREHENSIVE MAPPING DICTIONARY
    mappings = {
        # === IDENTITAS PELANGGAN ===
        'NOMEN': ['CMR_ACCOUNT', 'Nomen', 'NOMEN', 'NO_SAMBUNGAN', 'NOSAMB', 'ACCOUNT_NO'],
        'NAMA': ['CMR_NAME', 'NAMA_PEL', 'Nama', 'PELANGGAN', 'NAMA_PELANGGAN'],
        'ALAMAT': ['CMR_ADDRESS', 'ALAMAT', 'ADDRESS'],
        
        # === LOKASI & GROUPING ===
        'RAYON': ['Rayon', 'KODERAYON', 'CMR_RAYON', 'KODE_RAYON'],
        'PC': ['PC', 'KODE_PC', 'CMR_PC', 'READ_CYCLE'],
        'PCEZ': ['PCEZ', 'CMR_PCEZ', 'EZ'],
        'TARIF': ['TARIF', 'CMR_TARIF', 'GOLONGAN', 'GOL_TARIF'],
        'METER': ['METER', 'METER_SIZE', 'UKURAN_METER', 'CMR_METER_SIZE'],
        
        # === METER READING (SBRS/Cycle) ===
        'CMR_READING': ['Curr_Read_1', 'STAN_AKIR', 'cmr_reading', 'KINI', 'CURRENT_READ', 'ANGKA_KINI'],
        'CMR_PREV_READ': ['Prev_Read_1', 'STAN_AWAL', 'cmr_prev_read', 'LALU', 'PREV_READ', 'ANGKA_LALU'],
        'CMR_SKIP_CODE': ['Force_reason', 'cmr_skip_code', 'KODE_BACA', 'SKIP_CODE'],
        'CMR_TRBL1_CODE': ['cmr_trbl1_code', 'TROUBLE_CODE', 'CODE_TROUBLE'],
        'CMR_CHG_SPCL_MSG': ['REMARK', 'cmr_trbl_msg', 'cmr_chg_spcl_msg', 'SPECIAL_MSG', 'PESAN_KHUSUS'],
        'CMR_RD_DATE': ['READ_DATE_1', 'cmr_rd_date', 'TGL_BACA', 'TANGGAL_BACA'],
        'CMR_MRID': ['MRID', 'cmr_mrid', 'METER_READER_ID', 'ID_PEMBACA'],
        'AVG_USAGE': ['AVG_USAGE', 'RATA_PEMAKAIAN', 'AVERAGE_USAGE'],
        
        # === BILLING (MC/MB/MainBill) ===
        'NOMINAL': ['JUMLAH', 'TAGIHAN_AIR', 'TOTAL_TAGIHAN', 'TAGIHAN', 'BILL_AMOUNT'],
        'KUBIK': ['KONSUMSI', 'KUBIKASI', 'PAKAI', 'USAGE', 'VOLUME', 'PEMAKAIAN'],
        'BILL_PERIOD': ['BILL_PERIOD', 'PERIODE', 'PERIODE_TAGIHAN', 'BLN_THN'],
        
        # === COLLECTION (Pembayaran) ===
        'AMT_COLLECT': ['AMT_COLLECT', 'JUMLAH_BAYAR', 'NOMINAL_BAYAR', 'AMOUNT_PAID'],
        'VOL_COLLECT': ['VOL_COLLECT', 'VOLUME_BAYAR', 'KUBIK_BAYAR'],
        'PAY_DT': ['PAY_DT', 'TGL_BAYAR', 'TANGGAL_BAYAR', 'PAYMENT_DATE'],
        'TYPE': ['TYPE', 'JENIS_BAYAR', 'PAYMENT_TYPE', 'TIPE_PEMBAYARAN'],
        
        # === ARREARS (Tunggakan) ===
        'JUMLAH': ['JUMLAH', 'NOMINAL', 'TUNGGAKAN', 'DEBT_AMOUNT'],
        'UMUR_TUNGGAKAN': ['UMUR_TUNGGAKAN', 'AGING', 'DEBT_AGE', 'LAMA_TUNGGAKAN'],
        
        # === STATUS ===
        'STATUS_LUNAS': ['STATUS_LUNAS', 'PAID_STATUS', 'LUNAS', 'STATUS_BAYAR']
    }

    # Lakukan mapping dengan prioritas
    for standard_key, variations in mappings.items():
        if standard_key not in new_row or new_row[standard_key] == 0:  # Jika key standar belum ada atau kosong
            for v in variations:
                v_upper = v.upper()
                if v_upper in new_row and new_row[v_upper] not in [0, '', 'nan', None]:
                    new_row[standard_key] = new_row[v_upper]
                    break
    
    return new_row

# --- DATA CLEANING DENGAN VALIDASI ---
def clean_dataframe(df):
    """
    Standarisasi Header & Isi Data dengan validasi
    """
    # 1. Header Uppercase & Strip
    df.columns = [str(col).strip().upper() for col in df.columns]
    
    # 2. Handle NaN dan tipe data
    df = df.fillna(0)
    
    # 3. String Trimming untuk kolom text
    for col in df.select_dtypes(include=['object']):
        df[col] = df[col].astype(str).str.strip()
        
    # 4. Konversi numerik untuk kolom angka
    numeric_cols = ['CMR_READING', 'CMR_PREV_READ', 'NOMINAL', 'KUBIK', 'AMT_COLLECT', 
                    'VOL_COLLECT', 'JUMLAH', 'AVG_USAGE']
    for col in numeric_cols:
        if col in df.columns:
            try:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            except:
                pass
    
    # 5. Apply Smart Mapping per Row
    records = df.to_dict('records')
    standardized_records = [standardize_row_keys(row) for row in records]
    
    return standardized_records

# --- 1. METER READING ANALYSIS (AKURAT) ---
def analyze_meter_anomalies(df_records):
    """
    Analisa akurat untuk:
    - Pemakaian Ekstrim
    - Stand Negatif
    - Pemakaian Zero
    - Pemakaian Turun
    - Salah Catat
    - Indikasi Rebill
    - Estimasi
    """
    anomalies = []
    
    # Auto-Save History ke MongoDB
    if db is not None and df_records:
        try:
            history_data = []
            for r in df_records:
                if r.get('NOMEN'):
                    history_data.append({
                        'nomen': str(r.get('NOMEN')),
                        'period': datetime.now().strftime('%Y-%m'),
                        'usage': float(r.get('CMR_READING', 0)) - float(r.get('CMR_PREV_READ', 0)),
                        'cmr_rd_date': r.get('CMR_RD_DATE', datetime.now().strftime('%Y-%m-%d')),
                        'cmr_reading': float(r.get('CMR_READING', 0)),
                        'cmr_prev_read': float(r.get('CMR_PREV_READ', 0)),
                        'cmr_skip_code': r.get('CMR_SKIP_CODE', ''),
                        'cmr_mrid': r.get('CMR_MRID', ''),
                        'rayon': r.get('RAYON', ''),
                        'created_at': datetime.now()
                    })
            if history_data:
                db.meter_history.insert_many(history_data, ordered=False)
        except Exception as e:
            print(f"Warning: Failed to save history - {e}")

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

        # === LOGIKA DETEKSI AKURAT ===
        
        # 1. STAND NEGATIF (cmr_reading - cmr_prev_read < 0)
        if usage < 0:
            status_list.append('STAND NEGATIF')
        
        # 2. PEMAKAIAN ZERO (cmr_reading = cmr_prev_read DAN prev_read > 0)
        if usage == 0 and prev > 0:
            status_list.append('PEMAKAIAN ZERO')
        
        # 3. PEMAKAIAN EKSTRIM (usage > 2x rata-rata DAN > 50m³)
        if usage > 0 and usage > (avg_usage * 2) and usage > 50:
            status_list.append('EKSTRIM')
        
        # 4. PEMAKAIAN TURUN SIGNIFIKAN (usage < 50% dari rata-rata)
        if 0 < usage < (avg_usage * 0.5) and avg_usage > 10:
            status_list.append('PEMAKAIAN TURUN')
        
        # 5. INDIKASI SALAH CATAT (usage sangat tidak wajar)
        if usage > 1000 or (usage < -100):
            status_list.append('SALAH CATAT')
        
        # 6. Deteksi Kode Masalah
        skip_code = str(row.get('CMR_SKIP_CODE', '0')).strip().upper()
        if skip_code not in ['0', 'NAN', '', 'NONE', 'NULL']:
            status_list.append(f'KODE: {skip_code}')
            
            # Sub-kategori kode
            if skip_code in ['EST', 'E', 'FORCE', 'ESTIMASI']:
                status_list.append('ESTIMASI')
        
        # 7. Pesan Khusus (Rebill, dll)
        msg = str(row.get('CMR_CHG_SPCL_MSG', '')).upper()
        if 'REBILL' in msg or 'RE-BILL' in msg:
            status_list.append('INDIKASI REBILL')

        # Hanya simpan jika ada anomali
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
                    'curr_read': curr,
                    'cmr_mrid': row.get('CMR_MRID', ''),
                    'cmr_rd_date': row.get('CMR_RD_DATE', ''),
                    'cmr_trbl1_code': row.get('CMR_TRBL1_CODE', ''),
                    'cmr_chg_spcl_msg': msg,
                    'avg_usage': avg_usage
                }
            })
            
    return sorted(anomalies, key=lambda x: abs(x['usage']), reverse=True)

# --- 2. SUMMARIZING REPORT (AKURAT) ---
def get_summarized_report(target, dimension, rayon_filter=None):
    """
    Summarizing untuk MC, MB, ARDEBT, MAINBILL, COLLECTION
    Berdasarkan dimensi: RAYON, PC, PCEZ, TARIF, METER
    """
    if db is None:
        return []
    
    col_map = {
        'mc': 'master_cetak',
        'mb': 'master_bayar',
        'ardebt': 'arrears',
        'mainbill': 'main_bill',
        'collection': 'collections'
    }
    coll_name = col_map.get(target, 'master_cetak')
    
    # Tentukan field nilai berdasarkan target
    if target == 'collection':
        val_field = '$AMT_COLLECT'
        vol_field = '$VOL_COLLECT'
    elif target == 'ardebt':
        val_field = '$JUMLAH'
        vol_field = '$KUBIK'
    else:  # mc, mb, mainbill
        val_field = '$NOMINAL'
        vol_field = '$KUBIK'

    pipeline = []
    
    # Filter Rayon jika ada
    if rayon_filter:
        pipeline.append({'$match': {'RAYON': str(rayon_filter)}})
    
    # Grouping berdasarkan dimensi
    pipeline.append({
        '$group': {
            '_id': f'${dimension}', 
            'nominal': {'$sum': val_field},
            'volume': {'$sum': vol_field},
            'count': {'$sum': 1}
        }
    })
    pipeline.append({'$sort': {'nominal': -1}})
    
    try:
        results = list(db[coll_name].aggregate(pipeline))
        
        # Hitung total untuk persentase realisasi
        total_nominal = sum(r.get('nominal', 0) for r in results)
        
        return [{
            'group': r['_id'] or 'LAINNYA',
            'nominal': r.get('nominal', 0),
            'volume': r.get('volume', 0),
            'count': r['count'],
            'realization_pct': round((r.get('nominal', 0) / total_nominal * 100), 2) if total_nominal > 0 else 0
        } for r in results]
    except Exception as e:
        print(f"Error in summarized_report: {e}")
        return []

# --- 3. COLLECTION ANALYSIS (AKURAT) ---
def get_customer_payment_status(rayon=None):
    """
    Analisa Collection yang akurat:
    - Undue (Bayar dimuka)
    - Current (Bayar lancar bulan ini)
    - Paid Arrears (Bayar tunggakan)
    - Outstanding Arrears (Masih punya tunggakan)
    - Unpaid Receivable No Arrears (Belum bayar piutang tapi tidak ada tunggakan)
    """
    if db is None:
        return {}
    
    match = {'RAYON': str(rayon)} if rayon else {}
    
    def calc(coll, query):
        try:
            res = list(db[coll].aggregate([
                {'$match': {**match, **query}}, 
                {'$group': {
                    '_id': None, 
                    'tot': {'$sum': '$NOMINAL' if coll != 'collections' else '$AMT_COLLECT'}, 
                    'cnt': {'$sum': 1}
                }}
            ]))
            return {'revenue': res[0]['tot'], 'count': res[0]['cnt']} if res else {'revenue': 0, 'count': 0}
        except:
            return {'revenue': 0, 'count': 0}

    stats = {
        # Pembayaran Undue (dimuka)
        'undue': calc('collections', {'TYPE': 'UNDUE'}),
        
        # Pembayaran Current (lancar bulan ini)
        'current': calc('collections', {'TYPE': 'CURRENT'}),
        
        # Pembayaran Tunggakan
        'paid_arrears': calc('collections', {'TYPE': 'ARREARS'}),
        
        # Tunggakan yang masih outstanding
        'outstanding_arrears': calc('arrears', {}),
        
        # Pelanggan belum bayar piutang (tidak ada tunggakan)
        # Query: Ada di MC tapi tidak ada di Arrears dan tidak ada di Collections bulan ini
        'unpaid_receivable_no_arrears': {'revenue': 0, 'count': 0}  # Perlu query kompleks
    }
    
    # Total cash masuk
    stats['total_cash'] = (stats['undue']['revenue'] + 
                           stats['current']['revenue'] + 
                           stats['paid_arrears']['revenue'])
    
    # Hitung pelanggan belum bayar piutang tanpa tunggakan
    try:
        # Ambil semua nomen dari MC
        all_mc = set([str(x['NOMEN']) for x in db.master_cetak.find(match, {'NOMEN': 1})])
        
        # Ambil nomen yang punya tunggakan
        with_arrears = set([str(x['NOMEN']) for x in db.arrears.find(match, {'NOMEN': 1})])
        
        # Ambil nomen yang sudah bayar current
        paid_current = set([str(x['NOMEN']) for x in db.collections.find({**match, 'TYPE': 'CURRENT'}, {'NOMEN': 1})])
        
        # Nomen yang belum bayar piutang tapi tidak punya tunggakan
        unpaid_no_arrears = all_mc - with_arrears - paid_current
        
        stats['unpaid_receivable_no_arrears'] = {
            'revenue': 0,
            'count': len(unpaid_no_arrears)
        }
    except:
        pass
    
    return stats

# --- 4. HISTORY (AKURAT) ---
def get_usage_history(dimension, value):
    """
    History kubikasi berdasarkan PELANGGAN, RAYON, PC, PCEZ, TARIF, METER
    """
    if db is None:
        return []
    
    query = {}
    if dimension == 'CUSTOMER':
        query['nomen'] = str(value)
    elif dimension == 'RAYON':
        query['rayon'] = str(value)
    elif dimension in ['PC', 'PCEZ', 'TARIF', 'METER']:
        # Query ke master untuk dapat list nomen
        master_query = {dimension: str(value)}
        nomens = [str(x['NOMEN']) for x in db.master_cetak.find(master_query, {'NOMEN': 1}).limit(100)]
        query['nomen'] = {'$in': nomens}
    
    hist = list(db.meter_history.find(query).sort('period', -1).limit(12))
    return [{
        'period': h.get('period'),
        'value': h.get('usage'),
        'desc': f"Pemakaian: {h.get('usage')} m³"
    } for h in hist]

def get_payment_history(nomen):
    """
    History pembayaran pelanggan (semua tipe)
    """
    if db is None:
        return []
    
    hist = list(db.collections.find({'NOMEN': str(nomen)}).sort('PAY_DT', -1).limit(12))
    return [{
        'date': h.get('PAY_DT'),
        'value': h.get('AMT_COLLECT'),
        'keterangan': h.get('TYPE', 'Payment')
    } for h in hist]

def get_payment_history_undue(nomen):
    """
    History pembayaran UNDUE pelanggan
    """
    if db is None:
        return []
    
    hist = list(db.collections.find({'NOMEN': str(nomen), 'TYPE': 'UNDUE'}).sort('PAY_DT', -1).limit(12))
    return [{
        'date': h.get('PAY_DT'),
        'value': h.get('AMT_COLLECT'),
        'keterangan': 'Pembayaran Dimuka'
    } for h in hist]

def get_payment_history_current(nomen):
    """
    History pembayaran CURRENT pelanggan
    """
    if db is None:
        return []
    
    hist = list(db.collections.find({'NOMEN': str(nomen), 'TYPE': 'CURRENT'}).sort('PAY_DT', -1).limit(12))
    return [{
        'date': h.get('PAY_DT'),
        'value': h.get('AMT_COLLECT'),
        'keterangan': 'Pembayaran Lancar'
    } for h in hist]

# --- 5. TOP 100 (AKURAT) ---
def get_top_100_premium(rayon):
    """
    Top 100 pelanggan premium (selalu bayar tepat waktu)
    Kriteria: Minimal 6 bulan terakhir selalu bayar current tepat waktu
    """
    if db is None:
        return []
    
    try:
        # Agregasi: hitung berapa kali bayar current dalam 6 bulan terakhir
        pipeline = [
            {'$match': {'RAYON': str(rayon), 'TYPE': 'CURRENT'}},
            {'$group': {
                '_id': '$NOMEN',
                'total_paid': {'$sum': '$AMT_COLLECT'},
                'payment_count': {'$sum': 1},
                'NAMA': {'$first': '$PELANGGAN'}
            }},
            {'$match': {'payment_count': {'$gte': 6}}},  # Minimal 6x bayar
            {'$sort': {'payment_count': -1, 'total_paid': -1}},
            {'$limit': 100}
        ]
        
        return list(db.collections.aggregate(pipeline))
    except:
        return []

def get_top_100_unpaid_current(rayon):
    """
    Top 100 belum bayar current (tagihan bulan ini)
    """
    if db is None:
        return []
    
    try:
        # Ambil dari MC yang belum ada di Collections current
        pipeline = [
            {'$match': {'RAYON': str(rayon)}},
            {'$lookup': {
                'from': 'collections',
                'let': {'nomen': '$NOMEN'},
                'pipeline': [
                    {'$match': {
                        '$expr': {'$and': [
                            {'$eq': ['$NOMEN', '$$nomen']},
                            {'$eq': ['$TYPE', 'CURRENT']}
                        ]}
                    }}
                ],
                'as': 'payments'
            }},
            {'$match': {'payments': {'$size': 0}}},  # Belum ada pembayaran current
            {'$project': {
                '_id': 0,
                'NAMA': '$NAMA_PEL',
                'NOMEN': 1,
                'outstanding': '$NOMINAL'
            }},
            {'$sort': {'outstanding': -1}},
            {'$limit': 100}
        ]
        
        return list(db.master_cetak.aggregate(pipeline))
    except:
        return []

def get_top_100_debt(rayon):
    """
    Top 100 pelanggan tunggakan terbesar
    """
    if db is None:
        return []
    
    try:
        return list(db.arrears.aggregate([
            {'$match': {'RAYON': str(rayon)}},
            {'$sort': {'JUMLAH': -1}},
            {'$limit': 100},
            {'$project': {
                '_id': 0,
                'NAMA': 1,
                'NOMEN': 1,
                'debt_amount': '$JUMLAH',
                'UMUR_TUNGGAKAN': 1
            }}
        ]))
    except:
        return []

def get_top_100_unpaid_debt(rayon):
    """
    Top 100 belum bayar tunggakan
    (Punya tunggakan tapi belum ada di Collections dengan TYPE=ARREARS)
    """
    if db is None:
        return []
    
    try:
        pipeline = [
            {'$match': {'RAYON': str(rayon)}},
            {'$lookup': {
                'from': 'collections',
                'let': {'nomen': '$NOMEN'},
                'pipeline': [
                    {'$match': {
                        '$expr': {'$and': [
                            {'$eq': ['$NOMEN', '$$nomen']},
                            {'$eq': ['$TYPE', 'ARREARS']}
                        ]}
                    }}
                ],
                'as': 'payments'
            }},
            {'$match': {'payments': {'$size': 0}}},  # Belum bayar tunggakan
            {'$project': {
                '_id': 0,
                'NAMA': 1,
                'NOMEN': 1,
                'debt_amount': '$JUMLAH',
                'UMUR_TUNGGAKAN': 1
            }},
            {'$sort': {'debt_amount': -1}},
            {'$limit': 100}
        ]
        
        return list(db.arrears.aggregate(pipeline))
    except:
        return []

# --- 6. DETECTIVE DATA ---
def get_audit_detective_data(nomen):
    """
    Data lengkap pelanggan untuk audit
    """
    if db is None:
        return {}
    
    try:
        customer = db.master_cetak.find_one({'NOMEN': str(nomen)}, {'_id': 0})
        reading_history = list(db.meter_history.find(
            {'nomen': str(nomen)}, 
            {'_id': 0}
        ).sort('period', -1).limit(12))
        
        return {
            'customer': customer or {'NAMA': 'Unknown'},
            'reading_history': reading_history
        }
    except:
        return {'customer': {}, 'reading_history': []}

def save_manual_audit(nomen, remark, user, status):
    """
    Simpan hasil audit manual
    """
    if db:
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
    return False
