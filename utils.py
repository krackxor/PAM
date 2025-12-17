import os
from pymongo import MongoClient
from pymongo.errors import OperationFailure, ConnectionFailure, ServerSelectionTimeoutError
from datetime import datetime, timedelta
from functools import wraps

# --- KONFIGURASI DAN KONEKSI GLOBAL ---
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB_NAME") or "pam_analytics"

client = None
db = None
collections = {}

def init_db(app):
    """Menginisialisasi koneksi MongoDB dan membuat index."""
    global client, db, collections
    
    if client:
        return
    
    if not MONGO_URI:
        print("KRITIS: Variabel lingkungan MONGO_URI tidak ditemukan. Koneksi gagal.")
        return
        
    display_uri = MONGO_URI.split('@')[-1] if '@' in MONGO_URI else MONGO_URI
    print(f"Mencoba koneksi ke URI: {display_uri.split('?')[0]}...")

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=12000, socketTimeoutMS=90000, connectTimeoutMS=30000)
        # client.admin.command('ping') # Optional check
        db = client[DB_NAME]
        
        # Inisialisasi Koleksi (Mapping)
        collections['mc'] = db['MasterCetak']
        collections['mb'] = db['MasterBayar']
        collections['cid'] = db['CustomerData']
        collections['sbrs'] = db['MeterReading']
        collections['ardebt'] = db['AccountReceivable']
        
        # ==========================================================
        # === OPTIMASI: INDEXING KRITIS ===
        # ==========================================================
        
        # CID (CustomerData)
        collections['cid'].create_index([('NOMEN', 1), ('TANGGAL_UPLOAD_CID', -1)], name='idx_cid_nomen_hist')
        collections['cid'].create_index([('RAYON', 1), ('TIPEPLGGN', 1)], name='idx_cid_rayon_tipe')
        collections['cid'].create_index([('PCEZ', 1)], name='idx_cid_pcez') 

        # MC (MasterCetak)
        collections['mc'].create_index([('NOMEN', 1), ('BULAN_TAGIHAN', -1)], name='idx_mc_nomen_hist')
        collections['mc'].create_index([('PERIODE', 1)], name='idx_mc_periode')
        collections['mc'].create_index([('BULAN_TAGIHAN', 1)], name='idx_mc_bulan_tagihan')
        collections['mc'].create_index([('RAYON', 1), ('PCEZ', 1)], name='idx_mc_rayon_pcez') 
        collections['mc'].create_index([('KODERAYON', 1)], name='idx_mc_koderayon')
        collections['mc'].create_index([('STATUS', 1)], name='idx_mc_status')
        collections['mc'].create_index([('TARIF', 1), ('KUBIK', 1), ('NOMINAL', 1)], name='idx_mc_tarif_volume')

        # MB (MasterBayar)
        collections['mb'].create_index([('NOTAGIHAN', 1), ('TGL_BAYAR', 1), ('NOMINAL', 1)], name='idx_mb_unique_transaction', unique=False)
        collections['mb'].create_index([('TGL_BAYAR', -1)], name='idx_mb_paydate_desc')
        collections['mb'].create_index([('BULAN_REK', 1)], name='idx_mb_bulan_rek')
        collections['mb'].create_index([('NOMEN', 1)], name='idx_mb_nomen')
        collections['mb'].create_index([('RAYON', 1), ('PCEZ', 1), ('TGL_BAYAR', -1)], name='idx_mb_rayon_pcez_date')

        # SBRS (MeterReading)
        try:
            collections['sbrs'].create_index([('CMR_ACCOUNT', 1), ('CMR_RD_DATE', 1)], name='idx_sbrs_unique_read', unique=True)
        except OperationFailure:
            collections['sbrs'].create_index([('CMR_ACCOUNT', 1), ('CMR_RD_DATE', 1)], name='idx_sbrs_unique_read', unique=False)
            
        collections['sbrs'].create_index([('CMR_ACCOUNT', 1), ('CMR_RD_DATE', -1)], name='idx_sbrs_history')
        
        # ARDEBT (AccountReceivable)
        collections['ardebt'].create_index([('NOMEN', 1), ('PERIODE_BILL', -1), ('JUMLAH', 1)], name='idx_ardebt_nomen_hist')
        collections['ardebt'].create_index([('RAYON', 1)], name='idx_ardebt_rayon')
        
        print("Koneksi MongoDB berhasil dan index dikonfigurasi!")
    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        print(f"Gagal terhubung ke MongoDB atau mengkonfigurasi index: {e}")
        client = None
    except Exception as e:
        print(f"Gagal mengkonfigurasi index: {e}")
        client = None

def get_db_status():
    """Mengembalikan status koneksi database dan koleksi yang aktif."""
    if client is None:
        return {'status': 'error', 'message': 'Server tidak terhubung ke Database. Cek koneksi MongoDB.'}
    return {'status': 'ok', 'collections': collections}

# --- HELPER LOGIC WAKTU ---

def _get_previous_month_year(bulan_tagihan):
    if not bulan_tagihan or len(str(bulan_tagihan)) != 6:
        return None
    try:
        month = int(str(bulan_tagihan)[:2])
        year = int(str(bulan_tagihan)[2:])
        target_date = datetime(year, month, 1) - timedelta(days=1)
        prev_month = target_date.month
        prev_year = target_date.year
        return f"{prev_month:02d}{prev_year}"
    except ValueError:
        return None
        
def _get_day_n_ago(n):
    return (datetime.now() - timedelta(days=n)).strftime('%Y-%m-%d')


# --- HELPER LOGIC AGREGASI KHUSUS ---

def _parse_zona_novak(zona_str):
    zona = str(zona_str).strip().upper()
    if len(zona) < 9:
        return {'RAYON_ZONA': 'N/A', 'PC_ZONA': 'N/A', 'EZ_ZONA': 'N/A', 'PCEZ_ZONA': 'N/A', 'BLOCK_ZONA': 'N/A'}
    
    try:
        rayon = zona[0:2]
        pc = zona[2:5]
        ez = zona[5:7]
        block = zona[7:9]
        
        return {
            'RAYON_ZONA': rayon,
            'PC_ZONA': pc,
            'EZ_ZONA': ez,
            'PCEZ_ZONA': pc + ez,
            'BLOCK_ZONA': block
        }
    except Exception:
        return {'RAYON_ZONA': 'N/A', 'PC_ZONA': 'N/A', 'EZ_ZONA': 'N/A', 'PCEZ_ZONA': 'N/A', 'BLOCK_ZONA': 'N/A'}

def _get_sbrs_anomalies(collection_sbrs, collection_cid):
    if collection_sbrs is None or collection_cid is None:
        return []
        
    pipeline_sbrs_history = [
        {'$sort': {'CMR_ACCOUNT': 1, 'CMR_RD_DATE': -1}},
        {'$group': {
            '_id': '$CMR_ACCOUNT',
            'history': {
                '$push': {
                    'kubik': {'$toDouble': {'$cond': [{'$ne': ['$CMR_KUBIK', None]}, '$CMR_KUBIK', 0]}}, 
                    'tanggal': '$CMR_RD_DATE'
                }
            }
        }},
        {'$project': {
            'NOMEN': '$_id',
            'latest': {'$arrayElemAt': ['$history', 0]},
            'previous': {'$arrayElemAt': ['$history', 1]},
            '_id': 0
        }},
        {'$match': {
            'previous': {'$ne': None},
            'latest': {'$ne': None},
            'latest.kubik': {'$ne': None},
            'previous.kubik': {'$ne': None}
        }},
        {'$project': { 
            'NOMEN': 1,
            'KUBIK_TERBARU': '$latest.kubik',
            'KUBIK_SEBELUMNYA': '$previous.kubik',
            'SELISIH_KUBIK': {'$subtract': ['$latest.kubik', '$previous.kubik']},
            'PERSEN_SELISIH': {
                '$cond': {
                    'if': {'$gt': ['$previous.kubik', 0]},
                    'then': {'$multiply': [{'$divide': [{'$subtract': ['$latest.kubik', '$previous.kubik']}, '$previous.kubik']}, 100]},
                    'else': 0 
                }
            }
        }},
        {'$project': { 
            'NOMEN': 1,
            'KUBIK_TERBARU': {'$round': ['$KUBIK_TERBARU', 0]},
            'KUBIK_SEBELUMNYA': {'$round': ['$KUBIK_SEBELUMNYA', 0]},
            'SELISIH_KUBIK': {'$round': ['$SELISIH_KUBIK', 0]},
            'PERSEN_SELISIH': {'$round': ['$PERSEN_SELISIH', 2]},
            'STATUS_PEMAKAIAN': {
                '$switch': {
                    'branches': [
                        { 'case': {'$gte': ['$KUBIK_TERBARU', 150]}, 'then': 'EKSTRIM (>150 m³)' },
                        { 'case': {'$gte': ['$PERSEN_SELISIH', 50]}, 'then': 'NAIK EKSTRIM (>=50%)' }, 
                        { 'case': {'$gte': ['$PERSEN_SELISIH', 10]}, 'then': 'NAIK SIGNIFIKAN (>=10%)' }, 
                        { 'case': {'$lte': ['$PERSEN_SELISIH', -50]}, 'then': 'TURUN EKSTRIM (<= -50%)' }, 
                        { 'case': {'$lte': ['$PERSEN_SELISIH', -10]}, 'then': 'TURUN SIGNIFIKAN (<= -10%)' }, 
                        { 'case': {'$eq': ['$KUBIK_TERBARU', 0]}, 'then': 'ZERO / NOL' },
                    ],
                    'default': 'STABIL / NORMAL'
                }
            }
        }},
        {'$lookup': {
           'from': 'CustomerData', 
           'localField': 'NOMEN',
           'foreignField': 'NOMEN',
           'as': 'customer_info'
        }},
        {'$unwind': {'path': '$customer_info', 'preserveNullAndEmptyArrays': True}},
        {'$project': {
            'NOMEN': 1,
            'NAMA': {'$cond': [{'$ne': ['$customer_info.NAMA', None]}, '$customer_info.NAMA', 'N/A']},
            'ALAMAT': {'$cond': [{'$ne': ['$customer_info.ALAMAT', None]}, '$customer_info.ALAMAT', 'N/A']}, 
            'RAYON': {'$cond': [{'$ne': ['$customer_info.RAYON', None]}, '$customer_info.RAYON', 'N/A']},
            
            'PCEZ': {'$cond': [{'$ne': ['$customer_info.PCEZ', None]}, '$customer_info.PCEZ', 'N/A']},
            'TARIF': {'$cond': [{'$ne': ['$customer_info.TARIF', None]}, '$customer_info.TARIF', 'N/A']},
            'MERK': {'$cond': [{'$ne': ['$customer_info.MERK', None]}, '$customer_info.MERK', 'N/A']},
            'CYCLE': {'$cond': [{'$ne': ['$customer_info.BOOKWALK', None]}, '$customer_info.BOOKWALK', 'N/A']},
            'AB_SUNTER': {'$cond': [ 
                {'$in': ['$customer_info.RAYON', ['34', '35']]}, 'AB SUNTER', 'LUAR AB SUNTER'
            ]},
            
            'KUBIK_TERBARU': 1,
            'KUBIK_SEBELUMNYA': 1,
            'SELISIH_KUBIK': 1,
            'PERSEN_SELISIH': 1,
            'STATUS_PEMAKAIAN': 1
        }},
        {'$match': { 
            '$or': [
                {'STATUS_PEMAKAIAN': {'$ne': 'STABIL / NORMAL'}},
           ]
        }},
        {'$limit': 100}
    ]

    anomalies = list(collection_sbrs.aggregate(pipeline_sbrs_history))
    
    for doc in anomalies:
        doc.pop('_id', None)
        
    return anomalies

def _generate_distribution_schema(group_fields):
    schema = []
    for field in group_fields:
        schema.append({'key': field, 'label': field, 'type': 'string', 'is_main_key': True})
    schema.append({'key': 'total_nomen', 'label': 'Jumlah Pelanggan', 'type': 'integer'})
    schema.append({'key': 'total_piutang', 'label': 'Total Piutang', 'type': 'currency'})
    return schema

# --- CORE DASHBOARD STATISTICS FUNCTIONS ---

def _aggregate_category(collection, money_field, usage_field, period, date_field=None):
    """
    Fungsi agregasi untuk dashboard Piutang, Tunggakan, dan Collection.
    """
    if collection is None:
        return {'totals': {'count':0, 'total_usage':0, 'total_nominal':0}, 'largest': {}, 'charts': {}, 'lists': {}}

    # 1. Setup Filter Periode
    match_stage = {}
    if period and date_field:
        # Regex match untuk support format "MMYYYY" atau variasi string
        match_stage = {date_field: {'$regex': f"{period}"}} 
    
    # 2. Base Totals
    try:
        totals_pipeline = [
            {'$match': match_stage},
            {'$group': {
                '_id': None,
                'count': {'$sum': 1},
                'total_usage': {'$sum': {'$toDouble': {'$ifNull': [f'${usage_field}', 0]}}},
                'total_nominal': {'$sum': {'$toDouble': {'$ifNull': [f'${money_field}', 0]}}}
            }}
        ]
        totals_res = list(collection.aggregate(totals_pipeline))
        base = totals_res[0] if totals_res else {'count': 0, 'total_usage': 0, 'total_nominal': 0}
    except Exception as e:
        print(f"Error aggregating totals: {e}")
        base = {'count': 0, 'total_usage': 0, 'total_nominal': 0}

    # 3. Largest Contributors (Rayon, PC, PCEZ)
    def get_largest(group_candidates, lookup_cid=False):
        local_match = match_stage.copy()
        pipeline = [{'$match': local_match}]

        # Add Lookup if needed
        prefix = ""
        if lookup_cid:
            pipeline.extend([
                {'$lookup': {
                    'from': 'CustomerData',
                    'localField': 'NOMEN',
                    'foreignField': 'NOMEN',
                    'as': 'cust_info'
                }},
                {'$unwind': {'path': '$cust_info', 'preserveNullAndEmptyArrays': True}}
            ])
            prefix = "$cust_info."

        # Construct ID Expression (Fallback logic)
        id_expression = {'$ifNull': []}
        for f in group_candidates:
            if lookup_cid:
                id_expression['$ifNull'].append(f"{prefix}{f}")
            id_expression['$ifNull'].append(f"${f}")
        id_expression['$ifNull'].append("N/A")

        pipeline.extend([
            {'$group': {
                '_id': id_expression,
                'total': {'$sum': {'$toDouble': {'$ifNull': [f'${money_field}', 0]}}}
            }},
            {'$sort': {'total': -1}},
            {'$limit': 1}
        ])

        try:
            res = list(collection.aggregate(pipeline))
            return res[0] if res else {'_id': '-', 'total': 0}
        except Exception as e:
            print(f"Error largest: {e}")
            return {'_id': '-', 'total': 0}

    # Definisikan kolom kandidat
    rayon_cols = ['RAYON', 'KODERAYON', 'RAYON_ZONA']
    pc_cols = ['PC', 'KODEPC', 'PC_ZONA']
    pcez_cols = ['PCEZ', 'KODEPCEZ', 'PCEZ_ZONA']

    largest = {
        'rayon': get_largest(rayon_cols),
        'pc': get_largest(pc_cols),
        'pcez': get_largest(pcez_cols, lookup_cid=True) # Aktifkan lookup untuk PCEZ jika tidak ada di MC
    }

    # 4. Breakdowns (PC, PCEZ, Tarif, Merek) - SMART VERSION
    def get_distribution_smart(group_candidates, rayon_filter=None, lookup_cid=False):
        """
        Agregasi pintar yang bisa lookup ke CustomerData jika field tidak ada di collection utama.
        Juga menghitung nominal untuk tabel rincian.
        """
        # A. Filter Rayon
        local_match = match_stage.copy()
        if rayon_filter:
            # Support nama field RAYON atau KODERAYON
            local_match['$or'] = [{'RAYON': rayon_filter}, {'KODERAYON': rayon_filter}]

        pipeline = [{'$match': local_match}]

        # B. Lookup ke CustomerData
        prefix = ""
        if lookup_cid:
            pipeline.extend([
                {'$lookup': {
                    'from': 'CustomerData',
                    'localField': 'NOMEN',
                    'foreignField': 'NOMEN',
                    'as': 'cust_info'
                }},
                {'$unwind': {'path': '$cust_info', 'preserveNullAndEmptyArrays': True}}
            ])
            prefix = "$cust_info." # Mengarahkan ke field hasil lookup

        # C. Konstruksi Field Grouping
        id_expression = {'$ifNull': []}
        for f in group_candidates:
            if lookup_cid:
                id_expression['$ifNull'].append(f"{prefix}{f}")
            id_expression['$ifNull'].append(f"${f}")
        id_expression['$ifNull'].append("N/A") # Fallback terakhir

        # D. Grouping (Count & Nominal)
        pipeline.extend([
            {'$group': {
                '_id': id_expression,
                'val': {'$sum': 1},
                'nominal': {'$sum': {'$toDouble': {'$ifNull': [f'${money_field}', 0]}}}
            }},
            {'$sort': {'val': -1}},
            {'$limit': 10}
        ])

        try:
            return list(collection.aggregate(pipeline))
        except Exception as e:
            print(f"Error distribution: {e}")
            return []

    # Nama Kolom Kandidat
    tarif_cols = ['TARIF', 'KODETARIF', 'GOLONGAN']
    merek_cols = ['MERK', 'KODEMEREK', 'MEREKMETER', 'METER_MAKE']
    pcez_dist_cols = ['PCEZ', 'KODEPCEZ', 'PCEZ_ZONA']
    pc_dist_cols = ['PC', 'KODEPC', 'PC_ZONA']

    breakdowns = {
        # Distribusi PC (BARU)
        'pc_all': get_distribution_smart(pc_dist_cols),
        'pc_34': get_distribution_smart(pc_dist_cols, '34'),
        'pc_35': get_distribution_smart(pc_dist_cols, '35'),

        # Distribusi PCEZ
        'pcez_all': get_distribution_smart(pcez_dist_cols, lookup_cid=True),
        'pcez_34': get_distribution_smart(pcez_dist_cols, '34', lookup_cid=True),
        'pcez_35': get_distribution_smart(pcez_dist_cols, '35', lookup_cid=True),

        # Distribusi Tarif
        'tarif_all': get_distribution_smart(tarif_cols),
        'tarif_34': get_distribution_smart(tarif_cols, '34'),
        'tarif_35': get_distribution_smart(tarif_cols, '35'),
        
        # Distribusi Merek
        'merek_all': get_distribution_smart(merek_cols, lookup_cid=True),
        'merek_34': get_distribution_smart(merek_cols, '34', lookup_cid=True),
        'merek_35': get_distribution_smart(merek_cols, '35', lookup_cid=True),
    }

    # 5. Top 500 Lists - WITH NAME LOOKUP
    def get_top_500(rayon):
        match = match_stage.copy()
        match['$or'] = [{'RAYON': rayon}, {'KODERAYON': rayon}]
        
        pipeline = [
            {'$match': match},
            {'$project': {
                'NOMEN': 1, 
                # Cek apakah nama ada di lokal
                'NAMA_TEMP': {'$ifNull': ['$NAMA', '$NAMA_PEL']},
                'money': {'$toDouble': {'$ifNull': [f'${money_field}', 0]}}
            }},
            {'$sort': {'money': -1}},
            {'$limit': 500},
            # Lookup Nama dari CustomerData jika di lokal kosong/tidak lengkap
            {'$lookup': {
                'from': 'CustomerData',
                'localField': 'NOMEN',
                'foreignField': 'NOMEN',
                'as': 'cust'
            }},
            {'$unwind': {'path': '$cust', 'preserveNullAndEmptyArrays': True}},
            {'$project': {
                'NOMEN': 1,
                # Prioritas: Nama Lokal -> Nama dari CID -> N/A
                'NAMA': {'$ifNull': ['$NAMA_TEMP', '$cust.NAMA', 'N/A']},
                money_field: '$money',
                '_id': 0
            }}
        ]
        
        try:
            return list(collection.aggregate(pipeline))
        except Exception:
            return []

    top_lists = {
        'top_34': get_top_500('34'),
        'top_35': get_top_500('35')
    }

    return {
        'totals': base,
        'largest': largest,
        'charts': breakdowns,
        'lists': top_lists
    }

def get_comprehensive_stats(period=None):
    """
    Mengambil statistik lengkap untuk Piutang (MC), Tunggakan (ARDEBT), dan Collection (MB).
    """
    # Pastikan collections sudah terisi (init_db sudah dipanggil)
    if not collections:
        return {}

    stats = {
        'piutang': _aggregate_category(
            collections.get('mc'), 
            money_field="NOMINAL", 
            usage_field="KUBIK",   
            period=period, 
            date_field="BULAN_TAGIHAN" 
        ),
        'tunggakan': _aggregate_category(
            collections.get('ardebt'), 
            money_field="JUMLAH", 
            usage_field="PEMAKAIAN", 
            period=None, # ARDEBT snapshot
            date_field=None 
        ),
        'collection': _aggregate_category(
            collections.get('mb'), 
            money_field="NOMINAL", 
            usage_field="KUBIKBAYAR",
            period=period, 
            date_field="BULAN_REK"
        )
    }
    return stats
