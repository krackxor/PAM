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
        # === OPTIMASI: INDEXING KRITIS (COMPOUND INDEXES) ===
        # ==========================================================
        
        print("Memeriksa dan membuat index database...")

        # 1. CID (CustomerData) - Lookup optimization
        collections['cid'].create_index([('NOMEN', 1)], name='idx_cid_nomen')
        
        # 2. MC (MasterCetak) - Dashboard aggregation optimization
        # Index untuk filter periode + grouping field yang sering dipakai
        collections['mc'].create_index([('BULAN_TAGIHAN', 1), ('RAYON', 1)], name='idx_mc_bulan_rayon')
        collections['mc'].create_index([('BULAN_TAGIHAN', 1), ('PC', 1)], name='idx_mc_bulan_pc')
        collections['mc'].create_index([('BULAN_TAGIHAN', 1), ('PCEZ', 1)], name='idx_mc_bulan_pcez')
        collections['mc'].create_index([('BULAN_TAGIHAN', 1), ('TARIF', 1)], name='idx_mc_bulan_tarif')
        collections['mc'].create_index([('BULAN_TAGIHAN', 1), ('NOMEN', 1)], name='idx_mc_bulan_nomen')
        # Index Tunggal
        collections['mc'].create_index([('NOMEN', 1)], name='idx_mc_nomen')
        collections['mc'].create_index([('PERIODE', 1)], name='idx_mc_periode')

        # 3. MB (MasterBayar)
        collections['mb'].create_index([('BULAN_REK', 1), ('RAYON', 1)], name='idx_mb_bulan_rayon')
        collections['mb'].create_index([('TGL_BAYAR', -1)], name='idx_mb_tgl_bayar')
        # FIX: Index conflict resolution. Use the existing key structure.
        collections['mb'].create_index([('NOTAGIHAN', 1), ('TGL_BAYAR', 1), ('NOMINAL', 1)], name='idx_mb_unique_transaction', unique=False)
        
        # 4. SBRS
        collections['sbrs'].create_index([('CMR_ACCOUNT', 1), ('CMR_RD_DATE', -1)], name='idx_sbrs_history')
        
        print("Koneksi MongoDB berhasil dan index dikonfigurasi!")
    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        print(f"Gagal terhubung ke MongoDB atau mengkonfigurasi index: {e}")
        client = None
    except Exception as e:
        print(f"Gagal mengkonfigurasi index: {e}")
        # Log error but don't stop app if index fails
        # client = None 

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
    def get_largest(group_candidates, lookup_cid=False, derive_pc_from_pcez=False):
        local_match = match_stage.copy()
        pipeline = [{'$match': local_match}]

        # Add Lookup if needed
        prefix = ""
        if lookup_cid:
            # Optimasi: Hanya ambil field yang diperlukan
            project_fields = {c: 1 for c in group_candidates}
            if derive_pc_from_pcez:
                project_fields['PCEZ'] = 1 # Pastikan PCEZ diambil jika mau derive PC
            project_fields['_id'] = 0

            pipeline.extend([
                {'$lookup': {
                    'from': 'CustomerData',
                    'localField': 'NOMEN',
                    'foreignField': 'NOMEN',
                    'pipeline': [{'$project': project_fields}],
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
        
        # LOGIKA BARU: Jika PC 0/Null, ambil 3 digit kiri dari PCEZ di CustomerData
        if derive_pc_from_pcez and lookup_cid:
             id_expression['$ifNull'].append({ '$substr': [ f"{prefix}PCEZ", 0, 3 ] })

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
    pc_cols = ['PC', 'KODEPC', 'PC_ZONA'] # PC kandidat
    pcez_cols = ['PCEZ', 'KODEPCEZ', 'PCEZ_ZONA']

    largest = {
        'rayon': get_largest(rayon_cols),
        # Aktifkan derive_pc_from_pcez=True untuk PC
        'pc': get_largest(pc_cols, lookup_cid=True, derive_pc_from_pcez=True), 
        'pcez': get_largest(pcez_cols, lookup_cid=True)
    }

    # 4. Breakdowns (PC, PCEZ, Tarif, Merek) - SMART VERSION
    def get_distribution_smart(group_candidates, rayon_filter=None, lookup_cid=False, derive_pc_from_pcez=False):
        # A. Filter Rayon
        local_match = match_stage.copy()
        if rayon_filter:
            local_match['$or'] = [{'RAYON': rayon_filter}, {'KODERAYON': rayon_filter}]

        pipeline = [{'$match': local_match}]

        # B. Lookup ke CustomerData
        prefix = ""
        if lookup_cid:
            project_fields = {c: 1 for c in group_candidates}
            if derive_pc_from_pcez:
                project_fields['PCEZ'] = 1
            project_fields['_id'] = 0

            pipeline.extend([
                {'$lookup': {
                    'from': 'CustomerData',
                    'localField': 'NOMEN',
                    'foreignField': 'NOMEN',
                    'pipeline': [{'$project': project_fields}], # Optimization
                    'as': 'cust_info'
                }},
                {'$unwind': {'path': '$cust_info', 'preserveNullAndEmptyArrays': True}}
            ])
            prefix = "$cust_info." 

        # C. Konstruksi Field Grouping
        id_expression = {'$ifNull': []}
        for f in group_candidates:
            if lookup_cid:
                id_expression['$ifNull'].append(f"{prefix}{f}")
            id_expression['$ifNull'].append(f"${f}")
        
        # LOGIKA BARU: Derive PC dari PCEZ (Left 3)
        if derive_pc_from_pcez and lookup_cid:
             id_expression['$ifNull'].append({ '$substr': [ f"{prefix}PCEZ", 0, 3 ] })

        id_expression['$ifNull'].append("N/A") 

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
        # Distribusi PC (Aktifkan Lookup & Derivation)
        'pc_all': get_distribution_smart(pc_dist_cols, lookup_cid=True, derive_pc_from_pcez=True),
        'pc_34': get_distribution_smart(pc_dist_cols, '34', lookup_cid=True, derive_pc_from_pcez=True),
        'pc_35': get_distribution_smart(pc_dist_cols, '35', lookup_cid=True, derive_pc_from_pcez=True),

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

    # 5. Top 500 Lists - WITH NAME LOOKUP (OPTIMIZED)
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
                'pipeline': [{'$project': {'NAMA': 1, '_id': 0}}], # Optimization: Fetch only NAME
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
