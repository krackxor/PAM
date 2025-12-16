from flask import Blueprint, request, jsonify, render_template, url_for, flash, redirect
from flask_login import login_required, current_user
from functools import wraps
from datetime import datetime
# Import get_comprehensive_stats untuk dashboard utama
from utils import get_db_status, _get_previous_month_year, _get_day_n_ago, _generate_distribution_schema, get_comprehensive_stats

# Definisikan Blueprint dengan nama 'bp_collection'
bp_collection = Blueprint('bp_collection', __name__, url_prefix='/collection')

# --- Middleware Dekorator untuk Cek Admin (Wajib di Blueprint) ---
def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.is_admin:
            flash('Anda tidak memiliki izin (Admin) untuk mengakses halaman ini.', 'danger')
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function

# --- HELPER INTERNAL UNTUK DISTRIBUSI (HANYA DIGUNAKAN DI SINI, BUKAN API ENDPOINT) ---
def _get_distribution_report(group_fields, collection_mc):
    """Menghitung distribusi metrik Piutang dan Kubikasi berdasarkan field grouping."""
    db_status = get_db_status()
    if db_status['status'] == 'error':
        return [], "N/A (Koneksi DB Gagal)"
        
    if isinstance(group_fields, str):
        group_fields = [group_fields]

    latest_mc_month_doc = collection_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
    latest_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None
    
    if not latest_month:
        return [], "N/A (Tidak Ada Data MC)"

    pipeline = [
        {"$match": {"BULAN_TAGIHAN": latest_month}},
        {"$project": {
            **{field: f"${field}" for field in group_fields},
            "NOMEN": 1,
            "NOMINAL": {"$toDouble": {'$cond': [{'$ne': ['$NOMINAL', None]}, '$NOMINAL', 0]}}, 
            "KUBIK": {"$toDouble": {'$cond': [{'$ne': ['$KUBIK', None]}, '$KUBIK', 0]}},
        }},
        {"$group": {
            "_id": {field: f"${field}" for field in group_fields},
            "total_nomen_set": {"$addToSet": "$NOMEN"},
            "total_piutang": {"$sum": "$NOMINAL"},
            "total_kubikasi": {"$sum": "$KUBIK"}
        }},
        {"$project": {
            **{field: f"$_id.{field}" for field in group_fields},
            "_id": 0,
            "total_nomen": {"$size": "$total_nomen_set"},
            "total_piutang": 1,
            "total_kubikasi": 1
        }},
        {"$sort": {"total_piutang": -1}}
    ]

    try:
        results = list(collection_mc.aggregate(pipeline, allowDiskUse=True))
    except Exception as e:
        print(f"Error during distribution aggregation: {e}")
        return [], latest_month

    return results, latest_month
    
# --- RUTE VIEW FRONTEND (Halaman HTML) ---

# Menu 1: Laporan Piutang & Koleksi (Ringkasan KPI Dashboard)
@bp_collection.route('/laporan', methods=['GET'])
@login_required 
def collection_laporan_view():
    # 1. Tentukan Periode Default (Bulan Saat Ini)
    current_date = datetime.now()
    default_html_period = current_date.strftime('%Y-%m') # Format HTML Input: YYYY-MM
    
    # 2. Ambil Periode dari Request (jika ada filter)
    raw_period = request.args.get('period', default_html_period)
    
    # 3. Konversi Format Tanggal untuk Database
    # Database biasanya menyimpan MMYYYY (misal: 112025)
    # Input HTML mengirim YYYY-MM (misal: 2025-11)
    try:
        if '-' in raw_period:
            year, month = raw_period.split('-')
            formatted_period = f"{month}{year}" # 2025-11 -> 112025
        else:
            formatted_period = raw_period # Fallback jika format beda
    except ValueError:
        formatted_period = raw_period.replace('-', '')

    # 4. Ambil Statistik Lengkap dari Utils (Server-Side Rendering)
    stats = get_comprehensive_stats(formatted_period)

    return render_template('collection_summary.html', 
                           title="Laporan Piutang & Koleksi",
                           description="Ringkasan Kinerja Piutang, Koleksi, dan Analisis Rayon Terbaru.",
                           stats=stats,       # Data KPI, Grafik, Tabel dikirim langsung
                           period=raw_period, # Nilai untuk input date (YYYY-MM)
                           is_admin=current_user.is_admin)

# Menu 2: Analisis Piutang & Koleksi (Halaman Menu Analisis)
@bp_collection.route('/analisis', methods=['GET'])
@login_required 
def collection_analisis_view():
    return render_template('collection_analysis.html', 
                           title="Analisis Piutang & Koleksi",
                           description="Pilih laporan analisis Piutang, Kubikasi, dan perbandingan MoM berdasarkan dimensi pelanggan.",
                           is_admin=current_user.is_admin)

# Menu 3: Top List Piutang (Top Debtors Report)
@bp_collection.route('/top', methods=['GET'])
@login_required 
def collection_top_view():
    return render_template('analysis_report_template.html', 
                            title="Top List Piutang Terbesar",
                            description="Daftar 500 pelanggan dengan total nominal piutang aktif terbesar.",
                            report_type="TOP_DEBTORS",
                            is_admin=current_user.is_admin,
                            api_endpoint=url_for("bp_collection.top_debtors_report_api"))

# Menu 4: Riwayat Koleksi (MoM Comparison)
@bp_collection.route('/riwayat', methods=['GET'])
@login_required 
def collection_riwayat_view():
    return render_template('analysis_report_template.html', 
                            title="Riwayat Piutang Bulanan (MoM)",
                            description="Perbandingan Piutang Aktif dan Kubikasi antara bulan terbaru (M) dengan bulan sebelumnya (M-1) berdasarkan kriteria lengkap.",
                            report_type="MOM_COMPARISON",
                            is_admin=current_user.is_admin,
                            api_endpoint=url_for("bp_collection.mom_comparison_report_api"))

# Sub-Menu Perbandingan Koleksi (DoD Comparison)
@bp_collection.route('/dod_comparison', methods=['GET'])
@login_required 
def analysis_dod_comparison():
    return render_template('analysis_report_template.html', 
                            title="Perbandingan Koleksi Harian (DoD) Komprehensif",
                            description="Perbandingan Koleksi Nominal dan Jumlah Transaksi antara hari ini (D) dengan hari sebelumnya (D-1) berdasarkan kriteria lengkap: AB Sunter, Rayon, PCEZ, Tarif, Merek, Cycle, dan Lokasi Pembayaran.",
                            report_type="DOD_COMPARISON",
                            is_admin=current_user.is_admin,
                            api_endpoint=url_for("bp_collection.dod_comparison_report_api"))

# --- API REPORTING (Endpoint JSON untuk Grafik/Tabel Distribusi) ---

@bp_collection.route("/api/distribution/rayon_report")
@login_required
def rayon_distribution_report():
    db_status = get_db_status()
    if db_status['status'] == 'error': return jsonify({"message": db_status['message']}), 500
    collection_mc = db_status['collections']['mc']
    group_fields = ["RAYON"]
    results, latest_month = _get_distribution_report(group_fields=group_fields, collection_mc=collection_mc)
    schema = _generate_distribution_schema(group_fields)
    for item in results: item['chart_label'] = item.get("RAYON", "N/A"); item['chart_data_piutang'] = round(item['total_piutang'], 2)
    return jsonify({"data": results, "schema": schema, "title": f"Distribusi Pelanggan per Rayon", "subtitle": f"Data Piutang & Kubikasi per Bulan Tagihan Terbaru: {latest_month}"})

@bp_collection.route("/api/distribution/pcez_report")
@login_required
def pcez_distribution_report():
    db_status = get_db_status()
    if db_status['status'] == 'error': return jsonify({"message": db_status['message']}), 500
    collection_mc = db_status['collections']['mc']
    group_fields = ["PCEZ"]
    results, latest_month = _get_distribution_report(group_fields=group_fields, collection_mc=collection_mc)
    schema = _generate_distribution_schema(group_fields)
    for item in results: item['chart_label'] = item.get("PCEZ", "N/A"); item['chart_data_piutang'] = round(item['total_piutang'], 2)
    return jsonify({"data": results, "schema": schema, "title": f"Distribusi Pelanggan per PCEZ", "subtitle": f"Data Piutang & Kubikasi per Bulan Tagihan Terbaru: {latest_month}"})

@bp_collection.route("/api/distribution/rayon_tarif_report")
@login_required
def rayon_tarif_distribution_report():
    db_status = get_db_status()
    if db_status['status'] == 'error': return jsonify({"message": db_status['message']}), 500
    collection_mc = db_status['collections']['mc']
    group_fields = ["RAYON", "TARIF"]
    results, latest_month = _get_distribution_report(group_fields=group_fields, collection_mc=collection_mc)
    schema = _generate_distribution_schema(group_fields)
    for item in results: item['chart_label'] = f"{item.get('RAYON', 'N/A')} - {item.get('TARIF', 'N/A')}"; item['chart_data_piutang'] = round(item['total_piutang'], 2)
    return jsonify({"data": results, "schema": schema, "title": f"Distribusi Pelanggan per Rayon / Tarif", "subtitle": f"Data Piutang & Kubikasi per Bulan Tagihan Terbaru: {latest_month}"})

@bp_collection.route("/api/distribution/rayon_meter_report")
@login_required
def rayon_meter_distribution_report():
    db_status = get_db_status()
    if db_status['status'] == 'error': return jsonify({"message": db_status['message']}), 500
    collection_mc = db_status['collections']['mc']
    group_fields = ["RAYON", "JENIS_METER"]
    results, latest_month = _get_distribution_report(group_fields=group_fields, collection_mc=collection_mc)
    schema = _generate_distribution_schema(group_fields)
    for item in results: item['chart_label'] = f"{item.get('RAYON', 'N/A')} - {item.get('JENIS_METER', 'N/A')}"; item['chart_data_piutang'] = round(item['total_piutang'], 2)
    return jsonify({"data": results, "schema": schema, "title": f"Distribusi Pelanggan per Rayon / Jenis Meter", "subtitle": f"Data Piutang & Kubikasi per Bulan Tagihan Terbaru: {latest_month}"})

# --- API REPORTING (KOMPARASI & TOP LIST) ---

# API Report: Top Debtors
@bp_collection.route('/api/top_debtors_report', methods=['GET'])
@login_required 
def top_debtors_report_api():
    db_status = get_db_status()
    if db_status['status'] == 'error':
        return jsonify({"message": db_status['message']}), 500
    
    collections = db_status['collections']
    collection_mc = collections['mc']

    try:
        pipeline = [
            {'$match': {'STATUS': {'$ne': 'PAYMENT'}, 'NOMINAL': {'$gt': 0}}},
            {'$project': {'NOMEN': 1, 'RAYON': 1, 'BULAN_TAGIHAN': 1, 'NOMINAL': {"$toDouble": {'$cond': [{'$ne': ['$NOMINAL', None]}, '$NOMINAL', 0]}}}},
            {'$group': {
                '_id': '$NOMEN',
                'TotalPiutangAktif': {'$sum': '$NOMINAL'},
                'JumlahBulanTunggakan': {'$sum': 1},
                'RayonMC': {'$first': '$RAYON'},
                'TagihanTerbaru': {'$max': '$BULAN_TAGIHAN'}
            }},
            {'$match': {'TotalPiutangAktif': {'$gt': 0}}},
            {'$lookup': { 'from': 'CustomerData', 'localField': '_id', 'foreignField': 'NOMEN', 'as': 'customer_info' }},
            {'$unwind': {'path': '$customer_info', 'preserveNullAndEmptyArrays': True}},
            {'$project': {
                '_id': 0, 'NOMEN': '$_id',
                'NAMA': {'$cond': [{'$ne': ['$customer_info.NAMA', None]}, '$customer_info.NAMA', 'N/A']},
                'ALAMAT': {'$cond': [{'$ne': ['$customer_info.ALAMAT', None]}, '$customer_info.ALAMAT', 'N/A']},
                'RAYON': {'$cond': [{'$ne': ['$customer_info.RAYON', None]}, '$customer_info.RAYON', '$RayonMC']},
                'TotalPiutang': {'$round': ['$TotalPiutangAktif', 0]},
                'BulanTunggakan': '$JumlahBulanTunggakan',
                'TagihanTerbaru': '$TagihanTerbaru'
            }},
            {'$sort': {'TotalPiutang': -1}},
            {'$limit': 500}
        ]

        top_debtors_data = list(collection_mc.aggregate(pipeline, allowDiskUse=True))
        
        top_debtors_schema = [
            {'key': 'NOMEN', 'label': 'No. Pelanggan', 'type': 'string', 'is_main_key': True},
            {'key': 'NAMA', 'label': 'Nama Pelanggan', 'type': 'string'},
            {'key': 'RAYON', 'label': 'Rayon', 'type': 'string'},
            {'key': 'ALAMAT', 'label': 'Alamat', 'type': 'string'},
            {'key': 'BulanTunggakan', 'label': 'Jumlah Bulan Tunggakan', 'type': 'integer', 'unit': 'bln'},
            {'key': 'TotalPiutang', 'label': 'Total Piutang Aktif (Rp)', 'type': 'currency', 'chart_key': 'TotalPiutang'},
            {'key': 'TagihanTerbaru', 'label': 'Bulan Tagihan Terbaru', 'type': 'string'},
        ]

        for item in top_debtors_data:
            item['chart_label'] = f"{item['NOMEN']} ({item['RAYON']})"
            item['chart_data_piutang'] = item['TotalPiutang']
            
        return jsonify({'status': 'success', 'data': top_debtors_data, 'schema': top_debtors_schema}), 200

    except Exception as e:
        print(f"Error saat membuat laporan top debtors: {e}")
        return jsonify({"status": 'error', "message": f"Gagal mengambil laporan Top Debtors: {e}"}), 500

# API Report: MoM Comparison
@bp_collection.route('/api/mom_comparison_report', methods=['GET'])
@login_required
def mom_comparison_report_api():
    db_status = get_db_status()
    if db_status['status'] == 'error':
        return jsonify({"message": db_status['message']}), 500
    
    collections = db_status['collections']
    collection_mc = collections['mc']
    collection_cid = collections['cid']

    try:
        latest_mc_month_doc = collection_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
        M_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None
        
        if not M_month: return jsonify({'status': 'error', 'message': 'Tidak ada data Master Cetak (MC) ditemukan.', 'data': []}), 404

        M_minus_1_month = _get_previous_month_year(M_month)
        if not M_minus_1_month: return jsonify({'status': 'error', 'message': f'Tidak dapat menghitung bulan sebelum {M_month}.', 'data': []}), 404

        pipeline_mc = [
            {'$match': {'BULAN_TAGIHAN': {'$in': [M_month, M_minus_1_month]}}},
            {'$project': {
                'NOMEN': 1, 'RAYON': 1, 'PCEZ': 1, 'TARIF': 1, 'BULAN_TAGIHAN': 1,
                'NOMINAL': {'$toDouble': {'$cond': [{'$ne': ['$NOMINAL', None]}, '$NOMINAL', 0]}},
                'KUBIK': {'$toDouble': {'$cond': [{'$ne': ['$KUBIK', None]}, '$KUBIK', 0]}}, 'STATUS': 1
            }},
            {'$lookup': { 'from': 'CustomerData', 'localField': 'NOMEN', 'foreignField': 'NOMEN', 'as': 'customer_info' }},
            {'$unwind': {'path': '$customer_info', 'preserveNullAndEmptyArrays': True}},
            {'$project': {
                '_id': 0, 'NOMEN': 1, 'RAYON': 1, 'PCEZ': 1, 'TARIF': 1, 'BULAN_TAGIHAN': 1, 'NOMINAL': 1, 'KUBIK': 1,
                'MERK': {'$cond': [{'$ne': ['$customer_info.MERK', None]}, '$customer_info.MERK', 'N/A']},
                'BOOKWALK': {'$cond': [{'$ne': ['$customer_info.BOOKWALK', None]}, '$customer_info.BOOKWALK', 'N/A']},
                'AB_SUNTER': {'$cond': [{'$in': ['$RAYON', ['34', '35']]}, 'AB SUNTER', 'LUAR AB SUNTER']}, 
                'NOMINAL_AKTIF': {'$cond': [{'$eq': ['$STATUS', 'PAYMENT']}, 0, '$NOMINAL']},
                'KUBIK_AKTIF': {'$cond': [{'$eq': ['$STATUS', 'PAYMENT']}, 0, '$KUBIK']}
            }},
            {'$group': {
                '_id': {'AB_SUNTER': '$AB_SUNTER', 'RAYON': '$RAYON', 'PCEZ': '$PCEZ', 'TARIF': '$TARIF', 'MERK': '$MERK', 'CYCLE': '$BOOKWALK', 'BULAN_TAGIHAN': '$BULAN_TAGIHAN'},
                'TotalNomen': {'$addToSet': '$NOMEN'},
                'PiutangAktif': {'$sum': '$NOMINAL_AKTIF'},
                'KubikAktif': {'$sum': '$KUBIK_AKTIF'}
            }},
            {'$project': {
                '_id': 0, 'AB_SUNTER': '$_id.AB_SUNTER', 'RAYON': '$_id.RAYON', 'PCEZ': '$_id.PCEZ', 'TARIF': '$_id.TARIF', 
                'MERK': '$_id.MERK', 'CYCLE': '$_id.CYCLE', 'BULAN_TAGIHAN': '$_id.BULAN_TAGIHAN',
                'TotalNomen': {'$size': '$TotalNomen'},
                'PiutangAktif': {'$round': ['$PiutangAktif', 0]},
                'KubikAktif': {'$round': ['$KubikAktif', 0]},
            }},
            {'$group': {
                '_id': {'AB_SUNTER': '$AB_SUNTER', 'RAYON': '$RAYON', 'PCEZ': '$PCEZ', 'TARIF': '$TARIF', 'MERK': '$MERK', 'CYCLE': '$CYCLE'},
                f'Nomen_M': {'$sum': {'$cond': [{'$eq': ['$BULAN_TAGIHAN', M_month]}, '$TotalNomen', 0]}},
                f'Nomen_M_1': {'$sum': {'$cond': [{'$eq': ['$BULAN_TAGIHAN', M_minus_1_month]}, '$TotalNomen', 0]}},
                f'Piutang_M': {'$sum': {'$cond': [{'$eq': ['$BULAN_TAGIHAN', M_month]}, '$PiutangAktif', 0]}},
                f'Piutang_M_1': {'$sum': {'$cond': [{'$eq': ['$BULAN_TAGIHAN', M_minus_1_month]}, '$PiutangAktif', 0]}},
                f'Kubik_M': {'$sum': {'$cond': [{'$eq': ['$BULAN_TAGIHAN', M_month]}, '$KubikAktif', 0]}},
                f'Kubik_M_1': {'$sum': {'$cond': [{'$eq': ['$BULAN_TAGIHAN', M_minus_1_month]}, '$KubikAktif', 0]}},
            }},
            {'$project': {
                '_id': 0,
                'AB_SUNTER': '$_id.AB_SUNTER', 'RAYON': '$_id.RAYON', 'PCEZ': '$_id.PCEZ', 'TARIF': '$_id.TARIF',
                'MERK': '$_id.MERK', 'CYCLE': '$_id.CYCLE',
                f'Nomen_{M_month}': '$Nomen_M', f'Nomen_{M_minus_1_month}': '$Nomen_M_1',
                f'Piutang_{M_month}': '$Piutang_M', f'Piutang_{M_minus_1_month}': '$Piutang_M_1',
                f'Kubik_{M_month}': '$Kubik_M', f'Kubik_{M_minus_1_month}': '$Kubik_M_1',
                'Piutang_MoM_Pct': {
                    '$cond': {
                        'if': {'$gt': ['$Piutang_M_1', 0]},
                        'then': {'$round': [{'$multiply': [{'$divide': [{'$subtract': ['$Piutang_M', '$Piutang_M_1']}, '$Piutang_M_1']}, 100]}, 2]},
                        'else': {'$cond': [{'$gt': ['$Piutang_M', 0]}, 100, 0]} 
                    }
                },
                'Kubik_MoM_Pct': {
                    '$cond': {
                        'if': {'$gt': ['$Kubik_M_1', 0]},
                        'then': {'$round': [{'$multiply': [{'$divide': [{'$subtract': ['$Kubik_M', '$Kubik_M_1']}, '$Kubik_M_1']}, 100]}, 2]},
                        'else': {'$cond': [{'$gt': ['$Kubik_M', 0]}, 100, 0]}
                    }
                }
            }},
            {'$sort': {'Piutang_MoM_Pct': -1}} 
        ]

        mom_data = list(collection_mc.aggregate(pipeline_mc, allowDiskUse=True))
        
        mom_schema = [
            {'key': 'AB_SUNTER', 'label': 'AB Sunter', 'type': 'string', 'is_main_key': True},
            {'key': 'RAYON', 'label': 'Rayon', 'type': 'string'},
            {'key': 'PCEZ', 'label': 'PCEZ', 'type': 'string'},
            {'key': 'TARIF', 'label': 'Tarif', 'type': 'string'},
            {'key': 'MERK', 'label': 'Merek Meter', 'type': 'string'},
            {'key': 'CYCLE', 'label': 'Cycle/Bookwalk', 'type': 'string'},
            
            {'key': f'Piutang_{M_month}', 'label': f'Piutang M ({M_month})', 'type': 'currency', 'chart_key': 'chart_piutang_m'},
            {'key': f'Piutang_{M_minus_1_month}', 'label': f'Piutang M-1 ({M_minus_1_month})', 'type': 'currency', 'chart_key': 'chart_piutang_m_1'},
            {'key': 'Piutang_MoM_Pct', 'label': 'Piutang MoM (%)', 'type': 'percent'},
            
            {'key': f'Kubik_{M_month}', 'label': f'Kubikasi M ({M_month})', 'type': 'integer', 'unit': 'm³'},
            {'key': f'Kubik_{M_minus_1_month}', 'label': f'Kubikasi M-1 ({M_minus_1_month})', 'type': 'integer', 'unit': 'm³'},
            {'key': 'Kubik_MoM_Pct', 'label': 'Kubikasi MoM (%)', 'type': 'percent'},
            
            {'key': f'Nomen_{M_month}', 'label': f'Nomen M ({M_month})', 'type': 'integer'},
            {'key': f'Nomen_{M_minus_1_month}', 'label': f'Nomen M-1 ({M_minus_1_month})', 'type': 'integer'},
        ]
        
        for item in mom_data:
            item['chart_label'] = f"R:{item['RAYON']} T:{item['TARIF']}"

        return jsonify({
            'status': 'success', 
            'data': mom_data, 
            'schema': mom_schema,
            'title': f'Perbandingan Piutang & Kubikasi MoM ({M_month} vs {M_minus_1_month})',
            'latest_month': M_month,
            'previous_month': M_minus_1_month,
        }), 200

    except Exception as e:
        print(f"Error saat membuat laporan MoM Comparison: {e}")
        return jsonify({"status": 'error', "message": f"Gagal mengambil laporan MoM: {e}"}), 500

# API Report: DoD Comparison
@bp_collection.route('/api/dod_comparison_report', methods=['GET'])
@login_required
def dod_comparison_report_api():
    db_status = get_db_status()
    if db_status['status'] == 'error':
        return jsonify({"message": db_status['message']}), 500
    
    collections = db_status['collections']
    collection_mb = collections['mb']
    collection_cid = collections['cid']

    try:
        D_date = _get_day_n_ago(0)
        D_minus_1_date = _get_day_n_ago(1)
        
        pipeline_mb = [
            {'$match': {'TGL_BAYAR': {'$in': [D_date, D_minus_1_date]}, 'BILL_REASON': 'BIAYA PEMAKAIAN AIR'}},
            {'$lookup': {'from': 'CustomerData', 'localField': 'NOMEN', 'foreignField': 'NOMEN', 'as': 'customer_info'}},
            {'$unwind': {'path': '$customer_info', 'preserveNullAndEmptyArrays': True}},
            {'$project': {
                'TGL_BAYAR': 1, 'LKS_BAYAR': 1, 
                'RAYON': {'$cond': [{'$ne': ['$customer_info.RAYON', None]}, '$customer_info.RAYON', '$RAYON']},
                'PCEZ': {'$cond': [{'$ne': ['$customer_info.PCEZ', None]}, '$customer_info.PCEZ', '$PCEZ']}, 
                'TARIF': {'$cond': [{'$ne': ['$customer_info.TARIF', None]}, '$customer_info.TARIF', 'N/A']},
                'MERK': {'$cond': [{'$ne': ['$customer_info.MERK', None]}, '$customer_info.MERK', 'N/A']},
                'CYCLE': {'$cond': [{'$ne': ['$customer_info.BOOKWALK', None]}, '$customer_info.BOOKWALK', 'N/A']},
                'AB_SUNTER': {'$cond': [{'$in': ['$RAYON', ['34', '35']]}, 'AB SUNTER', 'LUAR AB SUNTER']}, 
                'NOMINAL': {'$toDouble': {'$cond': [{'$ne': ['$NOMINAL', None]}, '$NOMINAL', 0]}},
                'TRANSAKSI': 1
            }},
            {'$group': {
                '_id': {'AB_SUNTER': '$AB_SUNTER', 'RAYON': '$RAYON', 'PCEZ': '$PCEZ', 'TARIF': '$TARIF', 'MERK': '$MERK', 'CYCLE': '$CYCLE', 'LKS_BAYAR': '$LKS_BAYAR', 'TGL_BAYAR': '$TGL_BAYAR'},
                'TotalKoleksi': {'$sum': '$NOMINAL'},
                'JumlahTransaksi': {'$sum': 1}
            }},
            {'$project': {
                '_id': 0, 'AB_SUNTER': '$_id.AB_SUNTER', 'RAYON': '$_id.RAYON', 'PCEZ': '$_id.PCEZ', 
                'TARIF': '$_id.TARIF', 'MERK': '$_id.MERK', 'CYCLE': '$_id.CYCLE',
                'LKS_BAYAR': '$_id.LKS_BAYAR',
                'TGL_BAYAR': '$_id.TGL_BAYAR', 'TotalKoleksi': {'$round': ['$TotalKoleksi', 0]}, 'JumlahTransaksi': '$JumlahTransaksi',
            }},
            {'$group': {
                '_id': {'AB_SUNTER': '$AB_SUNTER', 'RAYON': '$RAYON', 'PCEZ': '$PCEZ', 'TARIF': '$TARIF', 'MERK': '$MERK', 'CYCLE': '$CYCLE', 'LKS_BAYAR': '$LKS_BAYAR'},
                f'Koleksi_D': {'$sum': {'$cond': [{'$eq': ['$TGL_BAYAR', D_date]}, '$TotalKoleksi', 0]}},
                f'Koleksi_D_1': {'$sum': {'$cond': [{'$eq': ['$TGL_BAYAR', D_minus_1_date]}, '$TotalKoleksi', 0]}},
                f'Transaksi_D': {'$sum': {'$cond': [{'$eq': ['$TGL_BAYAR', D_date]}, '$JumlahTransaksi', 0]}},
                f'Transaksi_D_1': {'$sum': {'$cond': [{'$eq': ['$TGL_BAYAR', D_minus_1_date]}, '$JumlahTransaksi', 0]}},
            }},
            {'$project': {
                '_id': 0,
                'AB_SUNTER': '$_id.AB_SUNTER', 'RAYON': '$_id.RAYON', 'PCEZ': '$_id.PCEZ', 
                'TARIF': '$_id.TARIF', 'MERK': '$_id.MERK', 'CYCLE': '$_id.CYCLE', 'LKS_BAYAR': '$_id.LKS_BAYAR',
                f'Koleksi_{D_date}': '$Koleksi_D', f'Koleksi_{D_minus_1_date}': '$Koleksi_D_1',
                f'Transaksi_{D_date}': '$Transaksi_D', f'Transaksi_{D_minus_1_date}': '$Transaksi_D_1',
                'Koleksi_DoD_Pct': {
                    '$cond': {
                        'if': {'$gt': ['$Koleksi_D_1', 0]},
                        'then': {'$round': [{'$multiply': [{'$divide': [{'$subtract': ['$Koleksi_D', '$Koleksi_D_1']}, '$Koleksi_D_1']}, 100]}, 2]},
                        'else': {'$cond': [{'$gt': ['$Koleksi_D', 0]}, 100, 0]} 
                    }
                },
                'Transaksi_DoD_Pct': {
                    '$cond': {
                        'if': {'$gt': ['$Transaksi_D_1', 0]},
                        'then': {'$round': [{'$multiply': [{'$divide': [{'$subtract': ['$Transaksi_D', '$Transaksi_D_1']}, '$Transaksi_D_1']}, 100]}, 2]},
                        'else': {'$cond': [{'$gt': ['$Transaksi_D', 0]}, 100, 0]}
                    }
                }
            }},
            {'$sort': {'Koleksi_DoD_Pct': -1}} 
        ]

        dod_data = list(collection_mb.aggregate(pipeline_mb, allowDiskUse=True))
        
        dod_schema = [
            {'key': 'AB_SUNTER', 'label': 'AB Sunter', 'type': 'string', 'is_main_key': True},
            {'key': 'RAYON', 'label': 'Rayon', 'type': 'string'},
            {'key': 'PCEZ', 'label': 'PCEZ', 'type': 'string'},
            {'key': 'TARIF', 'label': 'Tarif', 'type': 'string'},
            {'key': 'MERK', 'label': 'Merek Meter', 'type': 'string'},
            {'key': 'CYCLE', 'label': 'Cycle/Bookwalk', 'type': 'string'},
            {'key': 'LKS_BAYAR', 'label': 'Lokasi Pembayaran', 'type': 'string'},
            
            {'key': f'Koleksi_{D_date}', 'label': f'Koleksi Hari Ini ({D_date})', 'type': 'currency', 'chart_key': 'chart_koleksi_d'},
            {'key': f'Koleksi_{D_minus_1_date}', 'label': f'Koleksi Kemarin ({D_minus_1_date})', 'type': 'currency', 'chart_key': 'chart_koleksi_d_1'},
            {'key': 'Koleksi_DoD_Pct', 'label': 'DoD Koleksi (%)', 'type': 'percent'},
            
            {'key': f'Transaksi_{D_date}', 'label': f'Transaksi Hari Ini', 'type': 'integer'},
            {'key': f'Transaksi_{D_minus_1_date}', 'label': f'Transaksi Kemarin', 'type': 'integer'},
            {'key': 'Transaksi_DoD_Pct', 'label': 'DoD Transaksi (%)', 'type': 'percent'},
        ]
        
        for item in dod_data:
            item['chart_label'] = f"R:{item['RAYON']} L:{item['LKS_BAYAR']}"

        return jsonify({
            'status': 'success', 
            'data': dod_data, 
            'schema': dod_schema,
            'title': f'Perbandingan Koleksi Harian (DoD) ({D_date} vs {D_minus_1_date})',
            'latest_day': D_date,
            'previous_day': D_minus_1_date,
        }), 200

    except Exception as e:
        print(f"Error saat membuat laporan DoD Comparison: {e}")
        return jsonify({"status": 'error', "message": f"Gagal mengambil laporan DoD: {e}"}), 500
