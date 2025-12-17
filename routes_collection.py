from flask import Blueprint, request, jsonify, render_template, url_for, flash, redirect
from flask_login import login_required, current_user
from functools import wraps
from datetime import datetime
from utils import (
    get_db_status, 
    _get_previous_month_year, 
    _get_day_n_ago, 
    _generate_distribution_schema, 
    get_comprehensive_stats
)

# Inisialisasi Blueprint
bp_collection = Blueprint('bp_collection', __name__, url_prefix='/collection')

# --- Middleware Dekorator Admin ---
def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.is_admin:
            flash('Hanya Admin yang dapat mengakses fitur ini.', 'danger')
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function

# --- RUTE VIEW FRONTEND ---

@bp_collection.route('/laporan', methods=['GET'])
@login_required 
def collection_laporan_view():
    """
    Dashboard Utama: Dioptimalkan agar hanya memuat angka ringkasan (KPI).
    Data tabel detail akan dimuat via API (AJAX) di sisi klien.
    """
    raw_period = request.args.get('period', datetime.now().strftime('%Y-%m'))
    
    try:
        # Konversi format YYYY-MM ke MMYYYY untuk query DB
        y, m = raw_period.split('-')
        formatted_period = f"{m}{y}"
    except:
        formatted_period = raw_period.replace('-', '')

    # get_comprehensive_stats kini hanya mengambil data agregat cepat
    stats = get_comprehensive_stats(formatted_period)

    return render_template('collection_summary.html', 
                           title="Laporan Piutang & Koleksi",
                           description="Ringkasan kinerja bulanan dan distribusi piutang pelanggan.",
                           stats=stats,
                           period=raw_period,
                           is_admin=current_user.is_admin)

@bp_collection.route('/analisis', methods=['GET'])
@login_required 
def collection_analisis_view():
    """Menu pusat untuk berbagai jenis analisis mendalam."""
    return render_template('collection_analysis.html', 
                           title="Analisis Piutang & Koleksi",
                           is_admin=current_user.is_admin)

@bp_collection.route('/top', methods=['GET'])
@login_required 
def collection_top_view():
    """Halaman Laporan 500 Tunggakan Terbesar."""
    return render_template('analysis_report_template.html', 
                            title="Top 500 Piutang Terbesar",
                            report_type="TOP_DEBTORS",
                            api_endpoint=url_for("bp_collection.top_debtors_report_api"),
                            is_admin=current_user.is_admin)

@bp_collection.route('/riwayat', methods=['GET'])
@login_required 
def collection_riwayat_view():
    """Halaman Perbandingan Month-over-Month (MoM)."""
    return render_template('analysis_report_template.html', 
                            title="Riwayat Piutang MoM",
                            report_type="MOM_COMPARISON",
                            api_endpoint=url_for("bp_collection.mom_comparison_report_api"),
                            is_admin=current_user.is_admin)

@bp_collection.route('/dod_comparison', methods=['GET'])
@login_required 
def analysis_dod_comparison():
    """Halaman Perbandingan Koleksi Harian (DoD)."""
    return render_template('analysis_report_template.html', 
                            title="Perbandingan Koleksi Harian (DoD)",
                            report_type="DOD_COMPARISON",
                            api_endpoint=url_for("bp_collection.dod_comparison_report_api"),
                            is_admin=current_user.is_admin)

# --- API ENDPOINTS (Proses Berat Dipisahkan ke Sini) ---

@bp_collection.route("/api/distribution/rayon_report")
@login_required
def rayon_distribution_report():
    """API untuk memuat data distribusi per Rayon secara asinkron."""
    db_status = get_db_status()
    if db_status['status'] == 'error': return jsonify({"message": "DB Offline"}), 500
    
    mc = db_status['collections']['mc']
    latest = mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
    month = latest.get('BULAN_TAGIHAN') if latest else ""

    # Pipeline teroptimasi: Grouping langsung di DB
    pipeline = [
        {"$match": {"BULAN_TAGIHAN": month}},
        {"$group": {
            "_id": "$RAYON",
            "unique_nomen": {"$addToSet": "$NOMEN"},
            "nominal": {"$sum": {"$toDouble": "$NOMINAL"}}
        }},
        {"$project": {
            "RAYON": "$_id",
            "total_nomen": {"$size": "$unique_nomen"},
            "total_piutang": "$nominal",
            "_id": 0
        }},
        {"$sort": {"total_piutang": -1}},
        {"$limit": 50} # Batasi jumlah baris agar rendering browser cepat
    ]
    
    data = list(mc.aggregate(pipeline))
    return jsonify({
        "data": data, 
        "title": f"Distribusi Piutang per Rayon",
        "subtitle": f"Periode: {month}"
    })

@bp_collection.route('/api/top_debtors_report', methods=['GET'])
@login_required 
def top_debtors_report_api():
    """API untuk mengambil daftar penunggak terbesar secara efisien."""
    db_status = get_db_status()
    if db_status['status'] == 'error': return jsonify({"message": "DB Offline"}), 500
    
    mc = db_status['collections']['mc']
    try:
        pipeline = [
            # Hanya ambil yang belum bayar dan nominal positif
            {'$match': {'STATUS': {'$ne': 'PAYMENT'}, 'NOMINAL': {'$gt': 0}}},
            {'$group': {
                '_id': '$NOMEN',
                'TotalPiutang': {'$sum': {'$toDouble': '$NOMINAL'}},
                'BulanTunggakan': {'$sum': 1},
                'Rayon': {'$first': '$RAYON'}
            }},
            {'$sort': {'TotalPiutang': -1}},
            {'$limit': 500}
        ]
        data = list(mc.aggregate(pipeline, allowDiskUse=True))
        return jsonify({'status': 'success', 'data': data})
    except Exception as e:
        return jsonify({"status": 'error', "message": str(e)}), 500

@bp_collection.route('/api/mom_comparison_report', methods=['GET'])
@login_required
def mom_comparison_report_api():
    """Analisis perbandingan bulan ke bulan (MoM)."""
    db_status = get_db_status()
    if db_status['status'] == 'error': return jsonify({"message": "DB Offline"}), 500
    
    mc = db_status['collections']['mc']
    try:
        latest = mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
        M = latest.get('BULAN_TAGIHAN') if latest else None
        M_1 = _get_previous_month_year(M)
        
        if not M or not M_1: return jsonify({'status': 'error', 'message': 'Periode tidak lengkap'}), 404

        pipeline = [
            {'$match': {'BULAN_TAGIHAN': {'$in': [M, M_1]}}},
            {'$group': {
                '_id': {'R': '$RAYON', 'B': '$BULAN_TAGIHAN'},
                'Nominal': {'$sum': {'$toDouble': '$NOMINAL'}},
                'NomenSet': {'$addToSet': '$NOMEN'}
            }},
            {'$group': {
                '_id': '$_id.R',
                'NomM': {'$sum': {'$cond': [{'$eq': ['$_id.B', M]}, '$Nominal', 0]}},
                'NomM1': {'$sum': {'$cond': [{'$eq': ['$_id.B', M_1]}, '$Nominal', 0]}},
                'CountM': {'$sum': {'$cond': [{'$eq': ['$_id.B', M]}, {'$size': '$NomenSet'}, 0]}},
                'CountM1': {'$sum': {'$cond': [{'$eq': ['$_id.B', M_1]}, {'$size': '$NomenSet'}, 0]}},
            }},
            {'$project': {
                'RAYON': '$_id', '_id': 0,
                'Nominal_M': '$NomM', 'Nominal_M1': '$NomM1',
                'Nomen_M': '$CountM', 'Nomen_M1': '$CountM1',
                'Change_Pct': {'$cond': [{'$gt': ['$NomM1', 0]}, {'$round': [{'$multiply': [{'$divide': [{'$subtract': ['$NomM', '$NomM1']}, '$NomM1']}, 100]}, 2]}, 0]}
            }},
            {'$sort': {'Change_Pct': -1}}
        ]
        res = list(mc.aggregate(pipeline))
        return jsonify({'status': 'success', 'data': res})
    except Exception as e:
        return jsonify({"status": 'error', "message": str(e)}), 500

@bp_collection.route('/api/dod_comparison_report', methods=['GET'])
@login_required
def dod_comparison_report_api():
    """Analisis perbandingan harian (DoD) untuk koleksi."""
    db_status = get_db_status()
    if db_status['status'] == 'error': return jsonify({"message": "DB Offline"}), 500
    
    mb = db_status['collections']['mb']
    try:
        D = _get_day_n_ago(0)
        D1 = _get_day_n_ago(1)
        
        pipeline = [
            {'$match': {'TGL_BAYAR': {'$in': [D, D1]}, 'BILL_REASON': 'BIAYA PEMAKAIAN AIR'}},
            {'$group': {
                '_id': {'R': '$RAYON', 'T': '$TGL_BAYAR'},
                'Koleksi': {'$sum': {'$toDouble': '$NOMINAL'}},
                'Count': {'$sum': 1}
            }},
            {'$group': {
                '_id': '$_id.R',
                'KD': {'$sum': {'$cond': [{'$eq': ['$_id.T', D]}, '$Koleksi', 0]}},
                'KD1': {'$sum': {'$cond': [{'$eq': ['$_id.T', D1]}, '$Koleksi', 0]}},
                'TD': {'$sum': {'$cond': [{'$eq': ['$_id.T', D]}, '$Count', 0]}},
                'TD1': {'$sum': {'$cond': [{'$eq': ['$_id.T', D1]}, '$Count', 0]}},
            }},
            {'$project': {
                'RAYON': '$_id', '_id': 0,
                'Koleksi_D': '$KD', 'Koleksi_D1': '$KD1',
                'Transaksi_D': '$TD', 'Transaksi_D1': '$TD1',
                'Change_Pct': {'$cond': [{'$gt': ['$KD1', 0]}, {'$round': [{'$multiply': [{'$divide': [{'$subtract': ['$KD', '$KD1']}, '$KD1']}, 100]}, 2]}, 0]}
            }},
            {'$sort': {'Change_Pct': -1}}
        ]
        res = list(mb.aggregate(pipeline))
        return jsonify({'status': 'success', 'data': res})
    except Exception as e:
        return jsonify({"status": 'error', "message": str(e)}), 500
