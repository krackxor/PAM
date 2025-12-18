from flask import Blueprint, request, jsonify, render_template, url_for, flash, redirect, Response
from flask_login import login_required, current_user
from functools import wraps
from datetime import datetime
import pandas as pd
import io
# Import helpers dari utils
from utils import get_db_status, _get_previous_month_year, _get_day_n_ago, _generate_distribution_schema, get_comprehensive_stats

# Definisikan Blueprint
bp_collection = Blueprint('bp_collection', __name__, url_prefix='/collection')

# --- Middleware Dekorator Admin ---
def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.is_admin:
            flash('Anda tidak memiliki izin (Admin) untuk mengakses halaman ini.', 'danger')
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function

# --- HELPER INTERNAL UNTUK DISTRIBUSI (Dioptimalkan dengan Limit) ---
def _get_distribution_report(group_fields, collection_mc, limit=None):
    """Menghitung distribusi metrik dengan limit opsional untuk performa UI."""
    db_status = get_db_status()
    if db_status['status'] == 'error':
        return [], "N/A"
        
    if isinstance(group_fields, str):
        group_fields = [group_fields]

    latest_mc_month_doc = collection_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
    latest_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None
    
    if not latest_month:
        return [], "N/A"

    pipeline = [
        {"$match": {"BULAN_TAGIHAN": latest_month}},
        {'$lookup': {
            'from': 'CustomerData',
            'localField': 'NOMEN',
            'foreignField': 'NOMEN',
            'as': 'cust_info'
        }},
        {'$unwind': {'path': '$cust_info', 'preserveNullAndEmptyArrays': True}},
        {"$project": {
            **{field: {"$ifNull": [f"${field}", f"$cust_info.{field}", "N/A"]} for field in group_fields},
            "NOMEN": 1,
            "NOMINAL": {"$toDouble": {'$ifNull': ['$NOMINAL', 0]}}, 
            "KUBIK": {"$toDouble": {'$ifNull': ['$KUBIK', 0]}},
        }},
        {"$group": {
            "_id": {field: f"${field}" for field in group_fields},
            "unique_nomen_set": {"$addToSet": "$NOMEN"},
            "total_piutang": {"$sum": "$NOMINAL"},
            "total_kubikasi": {"$sum": "$KUBIK"}
        }},
        {"$project": {
            **{field: f"$_id.{field}" for field in group_fields},
            "_id": 0,
            "total_nomen": {"$size": "$unique_nomen_set"},
            "total_piutang": 1,
            "total_kubikasi": 1
        }},
        {"$sort": {"total_piutang": -1}}
    ]

    if limit:
        pipeline.append({"$limit": limit})

    try:
        results = list(collection_mc.aggregate(pipeline, allowDiskUse=True))
    except Exception as e:
        print(f"Error aggregation: {e}")
        return [], latest_month

    return results, latest_month
    
# --- RUTE VIEW FRONTEND ---

@bp_collection.route('/laporan', methods=['GET'])
@login_required 
def collection_laporan_view():
    current_date = datetime.now()
    default_html_period = current_date.strftime('%Y-%m')
    raw_period = request.args.get('period', default_html_period)
    
    try:
        if '-' in raw_period:
            year, month = raw_period.split('-')
            formatted_period = f"{month}{year}"
        else:
            formatted_period = raw_period
    except:
        formatted_period = raw_period.replace('-', '')

    stats = get_comprehensive_stats(formatted_period)

    return render_template('collection_summary.html', 
                           title="Laporan Piutang & Koleksi",
                           stats=stats,
                           period=raw_period,
                           is_admin=current_user.is_admin)

@bp_collection.route('/analisis', methods=['GET'])
@login_required 
def collection_analisis_view():
    return render_template('collection_analysis.html', 
                           title="Analisis Kontributor Piutang",
                           is_admin=current_user.is_admin)

@bp_collection.route('/top', methods=['GET'])
@login_required 
def collection_top_view():
    return render_template('analysis_report_template.html', 
                            title="Top List Piutang Terbesar",
                            report_type="TOP_DEBTORS",
                            is_admin=current_user.is_admin,
                            api_endpoint=url_for("bp_collection.top_debtors_report_api"))

@bp_collection.route('/riwayat', methods=['GET'])
@login_required 
def collection_riwayat_view():
    return render_template('analysis_report_template.html', 
                            title="Riwayat Piutang Bulanan (MoM)",
                            report_type="MOM_COMPARISON",
                            is_admin=current_user.is_admin,
                            api_endpoint=url_for("bp_collection.mom_comparison_report_api"))

@bp_collection.route('/dod_comparison', methods=['GET'])
@login_required 
def analysis_dod_comparison():
    return render_template('analysis_report_template.html', 
                            title="Perbandingan Koleksi Harian (DoD)",
                            report_type="DOD_COMPARISON",
                            is_admin=current_user.is_admin,
                            api_endpoint=url_for("bp_collection.dod_comparison_report_api"))

# --- API DISTRIBUTION ENDPOINTS (DENGAN TOP 5 LIMIT) ---

@bp_collection.route("/api/distribution/<category>")
@login_required
def category_distribution_api(category):
    cat_map = {"rayon": "RAYON", "pc": "PC", "pcez": "PCEZ", "tarif": "TARIF", "merk": "MERK_METER"}
    field = cat_map.get(category.lower())
    if not field: return jsonify({"message": "Kategori tidak valid"}), 400

    db_status = get_db_status()
    if db_status['status'] == 'error': return jsonify({"message": db_status['message']}), 500
    
    # UI Hanya butuh Top 5 agar cepat
    results, latest_month = _get_distribution_report([field], db_status['collections']['mc'], limit=5)
    for item in results: 
        item['chart_label'] = item.get(field, "N/A")
        item['chart_data_piutang'] = round(item['total_piutang'], 2)
        item['nominal'] = item['total_piutang'] 

    return jsonify({"data": results, "category": field, "title": f"Top 5 {field}", "subtitle": f"Bulan: {latest_month}"})

# --- API DOWNLOAD FULL SUMMARY (SEMUA DATA TANPA LIMIT) ---

@bp_collection.route("/api/download_summary")
@login_required
def download_summary_csv():
    db_status = get_db_status()
    if db_status['status'] == 'error': return "Koneksi Database Gagal", 500
    
    categories = ["RAYON", "PC", "PCEZ", "TARIF", "MERK_METER"]
    all_rows = []
    latest_month = "N/A"

    for field in categories:
        results, month = _get_distribution_report([field], db_status['collections']['mc'])
        latest_month = month
        for row in results:
            all_rows.append({
                "Kategori": field, "Value": row.get(field, "N/A"),
                "Pelanggan": row.get("total_nomen", 0), "Piutang (Rp)": row.get("total_piutang", 0),
                "Pemakaian (m3)": row.get("total_kubikasi", 0), "Periode": month
            })
    
    if not all_rows: return "Tidak ada data", 404

    df = pd.DataFrame(all_rows)
    output = io.StringIO()
    df.to_csv(output, index=False)
    
    filename = f"Summary_Kontributor_{latest_month}.csv"
    return Response(output.getvalue(), mimetype="text/csv", headers={"Content-disposition": f"attachment; filename={filename}"})

# --- API TOP LIST & COMPARISON ---

@bp_collection.route('/api/top_debtors_report', methods=['GET'])
@login_required 
def top_debtors_report_api():
    db_status = get_db_status()
    if db_status['status'] == 'error': return jsonify({"message": db_status['message']}), 500
    
    collection_mc = db_status['collections']['mc']
    try:
        pipeline = [
            {'$match': {'STATUS': {'$ne': 'PAYMENT'}, 'NOMINAL': {'$gt': 0}}},
            {'$group': {
                '_id': '$NOMEN',
                'TotalPiutangAktif': {'$sum': {'$toDouble': '$NOMINAL'}},
                'JumlahBulanTunggakan': {'$sum': 1},
                'RayonMC': {'$first': '$RAYON'},
                'TagihanTerbaru': {'$max': '$BULAN_TAGIHAN'}
            }},
            {'$lookup': { 'from': 'CustomerData', 'localField': '_id', 'foreignField': 'NOMEN', 'as': 'cust' }},
            {'$unwind': {'path': '$cust', 'preserveNullAndEmptyArrays': True}},
            {'$project': {
                '_id': 0, 'NOMEN': '$_id',
                'NAMA': {'$ifNull': ['$cust.NAMA', 'N/A']},
                'RAYON': {'$ifNull': ['$RayonMC', '$cust.RAYON', 'N/A']},
                'TotalPiutang': {'$round': ['$TotalPiutangAktif', 0]},
                'BulanTunggakan': '$JumlahBulanTunggakan',
                'TagihanTerbaru': '$TagihanTerbaru'
            }},
            {'$sort': {'TotalPiutang': -1}},
            {'$limit': 500}
        ]
        data = list(collection_mc.aggregate(pipeline, allowDiskUse=True))
        schema = [
            {'key': 'NOMEN', 'label': 'No. Pelanggan', 'type': 'string', 'is_main_key': True},
            {'key': 'NAMA', 'label': 'Nama', 'type': 'string'},
            {'key': 'RAYON', 'label': 'Rayon', 'type': 'string'},
            {'key': 'BulanTunggakan', 'label': 'Tunggakan', 'type': 'integer', 'unit': 'bln'},
            {'key': 'TotalPiutang', 'label': 'Total Piutang (Rp)', 'type': 'currency'},
            {'key': 'TagihanTerbaru', 'label': 'Tagihan Terakhir', 'type': 'string'},
        ]
        return jsonify({'status': 'success', 'data': data, 'schema': schema})
    except Exception as e:
        return jsonify({"status": 'error', "message": str(e)}), 500

@bp_collection.route('/api/mom_comparison_report', methods=['GET'])
@login_required
def mom_comparison_report_api():
    db_status = get_db_status()
    if db_status['status'] == 'error': return jsonify({"message": db_status['message']}), 500
    col_mc = db_status['collections']['mc']
    try:
        latest_doc = col_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
        M = latest_doc.get('BULAN_TAGIHAN') if latest_doc else None
        M_1 = _get_previous_month_year(M)
        if not M or not M_1: return jsonify({'status': 'error', 'message': 'Data tidak lengkap.'}), 404
        pipeline = [
            {'$match': {'BULAN_TAGIHAN': {'$in': [M, M_1]}}},
            {'$lookup': { 'from': 'CustomerData', 'localField': 'NOMEN', 'foreignField': 'NOMEN', 'as': 'c' }},
            {'$unwind': {'path': '$c', 'preserveNullAndEmptyArrays': True}},
            {'$project': {
                'NOMEN': 1, 'BULAN_TAGIHAN': 1,
                'RAYON': {'$ifNull': ['$RAYON', '$c.RAYON', 'N/A']},
                'PCEZ': {'$ifNull': ['$PCEZ', '$c.PCEZ', 'N/A']},
                'NOMINAL': {'$toDouble': {'$cond': [{'$eq': ['$STATUS', 'PAYMENT']}, 0, '$NOMINAL']}}
            }},
            {'$group': {
                '_id': {'R': '$RAYON', 'P': '$PCEZ', 'B': '$BULAN_TAGIHAN'},
                'NomenSet': {'$addToSet': '$NOMEN'},
                'TotalNominal': {'$sum': '$NOMINAL'}
            }},
            {'$group': {
                '_id': {'R': '$_id.R', 'P': '$_id.P'},
                'ValM': {'$sum': {'$cond': [{'$eq': ['$_id.B', M]}, {'$size': '$NomenSet'}, 0]}},
                'ValM1': {'$sum': {'$cond': [{'$eq': ['$_id.B', M_1]}, {'$size': '$NomenSet'}, 0]}},
                'NomM': {'$sum': {'$cond': [{'$eq': ['$_id.B', M]}, '$TotalNominal', 0]}},
                'NomM1': {'$sum': {'$cond': [{'$eq': ['$_id.B', M_1]}, '$TotalNominal', 0]}}
            }},
            {'$project': {
                '_id': 0, 'RAYON': '$_id.R', 'PCEZ': '$_id.P',
                f'Piutang_{M}': '$NomM', f'Piutang_{M_1}': '$NomM1',
                f'Nomen_{M}': '$ValM', f'Nomen_{M_1}': '$ValM1',
                'MoM_Pct': {'$cond': [{'$gt': ['$NomM1', 0]}, {'$round': [{'$multiply': [{'$divide': [{'$subtract': ['$NomM', '$NomM1']}, '$NomM1']}, 100]}, 2]}, 0]}
            }},
            {'$sort': {'MoM_Pct': -1}}
        ]
        res = list(col_mc.aggregate(pipeline))
        return jsonify({'status': 'success', 'data': res, 'title': f'MoM {M} vs {M_1}'})
    except Exception as e: return jsonify({"status": 'error', "message": str(e)}), 500

@bp_collection.route('/api/dod_comparison_report', methods=['GET'])
@login_required
def dod_comparison_report_api():
    db_status = get_db_status()
    if db_status['status'] == 'error': return jsonify({"message": db_status['message']}), 500
    col_mb = db_status['collections']['mb']
    try:
        D = _get_day_n_ago(0); D1 = _get_day_n_ago(1)
        pipeline = [
            {'$match': {'TGL_BAYAR': {'$in': [D, D1]}, 'BILL_REASON': 'BIAYA PEMAKAIAN AIR'}},
            {'$lookup': {'from': 'CustomerData', 'localField': 'NOMEN', 'foreignField': 'NOMEN', 'as': 'c'}},
            {'$unwind': {'path': '$c', 'preserveNullAndEmptyArrays': True}},
            {'$group': {
                '_id': {'R': {'$ifNull': ['$RAYON', '$c.RAYON', 'N/A']}, 'T': '$TGL_BAYAR'},
                'TotalKoleksi': {'$sum': {'$toDouble': '$NOMINAL'}},
                'UniqNomen': {'$addToSet': '$NOMEN'}
            }},
            {'$group': {
                '_id': '$_id.R',
                'KoleksiD': {'$sum': {'$cond': [{'$eq': ['$_id.T', D]}, '$TotalKoleksi', 0]}},
                'KoleksiD1': {'$sum': {'$cond': [{'$eq': ['$_id.T', D1]}, '$TotalKoleksi', 0]}},
                'NomenD': {'$sum': {'$cond': [{'$eq': ['$_id.T', D]}, {'$size': '$UniqNomen'}, 0]}},
                'NomenD1': {'$sum': {'$cond': [{'$eq': ['$_id.T', D1]}, {'$size': '$UniqNomen'}, 0]}},
            }},
            {'$project': {
                '_id': 0, 'RAYON': '$_id',
                f'Koleksi_{D}': '$KoleksiD', f'Koleksi_{D1}': '$KoleksiD1',
                f'Pelanggan_{D}': '$NomenD', f'Pelanggan_{D1}': '$NomenD1',
                'DoD_Pct': {'$cond': [{'$gt': ['$KoleksiD1', 0]}, {'$round': [{'$multiply': [{'$divide': [{'$subtract': ['$KoleksiD', '$KoleksiD1']}, '$KoleksiD1']}, 100]}, 2]}, 0]}
            }},
            {'$sort': {'DoD_Pct': -1}}
        ]
        res = list(col_mb.aggregate(pipeline))
        return jsonify({'status': 'success', 'data': res, 'title': f'DoD {D} vs {D1}'})
    except Exception as e: return jsonify({"status": 'error', "message": str(e)}), 500
