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

# --- HELPER INTERNAL UNTUK DISTRIBUSI (EKSTRAKSI ZONA_NOVAK) ---
def _get_distribution_report(group_field, collection_mc, period=None):
    """
    Menghitung distribusi metrik dengan ekstraksi otomatis dari ZONA_NOVAK.
    Pola ZONA_NOVAK: RR PPP EE BB (Rayon 2 digit, PC 3 digit, EZ 2 digit, Block 2 digit)
    """
    db_status = get_db_status()
    if db_status['status'] == 'error':
        return [], "N/A"

    # Penentuan periode target
    if period:
        target_month = period
    else:
        latest_mc_month_doc = collection_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
        target_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None
    
    if not target_month:
        return [], "N/A"

    # Pipeline utama dengan ekstraksi string ZONA_NOVAK
    pipeline = [
        # Tahap 1: Filter periode dan pastikan ZONA_NOVAK tersedia
        {"$match": {"BULAN_TAGIHAN": target_month, "ZONA_NOVAK": {"$exists": True, "$ne": ""}}},
        
        # Tahap 2: Pecah string ZONA_NOVAK menjadi field virtual (Virtual Fields)
        {"$addFields": {
            "v_RAYON": {"$substrCP": ["$ZONA_NOVAK", 0, 2]},
            "v_PC": {"$substrCP": ["$ZONA_NOVAK", 2, 3]},
            "v_EZ": {"$substrCP": ["$ZONA_NOVAK", 5, 2]},
            "v_BLOCK": {"$substrCP": ["$ZONA_NOVAK", 7, 2]},
            "v_PCEZ": {
                "$concat": [
                    {"$substrCP": ["$ZONA_NOVAK", 2, 3]}, "/", {"$substrCP": ["$ZONA_NOVAK", 5, 2]}
                ]
            }
        }},
        
        # Tahap 3: Mapping input group_field ke field virtual yang baru dibuat
        {"$addFields": {
            "mapped_id": f"$v_{group_field}" if group_field in ["RAYON", "PC", "EZ", "BLOCK", "PCEZ"] else f"${group_field}"
        }},
        
        # Tahap 4: Grouping (Sertakan Rayon Origin untuk filter di Frontend)
        {"$group": {
            "_id": {"val": "$mapped_id", "r": "$v_RAYON"},
            "unique_nomen_set": {"$addToSet": "$NOMEN"},
            "total_piutang": {"$sum": {"$toDouble": {"$ifNull": ["$NOMINAL", 0]}}},
            "total_kubikasi": {"$sum": {"$toDouble": {"$ifNull": ["$KUBIK", 0]}}}
        }},
        
        # Tahap 5: Proyeksi hasil akhir
        {"$project": {
            "_id": 0,
            "id_value": "$_id.val",
            "rayon_origin": "$_id.r",
            "total_nomen": {"$size": "$unique_nomen_set"},
            "total_piutang": 1,
            "total_kubikasi": 1
        }},
        
        # Urutkan berdasarkan nominal terbesar
        {"$sort": {"total_piutang": -1}}
    ]

    try:
        # allowDiskUse=True penting jika dataset sangat besar
        results = list(collection_mc.aggregate(pipeline, allowDiskUse=True))
    except Exception as e:
        print(f"Error Aggregation ZONA_NOVAK: {e}")
        return [], target_month

    return results, target_month

# --- RUTE VIEW FRONTEND ---

@bp_collection.route('/laporan', methods=['GET'])
@login_required 
def collection_laporan_view():
    current_date = datetime.now()
    raw_period = request.args.get('period', current_date.strftime('%Y-%m'))
    return render_template('collection_summary.html', 
                           title="Laporan Piutang & Koleksi", 
                           period=raw_period, 
                           is_admin=current_user.is_admin)

@bp_collection.route('/analisis', methods=['GET'])
@login_required 
def collection_analisis_view():
    return render_template('collection_analysis.html', 
                           title="Analisis Kontributor", 
                           is_admin=current_user.is_admin)

@bp_collection.route('/top_list', methods=['GET'])
@login_required
def collection_top_view():
    """Missing route referenced in base.html"""
    return render_template('analysis_report_template.html',
                           title="Top List Piutang",
                           description="Daftar 1000 pelanggan dengan piutang aktif terbesar.",
                           report_type="TOP_DEBTORS",
                           api_endpoint=url_for('bp_collection.top_debtors_report_api'),
                           is_admin=current_user.is_admin)

@bp_collection.route('/riwayat_mom', methods=['GET'])
@login_required
def collection_riwayat_view():
    """Missing route referenced in base.html"""
    return render_template('analysis_report_template.html',
                           title="Riwayat MoM",
                           description="Perbandingan performa antar bulan (Month over Month).",
                           report_type="MOM_COMPARISON",
                           api_endpoint=url_for('bp_collection.mom_comparison_report_api'),
                           is_admin=current_user.is_admin)

@bp_collection.route('/dod_comparison', methods=['GET'])
@login_required
def analysis_dod_comparison():
    """Missing route referenced in base.html"""
    return render_template('analysis_report_template.html',
                           title="Koleksi Day over Day",
                           description="Perbandingan koleksi harian (DoD).",
                           report_type="DOD_COMPARISON",
                           api_endpoint=url_for('bp_collection.mom_comparison_report_api'), # Placeholder as actual endpoint not visible
                           is_admin=current_user.is_admin)


# --- API ENDPOINTS (FOR ASYNC LOADING) ---

@bp_collection.route("/api/stats_summary")
@login_required
def get_stats_summary_api():
    raw_period = request.args.get('period', datetime.now().strftime('%Y-%m'))
    try:
        # Konversi format YYYY-MM ke MMYYYY untuk kompatibilitas database
        if '-' in raw_period:
            y, m = raw_period.split('-')
            formatted_period = f"{m}{y}"
        else:
            formatted_period = raw_period
    except:
        formatted_period = raw_period.replace('-', '')
    
    stats = get_comprehensive_stats(formatted_period)
    return jsonify(stats)

@bp_collection.route("/api/distribution/<category>")
@login_required
def category_distribution_api(category):
    raw_period = request.args.get('period')
    formatted_period = None
    if raw_period and '-' in raw_period:
        y, m = raw_period.split('-')
        formatted_period = f"{m}{y}"

    # Mapping kategori ke field database
    cat_map = {
        "rayon": "RAYON", 
        "pc": "PC", 
        "pcez": "PCEZ", 
        "ez": "EZ", 
        "block": "BLOCK", 
        "tarif": "TARIF", 
        "merk": "MERK_METER"
    }
    field = cat_map.get(category.lower())
    if not field: return jsonify({"message": "Kategori tidak valid"}), 400

    db_status = get_db_status()
    if db_status['status'] == 'error': return jsonify({"message": db_status['message']}), 500
    
    results, latest_month = _get_distribution_report(field, db_status['collections']['mc'], period=formatted_period)
    
    return jsonify({
        "data": results, 
        "category": field, 
        "title": f"Kontributor {field}", 
        "subtitle": f"Bulan: {latest_month}"
    })

@bp_collection.route("/api/download_summary")
@login_required
def download_summary_csv():
    db_status = get_db_status()
    if db_status['status'] == 'error': return "Database Error", 500
    
    categories = ["RAYON", "PC", "PCEZ", "EZ", "BLOCK", "TARIF", "MERK_METER"]
    all_rows = []
    raw_period = request.args.get('period')
    formatted_period = None
    if raw_period and '-' in raw_period:
        y, m = raw_period.split('-')
        formatted_period = f"{m}{y}"

    for field in categories:
        results, month = _get_distribution_report(field, db_status['collections']['mc'], period=formatted_period)
        for row in results:
            all_rows.append({
                "Kategori": field, 
                "Nilai": row.get("id_value"), 
                "Rayon_Asal": row.get("rayon_origin"),
                "Pelanggan": row.get("total_nomen"), 
                "Piutang_Rp": row.get("total_piutang"),
                "Kubikasi": row.get("total_kubikasi"),
                "Periode": month
            })
    
    if not all_rows: return "Data Kosong", 404

    df = pd.DataFrame(all_rows)
    output = io.StringIO()
    df.to_csv(output, index=False)
    return Response(output.getvalue(), mimetype="text/csv", headers={"Content-disposition": "attachment; filename=Summary_Novak_Analytics.csv"})

# --- API LAINNYA (TOP LIST & MoM) ---

@bp_collection.route('/api/top_debtors_report', methods=['GET'])
@login_required 
def top_debtors_report_api():
    db_status = get_db_status()
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
            {'$limit': 1000}
        ]
        data = list(collection_mc.aggregate(pipeline, allowDiskUse=True))
        
        # Schema for top debtors
        schema = [
            {'key': 'NOMEN', 'label': 'No. Pelanggan', 'type': 'string', 'is_main_key': True},
            {'key': 'NAMA', 'label': 'Nama', 'type': 'string'},
            {'key': 'RAYON', 'label': 'Rayon', 'type': 'string'},
            {'key': 'TotalPiutang', 'label': 'Total Piutang', 'type': 'currency', 'chart_key': True},
            {'key': 'BulanTunggakan', 'label': 'Jml Bulan', 'type': 'integer'},
            {'key': 'TagihanTerbaru', 'label': 'Bulan Terakhir', 'type': 'string'},
        ]
        
        return jsonify({'status': 'success', 'data': data, 'schema': schema})
    except Exception as e: return jsonify({"status": 'error', "message": str(e)}), 500

@bp_collection.route('/api/mom_comparison_report', methods=['GET'])
@login_required
def mom_comparison_report_api():
    db_status = get_db_status()
    col_mc = db_status['collections']['mc']
    try:
        latest_doc = col_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
        M = latest_doc.get('BULAN_TAGIHAN') if latest_doc else None
        M_1 = _get_previous_month_year(M)
        
        # Simple placeholder for actual MoM comparison logic
        return jsonify({
            'status': 'success', 
            'message': 'Endpoint MoM Aktif', 
            'data': [], 
            'schema': [],
            'latest_month': M,
            'previous_month': M_1
        })
    except Exception as e:
        return jsonify({"status": 'error', "message": str(e)}), 500
