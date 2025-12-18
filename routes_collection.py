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

# --- HELPER INTERNAL UNTUK DISTRIBUSI (PIUTANG/COLLECTION/TUNGGAKAN) ---
def _get_distribution_report(group_field, period=None, report_type='PIUTANG'):
    """
    Menghitung distribusi metrik. 
    report_type: 
    - PIUTANG: Data MC (Master Cetak) status != PAYMENT (Bulan Berjalan)
    - COLLECTION: Data MC status == PAYMENT (Bulan Berjalan)
    - TUNGGAKAN: Data ARDEBT (Account Receivable Debt - Kumulatif dari file ARDEBT)
    """
    db_status = get_db_status()
    if db_status['status'] == 'error':
        return [], "N/A"

    collections = db_status['collections']
    
    # Penentuan Target Bulan (hanya berlaku untuk laporan berbasis MC)
    if period:
        target_month = period.replace('-', '')
    else:
        latest_doc = collections['mc'].find_one(sort=[('BULAN_TAGIHAN', -1)])
        target_month = latest_doc.get('BULAN_TAGIHAN') if latest_doc else datetime.now().strftime('%m%Y')

    # 1. Konfigurasi sumber data dan field mapping
    if report_type == 'TUNGGAKAN':
        source_col = collections['ardebt']
        match_filter = {} # ARDEBT biasanya data snapshot terbaru (keseluruhan)
        val_field = "$JUMLAH"
        usage_field = {"$literal": 0} # ARDEBT tidak memiliki kolom m3
    else:
        source_col = collections['mc']
        match_filter = {"BULAN_TAGIHAN": target_month}
        if report_type == 'COLLECTION':
            match_filter["STATUS"] = "PAYMENT"
        else:
            match_filter["STATUS"] = {"$ne": "PAYMENT"}
        val_field = "$NOMINAL"
        usage_field = "$KUBIK"

    # 2. Pipeline Agregasi
    pipeline = [{"$match": match_filter}]

    # 3. Ekstraksi Field Virtual (Rayon, PC, Tarif)
    if report_type == 'TUNGGAKAN':
        # Mapping dari Header ARDEBT (RAYON, PCEZ, TIPEPLGGN)
        pipeline.append({"$addFields": {
            "v_RAYON": "$RAYON",
            "v_PC": "$PCEZ",
            "v_PCEZ": "$PCEZ",
            "v_TARIF": "$TIPEPLGGN"
        }})
    else:
        # Mapping dari Header MC (ZONA_NOVAK)
        pipeline.append({"$addFields": {
            "v_RAYON": {"$substrCP": ["$ZONA_NOVAK", 0, 2]},
            "v_PC": {"$substrCP": ["$ZONA_NOVAK", 2, 3]},
            "v_PCEZ": {"$concat": [{"$substrCP": ["$ZONA_NOVAK", 2, 3]}, "/", {"$substrCP": ["$ZONA_NOVAK", 5, 2]}]},
            "v_TARIF": "$TARIF"
        }})

    # 4. Mapping Grouping Field & Lookup (untuk Merk/Method)
    if group_field in ["MERK", "READ_METHOD"]:
        # ARDEBT sudah punya kolom MERK dan READ_METHOD, MC perlu lookup ke CID
        if report_type != 'TUNGGAKAN':
            pipeline.append({
                "$lookup": {
                    "from": "CustomerData",
                    "localField": "NOMEN",
                    "foreignField": "NOMEN",
                    "as": "cust"
                }
            })
            pipeline.append({"$unwind": {"path": "$cust", "preserveNullAndEmptyArrays": True}})
            mapped_id_field = f"$cust.{group_field}"
        else:
            mapped_id_field = f"${group_field}"
    else:
        # Field virtual atau default
        field_map = {"RAYON": "$v_RAYON", "PC": "$v_PC", "PCEZ": "$v_PCEZ", "TARIF": "$v_TARIF"}
        mapped_id_field = field_map.get(group_field, f"${group_field}")

    # 5. Grouping
    pipeline.append({
        "$group": {
            "_id": {"val": mapped_id_field, "rayon": "$v_RAYON"},
            "unique_nomen_set": {"$addToSet": "$NOMEN"},
            "total_val": {"$sum": {"$toDouble": {"$ifNull": [val_field, 0]}}},
            "total_use": {"$sum": {"$toDouble": {"$ifNull": [usage_field, 0]}}}
        }
    })

    # 6. Proyeksi Akhir
    pipeline.append({
        "$project": {
            "_id": 0,
            "id_value": {"$ifNull": ["$_id.val", "TIDAK TERDATA"]},
            "rayon_origin": "$_id.rayon",
            "total_nomen": {"$size": "$unique_nomen_set"},
            "total_piutang": "$total_val",
            "total_kubikasi": "$total_use"
        }
    })

    pipeline.append({"$sort": {"total_piutang": -1}})

    try:
        results = list(source_col.aggregate(pipeline, allowDiskUse=True))
        return results, target_month
    except Exception as e:
        print(f"Aggregation Error: {e}")
        return [], target_month

# --- API ENDPOINTS ---

@bp_collection.route("/api/stats_summary")
@login_required
def get_stats_summary_api():
    """
    Menghitung KPI Header Dashboard.
    Sinkronisasi: Tunggakan dihitung dari koleksi 'ardebt'.
    """
    raw_period = request.args.get('period', datetime.now().strftime('%Y-%m'))
    formatted_period = raw_period.replace('-', '') if '-' in raw_period else raw_period
    
    db_status = get_db_status()
    col_mc = db_status['collections']['mc']
    col_ar = db_status['collections']['ardebt']
    
    def get_mc_summary(match_filter):
        pipeline = [
            {"$match": match_filter},
            {"$group": {
                "_id": None,
                "usage": {"$sum": {"$toDouble": {"$ifNull": ["$KUBIK", 0]}}},
                "nominal": {"$sum": {"$toDouble": {"$ifNull": ["$NOMINAL", 0]}}},
                "unique_nomen": {"$addToSet": "$NOMEN"}
            }},
            {"$project": {"_id": 0, "count": {"$size": "$unique_nomen"}, "usage": 1, "nominal": 1}}
        ]
        res = list(col_mc.aggregate(pipeline))
        return res[0] if res else {"count": 0, "usage": 0, "nominal": 0}

    def get_ar_summary():
        # Menghitung nominal dari 'JUMLAH' dan Nomen unik dari 'NOMEN' di koleksi ardebt
        pipeline = [
            {"$group": {
                "_id": None,
                "nominal": {"$sum": {"$toDouble": {"$ifNull": ["$JUMLAH", 0]}}},
                "unique_nomen": {"$addToSet": "$NOMEN"}
            }},
            {"$project": {"_id": 0, "count": {"$size": "$unique_nomen"}, "usage": {"$literal": 0}, "nominal": 1}}
        ]
        res = list(col_ar.aggregate(pipeline))
        return res[0] if res else {"count": 0, "usage": 0, "nominal": 0}

    data = {
        "piutang": get_mc_summary({"BULAN_TAGIHAN": formatted_period, "STATUS": {"$ne": "PAYMENT"}}),
        "collection": get_mc_summary({"BULAN_TAGIHAN": formatted_period, "STATUS": "PAYMENT"}),
        "tunggakan": get_ar_summary()
    }
    return jsonify(data)

@bp_collection.route("/api/distribution/<category>")
@login_required
def category_distribution_api(category):
    """API untuk data grafik dan tabel kontributor."""
    raw_period = request.args.get('period')
    report_type = request.args.get('type', 'PIUTANG').upper()
    
    # Mapping nama kategori agar sesuai dengan helper
    cat_field = category.upper()
    if cat_field == 'MERK': cat_field = 'MERK_METER'

    results, latest_month = _get_distribution_report(cat_field, period=raw_period, report_type=report_type)
    
    # Hitung Persentase Kontribusi secara dinamis
    total_p = sum(item['total_piutang'] for item in results) or 1
    for item in results:
        item['pct_piutang'] = (item['total_piutang'] / total_p) * 100
    
    return jsonify({
        "data": results, 
        "title": f"Kontributor {category.upper()}", 
        "subtitle": f"Sumber: {'ARDEBT' if report_type == 'TUNGGAKAN' else 'MC'} - {latest_month}"
    })

@bp_collection.route("/api/download_summary")
@login_required
def download_summary_csv():
    """Endpoint untuk mengekspor data summary kontributor ke CSV."""
    raw_period = request.args.get('period', datetime.now().strftime('%Y-%m'))
    categories = ["RAYON", "PC", "TARIF", "MERK", "READ_METHOD"]
    report_types = ["PIUTANG", "TUNGGAKAN", "COLLECTION"]
    
    all_data = []
    for r_type in report_types:
        for cat in categories:
            results, month = _get_distribution_report(cat, period=raw_period, report_type=r_type)
            for row in results:
                all_data.append({
                    "Periode": month,
                    "Tipe_Laporan": r_type,
                    "Kategori": cat,
                    "Grup": row.get("id_value"),
                    "Rayon_Asal": row.get("rayon_origin"),
                    "Total_Nominal": row.get("total_piutang"),
                    "Total_Nomen": row.get("total_nomen"),
                    "Total_Kubikasi": row.get("total_kubikasi")
                })
    
    if not all_data:
        return jsonify({"status": "error", "message": "Tidak ada data untuk periode ini"}), 404
        
    df = pd.DataFrame(all_data)
    output = io.StringIO()
    df.to_csv(output, index=False)
    
    filename = f"Summary_Analitik_{raw_period}.csv"
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-disposition": f"attachment; filename={filename}"}
    )

@bp_collection.route('/laporan', methods=['GET'])
@login_required 
def collection_laporan_view():
    raw_period = request.args.get('period', datetime.now().strftime('%Y-%m'))
    return render_template('collection_summary.html', title="Laporan Piutang & Koleksi", period=raw_period, is_admin=current_user.is_admin)

@bp_collection.route('/analisis', methods=['GET'])
@login_required 
def collection_analisis_view():
    return render_template('collection_analysis.html', title="Analisis Kontributor", is_admin=current_user.is_admin)

@bp_collection.route('/api/top_debtors_report', methods=['GET'])
@login_required 
def top_debtors_report_api():
    # Placeholder atau implementasi list 1000 penunggak terbesar dari ARDEBT
    return jsonify({'status': 'success', 'data': []})

@bp_collection.route('/top_list', methods=['GET'])
@login_required
def collection_top_view():
    return render_template('analysis_report_template.html', title="Top List Piutang", report_type="TOP_DEBTORS", api_endpoint=url_for('bp_collection.top_debtors_report_api'), is_admin=current_user.is_admin)

@bp_collection.route('/riwayat_mom', methods=['GET'])
@login_required
def collection_riwayat_view():
    return render_template('analysis_report_template.html', title="Riwayat MoM", report_type="MOM_COMPARISON", api_endpoint=url_for('bp_collection.mom_comparison_report_api'), is_admin=current_user.is_admin)

@bp_collection.route('/dod_comparison', methods=['GET'])
@login_required
def analysis_dod_comparison():
    return render_template('analysis_report_template.html', title="Koleksi DoD", report_type="DOD_COMPARISON", api_endpoint=url_for('bp_collection.mom_comparison_report_api'), is_admin=current_user.is_admin)

@bp_collection.route('/api/mom_comparison_report', methods=['GET'])
@login_required
def mom_comparison_report_api():
    return jsonify({'status': 'success', 'data': []})
