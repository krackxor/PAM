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

# --- HELPER INTERNAL UNTUK DISTRIBUSI ---
def _get_distribution_report(group_field, collection_mc, period=None, report_type='PIUTANG'):
    """
    Menghitung distribusi metrik. 
    GROUPING: Dilakukan per Kategori DAN per Rayon agar filter 34/35 di frontend berfungsi.
    """
    db_status = get_db_status()
    if db_status['status'] == 'error':
        return [], "N/A"

    # Penentuan periode target (Format mmyyyy)
    if period:
        target_month = period.replace('-', '')
    else:
        latest_mc_month_doc = collection_mc.find_one(sort=[('BULAN_TAGIHAN', -1)])
        target_month = latest_mc_month_doc.get('BULAN_TAGIHAN') if latest_mc_month_doc else None
    
    if not target_month:
        return [], "N/A"

    # Filter dasar berdasarkan Tab yang dipilih di UI
    base_filter = {"BULAN_TAGIHAN": target_month}
    if report_type == 'COLLECTION':
        base_filter["STATUS"] = "PAYMENT"
    elif report_type == 'TUNGGAKAN':
        # Tunggakan biasanya kumulatif (semua bulan yang belum bayar)
        base_filter = {"STATUS": {"$ne": "PAYMENT"}} 
    else: # Default PIUTANG AKTIF
        base_filter["STATUS"] = {"$ne": "PAYMENT"}

    # 1. Hitung Grand Total untuk % (Dasar kalkulasi di baris tabel)
    totals_pipeline = [
        {"$match": base_filter},
        {"$group": {
            "_id": None,
            "grand_piutang": {"$sum": {"$toDouble": {"$ifNull": ["$NOMINAL", 0]}}},
            "grand_kubikasi": {"$sum": {"$toDouble": {"$ifNull": ["$KUBIK", 0]}}},
            "unique_nomen": {"$addToSet": "$NOMEN"}
        }},
        {"$project": {
            "grand_piutang": 1, "grand_kubikasi": 1,
            "grand_nomen": {"$size": "$unique_nomen"}
        }}
    ]
    
    grand_results = list(collection_mc.aggregate(totals_pipeline))
    g = grand_results[0] if grand_results else {"grand_piutang": 1, "grand_kubikasi": 1, "grand_nomen": 1}
    g_p = g.get('grand_piutang', 1) or 1

    # 2. Pipeline Utama Distribusi
    pipeline = [{"$match": base_filter}]

    # Ekstraksi Rayon dari ZONA_NOVAK
    pipeline.append({"$addFields": {
        "v_RAYON": {"$substrCP": ["$ZONA_NOVAK", 0, 2]},
        "v_PC": {"$substrCP": ["$ZONA_NOVAK", 2, 3]},
        "v_PCEZ": {"$concat": [{"$substrCP": ["$ZONA_NOVAK", 2, 3]}, "/", {"$substrCP": ["$ZONA_NOVAK", 5, 2]}]}
    }})

    # Join ke CustomerData (CID) jika kategori adalah MERK atau READ_METHOD
    if group_field in ["MERK_METER", "READ_METHOD"]:
        pipeline.append({
            "$lookup": {
                "from": "CustomerData",
                "localField": "NOMEN",
                "foreignField": "NOMEN",
                "as": "cust"
            }
        })
        pipeline.append({"$unwind": {"path": "$cust", "preserveNullAndEmptyArrays": True}})
        
        if group_field == "MERK_METER":
            pipeline.append({"$addFields": {
                "mapped_id": {
                    "$ifNull": [
                        "$cust.MERK_METER", 
                        {"$ifNull": ["$cust.MERK", {"$ifNull": ["$cust.MET_BRAND", "TIDAK TERDATA"]}]}
                    ]
                }
            }})
        else:
            pipeline.append({"$addFields": {
                "mapped_id": {"$ifNull": [f"$cust.{group_field}", "TIDAK TERDATA"]}
            }})
    else:
        # Mapping field virtual atau asli
        if group_field == "PC":
            pipeline.append({"$addFields": {"mapped_id": "$v_PC"}})
        elif group_field == "PCEZ":
            pipeline.append({"$addFields": {"mapped_id": "$v_PCEZ"}})
        else:
            pipeline.append({"$addFields": {"mapped_id": f"$v_{group_field}" if group_field == "RAYON" else f"${group_field}"}})

    # GROUPING: PENTING! menyertakan 'rayon' agar filter tombol 34/35 di UI berfungsi
    pipeline.append({
        "$group": {
            "_id": {"val": "$mapped_id", "rayon": "$v_RAYON"},
            "unique_nomen_set": {"$addToSet": "$NOMEN"},
            "total_piutang": {"$sum": {"$toDouble": {"$ifNull": ["$NOMINAL", 0]}}},
            "total_kubikasi": {"$sum": {"$toDouble": {"$ifNull": ["$KUBIK", 0]}}}
        }
    })

    # PROYEKSI
    pipeline.append({
        "$project": {
            "_id": 0,
            "id_value": "$_id.val",
            "rayon_origin": "$_id.rayon",
            "total_nomen": {"$size": "$unique_nomen_set"},
            "total_piutang": 1,
            "total_kubikasi": 1,
            "pct_piutang": {"$multiply": [{"$divide": ["$total_piutang", g_p]}, 100]}
        }
    })

    pipeline.append({"$sort": {"total_piutang": -1}})

    try:
        results = list(collection_mc.aggregate(pipeline, allowDiskUse=True))
        return results, target_month
    except Exception as e:
        print(f"Aggregation Error: {e}")
        return [], target_month

# --- RUTE VIEW FRONTEND ---

@bp_collection.route('/laporan', methods=['GET'])
@login_required 
def collection_laporan_view():
    raw_period = request.args.get('period', datetime.now().strftime('%Y-%m'))
    return render_template('collection_summary.html', title="Laporan Piutang & Koleksi", period=raw_period, is_admin=current_user.is_admin)

# --- API ENDPOINTS ---

@bp_collection.route("/api/distribution/<category>")
@login_required
def category_distribution_api(category):
    raw_period = request.args.get('period')
    report_type = request.args.get('type', 'PIUTANG') # Menyesuaikan dengan Tab di UI
    
    formatted_period = None
    if raw_period and '-' in raw_period:
        y, m = raw_period.split('-')
        formatted_period = f"{m}{y}"

    cat_map = {
        "rayon": "RAYON", "pc": "PC", "pcez": "PCEZ", 
        "tarif": "TARIF", "merk": "MERK_METER", "read_method": "READ_METHOD"
    }
    field = cat_map.get(category.lower())
    if not field: return jsonify({"message": "Kategori tidak valid"}), 400

    db_status = get_db_status()
    if db_status['status'] == 'error': return jsonify({"message": db_status['message']}), 500
    
    results, latest_month = _get_distribution_report(field, db_status['collections']['mc'], period=formatted_period, report_type=report_type.upper())
    
    return jsonify({
        "data": results, 
        "title": f"Kontributor {category}", 
        "subtitle": f"Bulan Tagihan: {latest_month}"
    })

@bp_collection.route("/api/stats_summary")
@login_required
def get_stats_summary_api():
    """
    Endpoint utama untuk KPI. 
    Menghitung detail per tab (count, usage, nominal) agar loading spinner di header berhenti.
    """
    raw_period = request.args.get('period', datetime.now().strftime('%Y-%m'))
    formatted_period = raw_period.replace('-', '') if '-' in raw_period else raw_period
    
    db_status = get_db_status()
    col = db_status['collections']['mc']
    
    def get_tab_summary(match_filter):
        pipeline = [
            {"$match": match_filter},
            {"$group": {
                "_id": None,
                "usage": {"$sum": {"$toDouble": {"$ifNull": ["$KUBIK", 0]}}},
                "nominal": {"$sum": {"$toDouble": {"$ifNull": ["$NOMINAL", 0]}}},
                "unique_nomen": {"$addToSet": "$NOMEN"}
            }},
            {"$project": {
                "_id": 0,
                "count": {"$size": "$unique_nomen"},
                "usage": 1,
                "nominal": 1
            }}
        ]
        res = list(col.aggregate(pipeline))
        return res[0] if res else {"count": 0, "usage": 0, "nominal": 0}

    # Data dikirim dengan kunci 'piutang', 'collection', 'tunggakan' agar JavaScript langsung berhenti loading
    data = {
        "piutang": get_tab_summary({"BULAN_TAGIHAN": formatted_period, "STATUS": {"$ne": "PAYMENT"}}),
        "collection": get_tab_summary({"BULAN_TAGIHAN": formatted_period, "STATUS": "PAYMENT"}),
        "tunggakan": get_tab_summary({"STATUS": {"$ne": "PAYMENT"}})
    }

    return jsonify(data)

@bp_collection.route("/api/download_summary")
@login_required
def download_summary_csv():
    db_status = get_db_status()
    categories = ["RAYON", "PC", "PCEZ", "TARIF", "MERK_METER", "READ_METHOD"]
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
                "Kategori": field, "Nilai": row.get("id_value"), "Rayon_Asal": row.get("rayon_origin"),
                "Pelanggan": row.get("total_nomen"), "Piutang_Rp": row.get("total_piutang"),
                "Kubikasi": row.get("total_kubikasi"), "Periode": month
            })
    
    df = pd.DataFrame(all_rows)
    output = io.StringIO()
    df.to_csv(output, index=False)
    return Response(output.getvalue(), mimetype="text/csv", headers={"Content-disposition": "attachment; filename=Analisis_Kontributor_Novak.csv"})

# --- Rute Navigasi (Pencegah BuildError) ---
@bp_collection.route('/analisis')
@login_required 
def collection_analisis_view():
    return render_template('collection_analysis.html', is_admin=current_user.is_admin)

@bp_collection.route('/top_list')
@login_required
def collection_top_view():
    return render_template('analysis_report_template.html', title="Top List Piutang", report_type="TOP_DEBTORS", api_endpoint=url_for('bp_collection.top_debtors_report_api'), is_admin=current_user.is_admin)

@bp_collection.route('/api/top_debtors_report')
@login_required 
def top_debtors_report_api():
    return jsonify({'status': 'success', 'data': [], 'schema': []})

@bp_collection.route('/riwayat_mom')
@login_required
def collection_riwayat_view():
    return render_template('analysis_report_template.html', title="Riwayat MoM", report_type="MOM_COMPARISON", api_endpoint=url_for('bp_collection.mom_comparison_report_api'), is_admin=current_user.is_admin)

@bp_collection.route('/api/mom_comparison_report', methods=['GET'])
@login_required
def mom_comparison_report_api():
    return jsonify({'status': 'success', 'data': []})
