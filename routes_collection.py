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
    - PIUTANG: Data MC status != PAYMENT (Bulan Berjalan)
    - COLLECTION: Data MC status == PAYMENT (Bulan Berjalan)
    - TUNGGAKAN: Data ARDEBT yang di-join ke MC untuk mengambil Tarif & Kubik
    """
    db_status = get_db_status()
    if db_status['status'] == 'error':
        return [], "N/A"

    collections = db_status['collections']
    
    # Perbaikan Konversi Periode: YYYY-MM -> MMYYYY
    if period and '-' in period:
        y, m = period.split('-')
        target_month = f"{m}{y}"
    elif period:
        target_month = period
    else:
        latest_doc = collections['mc'].find_one(sort=[('BULAN_TAGIHAN', -1)])
        target_month = latest_doc.get('BULAN_TAGIHAN') if latest_doc else datetime.now().strftime('%m%Y')

    pipeline = []

    # 1. Konfigurasi sumber data dan field mapping
    if report_type == 'TUNGGAKAN':
        # MULAI DARI ARDEBT (Daftar Penunggak)
        source_col = collections['ardebt']
        pipeline.append({"$match": {}}) # ARDEBT adalah snapshot keseluruhan
        
        # JOIN ke MC untuk mengambil TARIF dan KUBIK sesuai NOMEN dan BULAN_TAGIHAN terpilih
        pipeline.append({
            "$lookup": {
                "from": "mc",
                "let": {"nomen_ar": "$NOMEN"},
                "pipeline": [
                    {"$match": {
                        "$expr": {
                            "$and": [
                                {"$eq": ["$NOMEN", "$$nomen_ar"]},
                                {"$eq": ["$BULAN_TAGIHAN", target_month]}
                            ]
                        }
                    }}
                ],
                "as": "mc_info"
            }
        })
        pipeline.append({"$unwind": {"path": "$mc_info", "preserveNullAndEmptyArrays": True}})
        
        # Mapping Field: Piutang dari ARDEBT, Kubik & Tarif dari MC
        val_field = "$JUMLAH"
        usage_field = "$mc_info.KUBIK"
        
        pipeline.append({"$addFields": {
            "v_RAYON": "$RAYON",
            "v_PC": {"$substrCP": [{"$ifNull": ["$PCEZ", ""]}, 0, 3]},
            "v_PCEZ": "$PCEZ",
            "v_TARIF": {"$ifNull": ["$mc_info.TARIF", "$TIPEPLGGN"]} # Ambil dari MC, fallback ke ARDEBT
        }})
    else:
        # LAPORAN STANDAR MC (Piutang Aktif / Koleksi)
        source_col = collections['mc']
        match_filter = {"BULAN_TAGIHAN": target_month}
        if report_type == 'COLLECTION':
            match_filter["STATUS"] = "PAYMENT"
        else:
            match_filter["STATUS"] = {"$ne": "PAYMENT"}
        
        pipeline.append({"$match": match_filter})
        val_field = "$NOMINAL"
        usage_field = "$KUBIK"

        pipeline.append({"$addFields": {
            "v_RAYON": {"$substrCP": ["$ZONA_NOVAK", 0, 2]},
            "v_PC": {"$substrCP": ["$ZONA_NOVAK", 2, 3]},
            "v_PCEZ": {"$concat": [{"$substrCP": ["$ZONA_NOVAK", 2, 3]}, "/", {"$substrCP": ["$ZONA_NOVAK", 5, 2]}]},
            "v_TARIF": "$TARIF"
        }})

    # 2. Mapping Grouping Field & Lookup CID (jika perlu Merk)
    g_field = group_field.upper()
    if g_field in ["MERK", "MERK_METER", "READ_METHOD"]:
        pipeline.append({
            "$lookup": {
                "from": "CustomerData",
                "localField": "NOMEN",
                "foreignField": "NOMEN",
                "as": "cust"
            }
        })
        pipeline.append({"$unwind": {"path": "$cust", "preserveNullAndEmptyArrays": True}})
        
        if g_field.startswith("MERK"):
            pipeline.append({"$addFields": {
                "mapped_id": {"$ifNull": ["$cust.MERK_METER", {"$ifNull": ["$cust.MERK", "TIDAK TERDATA"]}]}
            }})
        else:
            pipeline.append({"$addFields": {
                "mapped_id": {"$ifNull": ["$cust.READ_METHOD", "TIDAK TERDATA"]}
            }})
    else:
        field_map = {"RAYON": "$v_RAYON", "PC": "$v_PC", "PCEZ": "$v_PCEZ", "TARIF": "$v_TARIF"}
        pipeline.append({"$addFields": {"mapped_id": {"$ifNull": [field_map.get(g_field, f"${g_field}"), "TIDAK TERDATA"]}}})

    # 3. Grouping
    pipeline.append({
        "$group": {
            "_id": {"val": "$mapped_id", "rayon": "$v_RAYON"},
            "unique_nomen_set": {"$addToSet": "$NOMEN"},
            "total_val": {"$sum": {"$toDouble": {"$ifNull": [val_field, 0]}}},
            "total_use": {"$sum": {"$toDouble": {"$ifNull": [usage_field, 0]}}}
        }
    })

    # 4. Proyeksi Akhir
    pipeline.append({
        "$project": {
            "_id": 0,
            "id_value": "$_id.val",
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
        print(f"Aggr Error: {e}")
        return [], target_month

# --- API ENDPOINTS ---

@bp_collection.route("/api/stats_summary")
@login_required
def get_stats_summary_api():
    """Menghitung KPI Header Dashboard dengan join MC untuk kubikasi tunggakan."""
    raw_period = request.args.get('period', datetime.now().strftime('%Y-%m'))
    
    if '-' in raw_period:
        y, m = raw_period.split('-')
        formatted_period = f"{m}{y}"
    else:
        formatted_period = raw_period
    
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
        # Menghitung nominal dari ARDEBT dan join MC untuk volume kubikasi
        pipeline = [
            {
                "$lookup": {
                    "from": "mc",
                    "let": {"nomen_ar": "$NOMEN"},
                    "pipeline": [
                        {"$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$NOMEN", "$$nomen_ar"]},
                                    {"$eq": ["$BULAN_TAGIHAN", formatted_period]}
                                ]
                            }
                        }}
                    ],
                    "as": "mc_info"
                }
            },
            {"$unwind": {"path": "$mc_info", "preserveNullAndEmptyArrays": True}},
            {
                "$group": {
                    "_id": None,
                    "nominal": {"$sum": {"$toDouble": {"$ifNull": ["$JUMLAH", 0]}}},
                    "usage": {"$sum": {"$toDouble": {"$ifNull": ["$mc_info.KUBIK", 0]}}},
                    "unique_nomen": {"$addToSet": "$NOMEN"}
                }
            },
            {"$project": {"_id": 0, "count": {"$size": "$unique_nomen"}, "usage": 1, "nominal": 1}}
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
    raw_period = request.args.get('period')
    report_type = request.args.get('type', 'PIUTANG').upper()
    
    results, latest_month = _get_distribution_report(category, period=raw_period, report_type=report_type)
    
    total_p = sum(item['total_piutang'] for item in results) or 1
    for item in results:
        item['pct_piutang'] = (item['total_piutang'] / total_p) * 100
    
    return jsonify({
        "data": results, 
        "title": f"Kontributor {category.upper()}", 
        "subtitle": f"Bulan: {latest_month} ({report_type})"
    })

@bp_collection.route("/api/download_summary")
@login_required
def download_summary_csv():
    raw_period = request.args.get('period', datetime.now().strftime('%Y-%m'))
    categories = ["RAYON", "PC", "TARIF", "MERK", "READ_METHOD"]
    report_types = ["PIUTANG", "TUNGGAKAN", "COLLECTION"]
    
    all_data = []
    for r_type in report_types:
        for cat in categories:
            results, month = _get_distribution_report(cat, period=raw_period, report_type=r_type)
            for row in results:
                all_data.append({
                    "Periode": month, "Tipe": r_type, "Kategori": cat,
                    "Grup": row.get("id_value"), "Total_Nominal": row.get("total_piutang"),
                    "Total_Nomen": row.get("total_nomen"), "Total_Kubikasi": row.get("total_kubikasi")
                })
    
    if not all_data: return jsonify({"status": "error", "message": "No data"}), 404
        
    df = pd.DataFrame(all_data)
    output = io.StringIO()
    df.to_csv(output, index=False)
    return Response(output.getvalue(), mimetype="text/csv", headers={"Content-disposition": f"attachment; filename=Summary_Analitik_{raw_period}.csv"})

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
