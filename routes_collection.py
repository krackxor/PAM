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
def _get_distribution_report(group_field, period=None, report_type='PIUTANG'):
    """
    Menghitung distribusi metrik. 
    FIX: Ambil KUBIK dari MC (join PERIODE_BILL) dan TARIFF dari CID.
    """
    db_status = get_db_status()
    if db_status['status'] == 'error':
        return [], "N/A"

    collections = db_status['collections']
    
    # Konversi Periode UI (YYYY-MM) ke DB (MMYYYY)
    if period and '-' in period:
        y, m = period.split('-')
        target_month = f"{m}{y}"
    elif period:
        target_month = period
    else:
        latest = collections['mc'].find_one(sort=[('BULAN_TAGIHAN', -1)])
        target_month = latest.get('BULAN_TAGIHAN') if latest else datetime.now().strftime('%m%Y')

    pipeline = []

    if report_type == 'TUNGGAKAN':
        source_col = collections['ardebt']
        
        # 1. Join ke CustomerData (CID) untuk mendapatkan TARIFF yang BENAR
        pipeline.append({
            "$lookup": {
                "from": "CustomerData",
                "let": {"n_ar": { "$toString": "$NOMEN" }},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": [{ "$toString": "$NOMEN" }, "$$n_ar"]}}},
                    {"$sort": {"TANGGAL_UPLOAD_CID": -1}},
                    {"$limit": 1}
                ],
                "as": "cid_info"
            }
        })
        pipeline.append({"$unwind": {"path": "$cid_info", "preserveNullAndEmptyArrays": True}})
        
        # 2. Join ke Master Cetak (MC) untuk mendapatkan KUBIKASI yang BENAR
        # Menggunakan PERIODE_BILL dari ardebt dengan padding (misal 5 -> 05)
        pipeline.append({
            "$lookup": {
                "from": "MasterCetak",
                "let": {"n_ar": { "$toString": "$NOMEN" }, "p_ar": "$PERIODE_BILL"},
                "pipeline": [
                    {"$match": {
                        "$expr": {
                            "$and": [
                                {"$eq": [{ "$toString": "$NOMEN" }, "$$n_ar"]},
                                {"$regexMatch": {
                                    "input": "$BULAN_TAGIHAN",
                                    "regex": {"$concat": ["^", {"$cond": [{"$lt": [{"$strLenCP": {"$toString": "$$p_ar"}}, 2]}, {"$concat": ["0", {"$toString": "$$p_ar"}]}, {"$toString": "$$p_ar"}]}]}
                                }}
                            ]
                        }
                    }}
                ],
                "as": "mc_info"
            }
        })
        pipeline.append({"$unwind": {"path": "$mc_info", "preserveNullAndEmptyArrays": True}})
        
        # 3. Mapping Data sesuai instruksi (Prioritas MC untuk Kubik, CID untuk Tarif)
        pipeline.append({"$addFields": {
            "v_RAYON": "$RAYON",
            "v_PC": {"$substrCP": [{"$ifNull": ["$PCEZ", ""]}, 0, 3]},
            "v_PCEZ": "$PCEZ",
            # Periksa TARIFF (2 F) atau TARIF (1 F) dari CID
            "v_TARIF": { "$ifNull": ["$cid_info.TARIFF", { "$ifNull": ["$cid_info.TARIF", { "$ifNull": ["$mc_info.TARIF", "$TIPEPLGGN"] }] }] },
            "v_KUBIK": { "$toDouble": { "$ifNull": ["$mc_info.KUBIK", { "$ifNull": ["$VOLUME", 0] }] } },
            "v_NOMINAL": { "$toDouble": { "$ifNull": ["$JUMLAH", 0] } }
        }})
        
        val_field, usage_field = "$v_NOMINAL", "$v_KUBIK"
        final_group_tarif = "$v_TARIF"
        
    else:
        source_col = collections['mc']
        match_filter = {"BULAN_TAGIHAN": target_month}
        if report_type == 'COLLECTION': match_filter["STATUS"] = "PAYMENT"
        else: match_filter["STATUS"] = {"$ne": "PAYMENT"}
        
        pipeline.append({"$match": match_filter})
        pipeline.append({"$addFields": {
            "v_RAYON": {"$substrCP": ["$ZONA_NOVAK", 0, 2]},
            "v_PC": {"$substrCP": ["$ZONA_NOVAK", 2, 3]},
            "v_PCEZ": {"$concat": [{"$substrCP": ["$ZONA_NOVAK", 2, 3]}, "/", {"$substrCP": ["$ZONA_NOVAK", 5, 2]}]},
            "v_TARIF": "$TARIF"
        }})
        val_field, usage_field = "$NOMINAL", "$KUBIK"
        final_group_tarif = "$v_TARIF"

    # 4. Mapping Grouping
    g_field = group_field.upper()
    if g_field in ["MERK", "READ_METHOD"]:
        pipeline.append({
            "$lookup": {
                "from": "CustomerData",
                "localField": "NOMEN",
                "foreignField": "NOMEN",
                "as": "cust"
            }
        })
        pipeline.append({"$unwind": {"path": "$cust", "preserveNullAndEmptyArrays": True}})
        mapped_id_field = f"$cust.{g_field}"
    else:
        field_map = {"RAYON": "$v_RAYON", "PC": "$v_PC", "PCEZ": "$v_PCEZ", "TARIF": final_group_tarif}
        mapped_id_field = field_map.get(g_field, f"${group_field}")

    # 5. Agregasi Pengelompokan
    pipeline.append({
        "$group": {
            "_id": {"val": mapped_id_field, "rayon": "$v_RAYON"},
            "unique_nomen_set": {"$addToSet": "$NOMEN"},
            "total_piutang": {"$sum": {"$toDouble": { "$ifNull": [val_field, 0] }}},
            "total_kubikasi": {"$sum": {"$toDouble": { "$ifNull": [usage_field, 0] }}}
        }
    })

    # 6. Proyeksi Akhir
    pipeline.append({
        "$project": {
            "_id": 0,
            "id_value": { "$ifNull": ["$_id.val", "N/A"] },
            "rayon_origin": "$_id.rayon",
            "total_nomen": {"$size": "$unique_nomen_set"},
            "total_piutang": 1,
            "total_kubikasi": 1
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
    """Update KPI Header dengan perbaikan join untuk Tunggakan AR."""
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
                "usage": {"$sum": {"$toDouble": { "$ifNull": ["$KUBIK", 0] }}},
                "nominal": {"$sum": {"$toDouble": { "$ifNull": ["$NOMINAL", 0] }}},
                "unique_nomen": {"$addToSet": "$NOMEN"}
            }},
            {"$project": {"_id": 0, "count": {"$size": "$unique_nomen"}, "usage": 1, "nominal": 1}}
        ]
        res = list(col_mc.aggregate(pipeline))
        return res[0] if res else {"count": 0, "usage": 0, "nominal": 0}

    def get_ar_summary():
        # Join AR ke MC untuk mendapatkan Kubikasi penunggak bulan tersebut (Support padding)
        pipeline = [
            {
                "$lookup": {
                    "from": "MasterCetak",
                    "let": {"n_ar": { "$toString": "$NOMEN" }, "p_ar": "$PERIODE_BILL"},
                    "pipeline": [
                        {"$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": [{ "$toString": "$NOMEN" }, "$$n_ar"]},
                                    {"$regexMatch": {
                                        "input": "$BULAN_TAGIHAN",
                                        "regex": {"$concat": ["^", {"$cond": [{"$lt": [{"$strLenCP": {"$toString": "$$p_ar"}}, 2]}, {"$concat": ["0", {"$toString": "$$p_ar"}]}, {"$toString": "$$p_ar"}]}]}
                                    }}
                                ]
                            }
                        }}
                    ],
                    "as": "m"
                }
            },
            {"$unwind": {"path": "$m", "preserveNullAndEmptyArrays": True}},
            {
                "$group": {
                    "_id": None,
                    "nominal": {"$sum": {"$toDouble": { "$ifNull": ["$JUMLAH", 0] }}},
                    "usage": {"$sum": {"$toDouble": { "$ifNull": ["$m.KUBIK", 0] }}},
                    "unique_nomen": {"$addToSet": "$NOMEN"}
                }
            },
            {"$project": {"_id": 0, "count": {"$size": "$unique_nomen"}, "usage": 1, "nominal": 1}}
        ]
        res = list(col_ar.aggregate(pipeline))
        return res[0] if res else {"count": 0, "usage": 0, "nominal": 0}

    return jsonify({
        "piutang": get_mc_summary({"BULAN_TAGIHAN": formatted_period, "STATUS": {"$ne": "PAYMENT"}}),
        "collection": get_mc_summary({"BULAN_TAGIHAN": formatted_period, "STATUS": "PAYMENT"}),
        "tunggakan": get_ar_summary()
    })

@bp_collection.route("/api/distribution/<category>")
@login_required
def category_distribution_api(category):
    raw_period = request.args.get('period')
    report_type = request.args.get('type', 'PIUTANG').upper()
    results, month = _get_distribution_report(category, period=raw_period, report_type=report_type)
    
    total_p = sum(item['total_piutang'] for item in results) or 1
    for item in results:
        item['pct_piutang'] = (item['total_piutang'] / total_p) * 100
        
    return jsonify({
        "data": results, 
        "title": f"Kontributor {category.upper()}", 
        "subtitle": f"Bulan: {month} ({report_type})"
    })

# --- Rute View & Lainnya tetap sama ---
@bp_collection.route('/laporan', methods=['GET'])
@login_required 
def collection_laporan_view():
    raw_period = request.args.get('period', datetime.now().strftime('%Y-%m'))
    return render_template('collection_summary.html', title="Laporan Piutang & Koleksi", period=raw_period, is_admin=current_user.is_admin)

@bp_collection.route('/analisis', methods=['GET'])
@login_required 
def collection_analisis_view():
    return render_template('collection_analysis.html', title="Analisis Kontributor", is_admin=current_user.is_admin)

@bp_collection.route('/api/mom_comparison_report', methods=['GET'])
@login_required
def mom_comparison_report_api():
    return jsonify({'status': 'success', 'data': []})

@bp_collection.route('/api/top_debtors_report', methods=['GET'])
@login_required 
def top_debtors_report_api():
    return jsonify({'status': 'success', 'data': []})
