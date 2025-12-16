from flask import Blueprint, request, jsonify, render_template, url_for, flash, current_app, make_response
from flask_login import login_required, current_user
from werkzeug.security import check_password_hash
from functools import wraps
from pymongo.errors import BulkWriteError
import pandas as pd
import io
import re
from datetime import datetime, timedelta
from utils import get_db_status, _get_previous_month_year, _get_day_n_ago, _parse_zona_novak, _generate_distribution_schema, _get_sbrs_anomalies

# Definisikan Blueprint
bp_meter_reading = Blueprint('bp_meter_reading', __name__, url_prefix='/meter_reading')

# --- Middleware Dekorator untuk Cek Admin ---
def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.is_admin:
            flash('Anda tidak memiliki izin (Admin) untuk mengakses halaman ini.', 'danger')
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function

# --- RUTE VIEW FRONTEND (Konsisten UI) ---

# Menu 1: Laporan Pembacaan Meter (Ringkasan KPI)
@bp_meter_reading.route('/laporan', methods=['GET'])
@login_required
def meter_reading_laporan_view():
    return render_template('meter_reading_report.html', 
                           title="Laporan Pembacaan Meter",
                           description="Ringkasan status pembacaan meter, total anomali, dan distribusi metode baca.",
                           is_admin=current_user.is_admin)

# Menu 2: Analisis Meter Reading (Volume Dasar & Metode Baca)
@bp_meter_reading.route('/analisis', methods=['GET'])
@login_required 
def meter_reading_analisis_view():
    return render_template('meter_reading_analysis.html', 
                           title="Analisis Meter Reading",
                           description="Pilih laporan analisis Volume Dasar dan Distribusi Metode Baca berdasarkan dimensi pelanggan.",
                           is_admin=current_user.is_admin)

# Menu 3: Top List Anomali (Anomali Komprehensif)
@bp_meter_reading.route('/top', methods=['GET'])
@login_required 
def meter_reading_top_view():
    return render_template('analysis_report_template.html', 
                            title="Top List Anomali Pemakaian",
                            description="Daftar 100 pelanggan dengan anomali pemakaian (Ekstrim/Zero) yang diperkaya data pelanggan lengkap.",
                            report_type="ANOMALY_COMPREHENSIVE",
                            is_admin=current_user.is_admin,
                            api_endpoint=url_for("bp_meter_reading.anomaly_comprehensive_api"))

# Menu 4: Riwayat Volume (Historis)
@bp_meter_reading.route('/riwayat', methods=['GET'])
@login_required 
def meter_reading_riwayat_view():
    return render_template('analysis_report_template.html', 
                            title="Riwayat Volume Dasar Historis",
                            description="Riwayat volume KUBIK bulanan agregat berdasarkan Rayon dari seluruh data Master Cetak (MC).",
                            report_type="BASIC_VOLUME",
                            is_admin=current_user.is_admin,
                            api_endpoint=url_for("bp_meter_reading.volume_historis_api"))

# --- API REPORTING ---

# API Report: Anomaly Comprehensive
@bp_meter_reading.route("/api/anomaly_comprehensive")
@login_required
def anomaly_comprehensive_api():
    db_status = get_db_status()
    if db_status['status'] == 'error':
        return jsonify({"message": db_status['message']}), 500
        
    collection_sbrs = db_status['collections']['sbrs']
    collection_cid = db_status['collections']['cid']

    try:
        anomalies = _get_sbrs_anomalies(collection_sbrs, collection_cid)
        
        anomaly_schema = [
            {'key': 'NOMEN', 'label': 'No. Pelanggan', 'type': 'string', 'is_main_key': True},
            {'key': 'NAMA', 'label': 'Nama Pelanggan', 'type': 'string'},
            {'key': 'STATUS_PEMAKAIAN', 'label': 'Status Anomali', 'type': 'string'},
            {'key': 'KUBIK_TERBARU', 'label': 'Kubik Terakhir (m³)', 'type': 'integer', 'unit': 'm³'},
            {'key': 'KUBIK_SEBELUMNYA', 'label': 'Kubik Sebelumnya (m³)', 'type': 'integer', 'unit': 'm³'},
            {'key': 'PERSEN_SELISIH', 'label': 'Selisih (%)', 'type': 'percent'},
            {'key': 'RAYON', 'label': 'Rayon', 'type': 'string'},
            {'key': 'PCEZ', 'label': 'PCEZ', 'type': 'string'},
            {'key': 'TARIF', 'label': 'Tarif', 'type': 'string'},
            {'key': 'MERK', 'label': 'Merek Meter', 'type': 'string'},
            {'key': 'CYCLE', 'label': 'Cycle/Bookwalk', 'type': 'string'},
            {'key': 'AB_SUNTER', 'label': 'AB Sunter', 'type': 'string'},
        ]
        
        for item in anomalies:
            item['chart_label'] = item['NOMEN']
            item['chart_data_piutang'] = item['KUBIK_TERBARU']
            
        return jsonify({
            'status': 'success',
            'data': anomalies,
            'schema': anomaly_schema,
            'title': "Top Anomali Pemakaian Air",
            'subtitle': f"Menampilkan {len(anomalies)} Anomali Terbesar (Ekstrim dan Zero)."
        }), 200

    except Exception as e:
        print(f"Error saat mengambil anomali komprehensif: {e}")
        return jsonify({"status": 'error', "message": f"Gagal mengambil laporan anomali: {e}"}), 500

# API Report: Volume Historis
@bp_meter_reading.route("/api/volume_historis")
@login_required
def volume_historis_api():
    db_status = get_db_status()
    if db_status['status'] == 'error':
        return jsonify({"message": db_status['message']}), 500
        
    collection_mc = db_status['collections']['mc']

    try:
        pipeline = [
            {'$project': {
                'BULAN_TAGIHAN': 1,
                'RAYON': 1,
                'KUBIK': {'$toDouble': {'$cond': [{'$ne': ['$KUBIK', None]}, '$KUBIK', 0]}}, 
            }},
            {'$group': {
                '_id': {'bulan': '$BULAN_TAGIHAN', 'rayon': '$RAYON'},
                'TotalKubikasi': {'$sum': '$KUBIK'}
            }},
            {'$project': {
                '_id': 0,
                'BULAN_TAGIHAN': '$_id.bulan',
                'RAYON': '$_id.rayon',
                'TotalKubikasi': {'$round': ['$TotalKubikasi', 0]}
            }},
            {'$sort': {'BULAN_TAGIHAN': -1, 'RAYON': 1}},
            {'$limit': 500}
        ]
        
        results = list(collection_mc.aggregate(pipeline))
        
        historis_schema = [
            {'key': 'BULAN_TAGIHAN', 'label': 'Periode Tagihan', 'type': 'string', 'is_main_key': True},
            {'key': 'RAYON', 'label': 'Rayon', 'type': 'string'},
            {'key': 'TotalKubikasi', 'label': 'Total Kubikasi (m³)', 'type': 'integer', 'unit': 'm³'},
        ]
        
        return jsonify({
            'status': 'success',
            'data': results,
            'schema': historis_schema,
            'title': "Riwayat Volume Kubikasi Bulanan per Rayon",
        }), 200

    except Exception as e:
        print(f"Error saat mengambil volume historis: {e}")
        return jsonify({"status": 'error', "message": f"Gagal mengambil laporan volume historis: {e}"}), 500

# API Report: Distribusi Metode Baca (dipindahkan dari app.py)
@bp_meter_reading.route("/api/read_method_distribution")
@login_required
def read_method_distribution_report():
    db_status = get_db_status()
    if db_status['status'] == 'error':
        return jsonify({"message": db_status['message']}), 500
    
    collection_cid = db_status['collections']['cid']

    try:
        total_nomen = len(collection_cid.distinct('NOMEN'))
        
        pipeline = [
            {'$group': {
                '_id': '$READ_METHOD',
                'total_nomen': {'$sum': 1}
            }},
            {'$project': {
                '_id': 0,
                'READ_METHOD': '$_id',
                'total_nomen': 1,
                'persentase': {'$multiply': [{'$divide': ['$total_nomen', total_nomen]}, 100]}
            }},
            {'$sort': {'total_nomen': -1}}
        ]
        
        results = list(collection_cid.aggregate(pipeline))
        
        if total_nomen == 0:
             results = []
             
        read_method_schema = [
            {'key': 'READ_METHOD', 'label': 'Metode Baca', 'type': 'string', 'is_main_key': True},
            {'key': 'total_nomen', 'label': 'Jumlah Pelanggan', 'type': 'integer', 'chart_key': 'chart_data_nomen'},
            {'key': 'persentase', 'label': 'Persentase Total', 'type': 'percent'},
        ]
        
        for item in results:
            item['chart_label'] = item.get("READ_METHOD", "N/A")
            item['chart_data_piutang'] = item['total_nomen']
            item['persentase'] = round(item['persentase'], 2)

        return jsonify({
            "data": results,
            "schema": read_method_schema,
            "title": f"Distribusi Metode Pembacaan Meter",
            "subtitle": f"Berdasarkan {total_nomen} pelanggan terakhir di Master Data.",
        })

    except Exception as e:
        print(f"Error saat membuat laporan distribusi metode baca: {e}")
        return jsonify({"status": 'error', "message": f"Gagal mengambil laporan distribusi metode baca: {e}"}), 500
