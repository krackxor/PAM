from flask import Blueprint, request, jsonify, render_template, url_for, flash, redirect
from flask_login import login_required, current_user
from functools import wraps
from utils import get_db_status, _get_sbrs_anomalies

# Definisikan Blueprint dengan prefix /meter_reading
bp_meter_reading = Blueprint('bp_meter_reading', __name__, url_prefix='/meter_reading')

# --- Middleware Dekorator untuk Cek Admin (Wajib di Blueprint jika diperlukan) ---
def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.is_admin:
            flash('Anda tidak memiliki izin (Admin) untuk mengakses halaman ini.', 'danger')
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function

# --- RUTE VIEW FRONTEND ---

@bp_meter_reading.route('/laporan', methods=['GET'])
@login_required 
def meter_reading_report_view():
    """Menampilkan halaman utama Laporan Baca Meter."""
    return render_template('meter_reading_report.html', 
                           title="Laporan Baca Meter SBRS",
                           description="Ringkasan data SBRS, Tren Baca Meter, dan Ringkasan Anomali AB SUNTER.",
                           is_admin=current_user.is_admin)

@bp_meter_reading.route('/analisis', methods=['GET'])
@login_required 
def meter_reading_analysis_view():
    """Menampilkan halaman Analisis Anomali Baca Meter."""
    return render_template('meter_reading_analysis.html', 
                           title="Analisis Anomali SBRS",
                           description="Laporan detail anomali pemakaian air (Naik Ekstrim, Turun Drastis, Zero Usage) berdasarkan data SBRS terbaru.",
                           is_admin=current_user.is_admin)

# --- API REPORTING ---

@bp_meter_reading.route("/api/anomalies", methods=['GET'])
@login_required
def get_anomalies_api():
    """API untuk mendapatkan daftar anomali pemakaian air pelanggan."""
    db_status = get_db_status()
    if db_status['status'] == 'error': 
        return jsonify({"message": db_status['message']}), 500
    
    collections = db_status['collections']

    try:
        # Mengambil data anomali melalui helper utils
        anomalies_data = _get_sbrs_anomalies(collections['sbrs'], collections['cid'])
        
        # Tambahkan skema kolom untuk tampilan tabel dinamis di frontend
        anomaly_schema = [
            {'key': 'NOMEN', 'label': 'No. Pelanggan', 'type': 'string', 'is_main_key': True},
            {'key': 'NAMA', 'label': 'Nama', 'type': 'string'},
            {'key': 'RAYON', 'label': 'Rayon', 'type': 'string'},
            {'key': 'TARIF', 'label': 'Tarif', 'type': 'string'},
            {'key': 'KUBIK_TERBARU', 'label': 'Kubik Baru (m³)', 'type': 'integer', 'unit': 'm³'},
            {'key': 'KUBIK_SEBELUMNYA', 'label': 'Kubik Lama (m³)', 'type': 'integer', 'unit': 'm³'},
            {'key': 'PERSEN_SELISIH', 'label': 'Persen Selisih (%)', 'type': 'percent'},
            {'key': 'STATUS_PEMAKAIAN', 'label': 'Status Anomali', 'type': 'string'},
            {'key': 'AB_SUNTER', 'label': 'Status AB Sunter', 'type': 'string'},
        ]
        
        # Tambahkan metadata visual (Chart Label) untuk frontend
        for item in anomalies_data:
            item['chart_label'] = f"R:{item.get('RAYON', '??')} - {item.get('NOMEN', 'N/A')}"
            item['chart_data_kubik'] = item.get('KUBIK_TERBARU', 0)

        return jsonify({
            'status': 'success', 
            'data': anomalies_data, 
            'schema': anomaly_schema, 
            'title': "Detail Anomali Pemakaian Air", 
            'subtitle': "Data anomali SBRS terbaru (AB SUNTER Analytic)"
        }), 200

    except Exception as e:
        print(f"Error fetching anomalies: {e}")
        return jsonify({"status": 'error', "message": f"Gagal mengambil laporan anomali: {str(e)}"}), 500

@bp_meter_reading.route("/api/monthly_read_trend", methods=['GET'])
@login_required
def get_monthly_read_trend_api():
    """API untuk mendapatkan tren jumlah pembacaan meter per bulan."""
    db_status = get_db_status()
    if db_status['status'] == 'error': 
        return jsonify({"message": db_status['message']}), 500
    
    collection_sbrs = db_status['collections']['sbrs']
    
    try:
        # Agregasi data berdasarkan bulan pembacaan
        pipeline = [
            {'$project': {
                '_id': 0,
                'CMR_RD_DATE': 1,
                'READ_MONTH': {'$substr': ['$CMR_RD_DATE', 0, 7]} # Ekstrak YYYY-MM
            }},
            {'$group': {
                '_id': '$READ_MONTH',
                'TotalReads': {'$sum': 1}
            }},
            {'$sort': {'_id': 1}} # Urutkan kronologis
        ]
        
        trend_data = list(collection_sbrs.aggregate(pipeline))
        
        # Format output khusus untuk konsumsi library Chart.js di frontend
        formatted_trend = []
        for item in trend_data:
            if item['_id']: # Pastikan ada tanggalnya
                formatted_trend.append({
                    'bulan': item['_id'],
                    'jumlah': item['TotalReads']
                })
            
        return jsonify({
            'status': 'success', 
            'data': formatted_trend, 
            'title': "Tren Jumlah Baca Meter Bulanan", 
            'metric_label': "Jumlah Pembacaan (SBRS)"
        }), 200
        
    except Exception as e:
        print(f"Error fetching monthly read trend: {e}")
        return jsonify({'status': 'error', 'message': f"Gagal mengambil tren baca meter: {str(e)}"}), 500
