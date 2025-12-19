import os
from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
from datetime import datetime

# Import fungsi dari utils.py yang telah kita buat sebelumnya
from utils import (
    init_db, 
    get_summarized_report, 
    get_collection_analysis, 
    analyze_meter_anomalies, 
    get_top_100, 
    get_audit_detective_data, 
    save_manual_audit
)

app = Flask(__name__)
# Mengizinkan akses dari frontend (React)
CORS(app)

# Inisialisasi database saat aplikasi dimulai
with app.app_context():
    init_db()

# --- 1. ENDPOINT UNTUK SUMMARIZING (MC, MB, ARDEBT, COLL, MAINBILL) ---

@app.route('/api/summary', methods=['GET'])
def api_summary():
    """
    Mengambil ringkasan data berdasarkan dimensi: 
    target: mc, mb, ardebt, coll, mainbill
    dimension: RAYON, PC, PCEZ, TARIF, METER
    """
    target = request.args.get('target', 'mc').lower()
    dimension = request.args.get('dimension', 'RAYON').upper()
    
    try:
        data = get_summarized_report(target, dimension)
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# --- 2. ENDPOINT UNTUK ANALISIS COLLECTION (UNDUE & CURRENT) ---

@app.route('/api/collection/analysis', methods=['GET'])
def api_collection_analysis():
    """Mengambil statistik arus kas (Undue vs Current)."""
    try:
        data = get_collection_analysis()
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# --- 3. ENDPOINT UNTUK METER READING (DETEKSI ANOMALI) ---

@app.route('/api/meter-reading/anomalies', methods=['POST'])
def api_meter_anomalies():
    """
    Menerima file SBRS atau menganalisis data SBRS yang ada di DB 
    untuk mencari anomali (Extreme, Zero, Negatif, dll).
    """
    # Jika ada file yang di-upload
    if 'file' in request.files:
        file = request.files['file']
        try:
            # Baca file (asumsi CSV atau Excel)
            if file.filename.endswith('.csv'):
                df = pd.read_csv(file, sep=';' if 'customer' in file.filename else ',')
            else:
                df = pd.read_excel(file)
            
            # Jalankan analisa detektif otomatis
            results = analyze_meter_anomalies(df)
            return jsonify({"status": "success", "data": results})
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)}), 400
    
    return jsonify({"status": "error", "message": "File SBRS tidak ditemukan"}), 400

# --- 4. ENDPOINT UNTUK TOP 100 ANALYTICS ---

@app.route('/api/top100', methods=['GET'])
def api_top_100():
    """
    Mengambil daftar Top 100: 
    category: PREMIUM, TUNGGAKAN, UNPAID_CURRENT, dll
    rayon: 34 atau 35
    """
    category = request.args.get('category', 'PREMIUM').upper()
    rayon = request.args.get('rayon', '34')
    
    try:
        data = get_top_100(category, rayon)
        # Menghapus ID MongoDB agar bisa di-JSON-kan
        for item in data: item.pop('_id', None)
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# --- 5. ENDPOINT DETEKTIF (HISTORY & AUDIT MANUAL) ---

@app.route('/api/detective/<nomen>', methods=['GET'])
def api_detective_history(nomen):
    """Mengambil history lengkap 12 bulan untuk analisa manual tim."""
    try:
        data = get_audit_detective_data(nomen)
        # Bersihkan data customer dari ID MongoDB
        if data['customer']: data['customer'].pop('_id', None)
        for log in data['audit_logs']: log.pop('_id', None)
            
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/audit/save', methods=['POST'])
def api_save_audit():
    """Menyimpan keterangan analisa manual dari tim audit."""
    req_data = request.json
    nomen = req_data.get('nomen')
    remark = req_data.get('remark')
    user = req_data.get('user', 'SYSTEM_USER')
    status = req_data.get('status', 'AUDITED')
    
    if not nomen or not remark:
        return jsonify({"status": "error", "message": "Nomen dan Keterangan wajib diisi"}), 400
        
    try:
        success = save_manual_audit(nomen, remark, user, status)
        if success:
            return jsonify({"status": "success", "message": "Analisa manual berhasil disimpan"})
        return jsonify({"status": "error", "message": "Gagal menyimpan analisa"}), 500
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# --- RUN APP ---

if __name__ == '__main__':
    # Pastikan MONGO_URI sudah diatur di environment
    app.run(debug=True, host='0.0.0.0', port=5000)
