import os
import io
import pandas as pd
from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime
from dotenv import load_dotenv

# Memuat variabel lingkungan dari file .env
load_dotenv()

# Import fungsi-fungsi dari utils.py
from utils import (
    init_db, 
    clean_dataframe, 
    analyze_meter_anomalies, 
    get_summarized_report,
    get_collection_detailed_analysis,
    get_customer_payment_status,
    get_usage_history,
    get_payment_history,
    get_audit_detective_data,
    save_manual_audit,
    get_top_100_premium,
    get_top_100_unpaid_current,
    get_top_100_debt,
    get_top_100_unpaid_debt
)

app = Flask(__name__)
# Mengizinkan CORS agar frontend (React/Vite) bisa mengakses API
CORS(app)

# Inisialisasi Database saat aplikasi dimulai
with app.app_context():
    try:
        init_db()
    except Exception as e:
        print(f"Warning: Database initialization failed: {e}")

# --- 1. STATUS ENDPOINT ---

@app.route('/api/status', methods=['GET'])
def get_status():
    """Cek status koneksi server dan database"""
    return jsonify({
        "status": "online", 
        "database": "connected" if os.getenv("MONGO_URI") else "config_missing",
        "timestamp": datetime.now().isoformat(),
        "version": "2.0 - Enhanced"
    })

# --- 2. UPLOAD & ANALYZE ---

@app.route('/api/upload-and-analyze', methods=['POST'])
def upload_and_analyze():
    """
    Endpoint serbaguna: Terima file, deteksi jenis data, dan langsung berikan hasil analisa
    """
    if 'file' not in request.files:
        return jsonify({"status": "error", "message": "File tidak ditemukan dalam permintaan."}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({"status": "error", "message": "Nama file tidak valid."}), 400

    try:
        filename_upper = file.filename.upper()
        
        # Membaca Data (CSV, TXT, atau Excel)
        if filename_upper.endswith('.CSV') or filename_upper.endswith('.TXT'):
            content = file.read().decode('utf-8', errors='ignore')
            # Deteksi delimiter otomatis
            delimiter = '|' if '|' in content[:1000] else (';' if ';' in content[:1000] else ',')
            df = pd.read_csv(io.StringIO(content), sep=delimiter, engine='python')
        else:
            df = pd.read_excel(file)

        # Standarisasi Header ke uppercase
        df.columns = [str(col).strip().upper() for col in df.columns]

        # --- DETEKSI JENIS DATA & PROSES ---

        # 1. METER READING (SBRS)
        if 'CMR_ACCOUNT' in df.columns or 'CMR_READING' in df.columns:
            df_cleaned = pd.DataFrame(clean_dataframe(df))
            anomalies = analyze_meter_anomalies(df_cleaned)
            
            return jsonify({
                "status": "success",
                "type": "METER_READING",
                "filename": file.filename,
                "data": {
                    "anomalies": anomalies,
                    "total_records": len(df_cleaned),
                    "summary": {
                        "extreme": len([a for a in anomalies if "EKSTRIM" in a['status']]),
                        "negative": len([a for a in anomalies if "STAND NEGATIF" in a['status']]),
                        "zero": len([a for a in anomalies if "PEMAKAIAN ZERO" in a['status']]),
                        "decrease": len([a for a in anomalies if "PEMAKAIAN TURUN" in a['status']]),
                        "wrong_record": len([a for a in anomalies if "SALAH CATAT" in a['status']]),
                        "rebill": len([a for a in anomalies if "INDIKASI REBILL" in a['status']]),
                        "estimate": len([a for a in anomalies if "ESTIMASI" in a['status']]),
                        "meter_issue": len([a for a in anomalies if "METER ISSUE" in a['status']])
                    }
                }
            })

        # 2. BILLING SUMMARY (MC/MB/ARDEBT/MAINBILL)
        elif 'NOMEN' in df.columns and ('NOMINAL' in df.columns or 'JUMLAH' in df.columns):
            df_cleaned = pd.DataFrame(clean_dataframe(df))
            val_col = 'NOMINAL' if 'NOMINAL' in df_cleaned.columns else 'JUMLAH'
            
            summary_rayon = df_cleaned.groupby('RAYON')[val_col].sum().to_dict()
            
            return jsonify({
                "status": "success",
                "type": "BILLING_SUMMARY",
                "filename": file.filename,
                "data": {
                    "total_nominal": float(df_cleaned[val_col].sum()),
                    "total_volume": float(df_cleaned['KUBIK'].sum() if 'KUBIK' in df_cleaned.columns else 0),
                    "by_rayon": {k: round(v/1000000, 2) for k, v in summary_rayon.items()},
                    "total_records": len(df_cleaned)
                }
            })

        # 3. COLLECTION REPORT
        elif 'AMT_COLLECT' in df.columns:
            df_cleaned = pd.DataFrame(clean_dataframe(df))
            return jsonify({
                "status": "success",
                "type": "COLLECTION_REPORT",
                "filename": file.filename,
                "data": {
                    "total_cash": round(float(df_cleaned['AMT_COLLECT'].sum()) / 1000000, 2),
                    "total_vol": float(df_cleaned['VOL_COLLECT'].sum() if 'VOL_COLLECT' in df_cleaned.columns else 0),
                    "status_split": {k: round(v/1000000, 2) for k, v in df_cleaned.groupby('STATUS')['AMT_COLLECT'].sum().to_dict().items()}
                }
            })

        return jsonify({"status": "error", "message": "Format data tidak dikenali oleh sistem DSS."}), 400

    except Exception as e:
        return jsonify({"status": "error", "message": f"Kesalahan Sistem: {str(e)}"}), 500

# --- 3. SUMMARIZING ENDPOINTS ---

@app.route('/api/summary', methods=['GET'])
def api_get_summary():
    """Summarizing dengan multi-dimensi (RAYON, PC, TARIF, dll)"""
    target = request.args.get('target', 'mc').lower()
    dimension = request.args.get('dimension', 'RAYON').upper()
    rayon_filter = request.args.get('rayon', None)
    
    try:
        data = get_summarized_report(target, dimension, rayon_filter)
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# --- 4. COLLECTION ANALYSIS ---

@app.route('/api/collection/detailed', methods=['GET'])
def api_get_collection_detailed():
    """Analisa penagihan mendalam"""
    rayon = request.args.get('rayon', None)
    try:
        data = get_collection_detailed_analysis(rayon)
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/collection/payment-status', methods=['GET'])
def api_get_payment_status():
    """Status pembayaran pelanggan (tunggakan vs lancar)"""
    rayon = request.args.get('rayon', None)
    try:
        data = get_customer_payment_status(rayon)
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# --- 5. HISTORY ENDPOINTS ---

@app.route('/api/history/usage', methods=['GET'])
def api_get_usage_history():
    """Riwayat pemakaian kubikasi"""
    dimension = request.args.get('dimension', 'CUSTOMER').upper()
    identifier = request.args.get('identifier', None)
    rayon = request.args.get('rayon', None)
    months = int(request.args.get('months', 12))
    
    try:
        data = get_usage_history(dimension, identifier, rayon, months)
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/history/payment', methods=['GET'])
def api_get_payment_hist():
    """Riwayat pembayaran pelanggan"""
    nomen = request.args.get('nomen', None)
    payment_type = request.args.get('type', 'ALL').upper()
    rayon = request.args.get('rayon', None)
    months = int(request.args.get('months', 12))
    
    try:
        data = get_payment_history(nomen, payment_type, rayon, months)
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# --- 6. TOP 100 ENDPOINTS ---

@app.route('/api/top100/premium', methods=['GET'])
def api_top_premium():
    rayon = request.args.get('rayon', '34')
    try:
        data = get_top_100_premium(rayon)
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/top100/unpaid-current', methods=['GET'])
def api_top_unpaid_current():
    rayon = request.args.get('rayon', '34')
    try:
        data = get_top_100_unpaid_current(rayon)
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/top100/debt', methods=['GET'])
def api_top_debt():
    rayon = request.args.get('rayon', '34')
    try:
        data = get_top_100_debt(rayon)
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/top100/unpaid-debt', methods=['GET'])
def api_top_unpaid_debt():
    rayon = request.args.get('rayon', '34')
    try:
        data = get_top_100_unpaid_debt(rayon)
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# --- 7. DETECTIVE & AUDIT ---

@app.route('/api/detective/<nomen>', methods=['GET'])
def api_get_detective(nomen):
    """Data lengkap untuk investigasi pelanggan tertentu"""
    try:
        data = get_audit_detective_data(nomen)
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/audit/save', methods=['POST'])
def api_save_audit():
    """Menyimpan hasil audit manual"""
    req = request.json
    try:
        result = save_manual_audit(
            req.get('nomen'), 
            req.get('remark'), 
            req.get('user', 'ADMIN_PAM'), 
            req.get('status', 'AUDITED')
        )
        if result:
            return jsonify({"status": "success", "message": "Hasil analisa berhasil disimpan."})
        else:
            return jsonify({"status": "error", "message": "Gagal menyimpan data."}), 500
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# --- 8. UTILITY ENDPOINTS ---

@app.route('/api/dimensions', methods=['GET'])
def api_get_dimensions():
    """Mendapatkan daftar dimensi yang tersedia di sistem"""
    return jsonify({
        "status": "success",
        "dimensions": {
            "summarizing": ["RAYON", "PC", "PCEZ", "TARIF", "METER"],
            "history": ["CUSTOMER", "RAYON", "PC", "PCEZ", "TARIF", "METER"],
            "collections": ["mc", "mb", "ardebt", "mainbill", "coll"]
        }
    })

if __name__ == '__main__':
    # Konfigurasi Port untuk VPS
    port = int(os.environ.get("PORT", 5000))
    print(f"PAM DSS Backend Version 2.0 running on port {port}...")
    # debug=False untuk lingkungan produksi di VPS
    app.run(host='0.0.0.0', port=port, debug=False)
