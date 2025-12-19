import os
import io
import pandas as pd
from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime

# Mengimpor logika bisnis dan analitik dari utils.py
from utils import (
    init_db, 
    clean_dataframe, 
    analyze_meter_anomalies, 
    get_summarized_report,
    get_collection_analysis,
    get_audit_detective_data,
    save_manual_audit,
    get_top_100_data
)

app = Flask(__name__)
# CORS diaktifkan agar frontend React bisa mengakses backend ini
CORS(app)

# Inisialisasi database dan indexing saat aplikasi mulai berjalan
with app.app_context():
    init_db()

# --- 1. ENDPOINT UTAMA: UPLOAD & ANALISA INSTAN ---

@app.route('/api/upload-and-analyze', methods=['POST'])
def upload_and_analyze():
    """
    Endpoint serbaguna: Terima file, deteksi jenis data, dan langsung berikan hasil analisa.
    Fitur: Summarizing, Anomali Meter, dan Status Koleksi.
    """
    if 'file' not in request.files:
        return jsonify({"status": "error", "message": "File tidak ditemukan dalam permintaan."}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({"status": "error", "message": "Nama file tidak valid."}), 400

    try:
        filename_upper = file.filename.upper()
        
        # A. Membaca Data (Mendukung CSV, TXT, dan Excel)
        if filename_upper.endswith('.CSV') or filename_upper.endswith('.TXT'):
            # Menangani encoding dan deteksi delimiter (|, ;, ,)
            content = file.read().decode('utf-8', errors='ignore')
            delimiter = '|' if '|' in content[:1000] else (';' if ';' in content[:1000] else ',')
            df = pd.read_csv(io.StringIO(content), sep=delimiter, engine='python')
        else:
            # Membaca file Excel (.xlsx, .xls)
            df = pd.read_excel(file)

        # Standarisasi Header Kolom
        df.columns = [str(col).strip().upper() for col in df.columns]

        # B. DETEKSI OTOMATIS & DISPATCH ANALISA

        # 1. Jika ini data Meter Reading (SBRS / Cycle)
        # Menggunakan field: cmr_account, cmr_reading, cmr_prev_read, dll.
        if 'CMR_ACCOUNT' in df.columns or 'CMR_READING' in df.columns:
            # Normalisasi data untuk kalkulasi angka
            df_cleaned = pd.DataFrame(clean_dataframe(df))
            # Jalankan mesin deteksi anomali (Extreme, Zero, Stand Negatif, Salah Catat, Estimasi, Rebill)
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
                        "wrong_record": len([a for a in anomalies if "SALAH CATAT" in a['status']])
                    }
                }
            })

        # 2. Jika ini data Master/Billing (MC / MB / ARDEBT / MAIN BILL)
        elif 'NOMEN' in df.columns and ('NOMINAL' in df.columns or 'JUMLAH' in df.columns):
            df_cleaned = pd.DataFrame(clean_dataframe(df))
            val_col = 'NOMINAL' if 'NOMINAL' in df_cleaned.columns else 'JUMLAH'
            
            # Berikan ringkasan (Summarizing) instan per Rayon
            summary_rayon = df_cleaned.groupby('RAYON')[val_col].sum().to_dict()
            
            return jsonify({
                "status": "success",
                "type": "BILLING_SUMMARY",
                "filename": file.filename,
                "data": {
                    "total_nominal": float(df_cleaned[val_col].sum()),
                    "total_volume": float(df_cleaned['KUBIK'].sum() if 'KUBIK' in df_cleaned.columns else 0),
                    "by_rayon": summary_rayon,
                    "total_records": len(df_cleaned)
                }
            })

        # 3. Jika ini data Collection (Harian)
        elif 'AMT_COLLECT' in df.columns:
            df_cleaned = pd.DataFrame(clean_dataframe(df))
            # Analisa kategori: Payment (Lancar) vs Cancellation
            return jsonify({
                "status": "success",
                "type": "COLLECTION_REPORT",
                "filename": file.filename,
                "data": {
                    "total_cash": float(df_cleaned['AMT_COLLECT'].sum()),
                    "total_vol": float(df_cleaned['VOL_COLLECT'].sum()),
                    "status_split": df_cleaned.groupby('STATUS')['AMT_COLLECT'].sum().to_dict()
                }
            })

        return jsonify({"status": "error", "message": "Format data tidak dikenali oleh sistem DSS."}), 400

    except Exception as e:
        return jsonify({"status": "error", "message": f"Kesalahan Sistem: {str(e)}"}), 500

# --- 2. ENDPOINT DASHBOARD & ANALITIK ---

@app.route('/api/summary', methods=['GET'])
def api_get_summary():
    """
    Mengambil Laporan Summarizing dari Database.
    Parameter: target (mc, mb, ardebt, coll, mainbill), dimension (RAYON, PC, PCEZ, TARIF, METER)
    """
    target = request.args.get('target', 'mc').lower()
    dimension = request.args.get('dimension', 'RAYON').upper()
    try:
        data = get_summarized_report(target, dimension)
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/collection/analysis', methods=['GET'])
def api_get_coll_analysis():
    """Mendapatkan rincian Collection (Undue vs Current)."""
    try:
        data = get_collection_analysis()
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/top100', methods=['GET'])
def api_get_top100():
    """
    Ranking Top 100 per Rayon.
    Kategori: PREMIUM (Lancar), TUNGGAKAN (Debt)
    """
    category = request.args.get('category', 'PREMIUM').upper()
    rayon = request.args.get('rayon', '34')
    try:
        data = get_top_100_data(category, rayon)
        # Menghapus ID internal MongoDB untuk format JSON yang bersih
        for item in data: item.pop('_id', None)
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# --- 3. ENDPOINT DETEKTIF & AUDIT ---

@app.route('/api/detective/<nomen>', methods=['GET'])
def api_get_detective(nomen):
    """Mengambil history lengkap 12 bulan untuk analisa manual tim."""
    try:
        data = get_audit_detective_data(nomen)
        if data['customer']: data['customer'].pop('_id', None)
        for log in data['audit']: log.pop('_id', None)
        # Tambahkan info tambahan jika diperlukan untuk mendukung analisa
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/audit/save', methods=['POST'])
def api_save_audit():
    """Menyimpan keterangan/remark hasil analisa tim ke database."""
    req = request.json
    try:
        # Menyimpan audit trail agar bisa dilacak siapa yang menganalisa
        save_manual_audit(
            req.get('nomen'), 
            req.get('remark'), 
            req.get('user', 'ADMIN_PAM'), 
            req.get('status', 'AUDITED')
        )
        return jsonify({"status": "success", "message": "Hasil analisa berhasil disimpan."})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# --- MENJALANKAN SERVER ---

if __name__ == '__main__':
    # Server berjalan di port 5000 sebagai default backend
    app.run(debug=True, host='0.0.0.0', port=5000)
