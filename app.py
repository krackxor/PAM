import os
import io
import pandas as pd
from flask import Flask, request, jsonify, render_template_string
from flask_cors import CORS
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Import fungsi logika dari utils (Pastikan utils.py Anda mendukung fungsi-fungsi ini)
# Jika ada fungsi yang belum ada di utils.py, Anda perlu menambahkannya atau menyesuaikan import ini.
from utils import (
    init_db, clean_dataframe, analyze_meter_anomalies, get_summarized_report,
    get_collection_detailed_analysis, get_customer_payment_status,
    get_usage_history, get_payment_history, get_audit_detective_data,
    save_manual_audit, get_top_100_premium, get_top_100_unpaid_current,
    get_top_100_debt, get_top_100_unpaid_debt
)

app = Flask(__name__)
CORS(app)

# --- DASHBOARD HTML (Tampilan Server) ---
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="id">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PAM DSS Server 2.0</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <style>body { background-color: #0f172a; color: white; font-family: sans-serif; }</style>
</head>
<body class="flex items-center justify-center h-screen">
    <div class="text-center p-10 bg-slate-800 rounded-3xl shadow-2xl max-w-lg border border-slate-700">
        <h1 class="text-4xl font-black text-blue-500 mb-2">PAM DSS SERVER</h1>
        <div class="text-xs font-mono text-slate-400 mb-8">API Gateway & Data Processing Unit</div>
        <div class="space-y-4 text-left bg-slate-900 p-6 rounded-xl border border-slate-700 text-sm mb-8">
            <div class="flex justify-between"><span>Status:</span> <span class="text-emerald-400 font-bold">ONLINE</span></div>
            <div class="flex justify-between"><span>Port:</span> <span class="text-blue-400">5000</span></div>
            <div class="flex justify-between"><span>Database:</span> <span class="text-orange-400">MongoDB Ready</span></div>
        </div>
        <a href="/api" class="inline-block bg-blue-600 hover:bg-blue-500 text-white font-bold py-3 px-8 rounded-full transition-all">Lihat Endpoint API</a>
    </div>
</body>
</html>
"""

# --- ROUTES ---

@app.route('/')
def index():
    return render_template_string(DASHBOARD_HTML)

@app.route('/api')
def api_info():
    return jsonify({
        "version": "2.2",
        "features": [
            "Summarizing (MC, MB, ARDEBT, MAINBILL, COLLECTION)",
            "Meter Analysis (Extreme, Zero, Negative, Rebill, etc)",
            "Collection Analysis (Undue, Current, Arrears)",
            "Top 100 Rankings",
            "History Tracking"
        ]
    })

# 1. STATUS
@app.route('/api/status', methods=['GET'])
def get_status():
    return jsonify({"status": "online", "timestamp": datetime.now().isoformat()})

# 2. UPLOAD & ANALYZE (INTI ANALISA)
@app.route('/api/upload-and-analyze', methods=['POST'])
def upload_and_analyze():
    if 'file' not in request.files:
        return jsonify({"status": "error", "message": "File wajib diunggah"}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({"status": "error", "message": "Nama file kosong"}), 400

    try:
        # Baca File
        filename = file.filename.upper()
        if filename.endswith('.CSV') or filename.endswith('.TXT'):
            content = file.read().decode('utf-8', errors='ignore')
            delimiter = '|' if '|' in content[:1000] else (';' if ';' in content[:1000] else ',')
            df = pd.read_csv(io.StringIO(content), sep=delimiter, engine='python')
        else:
            df = pd.read_excel(file)
            
        # Standarisasi Header
        df.columns = [str(col).strip().upper() for col in df.columns]
        
        # LOGIKA DETEKSI TIPE DATA
        
        # A. METER READING (SBRS)
        if 'CMR_READING' in df.columns or 'CMR_ACCOUNT' in df.columns:
            df_clean = pd.DataFrame(clean_dataframe(df))
            anomalies = analyze_meter_anomalies(df_clean)
            
            # Hitung ringkasan anomali untuk dashboard
            summary_stats = {
                "extreme": len([x for x in anomalies if "EKSTRIM" in x['status']]),
                "negative": len([x for x in anomalies if "STAND NEGATIF" in x['status']]),
                "zero": len([x for x in anomalies if "PEMAKAIAN ZERO" in x['status']]),
                "decrease": len([x for x in anomalies if "PEMAKAIAN TURUN" in x['status']]),
                "wrong_record": len([x for x in anomalies if "SALAH CATAT" in x['status']]),
                "rebill": len([x for x in anomalies if "INDIKASI REBILL" in x['status']]),
                "estimate": len([x for x in anomalies if "ESTIMASI" in x['status']])
            }
            
            return jsonify({
                "status": "success", "type": "METER_READING",
                "filename": file.filename,
                "data": { "anomalies": anomalies, "summary": summary_stats }
            })

        # B. BILLING/COLLECTION SUMMARY (MC, MB, ARDEBT, MAINBILL, COLL)
        # Mendukung kolom NOMEN, NOMINAL/JUMLAH, dll
        elif 'NOMEN' in df.columns or 'AMT_COLLECT' in df.columns:
            df_clean = pd.DataFrame(clean_dataframe(df))
            
            # Tentukan kolom nilai uang
            if 'AMT_COLLECT' in df_clean.columns:
                val_col = 'AMT_COLLECT'
                tipe = "COLLECTION_REPORT"
            elif 'NOMINAL' in df_clean.columns:
                val_col = 'NOMINAL'
                tipe = "BILLING_SUMMARY"
            elif 'JUMLAH' in df_clean.columns:
                val_col = 'JUMLAH'
                tipe = "BILLING_SUMMARY"
            else:
                val_col = None

            total_nominal = float(df_clean[val_col].sum()) if val_col else 0
            total_volume = float(df_clean['KUBIK'].sum()) if 'KUBIK' in df_clean.columns else 0
            
            return jsonify({
                "status": "success", "type": tipe,
                "filename": file.filename,
                "data": { "total_nominal": total_nominal, "total_volume": total_volume, "records": len(df_clean) }
            })

        return jsonify({"status": "error", "message": "Format kolom tidak dikenali"}), 400

    except Exception as e:
        return jsonify({"status": "error", "message": f"Server Error: {str(e)}"}), 500

# 3. SUMMARIZING REPORT
@app.route('/api/summary', methods=['GET'])
def api_summary():
    # Target: mc, mb, ardebt, mainbill, collection
    target = request.args.get('target', 'mc').lower()
    # Dimension: RAYON, PC, PCEZ, TARIF, METER
    dimension = request.args.get('dimension', 'RAYON').upper()
    rayon_filter = request.args.get('rayon', None) # Opsional filter rayon
    
    try:
        data = get_summarized_report(target, dimension, rayon_filter)
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# 4. COLLECTION ANALYSIS
@app.route('/api/collection/status', methods=['GET'])
def api_collection_status():
    rayon = request.args.get('rayon', None)
    try:
        # Mengembalikan statistik: Undue, Current, Arrears, Unpaid Receivable (No Arrears), dll
        data = get_customer_payment_status(rayon)
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# 5. HISTORY
@app.route('/api/history', methods=['GET'])
def api_history():
    # Type: usage, payment
    hist_type = request.args.get('type', 'usage')
    # Filter: customer (nomen), rayon, pc, pcez, tarif, meter
    filter_by = request.args.get('filter_by', 'CUSTOMER').upper()
    filter_val = request.args.get('value', None)
    
    try:
        if hist_type == 'usage':
            data = get_usage_history(filter_by, filter_val)
        else:
            data = get_payment_history(filter_val) # Asumsi filter_val adalah nomen untuk payment
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# 6. TOP 100
@app.route('/api/top100', methods=['GET'])
def api_top100():
    # Category: premium, unpaid_current, debt, unpaid_debt
    category = request.args.get('category', 'debt')
    # Rayon: 34, 35
    rayon = request.args.get('rayon', '34')
    
    try:
        if category == 'premium':
            data = get_top_100_premium(rayon)
        elif category == 'unpaid_current':
            data = get_top_100_unpaid_current(rayon)
        elif category == 'debt':
            data = get_top_100_debt(rayon)
        elif category == 'unpaid_debt':
            data = get_top_100_unpaid_debt(rayon)
        else:
            data = []
            
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# 7. DETECTIVE (DETAIL PELANGGAN)
@app.route('/api/detective/<nomen>', methods=['GET'])
def api_detective(nomen):
    try:
        data = get_audit_detective_data(nomen)
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# 8. AUDIT SAVE
@app.route('/api/audit/save', methods=['POST'])
def api_audit_save():
    req = request.json
    try:
        save_manual_audit(req.get('nomen'), req.get('remark'), req.get('user', 'ADMIN'), req.get('status'))
        return jsonify({"status": "success"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
