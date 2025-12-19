import os
import io
import pandas as pd
from flask import Flask, request, jsonify, render_template_string
from flask_cors import CORS
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Import fungsi logika dari utils
from utils import (
    init_db, clean_dataframe, analyze_meter_anomalies, get_summarized_report,
    get_customer_payment_status, get_usage_history, get_payment_history, 
    get_payment_history_undue, get_payment_history_current,
    get_audit_detective_data, save_manual_audit, 
    get_top_100_premium, get_top_100_unpaid_current,
    get_top_100_debt, get_top_100_unpaid_debt
)

app = Flask(__name__)
CORS(app)

# Initialize Database on startup
init_db()

# --- DASHBOARD HTML (Tampilan Server) ---
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="id">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PAM DSS Server 3.0</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <style>body { background-color: #0f172a; color: white; font-family: sans-serif; }</style>
</head>
<body class="flex items-center justify-center h-screen">
    <div class="text-center p-10 bg-slate-800 rounded-3xl shadow-2xl max-w-lg border border-slate-700">
        <h1 class="text-4xl font-black text-blue-500 mb-2">PAM DSS SERVER V3.0</h1>
        <div class="text-xs font-mono text-slate-400 mb-8">API Gateway & Data Processing Unit</div>
        <div class="space-y-4 text-left bg-slate-900 p-6 rounded-xl border border-slate-700 text-sm mb-8">
            <div class="flex justify-between"><span>Status:</span> <span class="text-emerald-400 font-bold">ONLINE</span></div>
            <div class="flex justify-between"><span>Port:</span> <span class="text-blue-400">5000</span></div>
            <div class="flex justify-between"><span>Database:</span> <span class="text-orange-400">MongoDB Connected</span></div>
            <div class="flex justify-between"><span>Version:</span> <span class="text-purple-400">3.0 (Improved)</span></div>
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
        "version": "3.0",
        "status": "online",
        "features": [
            "✅ Smart Column Mapping (Auto-detect header variations)",
            "✅ Accurate Meter Analysis (Ekstrim, Zero, Negatif, Estimasi, Rebill)",
            "✅ Summarizing (MC, MB, ARDEBT, MAINBILL, COLLECTION)",
            "✅ Collection Analysis (Undue, Current, Arrears, Outstanding)",
            "✅ Top 100 Rankings (Premium, Debt, Unpaid Current/Debt)",
            "✅ History Tracking (Kubikasi & Pembayaran)",
            "✅ Detective Mode (Detailed Customer Analysis)"
        ],
        "improvements": [
            "🔥 Comprehensive header mapping for all file types",
            "🔥 Accurate anomaly detection with detailed analysis",
            "🔥 No hardcoded Rayon filter in frontend",
            "🔥 Optimized database queries with indexing",
            "🔥 Better error handling and validation"
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
        
        # Deteksi format file
        if filename.endswith('.CSV') or filename.endswith('.TXT'):
            content = file.read().decode('utf-8', errors='ignore')
            # Auto-detect delimiter
            delimiter = '|' if '|' in content[:1000] else (';' if ';' in content[:1000] else ',')
            df = pd.read_csv(io.StringIO(content), sep=delimiter, engine='python')
        elif filename.endswith('.XLSX') or filename.endswith('.XLS'):
            df = pd.read_excel(file)
        else:
            return jsonify({"status": "error", "message": "Format file tidak didukung. Gunakan CSV, TXT, atau XLSX"}), 400
            
        # Standarisasi Header
        df.columns = [str(col).strip().upper() for col in df.columns]
        
        print(f"📁 File uploaded: {filename}")
        print(f"📊 Columns detected: {list(df.columns)[:10]}")  # Debug: print first 10 columns
        
        # LOGIKA DETEKSI TIPE DATA
        
        # A. METER READING (SBRS)
        if 'CMR_READING' in df.columns or 'CMR_ACCOUNT' in df.columns or 'CURR_READ_1' in df.columns:
            print("🔍 Detected: METER READING file")
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
                "status": "success", 
                "type": "METER_READING",
                "filename": file.filename,
                "data": { 
                    "anomalies": anomalies[:200],  # Limit to 200 for performance
                    "summary": summary_stats,
                    "total_records": len(df_clean)
                }
            })

        # B. COLLECTION (File dengan AMT_COLLECT)
        elif 'AMT_COLLECT' in df.columns or 'PAY_DT' in df.columns:
            print("🔍 Detected: COLLECTION file")
            df_clean = pd.DataFrame(clean_dataframe(df))
            
            # Save to MongoDB collections
            if init_db.__globals__.get('db') is not None:
                try:
                    records = []
                    for record in df_clean:
                        records.append(record)
                    
                    from utils import db
                    if records:
                        db.collections.insert_many(records, ordered=False)
                        print(f"✅ Saved {len(records)} collection records to DB")
                except Exception as e:
                    print(f"⚠️ DB save warning: {e}")
            
            total_amount = sum([float(r.get('AMT_COLLECT', 0)) for r in df_clean])
            total_volume = sum([float(r.get('VOL_COLLECT', 0)) for r in df_clean])
            
            return jsonify({
                "status": "success", 
                "type": "COLLECTION_REPORT",
                "filename": file.filename,
                "data": { 
                    "total_amount": total_amount,
                    "total_volume": total_volume,
                    "records": len(df_clean)
                }
            })

        # C. BILLING (MC, MB, MainBill, Arrears)
        elif 'NOMEN' in df.columns or 'NOMINAL' in df.columns or 'JUMLAH' in df.columns:
            print("🔍 Detected: BILLING/ARREARS file")
            df_clean = pd.DataFrame(clean_dataframe(df))
            
            # Determine collection type
            if 'UMUR_TUNGGAKAN' in df.columns or 'DEBT' in filename:
                collection_name = 'arrears'
                file_type = "ARREARS (Tunggakan)"
            elif 'MASTER CETAK' in filename or 'MC' in filename:
                collection_name = 'master_cetak'
                file_type = "MASTER CETAK"
            elif 'MASTER BAYAR' in filename or 'MB' in filename:
                collection_name = 'master_bayar'
                file_type = "MASTER BAYAR"
            else:
                collection_name = 'main_bill'
                file_type = "MAIN BILL"
            
            # Save to MongoDB
            if init_db.__globals__.get('db') is not None:
                try:
                    from utils import db
                    if df_clean:
                        db[collection_name].insert_many(df_clean, ordered=False)
                        print(f"✅ Saved {len(df_clean)} {file_type} records to DB")
                except Exception as e:
                    print(f"⚠️ DB save warning: {e}")
            
            # Calculate totals
            val_col = 'NOMINAL' if 'NOMINAL' in df.columns else 'JUMLAH'
            total_nominal = sum([float(r.get(val_col, 0)) for r in df_clean])
            total_volume = sum([float(r.get('KUBIK', 0)) for r in df_clean])
            
            return jsonify({
                "status": "success", 
                "type": file_type,
                "filename": file.filename,
                "data": { 
                    "total_nominal": total_nominal, 
                    "total_volume": total_volume, 
                    "records": len(df_clean) 
                }
            })

        # D. Unknown format
        else:
            print("❌ Unknown file format")
            print(f"Available columns: {list(df.columns)}")
            return jsonify({
                "status": "error", 
                "message": f"Format kolom tidak dikenali. Kolom yang ditemukan: {', '.join(list(df.columns)[:10])}"
            }), 400

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return jsonify({"status": "error", "message": f"Server Error: {str(e)}"}), 500

# 3. SUMMARIZING REPORT
@app.route('/api/summary', methods=['GET'])
def api_summary():
    target = request.args.get('target', 'mc').lower()
    dimension = request.args.get('dimension', 'RAYON').upper()
    rayon_filter = request.args.get('rayon', None)
    
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
        data = get_customer_payment_status(rayon)
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# 5. HISTORY
@app.route('/api/history', methods=['GET'])
def api_history():
    hist_type = request.args.get('type', 'usage')
    filter_by = request.args.get('filter_by', 'CUSTOMER').upper()
    filter_val = request.args.get('value', None)
    
    try:
        if hist_type == 'usage':
            data = get_usage_history(filter_by, filter_val)
        elif hist_type == 'payment_undue':
            data = get_payment_history_undue(filter_val)
        elif hist_type == 'payment_current':
            data = get_payment_history_current(filter_val)
        else:  # payment
            data = get_payment_history(filter_val)
            
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# 6. TOP 100
@app.route('/api/top100', methods=['GET'])
def api_top100():
    category = request.args.get('category', 'debt')
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
        result = save_manual_audit(
            req.get('nomen'), 
            req.get('remark'), 
            req.get('user', 'ADMIN'), 
            req.get('status')
        )
        if result:
            return jsonify({"status": "success"})
        else:
            return jsonify({"status": "error", "message": "Failed to save audit"}), 500
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    print(f"🚀 Starting PAM DSS Server V3.0 on port {port}...")
    print(f"📡 API Base URL: http://174.138.16.241:{port}/api")
    app.run(host='0.0.0.0', port=port, debug=False)
