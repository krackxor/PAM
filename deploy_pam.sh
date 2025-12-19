#!/bin/bash

# ==========================================
# PAM DSS - AUTO DEPLOYMENT (ALL-IN-ONE)
# ==========================================

echo "🔥 [1/7] MEMULAI INSTALASI OTOMATIS..."

# 1. BERSIHKAN PROSES LAMA
pkill -f "python3 app.py"
pkill -f "npm run dev"
pkill -f vite
rm -rf backend.log frontend.log
echo "✅ Proses lama dibersihkan."

# 2. INSTALL MONGODB (DATABASE)
if ! command -v mongod &> /dev/null; then
    echo "📦 Menginstall MongoDB..."
    sudo apt-get install gnupg curl -y
    curl -fsSL https://www.mongodb.org/static/pgp/server-7.0.asc | \
       sudo gpg -o /usr/share/keyrings/mongodb-server-7.0.gpg --dearmor
    echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/7.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-7.0.list
    sudo apt-get update
    sudo apt-get install -y mongodb-org
    sudo systemctl start mongod
    sudo systemctl enable mongod
else
    echo "✅ MongoDB sudah terinstall."
fi

# 3. INSTALL NODE.JS V20 (WAJIB UTK VITE 5)
ver=$(node -v 2>/dev/null | cut -d'.' -f1 | sed 's/v//')
if [ "$ver" != "20" ] && [ "$ver" != "21" ] && [ "$ver" != "22" ]; then
    echo "📦 Mengupdate Node.js ke versi 20..."
    sudo apt-get remove nodejs npm -y >/dev/null 2>&1
    curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
    sudo apt-get install -y nodejs
fi
echo "✅ Node.js Version: $(node -v)"

# 4. BUAT FILE SYSTEM (PYTHON & REACT)
echo "📝 Membuat file aplikasi..."

# --- REQUIREMENTS.TXT ---
cat > requirements.txt <<EOF
Flask==3.0.0
flask-cors==4.0.0
pandas==2.1.4
python-dotenv==1.0.0
pymongo==4.6.1
openpyxl==3.1.2
EOF

# --- UTILS.PY (SMART MAPPING) ---
cat > utils.py << 'EOF'
import os
import pandas as pd
from pymongo import MongoClient, DESCENDING, ASCENDING
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")

db = None
def init_db():
    global db
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client["PAM_DSS_DB"]
        # Indexes
        db.meter_history.create_index([("nomen", ASCENDING), ("period", DESCENDING)])
        db.collections.create_index([("RAYON", ASCENDING)])
        db.arrears.create_index([("RAYON", ASCENDING), ("JUMLAH", DESCENDING)])
        db.master_cetak.create_index([("RAYON", ASCENDING)])
    except: pass

def standardize_row_keys(row):
    new_row = row.copy()
    mappings = {
        'NOMEN': ['CMR_ACCOUNT', 'Nomen', 'NOMEN', 'NO_SAMBUNGAN', 'NOTAG'],
        'NAMA': ['CMR_NAME', 'NAMA_PEL', 'Nama', 'PELANGGAN'],
        'RAYON': ['Rayon', 'KODERAYON', 'RAYON', 'CC'],
        'CMR_READING': ['Curr_Read_1', 'STAN_AKIR', 'cmr_reading', 'KINI', 'SB_Stand'],
        'CMR_PREV_READ': ['Prev_Read_1', 'STAN_AWAL', 'cmr_prev_read', 'LALU'],
        'CMR_SKIP_CODE': ['Force_reason', 'cmr_skip_code', 'KODE_BACA', 'Force_Read'],
        'NOMINAL': ['JUMLAH', 'TAGIHAN_AIR', 'AMT_COLLECT', 'TOTAL_TAGIHAN', 'Bill_Amount', 'REK_AIR'],
        'KUBIK': ['KONSUMSI', 'VOL_COLLECT', 'KUBIKASI', 'PAKAI']
    }
    keys_upper = {k.upper(): k for k in new_row.keys()}
    for std, vars in mappings.items():
        if std not in new_row:
            for v in vars:
                if v in new_row: new_row[std] = new_row[v]; break
                if v.upper() in keys_upper: new_row[std] = new_row[keys_upper[v.upper()]]; break
    return new_row

def clean_dataframe(df):
    df.columns = [str(col).strip() for col in df.columns]
    df = df.fillna(0)
    for col in df.select_dtypes(include=['object']): df[col] = df[col].astype(str).str.strip()
    return [standardize_row_keys(r) for r in df.to_dict('records')]

def save_to_db(data, type_str):
    if db is None or not data: return
    try:
        coll_name = 'master_cetak'
        if 'COLLECTION' in type_str: coll_name = 'collections'
        elif 'ARREARS' in type_str: coll_name = 'arrears'
        elif 'MAINBILL' in type_str: coll_name = 'main_bill'
        elif 'METER' in type_str: coll_name = 'meter_history'
        db[coll_name].insert_many(data, ordered=False)
    except: pass

def analyze_meter_anomalies(df_records):
    anomalies = []
    if db is not None:
        hist = []
        for r in df_records:
            hist.append({
                'nomen': str(r.get('NOMEN')), 'period': datetime.now().strftime('%Y-%m'),
                'usage': float(r.get('CMR_READING',0))-float(r.get('CMR_PREV_READ',0)),
                'cmr_rd_date': r.get('READ_DATE_1', datetime.now().strftime('%Y-%m-%d')),
                'CMR_READING': float(r.get('CMR_READING',0)), 'CMR_PREV_READ': float(r.get('CMR_PREV_READ',0))
            })
        try: db.meter_history.insert_many(hist, ordered=False)
        except: pass

    for row in df_records:
        status = []
        try:
            prev, curr = float(row.get('CMR_PREV_READ', 0)), float(row.get('CMR_READING', 0))
            usage = curr - prev
            avg = float(row.get('AVG_USAGE', 20))
        except: continue
        
        if usage < 0: status.append('STAND NEGATIF')
        if usage == 0: status.append('PEMAKAIAN ZERO')
        if usage > 0 and (usage > avg*2 and usage > 50): status.append('EKSTRIM')
        skip = str(row.get('CMR_SKIP_CODE', '0')).strip()
        if skip not in ['0','nan','','None','NULL','0.0']:
            status.append(f'KODE: {skip}')
            if skip in ['EST','E','FORCE','1']: status.append('ESTIMASI')
            
        if status:
            anomalies.append({'nomen': str(row.get('NOMEN')), 'name': row.get('NAMA',''), 'usage': usage, 'status': status, 'details': {'anomaly_reason': ', '.join(status), 'skip_desc': skip}})
    return anomalies

def get_summarized_report(target, dimension, rayon_filter=None):
    if db is None: return []
    coll_map = {'mc':'master_cetak','mb':'master_bayar','ardebt':'arrears','mainbill':'main_bill','collection':'collections'}
    coll = db[coll_map.get(target, 'master_cetak')]
    val_field = '$NOMINAL' 
    pipeline = []
    if rayon_filter: pipeline.append({'$match': {'RAYON': str(rayon_filter)}})
    pipeline.append({'$group': {'_id': f'${dimension}', 'nominal': {'$sum': val_field}, 'volume': {'$sum': '$KUBIK'}, 'count': {'$sum': 1}}})
    pipeline.append({'$sort': {'_id': 1}})
    try: return [{'group': r['_id'] or 'LAIN', 'nominal': r.get('nominal',0), 'volume': r.get('volume',0), 'count': r['count']} for r in list(coll.aggregate(pipeline))]
    except: return []

def get_customer_payment_status(rayon=None):
    if db is None: return {}
    match = {'RAYON': str(rayon)} if rayon else {}
    def calc(c, q):
        try: res=list(db[c].aggregate([{'$match':{**match,**q}},{'$group':{'_id':None,'tot':{'$sum':'$NOMINAL'},'cnt':{'$sum':1}}}])); return {'revenue':res[0]['tot'],'count':res[0]['cnt']} if res else {'revenue':0,'count':0}
        except: return {'revenue':0,'count':0}
    return {
        'undue': calc('collections', {'TYPE': 'UNDUE'}),
        'current': calc('collections', {'TYPE': {'$in':['CURRENT',None]}}),
        'paid_arrears': calc('collections', {'TYPE': 'ARREARS'}),
        'outstanding_arrears': calc('arrears', {}),
        'unpaid_receivable_no_arrears': calc('master_cetak', {'STATUS_LUNAS': {'$ne': True}})
    }

def get_top_100_debt(rayon):
    if db is None: return []
    return list(db.arrears.aggregate([{'$match':{'RAYON':str(rayon)}},{'$sort':{'NOMINAL':-1}},{'$limit':100},{'$project':{'_id':0,'NAMA':1,'NOMEN':1,'debt_amount':'$NOMINAL','UMUR_TUNGGAKAN':1}}]))

def get_top_100_premium(rayon):
    if db is None: return []
    return list(db.collections.aggregate([{'$match':{'RAYON':str(rayon)}},{'$group':{'_id':'$NOMEN','total_paid':{'$sum':'$NOMINAL'},'NAMA':{'$first':'$NAMA'}}},{'$sort':{'total_paid':-1}},{'$limit':100}]))

def get_top_100_unpaid_current(rayon):
    if db is None: return []
    return list(db.master_cetak.aggregate([{'$match':{'RAYON':str(rayon),'STATUS_LUNAS':{'$ne':True}}},{'$sort':{'NOMINAL':-1}},{'$limit':100},{'$project':{'_id':0,'NAMA':1,'NOMEN':1,'outstanding':'$NOMINAL'}}]))
    
def get_usage_history(dim, val):
    if db is None: return []
    q = {'nomen': val} if dim == 'CUSTOMER' else {'RAYON': val}
    return [{'period': h.get('period'), 'value': h.get('usage'), 'desc': 'Usage'} for h in list(db.meter_history.find(q).sort('period', -1).limit(12))]
    
def get_audit_detective_data(nomen):
    if db is None: return {}
    return {'customer': db.master_cetak.find_one({'NOMEN': str(nomen)},{'_id':0}) or {'NAMA':'Unknown'}, 'reading_history': list(db.meter_history.find({'nomen': str(nomen)},{'_id':0}).limit(12))}

def save_manual_audit(n, r, u, s):
    if db: db.audit_logs.insert_one({'nomen':n, 'remark':r, 'user':u, 'status':s, 'ts': datetime.now()})
EOF

# --- APP.PY (BACKEND) ---
cat > app.py << 'EOF'
import os, io, pandas as pd
from flask import Flask, request, jsonify
from flask_cors import CORS
from utils import *

app = Flask(__name__)
CORS(app)
with app.app_context(): init_db()

@app.route('/')
def idx(): return "PAM DSS ONLINE"

@app.route('/api/upload-and-analyze', methods=['POST'])
def upload():
    file = request.files['file']
    fname = file.filename.upper()
    try:
        if fname.endswith(('CSV','TXT')):
            content = file.read().decode('utf-8', errors='ignore')
            delim = '|' if '|' in content[:1000] else (';' if ';' in content[:1000] else ',')
            df = pd.read_csv(io.StringIO(content), sep=delim, engine='python')
        else: df = pd.read_excel(file)
        
        data = clean_dataframe(df)
        cols = [k.upper() for k in data[0].keys()] if data else []
        
        if 'CMR_READING' in cols or 'CMR_ACCOUNT' in cols:
            anom = analyze_meter_anomalies(data)
            stats = {"extreme": len([x for x in anom if "EKSTRIM" in x['status']]), "negative": len([x for x in anom if "STAND NEGATIF" in x['status']]), "zero": len([x for x in anom if "ZERO" in x['status']])}
            return jsonify({"status": "success", "type": "METER_READING", "data": {"anomalies": anom, "summary": stats}})
        
        type_res = "BILLING_SUMMARY"
        if 'AMT_COLLECT' in cols or 'PAY_DT' in cols: type_res = "COLLECTION_REPORT"
        elif 'JUMLAH' in cols or 'ARDEBT' in fname: type_res = "ARREARS_REPORT"
        elif 'TOTAL_TAGIHAN' in cols or 'MAINBILL' in fname: type_res = "MAINBILL_REPORT"
        elif 'MASTER_BAYAR' in fname or 'MB' in fname: type_res = "MASTER_BAYAR"
        
        save_to_db(data, type_res)
        return jsonify({"status": "success", "type": type_res, "data": {"total": sum(x.get('NOMINAL',0) for x in data)}})
    except Exception as e: return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/summary', methods=['GET'])
def sum_rep(): return jsonify({"status":"success", "data": get_summarized_report(request.args.get('target','mc'), request.args.get('dimension','RAYON'), request.args.get('rayon'))})

@app.route('/api/collection/status', methods=['GET'])
def coll_stat(): return jsonify({"status":"success", "data": get_customer_payment_status(request.args.get('rayon'))})

@app.route('/api/top100', methods=['GET'])
def top():
    cat, ray = request.args.get('category','debt'), request.args.get('rayon','34')
    if cat == 'premium': d = get_top_100_premium(ray)
    elif cat == 'unpaid_current': d = get_top_100_unpaid_current(ray)
    else: d = get_top_100_debt(ray)
    return jsonify({"status":"success", "data": d})

@app.route('/api/history', methods=['GET'])
def hist():
    t = request.args.get('type','usage')
    d = get_usage_history(request.args.get('filter_by'), request.args.get('value')) if t == 'usage' else []
    return jsonify({"status":"success", "data": d})

@app.route('/api/detective/<nomen>', methods=['GET'])
def det(nomen): return jsonify({"status":"success", "data": get_audit_detective_data(nomen)})

@app.route('/api/audit/save', methods=['POST'])
def aud(): 
    req = request.json
    save_manual_audit(req.get('nomen'), req.get('remark'), 'ADMIN', 'AUDITED')
    return jsonify({"status":"success"})

if __name__ == '__main__': app.run(host='0.0.0.0', port=5000)
EOF

# --- FRONTEND CONFIG ---
cat > package.json <<EOF
{ "name": "pam-dashboard", "version": "1.0.0", "type": "module", "scripts": { "dev": "vite", "build": "vite build", "preview": "vite preview" }, "dependencies": { "lucide-react": "^0.294.0", "react": "^18.2.0", "react-dom": "^18.2.0" }, "devDependencies": { "@types/react": "^18.2.43", "@vitejs/plugin-react": "^4.2.1", "autoprefixer": "^10.4.16", "postcss": "^8.4.32", "tailwindcss": "^3.4.0", "vite": "^5.0.0" } }
EOF
cat > vite.config.js <<EOF
import { defineConfig } from 'vite'; import react from '@vitejs/plugin-react';
export default defineConfig({ plugins: [react()], server: { host: '0.0.0.0', port: 5173, proxy: { '/api': { target: 'http://127.0.0.1:5000', changeOrigin: true } } } })
EOF
cat > tailwind.config.js <<EOF
export default { content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"], theme: { extend: {} }, plugins: [], }
EOF
cat > postcss.config.js <<EOF
export default { plugins: { tailwindcss: {}, autoprefixer: {} } }
EOF

mkdir -p src
# --- APP.JSX ---
cat > src/App.jsx << 'EOF'
import React, { useState, useEffect } from 'react';
import { ShieldCheck, Upload, Activity, BarChart3, Users, AlertTriangle, ArrowLeft, Save, Database, CreditCard, Droplets, RefreshCw, TrendingUp, Layers, History, User, MessageSquare, Search, FileText, CheckCircle, MapPin } from 'lucide-react';

const API_BASE_URL = '/api'; 
const apiCall = (url, opts) => fetch(url, opts);

const App = () => {
  const [activeTab, setActiveTab] = useState('upload');
  const [loading, setLoading] = useState(false);
  const [uploadRes, setUploadRes] = useState(null);
  const [summaryData, setSummaryData] = useState([]);
  const [collectionData, setCollData] = useState(null);
  const [topData, setTopData] = useState([]);
  const [histData, setHistData] = useState([]);
  const [selAnom, setSelAnom] = useState(null);
  const [detData, setDetData] = useState(null);
  const [rayon, setRayon] = useState('34');
  const [sumTarget, setSumTarget] = useState('mc');
  const [histVal, setHistVal] = useState('');

  const handleUpload = async (e) => {
    setLoading(true); const fd = new FormData(); fd.append('file', e.target.files[0]);
    try {
      const res = await apiCall(`${API_BASE_URL}/upload-and-analyze`, { method: 'POST', body: fd });
      const json = await res.json();
      if (json.status === 'success') {
        setUploadRes(json);
        if (json.type === 'METER_READING') setActiveTab('meter');
        else if (json.type.includes('COLLECTION')) setActiveTab('collection');
        else setActiveTab('summary');
        alert(`Sukses Upload ${json.type}. Total: ${json.data.total||0}`);
      } else alert(json.message);
    } catch (e) { alert("Upload Failed: " + e); }
    setLoading(false);
  };

  const fetchSum = async () => {
    setLoading(true); try { setSummaryData((await (await apiCall(`${API_BASE_URL}/summary?target=${sumTarget}&dimension=RAYON&rayon=${rayon}`)).json()).data); } catch {} setLoading(false);
  };
  const fetchColl = async () => {
    setLoading(true); try { setCollData((await (await apiCall(`${API_BASE_URL}/collection/status?rayon=${rayon}`)).json()).data); } catch {} setLoading(false);
  };
  const fetchTop = async () => {
    setLoading(true); try { setTopData((await (await apiCall(`${API_BASE_URL}/top100?category=debt&rayon=${rayon}`)).json()).data); } catch {} setLoading(false);
  };
  const fetchHist = async () => {
    setLoading(true); try { setHistData((await (await apiCall(`${API_BASE_URL}/history?type=usage&filter_by=CUSTOMER&value=${histVal}`)).json()).data); } catch {} setLoading(false);
  };
  const fetchDet = async (nomen) => {
    setLoading(true); try { setDetData((await (await apiCall(`${API_BASE_URL}/detective/${nomen}`)).json()).data); } catch {} setLoading(false);
  };

  useEffect(() => {
    if(activeTab==='summary') fetchSum();
    if(activeTab==='collection') fetchColl();
    if(activeTab==='top') fetchTop();
  }, [activeTab, rayon, sumTarget]);

  const Nav = ({id, icon, l}) => (
    <button onClick={()=>{setActiveTab(id); setSelAnom(null)}} className={`flex gap-3 w-full px-6 py-4 rounded-xl font-bold mb-2 transition-all ${activeTab===id ? 'bg-blue-600 text-white shadow-lg' : 'text-slate-400 hover:bg-slate-900'}`}>{icon} {l}</button>
  );

  return (
    <div className="flex min-h-screen bg-slate-50 text-slate-900 font-sans">
      <aside className="w-72 bg-slate-950 p-6 flex flex-col fixed h-full z-20 shadow-2xl">
        <div className="flex items-center gap-3 mb-10 text-white"><ShieldCheck size={28} className="text-blue-500"/> <h1 className="font-black text-2xl">PAM DSS</h1></div>
        <nav className="flex-1">
          <Nav id="upload" icon={<Upload size={20}/>} l="Upload"/>
          <Nav id="summary" icon={<BarChart3 size={20}/>} l="Summary"/>
          <Nav id="collection" icon={<CreditCard size={20}/>} l="Collection"/>
          <Nav id="meter" icon={<Activity size={20}/>} l="Meter"/>
          <Nav id="history" icon={<History size={20}/>} l="History"/>
          <Nav id="top" icon={<Users size={20}/>} l="Top 100"/>
        </nav>
        <div className="bg-slate-900 p-4 rounded-xl">
           <div className="text-xs text-slate-500 font-bold mb-2 text-center">FILTER RAYON</div>
           <div className="flex gap-2"><button onClick={()=>setRayon('34')} className={`flex-1 py-2 rounded font-bold text-xs ${rayon==='34'?'bg-blue-600 text-white':'bg-slate-800 text-slate-400'}`}>34</button><button onClick={()=>setRayon('35')} className={`flex-1 py-2 rounded font-bold text-xs ${rayon==='35'?'bg-blue-600 text-white':'bg-slate-800 text-slate-400'}`}>35</button></div>
        </div>
      </aside>

      <main className="ml-72 flex-1 p-10">
        {selAnom ? (
           <div className="max-w-4xl mx-auto">
              <button onClick={()=>setSelAnom(null)} className="mb-6 flex gap-2 font-bold text-slate-400 hover:text-blue-600"><ArrowLeft/> Back</button>
              <div className="bg-white p-8 rounded-3xl shadow-sm border border-slate-100 mb-6 flex justify-between">
                 <div><h2 className="text-3xl font-black">{selAnom.name}</h2><div className="mt-2 bg-slate-100 px-3 py-1 rounded inline-block font-mono text-sm">{selAnom.nomen}</div></div>
                 <div className="text-right"><div className="text-4xl font-black text-blue-600">{selAnom.usage} m³</div><div className="text-xs font-bold text-slate-400 uppercase">Usage</div></div>
              </div>
              <div className="bg-white p-8 rounded-3xl shadow-sm border border-slate-100">
                 <h3 className="font-bold mb-4 flex gap-2"><History/> History</h3>
                 <table className="w-full text-sm text-left"><thead className="bg-slate-50 text-slate-400"><tr><th className="p-3">Date</th><th>Prev</th><th>Curr</th><th>Use</th></tr></thead>
                 <tbody>{detData?.reading_history?.map((h,i)=>(<tr key={i} className="border-b border-slate-50"><td className="p-3">{h.cmr_rd_date}</td><td>{h.cmr_prev_read}</td><td>{h.cmr_reading}</td><td className="font-bold">{h.cmr_reading - h.cmr_prev_read}</td></tr>))}</tbody></table>
              </div>
           </div>
        ) : (
           <div className="max-w-6xl mx-auto space-y-8 animate-in fade-in">
              <header><h2 className="text-4xl font-black uppercase mb-2">{activeTab} DASHBOARD</h2><p className="text-slate-500">Rayon {rayon} View</p></header>
              
              {activeTab==='upload' && (
                <div className="bg-white p-20 rounded-[3rem] border-4 border-dashed text-center">
                   <Upload size={60} className="mx-auto text-blue-500 mb-6"/>
                   <h3 className="text-2xl font-black mb-2">Upload Excel / CSV</h3>
                   <input type="file" onChange={handleUpload} className="mt-6 block w-full text-sm text-slate-500 file:mr-4 file:py-3 file:px-6 file:rounded-full file:border-0 file:text-sm file:font-semibold file:bg-blue-50 file:text-blue-700 hover:file:bg-blue-100"/>
                   {loading && <div className="mt-4 text-blue-600 font-bold animate-pulse">Processing...</div>}
                </div>
              )}

              {activeTab==='summary' && (
                <div className="space-y-4">
                    <select value={sumTarget} onChange={e=>setSumTarget(e.target.value)} className="p-3 rounded-xl border"><option value="mc">MC</option><option value="mb">MB</option><option value="ardebt">Arrears</option><option value="collection">Collection</option></select>
                    <div className="bg-white rounded-3xl overflow-hidden shadow-sm border border-slate-100">
                    <table className="w-full text-left"><thead className="bg-slate-50 text-xs font-black uppercase text-slate-400"><tr><th className="p-6">Group</th><th className="text-right p-6">Nominal</th><th className="text-right p-6">Vol</th><th className="text-right p-6">Count</th></tr></thead>
                    <tbody>{summaryData.map((r,i)=>(<tr key={i} className="border-b border-slate-50 hover:bg-slate-50"><td className="p-6 font-bold">{r.group}</td><td className="p-6 text-right font-mono text-emerald-600">{r.nominal.toLocaleString()}</td><td className="p-6 text-right text-blue-600">{r.volume.toLocaleString()}</td><td className="p-6 text-right">{r.count}</td></tr>))}</tbody></table>
                    </div>
                </div>
              )}

              {activeTab==='collection' && collectionData && (
                 <div className="grid grid-cols-2 lg:grid-cols-4 gap-6">
                    {['undue','current','paid_arrears','outstanding_arrears'].map(k => (
                      <div key={k} className="bg-white p-6 rounded-3xl border border-slate-100 shadow-sm">
                         <div className="text-3xl font-black mb-1">{(collectionData[k].revenue/1000000).toFixed(1)}M</div>
                         <div className="text-[10px] uppercase font-bold text-slate-400">{k.replace('_',' ')}</div>
                         <div className="mt-2 text-xs font-bold text-blue-600">{collectionData[k].count} Plg</div>
                      </div>
                    ))}
                 </div>
              )}

              {activeTab==='meter' && (
                 <div className="space-y-4">
                    <div className="grid grid-cols-4 gap-4 text-center">
                       <div className="bg-orange-50 p-4 rounded-2xl text-orange-600 font-bold">Ekstrim: {uploadRes?.data?.summary?.extreme||0}</div>
                       <div className="bg-rose-50 p-4 rounded-2xl text-rose-600 font-bold">Negatif: {uploadRes?.data?.summary?.negative||0}</div>
                    </div>
                    <div className="bg-white rounded-3xl border border-slate-100 divide-y divide-slate-50">
                       {uploadRes?.data?.anomalies?.map((a,i)=>(
                          <div key={i} onClick={()=>{setSelAnom(a); fetchDet(a.nomen)}} className="p-6 flex justify-between hover:bg-slate-50 cursor-pointer">
                             <div><div className="font-bold">{a.name}</div><div className="text-xs text-slate-400">{a.nomen}</div></div>
                             <div className={`font-black text-xl ${a.usage<0?'text-rose-600':'text-blue-600'}`}>{a.usage}</div>
                          </div>
                       ))}
                    </div>
                 </div>
              )}

              {activeTab==='history' && (
                 <div>
                    <div className="flex gap-4 mb-6"><input value={histVal} onChange={e=>setHistVal(e.target.value)} placeholder="Search Nomen..." className="flex-1 p-4 rounded-2xl border border-slate-200"/><button onClick={fetchHist} className="bg-blue-600 text-white px-8 rounded-2xl font-bold">Search</button></div>
                    <div className="grid grid-cols-3 gap-4">
                       {histData.map((h,i)=>(<div key={i} className="bg-white p-6 rounded-2xl border border-slate-100"><div className="text-xs text-slate-400 font-bold">{h.period}</div><div className="text-2xl font-black">{h.value}</div></div>))}
                    </div>
                 </div>
              )}

              {activeTab==='top' && (
                 <div className="bg-white rounded-3xl overflow-hidden border border-slate-100">
                    <table className="w-full text-left"><thead className="bg-slate-50 text-xs text-slate-400"><tr><th className="p-4">Rank</th><th className="p-4">Name</th><th className="p-4 text-right">Value</th></tr></thead>
                    <tbody>{topData.map((r,i)=>(<tr key={i} className="border-b border-slate-50"><td className="p-4 font-black text-slate-300">#{i+1}</td><td className="p-4 font-bold">{r.NAMA}<br/><span className="text-xs text-slate-400 font-mono">{r.NOMEN}</span></td><td className="p-4 text-right font-mono font-bold text-blue-600">{(r.total_paid||r.debt_amount||0).toLocaleString()}</td></tr>))}</tbody></table>
                 </div>
              )}
           </div>
        )}
      </main>
    </div>
  );
}
export default App;
EOF
cat > src/main.jsx <<EOF
import React from 'react'; import ReactDOM from 'react-dom/client'; import App from './App.jsx'; import './index.css';
ReactDOM.createRoot(document.getElementById('root')).render(<React.StrictMode><App /></React.StrictMode>,)
EOF
cat > src/index.css <<EOF
@tailwind base; @tailwind components; @tailwind utilities;
EOF
cat > index.html <<EOF
<!doctype html><html lang="en"><head><meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1.0"/><title>PAM DSS</title></head><body><div id="root"></div><script type="module" src="/src/main.jsx"></script></body></html>
EOF

# 5. INSTALL & RUN
echo "📦 [5/7] MENGINSTALL DEPENDENCIES..."
pip3 install -r requirements.txt > /dev/null 2>&1
npm install > /dev/null 2>&1

echo "🚀 [6/7] MENJALANKAN SERVER..."
nohup python3 app.py > backend.log 2>&1 &
nohup npm run dev > frontend.log 2>&1 &

echo "=========================================="
echo "✅ DEPLOY SUKSES!"
echo "👉 Dashboard: http://174.138.16.241:5173"
echo "=========================================="
