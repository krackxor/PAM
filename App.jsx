import React, { useState, useEffect } from 'react';
import { 
  ShieldCheck, Upload, Activity, BarChart3, Users, 
  AlertTriangle, ArrowLeft, Save, Database, CreditCard, 
  Droplets, RefreshCw, TrendingUp, Layers, History, 
  User, MessageSquare, Search, FileText, CheckCircle, MapPin
} from 'lucide-react';

// --- CONFIG ---
// Ubah ke true jika ingin mencoba tampilan tanpa koneksi server (Mock Mode)
const USE_MOCK = false; 
const API_BASE_URL = 'http://174.138.16.241:5000/api';

// --- HELPER UNTUK FETCH API ---
const apiCall = async (url, options = {}) => {
  if (USE_MOCK) {
    // Simulasi delay jaringan jika mock
    await new Promise(r => setTimeout(r, 500)); 
    return { ok: true, json: async () => ({ status: 'success', data: [] }) };
  }
  return fetch(url, options);
};

const App = () => {
  // --- STATE UI ---
  const [activeTab, setActiveTab] = useState('upload'); // upload, summary, collection, meter, top, history
  const [loading, setLoading] = useState(false);
  const [errorMsg, setErrorMsg] = useState('');

  // --- STATE DATA ---
  const [uploadRes, setUploadRes] = useState(null);
  const [summaryData, setSummaryData] = useState([]);
  const [collectionData, setCollectionData] = useState(null);
  const [topData, setTopData] = useState([]);
  const [historyData, setHistoryData] = useState([]);
  
  // Detective Mode State
  const [selectedAnomaly, setSelectedAnomaly] = useState(null);
  const [detectiveData, setDetectiveData] = useState(null);
  const [auditRemark, setAuditRemark] = useState('');

  // --- FILTERS ---
  const [rayonFilter, setRayonFilter] = useState('34');
  
  // Summarizing Filters
  const [sumTarget, setSumTarget] = useState('mc'); // mc, mb, ardebt, mainbill, collection
  const [sumDim, setSumDim] = useState('RAYON'); // RAYON, PC, PCEZ, TARIF, METER
  
  // Top 100 Filters
  const [topCategory, setTopCategory] = useState('premium'); // premium, debt, unpaid_current, unpaid_debt
  
  // History Filters
  const [historyType, setHistoryType] = useState('usage'); // usage, payment
  const [historyFilterBy, setHistoryFilterBy] = useState('CUSTOMER'); // CUSTOMER, RAYON, PC, PCEZ, TARIF, METER
  const [historyValue, setHistoryValue] = useState('');

  // --- API HANDLERS ---

  const handleUpload = async (e) => {
    const file = e.target.files[0];
    if (!file) return;

    setLoading(true);
    setErrorMsg('');
    
    const fd = new FormData();
    fd.append('file', file);

    try {
      const res = await apiCall(`${API_BASE_URL}/upload-and-analyze`, { method: 'POST', body: fd });
      const json = await res.json();
      
      if (json.status === 'success') {
        setUploadRes(json);
        // Auto-switch tab berdasarkan tipe file
        if (json.type === 'METER_READING') setActiveTab('meter');
        else if (json.type.includes('COLLECTION')) setActiveTab('collection');
        else setActiveTab('summary');
      } else {
        alert("Gagal: " + json.message);
      }
    } catch (err) {
      setErrorMsg("Koneksi ke server gagal. Pastikan VPS aktif.");
      console.error(err);
    }
    setLoading(false);
  };

  const fetchSummary = async () => {
    setLoading(true);
    try {
      const res = await apiCall(`${API_BASE_URL}/summary?target=${sumTarget}&dimension=${sumDim}&rayon=${rayonFilter}`);
      const json = await res.json();
      if (json.status === 'success') setSummaryData(json.data);
    } catch (e) { console.error(e); }
    setLoading(false);
  };

  const fetchCollection = async () => {
    setLoading(true);
    try {
      const res = await apiCall(`${API_BASE_URL}/collection/status?rayon=${rayonFilter}`);
      const json = await res.json();
      if (json.status === 'success') setCollectionData(json.data);
    } catch (e) { console.error(e); }
    setLoading(false);
  };

  const fetchTop100 = async () => {
    setLoading(true);
    try {
      const res = await apiCall(`${API_BASE_URL}/top100?category=${topCategory}&rayon=${rayonFilter}`);
      const json = await res.json();
      if (json.status === 'success') setTopData(json.data);
    } catch (e) { console.error(e); }
    setLoading(false);
  };

  const fetchHistory = async () => {
    if (!historyValue) return alert("Masukkan nilai pencarian dulu (misal: Nomen atau Kode Rayon)");
    setLoading(true);
    try {
      const res = await apiCall(`${API_BASE_URL}/history?type=${historyType}&filter_by=${historyFilterBy}&value=${historyValue}`);
      const json = await res.json();
      if (json.status === 'success') setHistoryData(json.data);
      else alert(json.message);
    } catch (e) { console.error(e); }
    setLoading(false);
  };

  const fetchDetective = async (nomen) => {
    setLoading(true);
    try {
      const res = await apiCall(`${API_BASE_URL}/detective/${nomen}`);
      const json = await res.json();
      if (json.status === 'success') setDetectiveData(json.data);
    } catch (e) { console.error(e); }
    setLoading(false);
  };

  const saveAudit = async () => {
    if(!auditRemark) return alert("Isi catatan audit dulu.");
    try {
      const res = await apiCall(`${API_BASE_URL}/audit/save`, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          nomen: selectedAnomaly.nomen,
          remark: auditRemark,
          status: 'AUDITED',
          user: 'ADMIN_WEB'
        })
      });
      const json = await res.json();
      if(json.status === 'success') {
        alert("Data audit tersimpan!");
        setAuditRemark('');
      }
    } catch(e) { alert("Gagal simpan."); }
  };

  // Auto-fetch data saat tab atau filter berubah
  useEffect(() => {
    if (activeTab === 'summary') fetchSummary();
    if (activeTab === 'collection') fetchCollection();
    if (activeTab === 'top') fetchTop100();
  }, [activeTab, sumTarget, sumDim, rayonFilter, topCategory]);

  // --- UI COMPONENTS ---
  
  const NavBtn = ({ id, icon, label }) => (
    <button onClick={() => {setActiveTab(id); setSelectedAnomaly(null); setErrorMsg('');}} 
      className={`flex items-center gap-3 w-full px-6 py-4 rounded-2xl font-bold text-sm transition-all mb-2 ${activeTab===id ? 'bg-blue-600 text-white shadow-xl shadow-blue-900/20 translate-x-2' : 'text-slate-400 hover:bg-slate-900 hover:text-white'}`}>
      {icon} <span>{label}</span>
    </button>
  );

  return (
    <div className="flex min-h-screen bg-[#f8fafc] text-slate-900 font-sans selection:bg-blue-100">
      
      {/* SIDEBAR NAVIGATION */}
      <aside className="w-80 bg-slate-950 p-6 flex flex-col border-r border-slate-900 fixed h-full z-20 shadow-2xl">
        <div className="flex items-center gap-4 mb-10 px-2 text-white">
          <div className="bg-gradient-to-br from-blue-600 to-indigo-600 p-3 rounded-2xl shadow-lg shadow-blue-500/30">
            <ShieldCheck size={28} className="text-white"/>
          </div>
          <div>
            <h1 className="font-black text-2xl tracking-tighter leading-none">PAM DSS</h1>
            <div className="text-[10px] text-blue-400 font-bold uppercase tracking-widest mt-1">Enterprise V2.0</div>
          </div>
        </div>

        <nav className="flex-1 overflow-y-auto pr-2">
          <NavBtn id="upload" icon={<Upload size={20}/>} label="Upload Center" />
          <NavBtn id="summary" icon={<BarChart3 size={20}/>} label="Summarizing" />
          <NavBtn id="collection" icon={<CreditCard size={20}/>} label="Collection & AR" />
          <NavBtn id="meter" icon={<Activity size={20}/>} label="Meter Analysis" />
          <NavBtn id="history" icon={<History size={20}/>} label="History Data" />
          <NavBtn id="top" icon={<Users size={20}/>} label="Top 100 Ranking" />
        </nav>

        {/* Global Rayon Filter */}
        <div className="mt-4 bg-slate-900 p-5 rounded-[1.5rem] border border-slate-800">
          <div className="text-[10px] font-bold text-slate-500 uppercase tracking-widest mb-3 text-center flex items-center justify-center gap-2">
            <MapPin size={10}/> Filter Rayon Aktif
          </div>
          <div className="flex gap-2 bg-slate-950 p-1 rounded-xl">
            {['34', '35'].map(r => (
              <button key={r} onClick={() => setRayonFilter(r)} className={`flex-1 py-2.5 rounded-lg text-xs font-black transition-all ${rayonFilter===r ? 'bg-blue-600 text-white shadow-lg' : 'text-slate-500 hover:text-slate-300'}`}>
                RAYON {r}
              </button>
            ))}
          </div>
        </div>
      </aside>

      {/* MAIN CONTENT AREA */}
      <main className="ml-80 flex-1 p-10 min-w-0">
        
        {/* Error Alert */}
        {errorMsg && (
          <div className="bg-rose-50 border border-rose-200 text-rose-700 px-6 py-4 rounded-2xl mb-8 flex items-center gap-3 font-bold animate-pulse">
            <AlertTriangle size={20}/> {errorMsg}
          </div>
        )}

        {selectedAnomaly ? (
          /* --- DETECTIVE MODE (DETAIL) --- */
          <div className="max-w-6xl mx-auto animate-in slide-in-from-bottom-8 duration-500">
            <button onClick={() => setSelectedAnomaly(null)} className="flex items-center gap-2 text-slate-400 hover:text-blue-600 font-bold mb-8 transition-colors group">
              <div className="bg-white p-2 rounded-lg border border-slate-200 group-hover:border-blue-200"><ArrowLeft size={20}/></div>
              Kembali ke Daftar
            </button>
            
            {/* Header Detail */}
            <div className="bg-white p-10 rounded-[3rem] shadow-sm border border-slate-100 mb-8 flex flex-wrap justify-between items-center gap-6">
              <div>
                <h2 className="text-4xl font-black text-slate-900 tracking-tight">{selectedAnomaly.name}</h2>
                <div className="flex items-center gap-4 mt-3">
                  <span className="bg-slate-100 text-slate-600 px-4 py-1.5 rounded-full text-xs font-bold font-mono tracking-wider">{selectedAnomaly.nomen}</span>
                  {selectedAnomaly.status.map(s => <span key={s} className="bg-rose-100 text-rose-600 px-4 py-1.5 rounded-full text-[10px] font-black uppercase tracking-widest">{s}</span>)}
                </div>
              </div>
              <div className="text-right bg-blue-50 px-8 py-4 rounded-[2rem]">
                <div className="text-xs font-bold text-blue-400 uppercase tracking-widest mb-1">Pemakaian</div>
                <div className="text-5xl font-black text-blue-600 tracking-tighter">{selectedAnomaly.usage} <span className="text-lg text-blue-400">m³</span></div>
              </div>
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
              {/* History Table */}
              <div className="lg:col-span-2 space-y-8">
                <div className="bg-white p-8 rounded-[2.5rem] shadow-sm border border-slate-100 overflow-hidden">
                  <div className="flex items-center gap-3 mb-6 px-2">
                    <div className="p-2 bg-indigo-50 text-indigo-600 rounded-xl"><History size={20}/></div>
                    <h3 className="font-black text-lg text-slate-800">Riwayat Bacaan (12 Periode)</h3>
                  </div>
                  <div className="overflow-x-auto">
                    <table className="w-full text-left text-sm">
                      <thead className="text-slate-400 font-black uppercase text-[10px] tracking-widest border-b border-slate-50 bg-slate-50/50">
                        <tr><th className="py-4 px-4">Tanggal</th><th className="px-4">Lalu</th><th className="px-4">Kini</th><th className="px-4">Pakai</th><th className="px-4">Kode</th><th className="px-4">Pesan</th></tr>
                      </thead>
                      <tbody className="divide-y divide-slate-50 font-medium text-slate-600">
                        {detectiveData?.reading_history?.map((h, i) => (
                          <tr key={i} className="hover:bg-slate-50 transition-colors">
                            <td className="py-4 px-4 text-slate-900 font-bold">{h.cmr_rd_date}</td>
                            <td className="px-4 font-mono text-slate-400">{h.cmr_prev_read}</td>
                            <td className="px-4 font-mono text-slate-900">{h.cmr_reading}</td>
                            <td className={`px-4 font-black ${h.cmr_reading - h.cmr_prev_read < 0 ? 'text-rose-500' : 'text-emerald-500'}`}>{h.cmr_reading - h.cmr_prev_read}</td>
                            <td className="px-4"><span className="bg-slate-100 px-2 py-1 rounded text-xs font-bold text-slate-500">{h.cmr_skip_code || '-'}</span></td>
                            <td className="px-4 text-xs italic">{h.cmr_chg_spcl_msg}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>
              
              {/* Analysis & Audit Box */}
              <div className="bg-white p-8 rounded-[2.5rem] shadow-sm border border-slate-100 h-fit space-y-6">
                 <div className="flex items-center gap-3 mb-2 px-2">
                    <div className="p-2 bg-orange-50 text-orange-600 rounded-xl"><MessageSquare size={20}/></div>
                    <h3 className="font-black text-lg text-slate-800">Audit Lapangan</h3>
                 </div>
                 
                 <div className="space-y-3">
                    <div className="p-5 bg-slate-50 rounded-[1.5rem] border border-slate-100">
                      <div className="text-[10px] font-bold text-slate-400 uppercase tracking-widest mb-1">Indikasi Sistem</div>
                      <div className="font-bold text-slate-800 leading-snug">{selectedAnomaly.details?.anomaly_reason || 'Tidak ada data spesifik'}</div>
                    </div>
                    <div className="p-5 bg-slate-50 rounded-[1.5rem] border border-slate-100">
                       <div className="text-[10px] font-bold text-slate-400 uppercase tracking-widest mb-1">Skip Code</div>
                       <div className="font-bold text-slate-800">{selectedAnomaly.details?.skip_desc || 'Normal'}</div>
                    </div>
                 </div>

                 <textarea 
                    value={auditRemark} 
                    onChange={e => setAuditRemark(e.target.value)}
                    placeholder="Tulis hasil cek lapangan disini..." 
                    className="w-full h-32 p-5 bg-white border-2 border-slate-100 rounded-[1.5rem] text-sm font-medium outline-none focus:border-blue-500 transition-all resize-none"
                 />
                 
                 <button onClick={saveAudit} className="w-full bg-blue-600 text-white py-4 rounded-2xl font-bold shadow-xl shadow-blue-200 hover:bg-blue-700 transition-all flex justify-center items-center gap-2 active:scale-95">
                   <Save size={18}/> Simpan Laporan
                 </button>
              </div>
            </div>
          </div>
        ) : (
          /* --- MAIN TABS VIEW --- */
          <div className="max-w-7xl mx-auto space-y-8 animate-in fade-in duration-700">
            <header className="mb-8">
              <h2 className="text-4xl font-black text-slate-900 uppercase tracking-tighter mb-2">
                {activeTab === 'upload' && 'Pusat Unggah Data'}
                {activeTab === 'summary' && 'Summarizing Report'}
                {activeTab === 'collection' && 'Collection & AR'}
                {activeTab === 'meter' && 'Analisa Meter'}
                {activeTab === 'history' && 'Pencarian Data'}
                {activeTab === 'top' && 'Top 100 Ranking'}
              </h2>
              <p className="text-slate-500 font-medium text-lg">Dashboard Analitik untuk Rayon {rayonFilter}</p>
            </header>

            {/* TAB: UPLOAD CENTER */}
            {activeTab === 'upload' && (
               <div className="bg-white p-20 rounded-[3.5rem] border-4 border-dashed border-slate-200 text-center shadow-sm hover:border-blue-300 transition-all group">
                 <div className="w-24 h-24 bg-blue-50 rounded-full flex items-center justify-center mx-auto mb-8 text-blue-500 group-hover:scale-110 transition-transform">
                    <Upload size={40}/>
                 </div>
                 <h3 className="text-3xl font-black text-slate-800 mb-4 tracking-tight">Unggah File Laporan</h3>
                 <p className="text-slate-400 mb-10 max-w-lg mx-auto leading-relaxed">
                   Sistem mendukung format <strong>.CSV / .TXT</strong> (SBRS/Cycle) dan <strong>.XLSX</strong> (Master, Billing, Collection, Arrears).
                 </p>
                 <label className="inline-flex items-center gap-3 bg-blue-600 text-white px-10 py-5 rounded-2xl font-black text-lg cursor-pointer hover:bg-blue-700 transition-all shadow-xl shadow-blue-200 active:translate-y-1">
                   {loading ? <RefreshCw className="animate-spin"/> : <FileText/>}
                   {loading ? 'Sedang Menganalisa...' : 'Pilih File dari Komputer'}
                   <input type="file" className="hidden" onChange={handleUpload}/>
                 </label>
               </div>
            )}

            {/* TAB: SUMMARIZING */}
            {activeTab === 'summary' && (
              <div className="space-y-6">
                {/* Control Bar */}
                <div className="bg-white p-6 rounded-[2rem] shadow-sm border border-slate-100 flex flex-wrap gap-6 items-center justify-between">
                  <div className="flex flex-wrap gap-4 items-center">
                     <div className="flex flex-col gap-1">
                        <label className="text-[10px] font-bold text-slate-400 uppercase tracking-widest pl-2">Jenis Laporan</label>
                        <select value={sumTarget} onChange={e => setSumTarget(e.target.value)} className="bg-slate-50 border-2 border-slate-100 px-5 py-3 rounded-2xl font-bold text-sm outline-none focus:border-blue-500 h-[50px] min-w-[200px]">
                          <option value="mc">MC (Master Cetak)</option>
                          <option value="mb">MB (Master Bayar)</option>
                          <option value="ardebt">ARDEBT (Piutang)</option>
                          <option value="mainbill">MAIN BILL</option>
                          <option value="collection">COLLECTION</option>
                        </select>
                     </div>
                     <div className="flex flex-col gap-1">
                        <label className="text-[10px] font-bold text-slate-400 uppercase tracking-widest pl-2">Dimensi Grouping</label>
                        <select value={sumDim} onChange={e => setSumDim(e.target.value)} className="bg-slate-50 border-2 border-slate-100 px-5 py-3 rounded-2xl font-bold text-sm outline-none focus:border-blue-500 h-[50px] min-w-[200px]">
                          <option value="RAYON">Per RAYON</option>
                          <option value="PC">Per PC (Kode Baca)</option>
                          <option value="PCEZ">Per PCEZ</option>
                          <option value="TARIF">Per TARIF</option>
                          <option value="METER">Per METER SIZE</option>
                        </select>
                     </div>
                  </div>
                  <button onClick={fetchSummary} className="bg-slate-900 text-white px-6 py-4 rounded-2xl font-bold text-sm hover:bg-slate-700 flex items-center gap-2 h-[50px] mt-auto">
                    <RefreshCw size={16}/> Refresh Data
                  </button>
                </div>

                {/* Result Table */}
                <div className="bg-white rounded-[2.5rem] shadow-sm border border-slate-100 overflow-hidden">
                   <table className="w-full text-left">
                     <thead className="bg-slate-50 text-slate-500 text-[10px] font-black uppercase tracking-widest">
                       <tr><th className="px-8 py-6">Grup {sumDim}</th><th className="px-8 py-6 text-right">Nominal (Rp)</th><th className="px-8 py-6 text-right">Volume (m³)</th><th className="px-8 py-6 text-right">Lembar</th><th className="px-8 py-6 text-right">Status</th></tr>
                     </thead>
                     <tbody className="divide-y divide-slate-50 text-sm font-medium text-slate-700">
                       {summaryData.length > 0 ? summaryData.map((row, i) => (
                         <tr key={i} className="hover:bg-slate-50">
                           <td className="px-8 py-5 font-black text-slate-900 text-lg">{row.group}</td>
                           <td className="px-8 py-5 text-right font-mono font-bold text-emerald-600 text-lg">{row.nominal?.toLocaleString()}</td>
                           <td className="px-8 py-5 text-right font-mono font-bold text-blue-600">{row.volume?.toLocaleString()}</td>
                           <td className="px-8 py-5 text-right font-mono text-slate-500">{row.count?.toLocaleString()}</td>
                           <td className="px-8 py-5 text-right"><span className="bg-emerald-50 text-emerald-600 px-3 py-1 rounded-lg text-xs font-bold">{row.realization_pct || '-'}%</span></td>
                         </tr>
                       )) : <tr><td colSpan="5" className="p-20 text-center text-slate-400 italic font-bold">Tidak ada data untuk filter ini.</td></tr>}
                     </tbody>
                   </table>
                </div>
              </div>
            )}

            {/* TAB: COLLECTION ANALYSIS */}
            {activeTab === 'collection' && (
              <div className="space-y-8 animate-in slide-in-from-bottom-4">
                 <div className="grid grid-cols-2 lg:grid-cols-4 gap-6">
                    <StatBox label="Undue (Dimuka)" val={collectionData?.undue?.revenue} color="blue" icon={<Layers/>} sub="Pembayaran Dimuka"/>
                    <StatBox label="Current (Lancar)" val={collectionData?.current?.revenue} color="emerald" icon={<CheckCircle/>} sub="Pembayaran Bulan Ini"/>
                    <StatBox label="Arrears (Tunggakan)" val={collectionData?.arrears?.revenue} color="rose" icon={<AlertTriangle/>} sub="Pelunasan Tunggakan"/>
                    <StatBox label="Total Cash Masuk" val={collectionData?.total_cash} color="indigo" icon={<Database/>} sub="Semua Penerimaan"/>
                 </div>
                 
                 <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                    <div className="bg-white p-10 rounded-[2.5rem] border border-slate-100 shadow-sm">
                       <h3 className="font-black text-xl mb-8 flex items-center gap-3"><Users className="text-slate-400"/> Status Pembayaran Pelanggan</h3>
                       <div className="space-y-6">
                          <StatusRow label="Pelanggan Bayar Undue" val={collectionData?.undue?.count} icon="🔹"/>
                          <StatusRow label="Pelanggan Bayar Current" val={collectionData?.current?.count} icon="✅"/>
                          <StatusRow label="Pelanggan Bayar Tunggakan" val={collectionData?.paid_arrears?.count} icon="💰"/>
                          <div className="border-t-2 border-dashed border-slate-100 my-4"></div>
                          <StatusRow label="Masih Menunggak (Ada Piutang)" val={collectionData?.outstanding_arrears?.count} highlight="red"/>
                          <StatusRow label="Belum Bayar Piutang (Tanpa Tunggakan)" val={collectionData?.unpaid_receivable_no_arrears?.count} highlight="orange" sub="Nomen Lancar tapi belum bayar bulan ini"/>
                       </div>
                    </div>
                 </div>
              </div>
            )}

            {/* TAB: METER ANALYSIS */}
            {activeTab === 'meter' && (
               <div className="space-y-6">
                 {/* Summary Cards */}
                 <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
                    <AnomalyCard label="Pemakaian Ekstrim" val={uploadRes?.data?.summary?.extreme} color="orange" desc="Lonjakan Signifikan"/>
                    <AnomalyCard label="Stand Negatif" val={uploadRes?.data?.summary?.negative} color="rose" desc="Angka Mundur"/>
                    <AnomalyCard label="Pemakaian Zero" val={uploadRes?.data?.summary?.zero} color="blue" desc="Tidak Ada Pemakaian"/>
                    <AnomalyCard label="Estimasi / Kode Salah" val={uploadRes?.data?.summary?.estimate} color="purple" desc="Perlu Cek Ulang"/>
                 </div>

                 {/* List */}
                 <div className="bg-white rounded-[2.5rem] shadow-sm border border-slate-100 overflow-hidden">
                    <div className="divide-y divide-slate-100">
                       {uploadRes?.data?.anomalies?.map((anom, i) => (
                         <div key={i} onClick={() => { setSelectedAnomaly(anom); fetchDetective(anom.nomen); }} className="p-8 hover:bg-slate-50 cursor-pointer flex justify-between items-center group transition-all">
                            <div className="flex gap-6 items-center">
                               <div className="bg-slate-100 p-4 rounded-2xl text-slate-400 group-hover:bg-blue-600 group-hover:text-white transition-all shadow-sm"><Activity size={24}/></div>
                               <div>
                                  <div className="font-black text-xl text-slate-900 group-hover:text-blue-600 transition-colors">{anom.name}</div>
                                  <div className="flex flex-wrap gap-2 text-[10px] font-black uppercase mt-2">
                                     <span className="bg-slate-100 px-3 py-1 rounded-lg text-slate-500 tracking-widest">{anom.nomen}</span>
                                     {anom.status.map(s => <span key={s} className="bg-rose-50 text-rose-600 px-3 py-1 rounded-lg border border-rose-100">{s}</span>)}
                                  </div>
                               </div>
                            </div>
                            <div className="text-right">
                               <div className={`font-black text-3xl tracking-tighter ${anom.usage < 0 ? 'text-rose-600' : 'text-slate-900'}`}>{anom.usage} m³</div>
                               <div className="text-[10px] text-slate-400 font-bold uppercase tracking-widest mt-1">Volume Air</div>
                            </div>
                         </div>
                       ))}
                       {(!uploadRes?.data?.anomalies || uploadRes.data.anomalies.length === 0) && (
                         <div className="p-20 text-center text-slate-400 italic font-bold">Tidak ada anomali atau belum upload file SBRS.</div>
                       )}
                    </div>
                 </div>
               </div>
            )}

            {/* TAB: HISTORY SEARCH */}
            {activeTab === 'history' && (
              <div className="space-y-8">
                 <div className="bg-white p-8 rounded-[2.5rem] shadow-sm border border-slate-100 flex flex-col md:flex-row gap-6 items-end">
                    <div className="flex-1 space-y-2 w-full">
                       <label className="text-[10px] font-bold text-slate-400 uppercase tracking-widest ml-2">Kata Kunci Pencarian</label>
                       <input value={historyValue} onChange={e => setHistoryValue(e.target.value)} type="text" placeholder="Masukkan Nomen, Rayon, atau Kode PC..." className="w-full bg-slate-50 border-2 border-slate-100 rounded-2xl px-6 py-4 font-bold outline-none focus:border-blue-500 transition-all text-lg"/>
                    </div>
                    <div className="space-y-2 w-full md:w-auto">
                       <label className="text-[10px] font-bold text-slate-400 uppercase tracking-widest ml-2">Filter Berdasarkan</label>
                       <select value={historyFilterBy} onChange={e => setHistoryFilterBy(e.target.value)} className="w-full bg-slate-50 border-2 border-slate-100 rounded-2xl px-6 py-4 font-bold outline-none h-[64px]">
                          <option value="CUSTOMER">ID Pelanggan (Nomen)</option>
                          <option value="RAYON">Kode Rayon</option>
                          <option value="PC">Kode PC</option>
                          <option value="PCEZ">Kode PCEZ</option>
                          <option value="TARIF">Golongan Tarif</option>
                          <option value="METER">Ukuran Meter</option>
                       </select>
                    </div>
                    <div className="space-y-2 w-full md:w-auto">
                       <label className="text-[10px] font-bold text-slate-400 uppercase tracking-widest ml-2">Tipe Data</label>
                       <select value={historyType} onChange={e => setHistoryType(e.target.value)} className="w-full bg-slate-50 border-2 border-slate-100 rounded-2xl px-6 py-4 font-bold outline-none h-[64px]">
                          <option value="usage">History Kubikasi</option>
                          <option value="payment">History Pembayaran</option>
                       </select>
                    </div>
                    <button onClick={fetchHistory} className="bg-blue-600 text-white h-[64px] px-8 rounded-2xl font-bold hover:bg-blue-700 transition-all shadow-xl shadow-blue-200 w-full md:w-auto">
                       <Search size={24}/>
                    </button>
                 </div>

                 {historyData.length > 0 && (
                   <div className="bg-white rounded-[2.5rem] shadow-sm border border-slate-100 overflow-hidden p-10">
                      <h3 className="font-black text-xl mb-8 flex items-center gap-2">
                        <span className="bg-blue-100 text-blue-600 px-3 py-1 rounded-lg text-sm">Hasil: {historyData.length} Data</span>
                        <span className="text-slate-400 font-medium text-sm">untuk "{historyValue}"</span>
                      </h3>
                      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                         {historyData.map((item, i) => (
                           <div key={i} className="bg-slate-50 p-6 rounded-[2rem] border border-slate-100 hover:bg-white hover:shadow-lg transition-all">
                              <div className="text-xs font-bold text-slate-400 mb-2 uppercase tracking-widest">{item.period || item.date}</div>
                              <div className="text-3xl font-black text-slate-800 mb-2 tracking-tight">
                                {item.value?.toLocaleString()} <span className="text-sm text-slate-400 font-medium">{historyType==='usage' ? 'm³' : 'IDR'}</span>
                              </div>
                              <div className="text-xs font-bold bg-white inline-block px-3 py-1 rounded-lg border border-slate-100 text-slate-500">{item.desc || item.keterangan || 'Data Tercatat'}</div>
                           </div>
                         ))}
                      </div>
                   </div>
                 )}
              </div>
            )}

            {/* TAB: TOP 100 */}
            {activeTab === 'top' && (
               <div className="space-y-6">
                  {/* Category Tabs */}
                  <div className="flex flex-wrap gap-3 pb-2">
                     {[
                       {id:'premium', l:'🏆 Premium (Tepat Waktu)'}, 
                       {id:'debt', l:'⚠️ Top Tunggakan'}, 
                       {id:'unpaid_current', l:'🕒 Belum Bayar Current'}, 
                       {id:'unpaid_debt', l:'🛑 Belum Bayar Tunggakan'}
                     ].map(opt => (
                       <button key={opt.id} onClick={() => setTopCategory(opt.id)} className={`px-6 py-3 rounded-2xl text-xs font-black uppercase tracking-wide transition-all ${topCategory===opt.id ? 'bg-slate-900 text-white shadow-lg' : 'bg-white border border-slate-200 text-slate-400 hover:bg-slate-50'}`}>
                         {opt.l}
                       </button>
                     ))}
                  </div>

                  <div className="bg-white rounded-[2.5rem] shadow-sm border border-slate-100 overflow-hidden">
                     <table className="w-full text-left">
                       <thead className="bg-slate-50 text-slate-500 text-[10px] font-black uppercase tracking-widest">
                         <tr><th className="px-8 py-6">Rank</th><th className="px-8 py-6">Pelanggan</th><th className="px-8 py-6 text-right">Total Nilai</th><th className="px-8 py-6 text-right">Info Tambahan</th></tr>
                       </thead>
                       <tbody className="divide-y divide-slate-50 text-sm font-medium text-slate-700">
                         {topData.map((row, i) => (
                           <tr key={i} className="hover:bg-slate-50">
                             <td className="px-8 py-5 font-black text-slate-300 italic text-xl">#{i+1}</td>
                             <td className="px-8 py-5">
                                <div className="font-black text-slate-900 text-lg">{row.name || row.NAMA}</div>
                                <div className="text-[10px] font-mono font-bold text-slate-400 mt-1 bg-slate-100 inline-block px-2 py-0.5 rounded">{row.nomen || row.NOMEN}</div>
                             </td>
                             <td className="px-8 py-5 text-right font-mono font-bold text-blue-600 text-lg">
                               Rp {(row.total_paid || row.debt_amount || row.outstanding || 0).toLocaleString()}
                             </td>
                             <td className="px-8 py-5 text-right text-xs font-bold text-slate-500">
                               {row.UMUR_TUNGGAKAN ? <span className="text-rose-500">{row.UMUR_TUNGGAKAN} Bulan</span> : '-'}
                             </td>
                           </tr>
                         ))}
                       </tbody>
                     </table>
                     {topData.length === 0 && <div className="p-20 text-center text-slate-400 italic font-bold">Tidak ada data untuk kategori ini.</div>}
                  </div>
               </div>
            )}
            
          </div>
        )}
      </main>
    </div>
  );
};

// --- SUB COMPONENTS ---

const StatBox = ({ label, val, color, icon, sub }) => {
  const colors = { blue: 'text-blue-600 bg-blue-50 border-blue-100', emerald: 'text-emerald-600 bg-emerald-50 border-emerald-100', rose: 'text-rose-600 bg-rose-50 border-rose-100', indigo: 'text-indigo-600 bg-indigo-50 border-indigo-100' };
  return (
    <div className={`p-8 rounded-[2.5rem] border shadow-sm bg-white ${colors[color].split(' ')[2]}`}>
      <div className={`w-12 h-12 rounded-2xl flex items-center justify-center mb-6 ${colors[color]} shadow-sm`}>{icon}</div>
      <div className="text-4xl font-black tracking-tighter mb-2 text-slate-900">
        {typeof val === 'number' ? (val/1000000).toFixed(1) + 'M' : '0'}
      </div>
      <div className="text-[10px] font-black uppercase text-slate-400 tracking-widest mb-1">{label}</div>
      <div className="text-[9px] font-bold text-slate-300 uppercase">{sub}</div>
    </div>
  );
}

const StatusRow = ({ label, val, highlight, icon, sub }) => (
  <div className="flex justify-between items-start py-3 group">
    <div className="flex items-center gap-3">
       {icon && <span className="text-lg">{icon}</span>}
       <div>
         <div className={`text-sm font-bold ${highlight ? (highlight==='red'?'text-rose-600':'text-orange-500') : 'text-slate-600'}`}>{label}</div>
         {sub && <div className="text-[9px] font-bold text-slate-300 uppercase mt-0.5">{sub}</div>}
       </div>
    </div>
    <span className="font-mono font-black text-slate-900 bg-slate-50 px-3 py-1 rounded-lg group-hover:bg-blue-50 group-hover:text-blue-600 transition-colors">{val ? val.toLocaleString() : 0}</span>
  </div>
);

const AnomalyCard = ({ label, val, color, desc }) => {
   const colors = { orange: 'bg-orange-50 text-orange-600 border-orange-100', rose: 'bg-rose-50 text-rose-600 border-rose-100', blue: 'bg-blue-50 text-blue-600 border-blue-100', purple: 'bg-purple-50 text-purple-600 border-purple-100' };
   return (
     <div className={`p-6 rounded-[2rem] border ${colors[color]} flex flex-col justify-between h-32 relative overflow-hidden`}>
        <div className="relative z-10">
           <div className="text-4xl font-black mb-1">{val || 0}</div>
           <div className="text-[10px] font-black uppercase tracking-widest opacity-80">{label}</div>
        </div>
        <div className="text-[9px] font-bold uppercase opacity-60 mt-auto">{desc}</div>
        <Activity className="absolute -right-2 -bottom-2 opacity-10 w-20 h-20"/>
     </div>
   )
}

export default App;
