import React, { useState, useEffect } from 'react';
import { 
  ShieldCheck, Upload, Activity, BarChart3, Users, 
  AlertTriangle, ArrowLeft, Save, ChevronRight, Search, 
  FileText, Database, CreditCard, Droplets, RefreshCw,
  TrendingUp, Layers, PieChart, Info, Download, Filter,
  CheckCircle, History, MapPin, User, MessageSquare
} from 'lucide-react';

// Configuration: Adjust this to your VPS IP if deploying remotely
const API_BASE_URL = 'http://174.138.16.241:5000/api';

const App = () => {
  // --- STATE MANAGEMENT ---
  const [activeTab, setActiveTab] = useState('upload');
  const [loading, setLoading] = useState(false);
  const [uploadResult, setUploadResult] = useState(null);
  const [selectedAnomaly, setSelectedAnomaly] = useState(null);
  const [detectiveHistory, setDetectiveHistory] = useState(null);

  // --- FILTERS & DATA ---
  const [rayonFilter, setRayonFilter] = useState('34');
  const [summarizingDim, setSummarizingDim] = useState('RAYON');
  const [summaryData, setSummaryData] = useState([]);
  const [summaryTarget, setSummaryTarget] = useState('mc');
  const [collectionData, setCollectionData] = useState(null);
  const [paymentStatus, setPaymentStatus] = useState(null);
  const [topPremium, setTopPremium] = useState([]);
  const [topDebt, setTopDebt] = useState([]);
  const [topUnpaidCurrent, setTopUnpaidCurrent] = useState([]);
  const [topUnpaidDebt, setTopUnpaidDebt] = useState([]);
  const [auditRemark, setAuditRemark] = useState('');
  const [auditStatus, setAuditStatus] = useState('RE-CHECK');

  // --- API SERVICE FUNCTIONS ---

  const handleFileUpload = async (e) => {
    const file = e.target.files[0];
    if (!file) return;

    setLoading(true);
    const formData = new FormData();
    formData.append('file', file);

    try {
      const response = await fetch(`${API_BASE_URL}/upload-and-analyze`, {
        method: 'POST',
        body: formData,
      });
      const result = await response.json();
      
      if (result.status === 'success') {
        setUploadResult(result);
        
        // Auto-navigate based on detected file type
        if (result.type === 'METER_READING') {
          setActiveTab('meter');
        } else if (result.type === 'BILLING_SUMMARY') {
          setActiveTab('summary');
          fetchSummaryData();
        } else if (result.type === 'COLLECTION_REPORT') {
          setActiveTab('collection');
          fetchCollectionData();
        }
      } else {
        alert(result.message);
      }
    } catch (error) {
      alert("Gagal koneksi ke server backend! Pastikan backend berjalan di port 5000.");
      console.error(error);
    }
    setLoading(false);
  };

  const fetchSummaryData = async () => {
    setLoading(true);
    try {
      const res = await fetch(`${API_BASE_URL}/summary?target=${summaryTarget}&dimension=${summarizingDim}&rayon=${rayonFilter}`);
      const data = await res.json();
      if (data.status === 'success') setSummaryData(data.data);
    } catch (e) {
      console.error("Error fetching summary:", e);
    }
    setLoading(false);
  };

  const fetchCollectionData = async () => {
    setLoading(true);
    try {
      const [collRes, statusRes] = await Promise.all([
        fetch(`${API_BASE_URL}/collection/detailed?rayon=${rayonFilter}`),
        fetch(`${API_BASE_URL}/collection/payment-status?rayon=${rayonFilter}`)
      ]);
      
      const collData = await collRes.json();
      const statusData = await statusRes.json();
      
      if (collData.status === 'success') setCollectionData(collData.data);
      if (statusData.status === 'success') setPaymentStatus(statusData.data);
    } catch (e) {
      console.error("Error fetching collection:", e);
    }
    setLoading(false);
  };

  const fetchTopData = async () => {
    setLoading(true);
    try {
      const [premiumRes, debtRes, unpaidCurrentRes, unpaidDebtRes] = await Promise.all([
        fetch(`${API_BASE_URL}/top100/premium?rayon=${rayonFilter}`),
        fetch(`${API_BASE_URL}/top100/debt?rayon=${rayonFilter}`),
        fetch(`${API_BASE_URL}/top100/unpaid-current?rayon=${rayonFilter}`),
        fetch(`${API_BASE_URL}/top100/unpaid-debt?rayon=${rayonFilter}`)
      ]);
      
      const premium = await premiumRes.json();
      const debt = await debtRes.json();
      const unpaidCurr = await unpaidCurrentRes.json();
      const unpaidDebt = await unpaidDebtRes.json();
      
      if (premium.status === 'success') setTopPremium(premium.data);
      if (debt.status === 'success') setTopDebt(debt.data);
      if (unpaidCurr.status === 'success') setTopUnpaidCurrent(unpaidCurr.data);
      if (unpaidDebt.status === 'success') setTopUnpaidDebt(unpaidDebt.data);
    } catch (e) {
      console.error("Error fetching top data:", e);
    }
    setLoading(false);
  };

  const fetchDetectiveData = async (nomen) => {
    setLoading(true);
    try {
      const res = await fetch(`${API_BASE_URL}/detective/${nomen}`);
      const data = await res.json();
      if (data.status === 'success') setDetectiveHistory(data.data);
    } catch (e) {
      console.error("Error fetching detective data:", e);
    }
    setLoading(false);
  };

  const saveAuditResult = async () => {
    if (!auditRemark) return alert("Keterangan audit wajib diisi!");
    
    try {
      const res = await fetch(`${API_BASE_URL}/audit/save`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          nomen: selectedAnomaly.nomen,
          remark: auditRemark,
          status: auditStatus
        })
      });
      
      const data = await res.json();
      
      if (data.status === 'success') {
        alert("Analisa manual berhasil disimpan!");
        setSelectedAnomaly(null);
        setAuditRemark('');
      } else {
        alert("Gagal menyimpan: " + data.message);
      }
    } catch (e) {
      alert("Gagal menyimpan data: " + e.message);
    }
  };

  // --- TRIGGER DATA ON STATE CHANGE ---
  useEffect(() => {
    if (activeTab === 'summary') {
      fetchSummaryData();
    } else if (activeTab === 'collection') {
      fetchCollectionData();
    } else if (activeTab === 'top') {
      fetchTopData();
    }
  }, [activeTab, summarizingDim, summaryTarget, rayonFilter]);

  // --- SHARED UI COMPONENTS ---

  const SidebarItem = ({ id, icon, label }) => (
    <button 
      onClick={() => { setActiveTab(id); setSelectedAnomaly(null); }}
      className={`w-full flex items-center gap-4 px-6 py-4 rounded-2xl transition-all font-bold text-sm ${
        activeTab === id ? 'bg-blue-600 text-white shadow-xl shadow-blue-900/40 translate-x-2' : 'hover:bg-slate-900 hover:text-white'
      }`}
    >
      {icon} <span>{label}</span>
    </button>
  );

  return (
    <div className="min-h-screen bg-[#f8fafc] flex font-sans text-slate-900 selection:bg-blue-100">
      {/* SIDEBAR NAVIGATION */}
      <aside className="fixed inset-y-0 left-0 w-72 bg-slate-950 text-slate-400 p-6 flex flex-col z-20 border-r border-white/5 shadow-2xl">
        <div className="flex items-center gap-4 mb-12 text-white px-2">
          <div className="bg-gradient-to-br from-blue-500 to-indigo-700 p-3 rounded-2xl shadow-xl shadow-blue-500/20">
            <ShieldCheck size={28} />
          </div>
          <div>
            <h1 className="font-black text-2xl tracking-tighter leading-none uppercase">PAM DSS</h1>
            <span className="text-[10px] font-bold text-blue-400 tracking-widest uppercase">Detective Analytics</span>
          </div>
        </div>
        
        <nav className="space-y-2 flex-1">
          <SidebarItem id="upload" icon={<Upload size={20}/>} label="Upload Center" />
          <SidebarItem id="summary" icon={<BarChart3 size={20}/>} label="Summarizing" />
          <SidebarItem id="collection" icon={<CreditCard size={20}/>} label="Collection & AR" />
          <SidebarItem id="meter" icon={<Activity size={20}/>} label="Meter Analysis" />
          <SidebarItem id="top" icon={<Users size={20}/>} label="Top 100 Analytics" />
        </nav>

        <div className="mt-auto bg-slate-900/50 p-5 rounded-[2rem] border border-white/5">
            <p className="text-[10px] font-black text-slate-500 uppercase mb-3 tracking-widest text-center">Rayon Monitoring</p>
            <div className="flex gap-2">
              <button 
                onClick={() => setRayonFilter('34')} 
                className={`flex-1 py-3 rounded-xl text-xs font-black transition-all ${rayonFilter === '34' ? 'bg-blue-600 text-white shadow-lg' : 'hover:bg-slate-800'}`}
              >
                R-34
              </button>
              <button 
                onClick={() => setRayonFilter('35')} 
                className={`flex-1 py-3 rounded-xl text-xs font-black transition-all ${rayonFilter === '35' ? 'bg-blue-600 text-white shadow-lg' : 'hover:bg-slate-800'}`}
              >
                R-35
              </button>
            </div>
        </div>
      </aside>

      {/* MAIN CONTENT AREA */}
      <main className="flex-1 ml-72 p-10">
        {!selectedAnomaly ? (
          <div className="animate-in fade-in duration-500 space-y-10 max-w-7xl mx-auto">
            <header className="flex justify-between items-end border-b border-slate-200 pb-8">
              <div>
                <h2 className="text-5xl font-black text-slate-900 uppercase tracking-tighter leading-none">
                  {activeTab === 'upload' && 'Pusat Unggah'}
                  {activeTab === 'summary' && 'Summarizing'}
                  {activeTab === 'collection' && 'Collection & AR'}
                  {activeTab === 'meter' && 'Hasil Analisa'}
                  {activeTab === 'top' && 'Top Rankings'}
                </h2>
                <p className="text-slate-500 font-medium mt-4 text-lg">Keputusan bisnis berbasis statistik deskriptif dan deteksi anomali.</p>
              </div>
              {uploadResult && (
                <div className="flex items-center gap-3 bg-emerald-50 px-6 py-3 rounded-2xl border border-emerald-100 shadow-sm">
                  <div className="w-2.5 h-2.5 bg-emerald-500 rounded-full animate-pulse"></div>
                  <span className="text-xs font-black text-emerald-700 uppercase tracking-widest">Aktif: {uploadResult.filename}</span>
                </div>
              )}
            </header>

            {/* VIEW: UPLOAD CENTER */}
            {activeTab === 'upload' && (
              <div className="p-32 border-4 border-dashed border-slate-200 rounded-[4rem] bg-white text-center hover:border-blue-400 transition-all group shadow-sm relative overflow-hidden">
                <div className="relative z-10">
                  <Upload size={80} className="mx-auto text-blue-500 mb-10 group-hover:scale-110 transition-transform" />
                  <h3 className="text-3xl font-black mb-4 text-slate-800">Unggah Data Untuk Analisa</h3>
                  <p className="text-slate-400 mb-12 max-w-md mx-auto font-bold text-lg">Mendukung file SBRS (Cycle), Master (MC/MB/ARDEBT), dan Collection (Harian).</p>
                  <label className="bg-blue-600 text-white px-16 py-6 rounded-[2rem] font-black text-lg cursor-pointer shadow-2xl shadow-blue-200 hover:bg-blue-700 transition-all inline-block active:scale-95 uppercase tracking-widest">
                    {loading ? 'Sedang Memproses...' : 'Pilih File Sekarang'}
                    <input type="file" className="hidden" onChange={handleFileUpload} accept=".csv,.txt,.xlsx,.xls" />
                  </label>
                </div>
                <Database size={300} className="absolute -right-20 -bottom-20 opacity-[0.03] -rotate-12" />
              </div>
            )}

            {/* VIEW: SUMMARIZING REPORT */}
            {activeTab === 'summary' && (
              <div className="space-y-8 animate-in slide-in-from-bottom-6 duration-700">
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
                   <HeroStat title="Revenue Impact" val={uploadResult?.data?.total_nominal ? `Rp ${(uploadResult.data.total_nominal/1000000).toFixed(1)} M` : 'Rp 0'} icon={<FileText/>} color="blue" />
                   <HeroStat title="Total Volume" val={uploadResult?.data?.total_volume ? `${uploadResult.data.total_volume.toLocaleString()} m³` : '0 m³'} icon={<Droplets/>} color="emerald" />
                   <HeroStat title="Total Records" val={uploadResult?.data?.total_records?.toLocaleString() || 0} icon={<Database/>} color="rose" />
                   <HeroStat title="Active Rayon" val={`R-${rayonFilter}`} icon={<TrendingUp/>} color="indigo" />
                </div>

                <div className="bg-white rounded-[3rem] shadow-sm border border-slate-100 overflow-hidden">
                  <div className="p-10 border-b border-slate-50 flex flex-wrap gap-6 justify-between items-center bg-slate-50/30">
                    <h3 className="font-black text-xl text-slate-800 uppercase tracking-widest">Tabel Ringkasan Multidimensi</h3>
                    <div className="flex flex-wrap gap-4">
                      <select 
                        value={summaryTarget}
                        onChange={(e) => setSummaryTarget(e.target.value)}
                        className="px-4 py-2 rounded-xl border-2 border-slate-200 font-bold text-sm bg-white outline-none focus:border-blue-500 transition-all"
                      >
                        <option value="mc">MC (Master Cetak)</option>
                        <option value="mb">MB (Master Bayar)</option>
                        <option value="ardebt">ARDEBT (Piutang)</option>
                        <option value="mainbill">Main Bill</option>
                        <option value="coll">Collection (Harian)</option>
                      </select>
                      
                      <div className="flex bg-white rounded-2xl p-1.5 border border-slate-200 shadow-sm ring-1 ring-slate-100">
                        {['RAYON', 'PC', 'PCEZ', 'TARIF', 'METER'].map(d => (
                          <button 
                            key={d} 
                            onClick={() => setSummarizingDim(d)}
                            className={`px-6 py-2.5 rounded-xl text-[10px] font-black tracking-[0.2em] transition-all ${summarizingDim === d ? 'bg-slate-900 text-white' : 'text-slate-400 hover:text-slate-600'}`}
                          >
                            {d}
                          </button>
                        ))}
                      </div>
                    </div>
                  </div>
                  <div className="overflow-x-auto">
                    <table className="w-full text-left">
                      <thead className="bg-slate-50 text-slate-400 text-[10px] uppercase tracking-[0.3em] font-black">
                        <tr>
                          <th className="px-10 py-6">Grup {summarizingDim}</th>
                          <th className="px-10 py-6 text-right">Nominal (Juta)</th>
                          <th className="px-10 py-6 text-right">Volume (m³)</th>
                          <th className="px-10 py-6 text-right">Count</th>
                          <th className="px-10 py-6 text-right">Realisasi %</th>
                          <th className="px-10 py-6 text-center">Aksi</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-slate-100">
                        {loading ? (
                          <tr><td colSpan="6" className="p-20 text-center"><RefreshCw size={40} className="animate-spin mx-auto text-blue-200 mb-4" /><span className="text-slate-400 font-bold italic">Loading data...</span></td></tr>
                        ) : summaryData.length > 0 ? (
                          summaryData.map((row, idx) => (
                            <TableRow 
                              key={idx}
                              label={row.group}
                              val={row.nominal.toFixed(2)}
                              vol={row.volume.toLocaleString()}
                              count={row.count}
                              pct={`${row.realization_pct}%`}
                            />
                          ))
                        ) : (
                          <tr><td colSpan="6" className="p-20 text-center text-slate-400 italic font-bold">Tidak ada data. Silakan upload file Master terlebih dahulu.</td></tr>
                        )}
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>
            )}

            {/* VIEW: METER ANALYSIS */}
            {activeTab === 'meter' && (
              <div className="space-y-8 animate-in slide-in-from-bottom-6 duration-700">
                <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
                   <MiniStat label="Ekstrim" val={uploadResult?.data?.summary?.extreme || 0} color="orange" />
                   <MiniStat label="Stand Negatif" val={uploadResult?.data?.summary?.negative || 0} color="rose" />
                   <MiniStat label="Nol Usage" val={uploadResult?.data?.summary?.zero || 0} color="blue" />
                   <MiniStat label="Salah Catat" val={uploadResult?.data?.summary?.wrong_record || 0} color="purple" />
                </div>

                <div className="bg-white rounded-[3rem] shadow-sm border border-slate-100 overflow-hidden">
                  <div className="p-10 border-b border-slate-50 flex justify-between items-center bg-orange-50/20">
                    <div className="flex items-center gap-4">
                      <div className="bg-orange-500 p-3 rounded-2xl text-white shadow-lg shadow-orange-200"><AlertTriangle size={24}/></div>
                      <h3 className="font-black text-2xl text-slate-800 uppercase tracking-tighter leading-none">Investigasi Temuan Lapangan</h3>
                    </div>
                    <button className="bg-white border border-slate-200 px-6 py-3 rounded-2xl text-xs font-black uppercase tracking-widest shadow-sm hover:bg-slate-50 flex items-center gap-2">
                       <Download size={16}/> Ekspor Laporan
                    </button>
                  </div>
                  <div className="divide-y divide-slate-100">
                    {uploadResult?.data?.anomalies && uploadResult.data.anomalies.length > 0 ? (
                      uploadResult.data.anomalies.map((anom, i) => (
                        <div 
                          key={i} 
                          onClick={() => { setSelectedAnomaly(anom); fetchDetectiveData(anom.nomen); }}
                          className="p-10 flex flex-wrap justify-between items-center hover:bg-blue-50/50 cursor-pointer transition-all group relative overflow-hidden"
                        >
                          <div className="flex gap-10 items-center relative z-10">
                            <div className={`p-6 rounded-[2rem] shadow-xl ${anom.status.includes('STAND NEGATIF') ? 'bg-rose-500 shadow-rose-100' : 'bg-orange-500 shadow-orange-100'}`}>
                              <Activity size={32} className="text-white" />
                            </div>
                            <div>
                              <div className="font-black text-3xl text-slate-900 group-hover:text-blue-600 transition-colors tracking-tight">{anom.name}</div>
                              <div className="flex items-center gap-4 mt-3">
                                 <span className="text-sm font-bold text-slate-400 tracking-widest uppercase">Nomen: {anom.nomen}</span>
                                 <span className="w-1.5 h-1.5 bg-slate-200 rounded-full"></span>
                                 <div className="flex flex-wrap gap-2">
                                    {anom.status.map((s, idx) => (
                                      <span key={idx} className="bg-slate-950 text-white text-[10px] font-black px-3 py-1 rounded-lg uppercase tracking-widest border border-white/20">
                                        {s}
                                      </span>
                                    ))}
                                 </div>
                              </div>
                            </div>
                          </div>
                          <div className="flex items-center gap-12 relative z-10 mt-6 md:mt-0">
                            <div className="text-right">
                              <div className={`text-4xl font-black ${anom.usage < 0 ? 'text-rose-600' : 'text-slate-900'} tracking-tighter`}>
                                {anom.usage} m³
                              </div>
                              <div className="text-[10px] font-black text-slate-400 uppercase tracking-[0.2em] mt-2 leading-none">Pemakaian</div>
                            </div>
                            <div className="p-4 bg-white rounded-2xl shadow-sm border border-slate-100 group-hover:bg-blue-600 group-hover:text-white transition-all">
                               <ChevronRight size={24} />
                            </div>
                          </div>
                        </div>
                      ))
                    ) : (
                       <div className="p-32 text-center text-slate-300 font-black uppercase tracking-widest italic opacity-50">
                         {uploadResult && uploadResult.type !== 'METER_READING' ? 
                           'Silakan Unggah File SBRS Untuk Melihat Anomali' : 
                           'Tidak ada anomali terdeteksi'}
                       </div>
                    )}
                  </div>
                </div>
              </div>
            )}

            {/* VIEW: COLLECTION ANALYTICS */}
            {activeTab === 'collection' && (
               <div className="space-y-10 animate-in slide-in-from-bottom-6 duration-700">
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                     <div className="p-12 rounded-[4rem] bg-gradient-to-br from-blue-600 to-indigo-700 text-white shadow-2xl shadow-blue-200 relative overflow-hidden group">
                        <div className="text-xs font-black uppercase tracking-[0.2em] opacity-60 mb-6">Collection Undue (Dimuka)</div>
                        <div className="text-6xl font-black tracking-tighter mb-4 leading-none">
                          Rp {collectionData?.undue?.revenue?.toFixed(1) || '0'} JT
                        </div>
                        <p className="text-sm font-bold opacity-80 flex items-center gap-2 tracking-wide">
                          <Users size={18}/> {collectionData?.undue?.count || 0} Pelanggan
                        </p>
                        <CreditCard size={200} className="absolute -right-20 -bottom-20 opacity-10 -rotate-12 group-hover:scale-110 transition-transform" />
                     </div>
                     <div className="p-12 rounded-[4rem] bg-gradient-to-br from-emerald-600 to-teal-700 text-white shadow-2xl shadow-emerald-200 relative overflow-hidden group">
                        <div className="text-xs font-black uppercase tracking-[0.2em] opacity-60 mb-6">Collection Current (Lancar)</div>
                        <div className="text-6xl font-black tracking-tighter mb-4 leading-none">
                          Rp {collectionData?.current?.revenue?.toFixed(1) || '0'} JT
                        </div>
                        <p className="text-sm font-bold opacity-80 flex items-center gap-2 tracking-wide">
                          <Users size={18}/> {collectionData?.current?.count || 0} Pelanggan
                        </p>
                        <Droplets size={200} className="absolute -right-20 -bottom-20 opacity-10 -rotate-12 group-hover:scale-110 transition-transform" />
                     </div>
                  </div>

                  <div className="bg-white rounded-[3rem] p-12 shadow-sm border border-slate-100">
                     <h3 className="font-black text-2xl mb-10 flex items-center gap-3 tracking-tighter">
                        <Layers className="text-blue-600" /> MONITORING PIUTANG & TUNGGAKAN
                     </h3>
                     <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
                        <StatusCard 
                          label="Pelanggan Tunggakan" 
                          val={paymentStatus?.with_debt?.count || 0} 
                          color="rose" 
                          sub={`Total: Rp ${paymentStatus?.with_debt?.total?.toFixed(1) || '0'} JT`}
                        />
                        <StatusCard 
                          label="Berhasil Ditagih (Debt)" 
                          val={paymentStatus?.paid_debt?.count || 0} 
                          color="emerald"
                          sub={`Total: Rp ${paymentStatus?.paid_debt?.total?.toFixed(1) || '0'} JT`}
                        />
                        <StatusCard 
                          label="Belum Bayar Piutang" 
                          val={paymentStatus?.unpaid_receivable?.count || 0} 
                          color="blue" 
                          sub="Nomen Tanpa Tunggakan" 
                        />
                     </div>
                  </div>
               </div>
            )}

            {/* VIEW: TOP 100 RANKINGS */}
            {activeTab === 'top' && (
              <div className="space-y-8 animate-in fade-in duration-1000 pb-20">
                 <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
                   <RankList 
                     title={`Top 100 Premium - R-${rayonFilter}`} 
                     type="premium" 
                     data={topPremium}
                     loading={loading}
                   />
                   <RankList 
                     title={`Top 100 Tunggakan - R-${rayonFilter}`} 
                     type="debt" 
                     data={topDebt}
                     loading={loading}
                   />
                 </div>
                 
                 <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
                   <RankList 
                     title={`Top 100 Unpaid Current - R-${rayonFilter}`} 
                     type="unpaid-current" 
                     data={topUnpaidCurrent}
                     loading={loading}
                   />
                   <RankList 
                     title={`Top 100 Unpaid Debt - R-${rayonFilter}`} 
                     type="unpaid-debt" 
                     data={topUnpaidDebt}
                     loading={loading}
                   />
                 </div>
              </div>
            )}
          </div>
        ) : (
          /* --- DETECTIVE MODE DETAIL --- */
          <div className="max-w-7xl mx-auto space-y-8 animate-in slide-in-from-bottom-12 duration-700 pb-32">
            <header className="flex flex-wrap items-center justify-between bg-white p-8 rounded-[2.5rem] border border-slate-100 shadow-sm gap-6">
              <div className="flex items-center gap-6">
                <button onClick={() => setSelectedAnomaly(null)} className="p-5 bg-slate-50 rounded-2xl hover:bg-slate-100 transition-all text-slate-400 hover:text-slate-900 border border-slate-100">
                  <ArrowLeft size={28} />
                </button>
                <div>
                  <h2 className="text-4xl font-black text-slate-900 leading-none tracking-tighter">{selectedAnomaly.name}</h2>
                  <div className="flex items-center gap-6 mt-3">
                    <span className="text-sm font-black text-slate-400 uppercase tracking-widest">Detective Analytics #{selectedAnomaly.nomen}</span>
                    <span className="w-1.5 h-1.5 bg-slate-300 rounded-full"></span>
                    <div className="flex flex-wrap gap-2">
                       {selectedAnomaly.status.map((s, idx) => (
                         <span key={idx} className="px-3 py-1 bg-rose-100 text-rose-700 rounded-lg text-[10px] font-black uppercase tracking-widest border border-rose-200">
                           {s}
                         </span>
                       ))}
                    </div>
                  </div>
                </div>
              </div>
            </header>

            <div className="grid grid-cols-1 lg:grid-cols-12 gap-10">
              <div className="lg:col-span-8 space-y-8">
                {/* READING HISTORY TABLE */}
                <div className="bg-white rounded-[3rem] shadow-sm border border-slate-100 overflow-hidden">
                  <div className="p-10 border-b border-slate-50 flex justify-between items-center bg-blue-50/20">
                    <h3 className="font-black text-slate-800 flex items-center gap-4 uppercase tracking-widest text-sm">
                      <History size={22} className="text-blue-600" /> Riwayat 12 Periode (Lengkap)
                    </h3>
                  </div>
                  <div className="overflow-x-auto">
                    <table className="w-full text-left">
                      <thead className="bg-slate-50 text-slate-400 text-[9px] uppercase tracking-[0.3em] font-black">
                        <tr>
                          <th className="px-8 py-6">Tgl Baca</th>
                          <th className="px-8 py-6 text-center">Prev Read</th>
                          <th className="px-8 py-6 text-center">Curr Read</th>
                          <th className="px-8 py-6 text-center">Volume</th>
                          <th className="px-8 py-6">Codes</th>
                          <th className="px-8 py-6">Message</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-slate-100 text-xs font-bold text-slate-600">
                        {detectiveHistory?.reading_history?.length > 0 ? (
                          detectiveHistory.reading_history.map((h, idx) => (
                            <tr key={idx} className="hover:bg-slate-50 transition-colors">
                              <td className="px-8 py-6 font-black text-slate-900">{h.cmr_rd_date || '-'}</td>
                              <td className="px-8 py-6 text-center font-mono text-slate-400">{h.cmr_prev_read?.toLocaleString() || 0}</td>
                              <td className="px-8 py-6 text-center font-mono font-black text-slate-900">{h.cmr_reading?.toLocaleString() || 0}</td>
                              <td className={`px-8 py-6 text-center font-black text-sm ${(h.cmr_reading - h.cmr_prev_read) > 100 ? 'text-rose-600' : 'text-blue-600'}`}>
                                {(h.cmr_reading - h.cmr_prev_read)?.toLocaleString() || 0} <span className="text-[10px] opacity-40 uppercase">m³</span>
                              </td>
                              <td className="px-8 py-6">
                                <div className="flex gap-1">
                                  {h.cmr_skip_code && <div className="text-[8px] bg-orange-100 text-orange-700 px-1.5 py-0.5 rounded font-black uppercase">S:{h.cmr_skip_code}</div>}
                                  {h.cmr_trbl1_code && <div className="text-[8px] bg-rose-100 text-rose-700 px-1.5 py-0.5 rounded font-black uppercase">T:{h.cmr_trbl1_code}</div>}
                                </div>
                              </td>
                              <td className="px-8 py-6 italic text-slate-400 text-[10px] max-w-[150px] truncate">{h.cmr_chg_spcl_msg || '-'}</td>
                            </tr>
                          ))
                        ) : (
                          <tr><td colSpan="6" className="p-10 text-center text-slate-400 italic">Mencari data history...</td></tr>
                        )}
                      </tbody>
                    </table>
                  </div>
                </div>

                <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                   <DetailBox 
                     title="Info Pelanggan" 
                     icon={<User/>} 
                     val={detectiveHistory?.customer?.NAMA || 'Mencari...'} 
                     sub={`Tarif: ${detectiveHistory?.customer?.TARIFF || '-'} | Rayon: ${detectiveHistory?.customer?.RAYON || '-'}`} 
                   />
                   <DetailBox 
                     title="Analisa Sistem" 
                     icon={<AlertTriangle/>} 
                     val={selectedAnomaly?.details?.anomaly_reason || 'Lonjakan Terdeteksi'} 
                     sub={`Skip: ${selectedAnomaly?.details?.skip_desc || '-'}`} 
                   />
                </div>
              </div>

              {/* AUDIT PANEL */}
              <div className="lg:col-span-4 flex flex-col gap-8">
                <div className="bg-white p-10 rounded-[4rem] border-[10px] border-blue-500 shadow-2xl flex flex-col h-full ring-2 ring-blue-100 relative overflow-hidden">
                  <div className="relative z-10">
                    <div className="mb-10">
                      <h3 className="font-black text-3xl flex items-center gap-3 text-slate-900 tracking-tighter leading-none uppercase">
                        <MessageSquare className="text-blue-600" size={32} /> Audit Lapangan
                      </h3>
                      <p className="text-[10px] text-slate-400 font-black uppercase tracking-[0.3em] mt-4 bg-slate-50 py-3 px-5 rounded-2xl w-fit border border-slate-100 shadow-inner">
                          Entry Keputusan Petugas
                      </p>
                    </div>
                    
                    <div className="mb-10 space-y-4">
                       <p className="text-[11px] font-black text-slate-400 uppercase tracking-widest pl-2">Status Investigasi:</p>
                       <div className="flex flex-wrap gap-2.5">
                          {['VALID', 'FRAUD', 'RE-CHECK', 'RE-READ', 'RUSAK'].map(s => (
                             <button 
                               key={s} 
                               onClick={() => setAuditStatus(s)}
                               className={`px-5 py-3 rounded-2xl text-[10px] font-black transition-all border-2 ${auditStatus === s ? 'bg-slate-900 border-slate-900 text-white shadow-xl shadow-slate-200 -translate-y-1' : 'bg-slate-50 border-slate-50 text-slate-400 hover:border-slate-100'}`}
                             >
                                {s}
                             </button>
                          ))}
                       </div>
                    </div>

                    <textarea 
                      value={auditRemark}
                      onChange={(e) => setAuditRemark(e.target.value)}
                      placeholder="Contoh: Berdasarkan history, pemakaian bulan ini valid karena adanya renovasi di rumah pelanggan."
                      className="w-full h-80 p-8 bg-slate-50 border-2 border-slate-50 rounded-[3rem] outline-none focus:ring-[12px] focus:ring-blue-500/10 font-medium text-slate-700 text-lg leading-relaxed shadow-inner placeholder:italic placeholder:text-slate-300"
                    />

                    <div className="mt-12 space-y-6">
                      <button 
                        onClick={saveAuditResult}
                        className="w-full bg-blue-600 text-white py-8 rounded-[2.5rem] font-black text-xl shadow-2xl shadow-blue-500/50 flex items-center justify-center gap-4 hover:bg-blue-700 hover:-translate-y-2 transition-all active:scale-95 uppercase tracking-widest"
                      >
                        <Save size={28}/> Simpan Audit
                      </button>
                      <p className="text-center text-[10px] font-bold text-slate-300 uppercase tracking-[0.3em]">
                          Laporan akan disimpan ke Koleksi ManualAudit
                      </p>
                    </div>
                  </div>
                  <MessageSquare size={150} className="absolute -right-16 -top-16 opacity-[0.03] -rotate-12" />
                </div>
              </div>
            </div>
          </div>
        )}
      </main>
    </div>
  );
};

// --- HELPER UI COMPONENTS ---

const HeroStat = ({ title, val, icon, color }) => {
  const cMap = {
    blue: 'from-blue-600 to-indigo-700 shadow-blue-100',
    emerald: 'from-emerald-600 to-teal-700 shadow-emerald-100',
    rose: 'from-rose-600 to-pink-700 shadow-rose-100',
    indigo: 'from-indigo-600 to-purple-700 shadow-indigo-100'
  };
  return (
    <div className={`p-8 rounded-[2.5rem] bg-gradient-to-br ${cMap[color]} text-white shadow-2xl group transition-all hover:scale-[1.02]`}>
        <div className="flex justify-between items-start mb-6">
          <div className="bg-white/20 p-3 rounded-2xl backdrop-blur-md group-hover:scale-110 transition-transform">{icon}</div>
          <TrendingUp size={16} className="opacity-40" />
        </div>
        <div className="text-[10px] font-black uppercase tracking-[0.2em] opacity-60 mb-2">{title}</div>
        <div className="text-3xl font-black tracking-tighter leading-none">{val}</div>
    </div>
  );
};

const TableRow = ({ label, val, vol, count, pct }) => (
  <tr className="hover:bg-slate-50/80 transition-colors group">
    <td className="px-10 py-6 font-black text-slate-800 text-lg tracking-tighter">{label}</td>
    <td className="px-10 py-6 text-right font-mono font-bold text-slate-600">{val}M</td>
    <td className="px-10 py-6 text-right font-mono font-bold text-blue-600">{vol}</td>
    <td className="px-10 py-6 text-right font-mono font-bold text-slate-500">{count}</td>
    <td className="px-10 py-6 text-right">
        <span className="bg-emerald-100 text-emerald-700 px-4 py-1.5 rounded-full text-[10px] font-black border border-emerald-200">{pct}</span>
    </td>
    <td className="px-10 py-6 text-center">
        <div className="flex justify-center"><ChevronRight size={18} className="text-slate-300 group-hover:text-blue-600 transition-all group-hover:translate-x-1" /></div>
    </td>
  </tr>
);

const MiniStat = ({ label, val, color }) => {
  const cMap = {
    orange: 'bg-orange-50 text-orange-600 border-orange-100',
    rose: 'bg-rose-50 text-rose-600 border-rose-100',
    blue: 'bg-blue-50 text-blue-600 border-blue-100',
    purple: 'bg-purple-50 text-purple-600 border-purple-100'
  };
  return (
    <div className={`p-8 rounded-3xl border-2 ${cMap[color]} text-center shadow-sm`}>
        <div className="text-4xl font-black mb-2 tracking-tighter">{val}</div>
        <div className="text-[10px] font-black uppercase tracking-[0.2em] opacity-70">{label}</div>
    </div>
  );
};

const StatusCard = ({ label, val, color, sub }) => {
  const cMap = { rose: 'text-rose-600', emerald: 'text-emerald-600', blue: 'text-blue-600' };
  return (
    <div className="bg-slate-50 p-8 rounded-[2rem] border border-slate-100 text-center shadow-inner group hover:bg-white hover:shadow-xl transition-all">
        <div className={`text-5xl font-black mb-3 tracking-tighter ${cMap[color]}`}>{val?.toLocaleString() || 0}</div>
        <div className="text-xs font-black text-slate-500 uppercase tracking-widest mb-1">{label}</div>
        {sub && <p className="text-[9px] font-bold text-slate-300 uppercase italic tracking-tighter">{sub}</p>}
    </div>
  );
};

const DetailBox = ({ title, icon, val, sub }) => (
  <div className="bg-white p-8 rounded-[2.5rem] border border-slate-100 shadow-sm flex items-start gap-6 group hover:shadow-md transition-all">
     <div className="bg-blue-50 p-5 rounded-3xl text-blue-600 group-hover:bg-blue-600 group-hover:text-white transition-all shadow-sm">
        {icon}
     </div>
     <div className="flex-1 min-w-0">
        <p className="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-2">{title}</p>
        <p className="text-2xl font-black text-slate-900 tracking-tighter truncate">{val}</p>
        <p className="text-xs font-bold text-slate-400 mt-1 uppercase tracking-tighter">{sub}</p>
     </div>
  </div>
);

const RankList = ({ title, type, data, loading }) => {
  const getColorClass = (type) => {
    if (type === 'premium') return 'bg-emerald-50 text-emerald-900';
    if (type === 'debt' || type === 'unpaid-debt') return 'bg-rose-50 text-rose-900';
    return 'bg-blue-50 text-blue-900';
  };

  const getValueColor = (type) => {
    if (type === 'premium') return 'text-emerald-600';
    if (type === 'debt' || type === 'unpaid-debt') return 'text-rose-600';
    return 'text-blue-600';
  };

  const getSubtitle = (item, type) => {
    if (type === 'premium') return `${item.ontime_count}x Bayar Tepat Waktu`;
    if (type === 'debt') return `Tunggakan: ${item.UMUR_TUNGGAKAN || '0'} Hari`;
    if (type === 'unpaid-current') return `Jatuh Tempo: ${item.TGL_JATUH_TEMPO || '-'}`;
    if (type === 'unpaid-debt') return `Umur: ${item.UMUR_TUNGGAKAN || '0'} Hari`;
    return '-';
  };

  const getValue = (item, type) => {
    if (type === 'premium') return `Rp ${item.total_paid || 0} JT`;
    if (type === 'debt') return `Rp ${item.debt_amount || 0} JT`;
    if (type === 'unpaid-current') return `Rp ${item.outstanding || 0} JT`;
    if (type === 'unpaid-debt') return `Rp ${item.unpaid_debt || 0} JT`;
    return '-';
  };

  return (
    <div className="bg-white rounded-[3rem] shadow-sm border border-slate-100 overflow-hidden flex flex-col h-full">
      <div className={`p-10 flex justify-between items-center ${getColorClass(type)}`}>
        <h3 className="font-black text-xl uppercase tracking-tighter leading-none">{title}</h3>
        <PieChart size={24} className="opacity-40" />
      </div>
      <div className="divide-y divide-slate-50 p-4 max-h-[600px] overflow-y-auto">
        {loading ? (
          <div className="p-10 text-center"><RefreshCw className="animate-spin mx-auto text-slate-200" /></div>
        ) : data && data.length > 0 ? (
          data.map((item, i) => (
            <div key={i} className="p-6 flex items-center justify-between hover:bg-slate-50 rounded-2xl transition-all group/item">
              <div className="flex items-center gap-6">
                <span className="text-3xl font-black text-slate-100 italic group-hover/item:text-blue-200 transition-colors">#{i+1}</span>
                <div>
                  <div className="font-black text-lg text-slate-900 leading-none">
                    {item.name || item.NAMA || `PELANGGAN ${i+1}`}
                  </div>
                  <div className="text-[10px] text-slate-400 font-bold uppercase tracking-widest mt-2">
                    {getSubtitle(item, type)}
                  </div>
                </div>
              </div>
              <div className={`text-lg font-black ${getValueColor(type)} tracking-tighter uppercase`}>
                {getValue(item, type)}
              </div>
            </div>
          ))
        ) : (
          <div className="p-10 text-center text-slate-300 font-bold italic">Tidak ada data terdeteksi</div>
        )}
      </div>
    </div>
  );
};

export default App;
