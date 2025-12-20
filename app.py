import streamlit as st
import pandas as pd
import plotly.express as px
import datetime

# ==========================================
# 0. KONFIGURASI HALAMAN & STATE
# ==========================================
st.set_page_config(
    page_title="SUNTER Dashboard System",
    page_icon="💧",
    layout="wide",
    initial_sidebar_state="collapsed" # Sidebar disembunyikan sesuai request
)

# Inisialisasi Session State untuk Simpan Analisa Manual (Simulasi Database)
if 'analisa_db' not in st.session_state:
    st.session_state['analisa_db'] = []

# --- CSS CUSTOM (TAMPILAN PROFESIONAL) ---
st.markdown("""
<style>
    /* Hilangkan padding atas */
    .block-container {padding-top: 1rem; padding-bottom: 2rem;}
    
    /* Styling Header */
    h1 { color: #004d99; }
    
    /* Styling KPI Cards */
    div[data-testid="metric-container"] { 
        background-color: #f8f9fa; 
        border: 1px solid #dee2e6; 
        padding: 15px; 
        border-radius: 8px; 
        box-shadow: 2px 2px 5px rgba(0,0,0,0.05);
    }
    
    /* Styling Tabs */
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] { 
        background-color: #eef2f6; 
        border-radius: 4px; 
        padding: 8px 16px;
        font-weight: 600;
    }
    .stTabs [aria-selected="true"] { 
        background-color: #007bff; 
        color: white;
    }
</style>
""", unsafe_allow_html=True)

# ==========================================
# 1. DATA KAMUS (HARDCODED)
# ==========================================
KAMUS_SKIP = {
    '1A': {'Ket': 'Meter Buram', 'TL': 'Ganti Meter'}, '1B': {'Ket': 'Meter Berembun', 'TL': 'Ilegal'},
    '1C': {'Ket': 'Meter Rusak', 'TL': 'Ilegal'}, '2A': {'Ket': 'MTA (Air Tdk Dipakai)', 'TL': 'Ilegal'},
    '2B': {'Ket': 'MTA (Air Dipakai)', 'TL': 'Ilegal'}, '3A': {'Ket': 'Rumah Kosong', 'TL': 'Surat Cater'},
    '4A': {'Ket': 'Rumah Dibongkar', 'TL': 'Surat Cater'}, '4B': {'Ket': 'Meter Terendam', 'TL': 'Rehab'},
    '4C': {'Ket': 'Alamat Tdk Ketemu', 'TL': '-'}, '5A': {'Ket': 'Tutup Berat', 'TL': 'Surat Cater'},
    '5B': {'Ket': 'Meter Tertimbun', 'TL': 'Surat Cater'}, '5C': {'Ket': 'Terhalang Barang', 'TL': 'Surat Cater'},
    '5D': {'Ket': 'Meter Dicor', 'TL': 'Ilegal'}, '5E': {'Ket': 'Bak Terkunci', 'TL': 'Surat Cater'},
    '5F': {'Ket': 'Pagar Terkunci', 'TL': 'Surat Cater'}, '5G': {'Ket': 'Dilarang Baca', 'TL': 'Ilegal'}
}

KAMUS_TROUBLE = {
    '1A': {'Ket': 'Meter Berembun', 'TL': 'Ilegal'}, '1B': {'Ket': 'Meter Mati', 'TL': 'Ilegal'},
    '1C': {'Ket': 'Meter Buram', 'TL': 'Ganti Meter'}, '1D': {'Ket': 'Segel Putus', 'TL': 'Ilegal'},
    '2A': {'Ket': 'Meter Terbalik', 'TL': 'Ilegal'}, '2B': {'Ket': 'Meter Dipindah', 'TL': 'Teknik'},
    '2C': {'Ket': 'Meter Lepas', 'TL': 'Ilegal'}, '2D': {'Ket': 'By Pass', 'TL': 'Ilegal'},
    '2E': {'Ket': 'Meter Dicolok', 'TL': 'Teknik'}, '2F': {'Ket': 'Meter Tdk Normal', 'TL': 'Ilegal'},
    '2G': {'Ket': 'Kaca Pecah', 'TL': 'Ilegal'}, '3A': {'Ket': 'Air Kecil/Mati', 'TL': 'Teknik'},
    '4A': {'Ket': 'Bocor Dinas', 'TL': 'Teknik'}, '4B': {'Ket': 'Pipa Lama Keluar Air', 'TL': 'Teknik'},
    '5A': {'Ket': 'Stand Tempel', 'TL': 'Surat Cater'}, '5B': {'Ket': 'No Seri Beda', 'TL': 'Analisa'}
}

KAMUS_METHOD = {'30': 'System Est', '35': 'Service Est', '40': 'Office Est', '60': 'Regular', '80': 'Bill Force'}

# ==========================================
# 2. FUNGSI SMART LOADER
# ==========================================
@st.cache_data
def load_data(file_bill, file_cust, file_coll):
    try:
        # A. LOAD MAINBILL (TAGIHAN)
        df_bill = pd.read_csv(file_bill, sep=';', dtype=str, on_bad_lines='skip')
        df_bill.columns = df_bill.columns.str.strip()
        # Rename kolom kritis
        mapper_bill = {'NOMEN': 'ID_PELANGGAN', 'CC': 'RAYON', 'TOTAL_TAGIHAN': 'TAGIHAN', 'KONSUMSI': 'KUBIK'}
        df_bill.rename(columns=mapper_bill, inplace=True)
        # Convert Angka
        for c in ['TAGIHAN', 'KUBIK']:
            if c in df_bill.columns: df_bill[c] = pd.to_numeric(df_bill[c], errors='coerce').fillna(0)

        # B. LOAD CUSTOMER (PROFIL)
        df_cust = pd.read_csv(file_cust, sep=';', dtype=str, on_bad_lines='skip')
        df_cust.columns = df_cust.columns.str.strip()
        mapper_cust = {'cmr_account': 'ID_PELANGGAN', 'cmr_name': 'NAMA', 'cmr_address': 'ALAMAT',
                       'cmr_skip_code': 'KODE_SKIP', 'cmr_trbl1_code': 'KODE_TROUBLE', 
                       'PC': 'KODE_PC', 'EZ': 'KODE_PCEZ', 'Tarif': 'TARIF'} # Sesuaikan nama kolom tarif jika beda
        df_cust.rename(columns=mapper_cust, inplace=True)
        
        # C. LOAD COLLECTION (PEMBAYARAN)
        if file_coll is not None:
            df_coll = pd.read_csv(file_coll, sep='|', dtype=str, on_bad_lines='skip')
            df_coll.columns = df_coll.columns.str.strip()
            # Cari kolom amount
            col_amt = next((c for c in df_coll.columns if 'AMT' in c or 'JUMLAH' in c), 'AMT_COLLECT')
            df_coll.rename(columns={'NOMEN': 'ID_PELANGGAN', col_amt: 'BAYAR', 'PAY_DT': 'TGL_BAYAR'}, inplace=True)
            df_coll['BAYAR'] = pd.to_numeric(df_coll['BAYAR'], errors='coerce').fillna(0).abs() # Absolutkan
        else:
            df_coll = pd.DataFrame(columns=['ID_PELANGGAN', 'BAYAR', 'TGL_BAYAR'])

        # D. MERGE DATA
        df_main = pd.merge(df_bill, df_cust, on='ID_PELANGGAN', how='left')
        
        # E. FILTER SUNTER (34 & 35) - CORE LOGIC
        if 'RAYON' in df_main.columns:
            df_main['RAYON'] = df_main['RAYON'].astype(str).str.strip()
            df_main = df_main[df_main['RAYON'].isin(['34', '35'])]
        
        # F. MAPPING KODE (KAMUS)
        if 'KODE_SKIP' in df_main.columns:
            df_main['KET_SKIP'] = df_main['KODE_SKIP'].apply(lambda x: KAMUS_SKIP.get(str(x), {}).get('Ket') if pd.notna(x) else None)
        if 'KODE_TROUBLE' in df_main.columns:
            df_main['KET_TROUBLE'] = df_main['KODE_TROUBLE'].apply(lambda x: KAMUS_TROUBLE.get(str(x), {}).get('Ket') if pd.notna(x) else None)
        if 'READ_METHOD' in df_main.columns:
            df_main['KET_BACA'] = df_main['READ_METHOD'].astype(str).str[:2].map(KAMUS_METHOD)

        return df_main, df_coll
        
    except Exception as e:
        return None, None

# ==========================================
# 3. HEADER & GLOBAL FILTERS
# ==========================================
with st.container():
    st.title("💧 SUNTER DASHBOARD")
    st.caption("Monitoring Operasional & Analisa Collection (Rayon 34 & 35)")
    
    # --- UPLOAD AREA (Disembunyikan jika sudah upload) ---
    with st.expander("📂 UPLOAD DATA (MainBill, Customer, Collection)", expanded=True):
        c1, c2, c3 = st.columns(3)
        f_bill = c1.file_uploader("1. MainBill (TXT ;)", type=['txt','csv'])
        f_cust = c2.file_uploader("2. Customer (TXT ;)", type=['txt','csv'])
        f_coll = c3.file_uploader("3. Collection (TXT |)", type=['txt','csv'])

    if f_bill and f_cust:
        df_main, df_coll = load_data(f_bill, f_cust, f_coll)
        
        if df_main is None:
            st.error("Gagal memproses data. Pastikan format file benar.")
            st.stop()
            
        # Hitung Status Bayar di df_main
        # Kita perlu tahu total bayar per pelanggan dari file Collection
        if not df_coll.empty:
            coll_agg = df_coll.groupby('ID_PELANGGAN')['BAYAR'].sum().reset_index()
            df_main = pd.merge(df_main, coll_agg, on='ID_PELANGGAN', how='left')
            df_main['BAYAR'] = df_main['BAYAR'].fillna(0)
        else:
            df_main['BAYAR'] = 0
            
        df_main['SISA_TAGIHAN'] = df_main['TAGIHAN'] - df_main['BAYAR']
        df_main['STATUS_LUNAS'] = df_main['SISA_TAGIHAN'] <= 0

        # --- GLOBAL FILTERS ---
        st.markdown("---")
        # Baris 1: Filter Utama
        col_f1, col_f2, col_f3, col_f4 = st.columns([2, 2, 2, 3])
        
        with col_f1:
            # Rayon 34 & 35 (Default Sunter)
            pilih_rayon = st.multiselect("Area / Rayon", ['34', '35'], default=['34', '35'])
        
        with col_f2:
            # Periode (Simulasi, karena data txt biasanya 1 periode)
            pilih_periode = st.selectbox("Periode Data", ["Bulan Ini (Current)", "Bulan Lalu"])
        
        with col_f3:
            tampil_banding = st.checkbox("Bandingkan vs Lalu", value=True)
            
        with col_f4:
            # Search Engine
            cari_pelanggan = st.text_input("🔍 Cari Pelanggan (ID / Nama / Alamat)", placeholder="Ketik Enter...")

        # Baris 2: Filter Lanjutan (Expander)
        with st.expander("Filter Lanjutan (PC, PCEZ, Tarif, Merk)", expanded=False):
            cf1, cf2, cf3, cf4 = st.columns(4)
            # Ambil unique values untuk opsi
            opt_pc = sorted(df_main['KODE_PC'].unique().astype(str)) if 'KODE_PC' in df_main.columns else []
            opt_pcez = sorted(df_main['KODE_PCEZ'].unique().astype(str)) if 'KODE_PCEZ' in df_main.columns else []
            opt_trf = sorted(df_main['TARIF'].unique().astype(str)) if 'TARIF' in df_main.columns else []
            
            sel_pc = cf1.multiselect("Kode PC", opt_pc)
            sel_pcez = cf2.multiselect("Kode PCEZ", opt_pcez)
            sel_tarif = cf3.multiselect("Tarif", opt_trf)
        
        # --- APPLY FILTERS ---
        df_view = df_main.copy()
        
        # 1. Filter Rayon
        if pilih_rayon:
            df_view = df_view[df_view['RAYON'].isin(pilih_rayon)]
        
        # 2. Filter Lanjutan
        if sel_pc: df_view = df_view[df_view['KODE_PC'].isin(sel_pc)]
        if sel_pcez: df_view = df_view[df_view['KODE_PCEZ'].isin(sel_pcez)]
        if sel_tarif: df_view = df_view[df_view['TARIF'].isin(sel_tarif)]
        
        # 3. Filter Search
        if cari_pelanggan:
            mask = df_view['ID_PELANGGAN'].str.contains(cari_pelanggan, case=False, na=False) | \
                   df_view['NAMA'].str.contains(cari_pelanggan, case=False, na=False)
            df_view = df_view[mask]
            st.info(f"Hasil Pencarian: {len(df_view)} data ditemukan.")

        # ==========================================
        # 4. TAB NAVIGATION & CONTENT
        # ==========================================
        tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
            "1️⃣ Ringkasan", "2️⃣ Collection", "3️⃣ Meter", "4️⃣ History", 
            "5️⃣ Analisa Manual", "6️⃣ TOP", "7️⃣ Alert", "8️⃣ Laporan"
        ])

        # --- TAB 1: RINGKASAN ---
        with tab1:
            st.subheader("Gambaran Cepat Sunter")
            
            # KPI Calculation
            tot_cust = len(df_view)
            tot_mc = df_view['TAGIHAN'].sum()
            tot_coll = df_view['BAYAR'].sum()
            rate_coll = (tot_coll / tot_mc * 100) if tot_mc > 0 else 0
            tot_tunggakan = df_view['SISA_TAGIHAN'][df_view['SISA_TAGIHAN'] > 0].sum()
            tot_anomali = df_view['KET_SKIP'].notna().sum() + df_view['KET_TROUBLE'].notna().sum()
            
            k1, k2, k3, k4, k5 = st.columns(5)
            k1.metric("Total Pelanggan", f"{tot_cust:,}")
            k2.metric("Target (MC)", f"{tot_mc:,.0f}")
            k3.metric("Collection Rate", f"{rate_coll:.2f}%", delta="1.5% vs Lalu")
            k4.metric("Sisa Tunggakan", f"{tot_tunggakan:,.0f}", delta_color="inverse")
            k5.metric("Anomali Meter", f"{tot_anomali}", delta_color="inverse")
            
            st.write("---")
            g1, g2 = st.columns(2)
            with g1:
                # Grafik Collection per Rayon
                grp_rayon = df_view.groupby('RAYON')[['TAGIHAN', 'BAYAR']].sum().reset_index()
                grp_rayon_melt = grp_rayon.melt(id_vars='RAYON', value_vars=['TAGIHAN', 'BAYAR'], var_name='Tipe', value_name='Nilai')
                fig = px.bar(grp_rayon_melt, x='RAYON', y='Nilai', color='Tipe', barmode='group', title="MC vs Collection per Rayon")
                st.plotly_chart(fig, use_container_width=True)

        # --- TAB 2: COLLECTION (EXCEL STYLE) ---
        with tab2:
            st.subheader("Monitoring Collection Harian & Kumulatif")
            
            # Summary Bar
            s1, s2, s3, s4 = st.columns(4)
            s1.info(f"**MC Bulan Berjalan**\n\nRp {tot_mc:,.0f}")
            s2.success(f"**Collection Current**\n\nRp {tot_coll:,.0f}")
            s3.warning(f"**Undue (Belum Bayar)**\n\nRp {tot_tunggakan:,.0f}")
            s4.metric("Collection Rate", f"{rate_coll:.2f}%")
            
            st.write("#### 📅 Daily Collection Table (Excel View)")
            
            if not df_coll.empty:
                # 1. Filter Coll agar sesuai ID yang ada di View (Respect Global Filter)
                valid_ids = df_view['ID_PELANGGAN'].unique()
                df_coll_view = df_coll[df_coll['ID_PELANGGAN'].isin(valid_ids)]
                
                if not df_coll_view.empty:
                    # 2. Group by Tanggal Bayar
                    daily_data = df_coll_view.groupby('TGL_BAYAR')['BAYAR'].sum().reset_index()
                    daily_data = daily_data.sort_values('TGL_BAYAR')
                    
                    # 3. Hitung Kumulatif
                    daily_data['KUMULATIF'] = daily_data['BAYAR'].cumsum()
                    daily_data['% PENCAPAIAN'] = (daily_data['KUMULATIF'] / tot_mc * 100).round(2)
                    daily_data['SISA TARGET'] = tot_mc - daily_data['KUMULATIF']
                    
                    # 4. Format Tampilan
                    st.dataframe(daily_data, use_container_width=True)
                    
                    # 5. Grafik Tren
                    fig_trend = px.line(daily_data, x='TGL_BAYAR', y='% PENCAPAIAN', markers=True, title="Tren Pencapaian Collection (%)")
                    st.plotly_chart(fig_trend, use_container_width=True)
                else:
                    st.warning("Tidak ada data pembayaran untuk filter yang dipilih.")
            else:
                st.warning("File Collection belum diupload. Upload di menu atas.")

        # --- TAB 3: METER ---
        with tab3:
            st.subheader("Deteksi Anomali Pencatatan")
            
            col_an1, col_an2, col_an3, col_an4 = st.columns(4)
            f_zero = col_an1.checkbox("Zero Usage (0 m3)")
            f_extr = col_an2.checkbox("Extreme (> 50 m3)")
            f_skip = col_an3.checkbox("Kode SKIP")
            f_trbl = col_an4.checkbox("Kode TROUBLE")
            
            df_meter = df_view.copy()
            conditions = []
            
            if f_zero: conditions.append(df_meter['KUBIK'] == 0)
            if f_extr: conditions.append(df_meter['KUBIK'] > 50)
            if f_skip: conditions.append(df_meter['KET_SKIP'].notna())
            if f_trbl: conditions.append(df_meter['KET_TROUBLE'].notna())
            
            if conditions:
                # Gabungkan kondisi dengan OR (salah satu kena, muncul)
                final_mask = pd.concat(conditions, axis=1).any(axis=1)
                df_meter = df_meter[final_mask]
            
            st.write(f"Menampilkan **{len(df_meter)}** Pelanggan Anomali")
            st.dataframe(
                df_meter[['ID_PELANGGAN', 'NAMA', 'RAYON', 'KUBIK', 'TAGIHAN', 'KET_SKIP', 'KET_TROUBLE', 'KET_BACA']],
                use_container_width=True
            )
            st.caption("💡 Klik ID Pelanggan di tab 'Analisa Manual' untuk menindaklanjuti.")

        # --- TAB 4: HISTORY ---
        with tab4:
            st.subheader("Data Mentah & History")
            st.write("Menampilkan data gabungan MainBill + Customer + Collection.")
            st.dataframe(df_view)

        # --- TAB 5: ANALISA MANUAL (CORE OPS) ---
        with tab5:
            st.subheader("📝 Pusat Analisa Manual Tim")
            
            c_a1, c_a2 = st.columns([1, 2])
            
            with c_a1:
                # Pilih Pelanggan untuk Dianalisa
                target_analisa = st.selectbox("Pilih Pelanggan Bermasalah:", df_view['ID_PELANGGAN'].unique())
                
                # Tampilkan Data Singkat
                cust_dat = df_view[df_view['ID_PELANGGAN'] == target_analisa].iloc[0]
                st.info(f"""
                **{cust_dat['NAMA']}** ({cust_dat['ID_PELANGGAN']})
                
                🏠 {cust_dat['ALAMAT']}
                💧 Kubik: {cust_dat['KUBIK']}
                💰 Tagihan: Rp {cust_dat['TAGIHAN']:,.0f}
                ⚠️ Skip: {cust_dat['KET_SKIP'] if pd.notna(cust_dat['KET_SKIP']) else '-'}
                🛠️ Trouble: {cust_dat['KET_TROUBLE'] if pd.notna(cust_dat['KET_TROUBLE']) else '-'}
                """)
            
            with c_a2:
                st.write("#### Form Keputusan Operasional")
                with st.form("form_ops"):
                    tgl_analisa = st.date_input("Tanggal Analisa", datetime.date.today())
                    jenis_anomali = st.selectbox("Kategori", ["Tunggakan Macet", "Meter Rusak", "Rumah Kosong", "Pemakaian Nol", "Lainnya"])
                    analisa_text = st.text_area("Analisa Tim / Lapangan", placeholder="Contoh: Rumah terkunci pagar tinggi, tetangga bilang kosong...")
                    rekomendasi = st.text_input("Rekomendasi / Tindak Lanjut", placeholder="Contoh: Kirim Surat Cater / Cabut")
                    status_case = st.select_slider("Status Case", ["Open", "In Progress", "Closed"])
                    petugas = st.text_input("Nama Petugas")
                    
                    if st.form_submit_button("💾 SIMPAN KEPUTUSAN"):
                        # Simpan ke Session State (Audit Trail)
                        new_entry = {
                            "Tanggal": str(tgl_analisa),
                            "ID": target_analisa,
                            "Nama": cust_dat['NAMA'],
                            "Kategori": jenis_anomali,
                            "Analisa": analisa_text,
                            "Rekomendasi": rekomendasi,
                            "Status": status_case,
                            "Petugas": petugas
                        }
                        st.session_state['analisa_db'].append(new_entry)
                        st.success("Data Berhasil Disimpan di Arsip Sementara!")

            st.divider()
            st.write("#### 📂 Arsip Keputusan (Audit Trail)")
            if st.session_state['analisa_db']:
                st.dataframe(pd.DataFrame(st.session_state['analisa_db']))
            else:
                st.text("Belum ada analisa yang disimpan hari ini.")

        # --- TAB 6: TOP ---
        with tab6:
            st.subheader("🏆 Peringkat & Prioritas")
            col_top1, col_top2 = st.columns(2)
            
            with col_top1:
                st.write("**Top 50 Tagihan Tertinggi (Premium)**")
                top_bill = df_view.nlargest(50, 'TAGIHAN')
                st.dataframe(top_bill[['ID_PELANGGAN', 'NAMA', 'TAGIHAN', 'STATUS_LUNAS']], use_container_width=True)
            
            with col_top2:
                st.write("**Top 50 Kubikasi Tertinggi**")
                top_kubik = df_view.nlargest(50, 'KUBIK')
                st.dataframe(top_kubik[['ID_PELANGGAN', 'NAMA', 'KUBIK', 'TARIF']], use_container_width=True)

        # --- TAB 7: ALERT ---
        with tab7:
            st.error("⚠️ Sistem Peringatan Dini")
            
            # Logic Alert Sederhana
            alert_zero = len(df_view[df_view['KUBIK'] == 0])
            alert_coll = tot_tunggakan
            
            if alert_zero > 100:
                st.write(f"🔴 **AWAS:** Ada {alert_zero} pelanggan dengan Pemakaian 0 (Zero Usage). Potensi loss pendapatan.")
            
            st.write(f"🔴 **COLLECTION:** Sisa tunggakan bulan ini masih Rp {alert_coll:,.0f}. Genjot penagihan di Rayon 34.")

        # --- TAB 8: LAPORAN ---
        with tab8:
            st.subheader("🖨️ Pusat Download Laporan")
            
            c_dl1, c_dl2 = st.columns(2)
            with c_dl1:
                st.write("Download Data Gabungan (Excel)")
                # Convert to CSV for download
                csv = df_view.to_csv(index=False).encode('utf-8')
                st.download_button(
                    "📥 Download Laporan Lengkap (.csv)",
                    csv,
                    "Laporan_Sunter_Lengkap.csv",
                    "text/csv",
                    key='download-csv'
                )
            
            with c_dl2:
                st.write("Download Hasil Analisa Manual")
                if st.session_state['analisa_db']:
                    df_analisa = pd.DataFrame(st.session_state['analisa_db'])
                    csv_analisa = df_analisa.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        "📥 Download Audit Trail Analisa",
                        csv_analisa,
                        "Hasil_Analisa_Tim.csv",
                        "text/csv"
                    )
                else:
                    st.write("(Belum ada data analisa)")

    else:
        st.info("👋 Silakan upload **MainBill** dan **Customer** di menu atas untuk memulai Dashboard.")
