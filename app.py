import streamlit as st
import pandas as pd
import plotly.express as px
import datetime
import os

# ==========================================
# 0. KONFIGURASI HALAMAN
# ==========================================
st.set_page_config(
    page_title="SUNTER Dashboard System",
    page_icon="💧",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# --- CSS CUSTOM (TAMPILAN PROFESIONAL) ---
st.markdown("""
<style>
    .block-container {padding-top: 1rem; padding-bottom: 2rem;}
    h1 { color: #004d99; }
    div[data-testid="metric-container"] { 
        background-color: #f8f9fa; border: 1px solid #dee2e6; 
        padding: 15px; border-radius: 8px; 
    }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] { 
        background-color: #eef2f6; border-radius: 4px; padding: 8px 16px; font-weight: 600;
    }
    .stTabs [aria-selected="true"] { 
        background-color: #007bff; color: white;
    }
</style>
""", unsafe_allow_html=True)

# ==========================================
# 1. KAMUS KODE (HARDCODED)
# ==========================================
KAMUS_SKIP = {
    '1A': 'Meter Buram', '1B': 'Meter Berembun', '1C': 'Meter Rusak', 
    '2A': 'MTA (Air Tdk Dipakai)', '2B': 'MTA (Air Dipakai)', '3A': 'Rumah Kosong',
    '4A': 'Rumah Dibongkar', '4B': 'Meter Terendam', '4C': 'Alamat Tdk Ketemu',
    '5A': 'Tutup Berat', '5B': 'Meter Tertimbun', '5C': 'Terhalang Barang',
    '5D': 'Meter Dicor', '5E': 'Bak Terkunci', '5F': 'Pagar Terkunci', '5G': 'Dilarang Baca'
}

KAMUS_TROUBLE = {
    '1A': 'Meter Berembun', '1B': 'Meter Mati', '1C': 'Meter Buram', '1D': 'Segel Putus',
    '2A': 'Meter Terbalik', '2B': 'Meter Dipindah', '2C': 'Meter Lepas', '2D': 'By Pass',
    '2E': 'Meter Dicolok', '2F': 'Meter Tdk Normal', '2G': 'Kaca Pecah', '3A': 'Air Kecil/Mati',
    '4A': 'Bocor Dinas', '4B': 'Pipa Lama Keluar Air', '5A': 'Stand Tempel', '5B': 'No Seri Beda'
}

KAMUS_METHOD = {'30': 'System Est', '35': 'Service Est', '40': 'Office Est', '60': 'Regular', '80': 'Bill Force'}

# ==========================================
# 2. FUNGSI LOAD DATA CERDAS
# ==========================================
@st.cache_data
def load_data(file_bill, file_cust, file_coll):
    try:
        # A. LOAD MAINBILL (TAGIHAN)
        df_bill = pd.read_csv(file_bill, sep=';', dtype=str, on_bad_lines='skip')
        df_bill.columns = df_bill.columns.str.strip()
        
        # Rename kolom kritis (Mapping nama kolom sistem ke standar kita)
        map_bill = {'NOMEN': 'ID_PELANGGAN', 'CC': 'RAYON', 'TOTAL_TAGIHAN': 'TAGIHAN', 'KONSUMSI': 'KUBIK'}
        df_bill.rename(columns=map_bill, inplace=True)
        
        # Convert Angka
        for c in ['TAGIHAN', 'KUBIK']:
            if c in df_bill.columns: df_bill[c] = pd.to_numeric(df_bill[c], errors='coerce').fillna(0)

        # B. LOAD CUSTOMER (PROFIL)
        df_cust = pd.read_csv(file_cust, sep=';', dtype=str, on_bad_lines='skip')
        df_cust.columns = df_cust.columns.str.strip()
        map_cust = {'cmr_account': 'ID_PELANGGAN', 'cmr_name': 'NAMA', 'cmr_address': 'ALAMAT',
                    'cmr_skip_code': 'KODE_SKIP', 'cmr_trbl1_code': 'KODE_TROUBLE', 
                    'PC': 'KODE_PC', 'EZ': 'KODE_PCEZ', 'Tarif': 'TARIF'} 
        df_cust.rename(columns=map_cust, inplace=True)
        
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
        
        # E. FILTER SUNTER (34 & 35)
        # Jika ada kolom RAYON, kita saring. Jika tidak ada, kita asumsikan aman dulu.
        if 'RAYON' in df_main.columns:
            df_main['RAYON'] = df_main['RAYON'].astype(str).str.strip()
            df_main = df_main[df_main['RAYON'].isin(['34', '35'])]
        
        # F. TERJEMAHKAN KODE
        if 'KODE_SKIP' in df_main.columns:
            df_main['KET_SKIP'] = df_main['KODE_SKIP'].apply(lambda x: KAMUS_SKIP.get(str(x)) if pd.notna(x) else None)
        if 'KODE_TROUBLE' in df_main.columns:
            df_main['KET_TROUBLE'] = df_main['KODE_TROUBLE'].apply(lambda x: KAMUS_TROUBLE.get(str(x)) if pd.notna(x) else None)
        if 'READ_METHOD' in df_main.columns:
            df_main['KET_BACA'] = df_main['READ_METHOD'].astype(str).str[:2].map(KAMUS_METHOD)

        return df_main, df_coll
        
    except Exception as e:
        return None, None

# ==========================================
# 3. HEADER & GLOBAL HEADER
# ==========================================
with st.container():
    st.title("💧 SUNTER DASHBOARD")
    st.caption("Monitoring Operasional & Analisa Collection (Rayon 34 & 35)")
    
    # --- UPLOAD AREA ---
    with st.expander("📂 UPLOAD DATA SYSTEM (MainBill, Customer, Collection)", expanded=True):
        c1, c2, c3 = st.columns(3)
        f_bill = c1.file_uploader("1. MainBill (TXT ;)", type=['txt','csv'])
        f_cust = c2.file_uploader("2. Customer (TXT ;)", type=['txt','csv'])
        f_coll = c3.file_uploader("3. Collection (TXT |)", type=['txt','csv'])

    if f_bill and f_cust:
        df_main, df_coll = load_data(f_bill, f_cust, f_coll)
        
        if df_main is None or df_main.empty:
            st.error("Data kosong atau format salah. Pastikan file MainBill punya kolom 'NOMEN'/'CC' dan Customer punya 'cmr_account'.")
            st.stop()
            
        # Hitung Status Bayar
        if not df_coll.empty:
            coll_agg = df_coll.groupby('ID_PELANGGAN')['BAYAR'].sum().reset_index()
            df_main = pd.merge(df_main, coll_agg, on='ID_PELANGGAN', how='left')
            df_main['BAYAR'] = df_main['BAYAR'].fillna(0)
        else:
            df_main['BAYAR'] = 0
            
        df_main['SISA_TAGIHAN'] = df_main['TAGIHAN'] - df_main['BAYAR']
        
        # --- GLOBAL FILTERS ---
        st.markdown("---")
        col_f1, col_f2, col_f3 = st.columns([2, 1, 3])
        
        with col_f1:
            # Rayon 34 & 35
            opsi_rayon = sorted(df_main['RAYON'].unique()) if 'RAYON' in df_main.columns else []
            pilih_rayon = st.multiselect("Area / Rayon", opsi_rayon, default=opsi_rayon)
        
        with col_f2:
            st.metric("Total Data", f"{len(df_main):,} Plg")
            
        with col_f3:
            cari_pelanggan = st.text_input("🔍 Cari Pelanggan (ID / Nama)", placeholder="Ketik ID atau Nama lalu Enter...")

        # Filter Lanjutan
        with st.expander("Filter Lanjutan (PC, Tarif, Anomali)", expanded=False):
            cf1, cf2, cf3 = st.columns(3)
            sel_pc = cf1.multiselect("Kode PC", sorted(df_main['KODE_PC'].unique().astype(str)) if 'KODE_PC' in df_main.columns else [])
            sel_tarif = cf2.multiselect("Tarif", sorted(df_main['TARIF'].unique().astype(str)) if 'TARIF' in df_main.columns else [])
        
        # --- APPLY FILTERS ---
        df_view = df_main.copy()
        
        if pilih_rayon: df_view = df_view[df_view['RAYON'].isin(pilih_rayon)]
        if sel_pc: df_view = df_view[df_view['KODE_PC'].isin(sel_pc)]
        if sel_tarif: df_view = df_view[df_view['TARIF'].isin(sel_tarif)]
        
        if cari_pelanggan:
            mask = df_view['ID_PELANGGAN'].str.contains(cari_pelanggan, case=False, na=False) | \
                   df_view['NAMA'].str.contains(cari_pelanggan, case=False, na=False)
            df_view = df_view[mask]
            st.info(f"Hasil Pencarian: {len(df_view)} data ditemukan.")

        # ==========================================
        # 4. TAB NAVIGATION & CONTENT
        # ==========================================
        tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
            "1️⃣ Ringkasan", "2️⃣ Collection", "3️⃣ Meter", "4️⃣ History", 
            "5️⃣ Analisa Manual", "6️⃣ TOP", "7️⃣ Laporan"
        ])

        # --- TAB 1: RINGKASAN ---
        with tab1:
            tot_mc = df_view['TAGIHAN'].sum()
            tot_coll = df_view['BAYAR'].sum()
            rate_coll = (tot_coll / tot_mc * 100) if tot_mc > 0 else 0
            tot_tunggakan = tot_mc - tot_coll
            
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Target (MC)", f"Rp {tot_mc:,.0f}")
            k2.metric("Collection Current", f"Rp {tot_coll:,.0f}")
            k3.metric("Rate (%)", f"{rate_coll:.2f}%")
            k4.metric("Sisa Tunggakan", f"Rp {tot_tunggakan:,.0f}", delta_color="inverse")
            
            st.write("---")
            if 'RAYON' in df_view.columns:
                grp_rayon = df_view.groupby('RAYON')[['TAGIHAN', 'BAYAR']].sum().reset_index()
                fig = px.bar(grp_rayon, x='RAYON', y=['TAGIHAN', 'BAYAR'], barmode='group', title="Perbandingan Rayon")
                st.plotly_chart(fig, use_container_width=True)

        # --- TAB 2: COLLECTION MATRIX ---
        with tab2:
            st.subheader("📅 Daily Collection Matrix")
            
            if not df_coll.empty:
                valid_ids = df_view['ID_PELANGGAN'].unique()
                df_coll_view = df_coll[df_coll['ID_PELANGGAN'].isin(valid_ids)]
                
                if not df_coll_view.empty:
                    daily = df_coll_view.groupby('TGL_BAYAR')['BAYAR'].sum().reset_index().sort_values('TGL_BAYAR')
                    daily['KUMULATIF'] = daily['BAYAR'].cumsum()
                    daily['% CAPAI'] = (daily['KUMULATIF'] / tot_mc * 100).round(2)
                    
                    st.dataframe(daily, use_container_width=True)
                    st.plotly_chart(px.line(daily, x='TGL_BAYAR', y='% CAPAI', markers=True, title="Tren Pencapaian"), use_container_width=True)
                else:
                    st.warning("Tidak ada transaksi bayar untuk data yang difilter.")
            else:
                st.warning("File Collection belum diupload.")

        # --- TAB 3: METER ---
        with tab3:
            st.subheader("Anomali Meter")
            c1, c2, c3 = st.columns(3)
            f_zero = c1.checkbox("Zero Usage (0 m3)")
            f_skip = c2.checkbox("Kode SKIP")
            f_trbl = c3.checkbox("Kode TROUBLE")
            
            df_m = df_view.copy()
            conds = []
            if f_zero: conds.append(df_m['KUBIK'] == 0)
            if f_skip: conds.append(df_m['KET_SKIP'].notna())
            if f_trbl: conds.append(df_m['KET_TROUBLE'].notna())
            
            if conds:
                mask = pd.concat(conds, axis=1).any(axis=1)
                df_m = df_m[mask]
                
            st.write(f"Ditemukan {len(df_m)} Anomali")
            st.dataframe(df_m[['ID_PELANGGAN', 'NAMA', 'ALAMAT', 'KUBIK', 'KET_SKIP', 'KET_TROUBLE']], use_container_width=True)

        # --- TAB 4: HISTORY ---
        with tab4:
            st.dataframe(df_view)

        # --- TAB 5: ANALISA MANUAL (PERSISTENT DB) ---
        with tab5:
            st.subheader("📝 Pusat Analisa Manual Tim (Database VPS)")
            
            DB_FILE = 'database_analisa.csv'

            # Load Database
            if os.path.exists(DB_FILE):
                try:
                    df_hist = pd.read_csv(DB_FILE)
                except:
                    df_hist = pd.DataFrame(columns=["Tanggal", "ID", "Nama", "Kategori", "Analisa", "Petugas"])
            else:
                df_hist = pd.DataFrame(columns=["Tanggal", "ID", "Nama", "Kategori", "Analisa", "Petugas"])

            col_form, col_data = st.columns([1, 2])
            
            with col_form:
                tgt_id = st.selectbox("Pilih ID Pelanggan:", df_view['ID_PELANGGAN'].unique())
                
                # Cek Info
                info_plg = df_view[df_view['ID_PELANGGAN'] == tgt_id].iloc[0]
                st.info(f"**{info_plg['NAMA']}**\nTagihan: {info_plg['TAGIHAN']:,.0f}")
                
                with st.form("save_analisa"):
                    tgl = st.date_input("Tanggal", datetime.date.today())
                    kat = st.selectbox("Masalah", ["Rumah Kosong", "Meter Rusak", "Tunggakan", "Lainnya"])
                    desc = st.text_area("Analisa")
                    petugas = st.text_input("Petugas")
                    
                    if st.form_submit_button("💾 SIMPAN PERMANEN"):
                        new_row = pd.DataFrame([{
                            "Tanggal": tgl, "ID": tgt_id, "Nama": info_plg['NAMA'],
                            "Kategori": kat, "Analisa": desc, "Petugas": petugas
                        }])
                        
                        # Append ke CSV
                        hdr = not os.path.exists(DB_FILE)
                        new_row.to_csv(DB_FILE, mode='a', header=hdr, index=False)
                        st.success("Tersimpan di VPS!")
                        st.rerun()

            with col_data:
                st.write("#### 📂 Riwayat Analisa")
                if not df_hist.empty:
                    st.dataframe(df_hist.sort_index(ascending=False), use_container_width=True)
                    st.download_button("Download Database", df_hist.to_csv(index=False), "db_analisa.csv")

        # --- TAB 6 & 7: TOP & REPORT ---
        with tab6:
            c1, c2 = st.columns(2)
            c1.write("**Top 50 Tagihan**")
            c1.dataframe(df_view.nlargest(50, 'TAGIHAN')[['ID_PELANGGAN', 'NAMA', 'TAGIHAN']])
            c2.write("**Top 50 Kubik**")
            c2.dataframe(df_view.nlargest(50, 'KUBIK')[['ID_PELANGGAN', 'NAMA', 'KUBIK']])
            
        with tab7:
            st.write("Download Data Gabungan")
            st.download_button("Download Excel/CSV", df_view.to_csv(index=False), "laporan_sunter.csv")

    else:
        st.info("👋 Silakan Upload 3 File Utama untuk memulai.")
