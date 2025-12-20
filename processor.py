import pandas as pd
import sqlite3
import os
from datetime import datetime
from config import DB_PATH, FILE_TYPE_MC, FILE_TYPE_COLLECTION, FILE_TYPE_MAINBILL, FILE_TYPE_SBRS, FILE_TYPE_ARDEBT

def get_filename(filepath):
    """Mengambil nama file saja dari path lengkap"""
    return os.path.basename(filepath)

def clean_nomen(val):
    """Membersihkan format Nomen (hilangkan .0 jika dari Excel, hapus spasi)"""
    if pd.isna(val): return None
    
    # Ubah ke string dan hapus spasi depan/belakang
    s = str(val).strip()
    
    # Hapus akhiran .0 (biasa terjadi saat import dari Excel/CSV angka)
    if s.endswith('.0'):
        s = s[:-2]
        
    if not s: return None
    return s

def clean_date(val, fmt_in='%d-%m-%Y', fmt_out='%Y-%m-%d'):
    """Standardisasi format tanggal ke YYYY-MM-DD"""
    try:
        return datetime.strptime(str(val).strip(), fmt_in).strftime(fmt_out)
    except:
        return val # Kembalikan aslinya jika gagal parsing

def clean_currency(val):
    """Membersihkan format mata uang (hapus koma/titik ribuan)"""
    try:
        if pd.isna(val): return 0.0
        # Hapus koma (jika format 1,000.00) atau titik (jika format 1.000)
        # Asumsi data CSV float standar menggunakan titik sebagai desimal
        s = str(val).strip().replace(',', '') 
        return float(s)
    except:
        return 0.0

# ==========================================
# 1. MODUL MASTER CUSTOMER (MC)
# ==========================================
def process_mc(filepath, cursor):
    # Format: CSV, Header: NOMEN, NAMA_PEL, ALM1_PEL, REK_AIR, ZONA_NOVAK
    df = pd.read_csv(filepath, dtype=str, low_memory=False)
    
    # Bersihkan nama kolom (strip spasi & uppercase)
    df.columns = df.columns.str.strip().str.upper()
    
    data_list = []
    
    # Mapping nama kolom fleksibel (sesuai variasi file yang mungkin diupload)
    col_nomen = 'NOMEN'
    col_nama = next((c for c in ['NAMA_PEL', 'NAMA'] if c in df.columns), 'NAMA_PEL')
    col_alamat = next((c for c in ['ALM1_PEL', 'ALAMAT'] if c in df.columns), 'ALM1_PEL')
    col_rayon = next((c for c in ['ZONA_NOVAK', 'RAYON', 'PC'] if c in df.columns), 'ZONA_NOVAK')
    col_tarif = next((c for c in ['TARIF', 'GOL'] if c in df.columns), 'TARIF')
    col_target = next((c for c in ['REK_AIR', 'TAGIHAN'] if c in df.columns), 'REK_AIR')

    for _, row in df.iterrows():
        nomen = clean_nomen(row.get(col_nomen))
        if not nomen: continue
        
        target = clean_currency(row.get(col_target, 0))
        
        data_list.append((
            nomen,
            str(row.get(col_nama, '')),
            str(row.get(col_alamat, '')),
            str(row.get(col_rayon, '')),
            str(row.get(col_tarif, '')),
            target,
            datetime.now().strftime('%Y-%m')
        ))
    
    cursor.executemany('''
        INSERT INTO master_pelanggan (nomen, nama, alamat, rayon, tarif, target_mc, periode)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(nomen) DO UPDATE SET
            nama=excluded.nama,
            alamat=excluded.alamat,
            rayon=excluded.rayon,
            tarif=excluded.tarif,
            target_mc=excluded.target_mc
    ''', data_list)
    return len(data_list)

# ==========================================
# 2. MODUL COLLECTION (PEMBAYARAN)
# ==========================================
def process_collection(filepath, cursor):
    filename = get_filename(filepath)
    # Format: TXT (Pipa |), Header: NOMEN|AMT_COLLECT|PAY_DT
    df = pd.read_csv(filepath, sep='|', dtype=str, low_memory=False)
    df.columns = df.columns.str.strip().str.upper()
    
    cursor.execute("DELETE FROM collection_harian WHERE sumber_file = ?", (filename,))
    
    data_list = []
    
    col_nomen = 'NOMEN'
    col_amt = next((c for c in ['AMT_COLLECT', 'JUMLAH'] if c in df.columns), 'AMT_COLLECT')
    col_tgl = next((c for c in ['PAY_DT', 'TGL_BAYAR'] if c in df.columns), 'PAY_DT')

    for _, row in df.iterrows():
        nomen = clean_nomen(row.get(col_nomen))
        if not nomen: continue
        
        amount = abs(clean_currency(row.get(col_amt, 0)))
        tgl_fix = clean_date(row.get(col_tgl, ''), '%d-%m-%Y')
        
        # Insert Dummy Master jika Nomen belum ada (agar tidak error constraint FK jika ada)
        cursor.execute("INSERT OR IGNORE INTO master_pelanggan (nomen) VALUES (?)", (nomen,))

        data_list.append((nomen, tgl_fix, amount, filename))
    
    cursor.executemany('''
        INSERT INTO collection_harian (nomen, tgl_bayar, jumlah_bayar, sumber_file) 
        VALUES (?, ?, ?, ?)
    ''', data_list)
    return len(data_list)

# ==========================================
# 3. MODUL SBRS (METER READING)
# ==========================================
def process_sbrs(filepath, cursor):
    # Format: CSV (Koma)
    df = pd.read_csv(filepath, dtype=str, low_memory=False)
    df.columns = df.columns.str.strip().str.upper()
    
    data_list = []
    col_nomen = 'NOMEN'
    col_stand = next((c for c in ['CURR_READ_1', 'STAND_AKHIR'] if c in df.columns), 'CURR_READ_1')
    col_tgl = next((c for c in ['READ_DATE_1', 'TGL_BACA'] if c in df.columns), 'READ_DATE_1')

    for _, row in df.iterrows():
        nomen = clean_nomen(row.get(col_nomen))
        if not nomen: continue
        
        stand = row.get(col_stand, 0)
        # Format Tanggal SBRS biasanya dd/mm/yyyy
        tgl = clean_date(row.get(col_tgl, ''), '%d/%m/%Y')
        
        data_list.append(('SUDAH BACA', stand, tgl, nomen))
        
    cursor.executemany('''
        INSERT INTO operasional (nomen, status_baca, stand_akhir, tgl_baca) VALUES (?, ?, ?, ?)
        ON CONFLICT(nomen) DO UPDATE SET 
            status_baca=excluded.status_baca, 
            stand_akhir=excluded.stand_akhir, 
            tgl_baca=excluded.tgl_baca
    ''', data_list)
    return len(data_list)

# ==========================================
# 4. MODUL MAINBILL (TAGIHAN FINAL)
# ==========================================
def process_mainbill(filepath, cursor):
    # Format: TXT (Titik Koma ;)
    df = pd.read_csv(filepath, sep=';', dtype=str, low_memory=False)
    df.columns = df.columns.str.strip().str.upper()
    
    data_list = []
    for _, row in df.iterrows():
        nomen = clean_nomen(row.get('NOMEN'))
        if not nomen: continue
        
        tagihan = clean_currency(str(row.get('TOTAL_TAGIHAN', '0')).replace(',', '.'))
        cycle = str(row.get('BILL_CYCLE', ''))
        
        data_list.append((tagihan, cycle, nomen))
        
    cursor.executemany('''
        INSERT INTO operasional (nomen, tagihan_final, cycle) VALUES (?, ?, ?)
        ON CONFLICT(nomen) DO UPDATE SET 
            tagihan_final=excluded.tagihan_final, 
            cycle=excluded.cycle
    ''', data_list)
    return len(data_list)

# ==========================================
# 5. MODUL ARDEBT (TUNGGAKAN) - PERBAIKAN
# ==========================================
def process_ardebt(filepath, cursor):
    # Format: CSV, Header biasanya: NOMEN, RAYON, JUMLAH
    df = pd.read_csv(filepath, dtype=str, low_memory=False)
    df.columns = df.columns.str.strip().str.upper()
    
    # 1. Bersihkan Kolom NOMEN
    # Ini langkah penting agar cocok dengan MC (menghilangkan .0)
    df['NOMEN_CLEAN'] = df['NOMEN'].apply(clean_nomen)
    df = df.dropna(subset=['NOMEN_CLEAN'])
    
    # 2. Cari Kolom Jumlah (Bisa JUMLAH, SumOfJUMLAH, atau TAGIHAN)
    possible_cols = ['JUMLAH', 'SUMOFJUMLAH', 'TAGIHAN', 'SALDO']
    col_jumlah = next((c for c in possible_cols if c in df.columns), None)
    
    if not col_jumlah:
        raise ValueError(f"Kolom JUMLAH tidak ditemukan. Kolom tersedia: {list(df.columns)}")

    # 3. Konversi Jumlah ke Float
    df['VAL_JUMLAH'] = df[col_jumlah].apply(clean_currency)
    
    # 4. Grouping per Nomen
    # Jika satu orang menunggak 3 bulan, kita totalkan tagihannya
    grouped = df.groupby('NOMEN_CLEAN')['VAL_JUMLAH'].sum().reset_index()
    
    # Ambil Rayon map (ambil baris pertama tiap nomen untuk info rayon)
    rayon_map = {}
    if 'RAYON' in df.columns:
        rayon_map = df.drop_duplicates('NOMEN_CLEAN').set_index('NOMEN_CLEAN')['RAYON'].to_dict()

    update_list = []
    insert_dummy_list = []

    for _, row in grouped.iterrows():
        nomen = str(row['NOMEN_CLEAN'])
        total_saldo = float(row['VAL_JUMLAH'])
        rayon = str(rayon_map.get(nomen, ''))
        
        # Siapkan data untuk insert jika nomen belum ada di MC
        insert_dummy_list.append((nomen, rayon))
        
        # Siapkan data update saldo
        update_list.append((total_saldo, nomen))

    # Eksekusi ke DB
    # A. Insert Nomen yang belum ada (agar tidak error)
    cursor.executemany('''
        INSERT OR IGNORE INTO master_pelanggan (nomen, rayon) 
        VALUES (?, ?)
    ''', insert_dummy_list)
    
    # B. Update Saldo (Reset saldo dulu menjadi 0 untuk semua, lalu update yang ada di file?)
    # Strategi: Timpa saldo user yang ada di file. User yg tidak ada di file ardebt saldonya tetap (atau harusnya 0?)
    # Untuk keamanan, kita hanya update yang ada di file Ardebt.
    cursor.executemany('''
        UPDATE master_pelanggan SET saldo_ardebt = ? WHERE nomen = ?
    ''', update_list)
    
    return len(update_list)

# ==========================================
# DISPATCHER UTAMA
# ==========================================
def process_file(filepath, jenis_file):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    filename = get_filename(filepath)
    
    print(f"[PROCESSOR] Memulai proses {jenis_file}: {filename}")
    rows_affected = 0

    try:
        if jenis_file == FILE_TYPE_MC:
            rows_affected = process_mc(filepath, cursor)
        elif jenis_file == FILE_TYPE_COLLECTION:
            rows_affected = process_collection(filepath, cursor)
        elif jenis_file == FILE_TYPE_SBRS:
            rows_affected = process_sbrs(filepath, cursor)
        elif jenis_file == FILE_TYPE_MAINBILL:
            rows_affected = process_mainbill(filepath, cursor)
        elif jenis_file == FILE_TYPE_ARDEBT:
            rows_affected = process_ardebt(filepath, cursor)
        else:
            raise ValueError(f"Jenis file tidak dikenali: {jenis_file}")

        conn.commit()
        
        log_msg = f"Upload {jenis_file} - {filename} ({rows_affected} rows)"
        cursor.execute("INSERT INTO audit_log (aktivitas, detail) VALUES (?, ?)", 
                       (f"Upload {jenis_file}", log_msg))
        conn.commit()
        
        print(f"[SUCCESS] {log_msg}")

    except Exception as e:
        print(f"[ERROR] Gagal memproses file: {e}")
        conn.rollback()
        raise e
    finally:
        conn.close()
