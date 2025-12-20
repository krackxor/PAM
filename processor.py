import pandas as pd
import sqlite3
import os
from datetime import datetime
from config import DB_PATH, FILE_TYPE_MC, FILE_TYPE_COLLECTION, FILE_TYPE_MAINBILL, FILE_TYPE_SBRS, FILE_TYPE_ARDEBT

def get_filename(filepath):
    """Mengambil nama file saja dari path lengkap"""
    return os.path.basename(filepath)

def clean_nomen(val):
    """Membersihkan format Nomen agar seragam (String bersih tanpa .0)"""
    if pd.isna(val): return None
    
    # Ubah ke string dan hapus spasi
    s = str(val).strip()
    
    # Hapus karakter aneh jika ada
    s = ''.join(filter(str.isdigit, s))
    
    # Hapus akhiran .0 (artifact Excel) secara manual jika lolos filter digit
    # Namun karena filter digit membuang titik, logic di atas sudah menghandle '3001.0' jadi '30010' salah.
    # REVISI: Pendekatan string standar
    s = str(val).strip()
    if s.endswith('.0'):
        s = s[:-2]
        
    if not s: return None
    return s

def clean_currency(val):
    """Membersihkan format mata uang"""
    try:
        if pd.isna(val): return 0.0
        s = str(val).strip().replace(',', '').replace('Rp', '').replace(' ', '')
        return float(s)
    except:
        return 0.0

def clean_date(val, fmt_in='%d-%m-%Y', fmt_out='%Y-%m-%d'):
    try:
        return datetime.strptime(str(val).strip(), fmt_in).strftime(fmt_out)
    except:
        return val

# ==========================================
# 1. MODUL MASTER CUSTOMER (MC)
# ==========================================
def process_mc(filepath, cursor):
    df = pd.read_csv(filepath, dtype=str, low_memory=False)
    df.columns = df.columns.str.strip().str.upper()
    
    data_list = []
    
    # Mapping kolom fleksibel
    col_nomen = 'NOMEN'
    col_nama = next((c for c in ['NAMA_PEL', 'NAMA'] if c in df.columns), 'NAMA_PEL')
    col_alamat = next((c for c in ['ALM1_PEL', 'ALAMAT'] if c in df.columns), 'ALM1_PEL')
    col_rayon = next((c for c in ['ZONA_NOVAK', 'RAYON', 'PC'] if c in df.columns), 'ZONA_NOVAK')
    col_tarif = next((c for c in ['TARIF', 'GOL'] if c in df.columns), 'TARIF')
    col_target = next((c for c in ['REK_AIR', 'TAGIHAN', 'TARGET'] if c in df.columns), 'REK_AIR')

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
# 2. MODUL COLLECTION
# ==========================================
def process_collection(filepath, cursor):
    filename = get_filename(filepath)
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
        
        cursor.execute("INSERT OR IGNORE INTO master_pelanggan (nomen) VALUES (?)", (nomen,))
        data_list.append((nomen, tgl_fix, amount, filename))
    
    cursor.executemany('INSERT INTO collection_harian (nomen, tgl_bayar, jumlah_bayar, sumber_file) VALUES (?, ?, ?, ?)', data_list)
    return len(data_list)

# ==========================================
# 3. MODUL SBRS
# ==========================================
def process_sbrs(filepath, cursor):
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
        tgl = clean_date(row.get(col_tgl, ''), '%d/%m/%Y')
        
        data_list.append(('SUDAH BACA', stand, tgl, nomen))
        
    cursor.executemany('''
        INSERT INTO operasional (nomen, status_baca, stand_akhir, tgl_baca) VALUES (?, ?, ?, ?)
        ON CONFLICT(nomen) DO UPDATE SET status_baca=excluded.status_baca, stand_akhir=excluded.stand_akhir, tgl_baca=excluded.tgl_baca
    ''', data_list)
    return len(data_list)

# ==========================================
# 4. MODUL MAINBILL
# ==========================================
def process_mainbill(filepath, cursor):
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
        ON CONFLICT(nomen) DO UPDATE SET tagihan_final=excluded.tagihan_final, cycle=excluded.cycle
    ''', data_list)
    return len(data_list)

# ==========================================
# 5. MODUL ARDEBT (TUNGGAKAN) - DIAGNOSTIC MODE
# ==========================================
def process_ardebt(filepath, cursor):
    df = pd.read_csv(filepath, dtype=str, low_memory=False)
    df.columns = df.columns.str.strip().str.upper()
    
    # 1. Cleaning NOMEN
    df['NOMEN_CLEAN'] = df['NOMEN'].apply(clean_nomen)
    df = df.dropna(subset=['NOMEN_CLEAN'])
    
    # 2. Identifikasi Kolom Jumlah
    possible_cols = ['JUMLAH', 'SUMOFJUMLAH', 'TAGIHAN', 'SALDO']
    col_jumlah = next((c for c in possible_cols if c in df.columns), None)
    
    if not col_jumlah:
        raise ValueError(f"Kolom JUMLAH tidak ditemukan. Header: {list(df.columns)}")

    df['VAL_JUMLAH'] = df[col_jumlah].apply(clean_currency)
    
    # 3. Cek Diagnostik Kecocokan Data
    mc_nomens_query = cursor.execute("SELECT nomen FROM master_pelanggan").fetchall()
    mc_nomens_set = {row[0] for row in mc_nomens_query}
    
    ardebt_nomens_set = set(df['NOMEN_CLEAN'].unique())
    intersection = ardebt_nomens_set.intersection(mc_nomens_set)
    
    print(f"[DIAGNOSTIC] Total MC DB: {len(mc_nomens_set)}")
    print(f"[DIAGNOSTIC] Total Ardebt File: {len(ardebt_nomens_set)}")
    print(f"[DIAGNOSTIC] Data Cocok (Match): {len(intersection)}")

    if len(intersection) == 0 and len(mc_nomens_set) > 0:
        # Ambil sampel untuk pesan error
        sample_mc = list(mc_nomens_set)[:3]
        sample_ardebt = list(ardebt_nomens_set)[:3]
        error_msg = (f"Tidak ada data Ardebt yang cocok dengan MC! "
                     f"MC: {sample_mc} vs Ardebt: {sample_ardebt}. "
                     f"Pastikan format NOMEN sama.")
        raise ValueError(error_msg)

    # 4. Grouping & Saving
    grouped = df.groupby('NOMEN_CLEAN')['VAL_JUMLAH'].sum().reset_index()
    
    rayon_map = {}
    if 'RAYON' in df.columns:
        rayon_map = df.drop_duplicates('NOMEN_CLEAN').set_index('NOMEN_CLEAN')['RAYON'].to_dict()

    update_list = []
    insert_dummy_list = []

    for _, row in grouped.iterrows():
        nomen = str(row['NOMEN_CLEAN'])
        total_saldo = float(row['VAL_JUMLAH'])
        rayon = str(rayon_map.get(nomen, ''))
        
        insert_dummy_list.append((nomen, rayon))
        update_list.append((total_saldo, nomen))

    # Pastikan Master Ada (Insert or Ignore)
    cursor.executemany('INSERT OR IGNORE INTO master_pelanggan (nomen, rayon) VALUES (?, ?)', insert_dummy_list)
    
    # Update Saldo
    cursor.executemany('UPDATE master_pelanggan SET saldo_ardebt = ? WHERE nomen = ?', update_list)
    
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
        
        # Log Sukses
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
