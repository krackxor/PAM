import pandas as pd
import sqlite3
import os
from datetime import datetime
from config import DB_PATH, FILE_TYPE_MC, FILE_TYPE_COLLECTION, FILE_TYPE_MAINBILL, FILE_TYPE_SBRS, FILE_TYPE_ARDEBT

def get_filename(filepath):
    """Mengambil nama file saja dari path lengkap"""
    return os.path.basename(filepath)

def clean_nomen(val):
    """Membersihkan format Nomen (hilangkan .0, spasi, jadi string murni)"""
    if pd.isna(val):
        return None
    
    # Ubah ke string dan strip spasi
    s = str(val).strip()
    
    # Hapus desimal .0 (artifact Excel)
    if s.endswith('.0'):
        s = s[:-2]
        
    # Validasi panjang (opsional, sesuaikan kebutuhan)
    if not s: 
        return None
        
    return s

def clean_date(val, fmt_in='%d-%m-%Y', fmt_out='%Y-%m-%d'):
    """Standardisasi format tanggal ke YYYY-MM-DD"""
    try:
        return datetime.strptime(str(val).strip(), fmt_in).strftime(fmt_out)
    except:
        return val # Kembalikan aslinya jika gagal parsing

# ==========================================
# 1. MODUL MASTER CUSTOMER (MC)
# ==========================================
def process_mc(filepath, cursor):
    # Format: CSV (Koma), Header biasanya: NOMEN, NAMA_PEL, ALM1_PEL, REK_AIR, dll
    df = pd.read_csv(filepath, dtype=str, low_memory=False)
    
    # Standardisasi Header (Huruf besar & tanpa spasi)
    df.columns = df.columns.str.strip().str.upper()
    
    data_list = []
    
    # Mapping nama kolom agar fleksibel
    # Cari kolom yang sesuai dengan prioritas nama
    col_nomen = 'NOMEN'
    col_nama = next((c for c in ['NAMA_PEL', 'NAMA'] if c in df.columns), 'NAMA_PEL')
    col_alamat = next((c for c in ['ALM1_PEL', 'ALAMAT'] if c in df.columns), 'ALM1_PEL')
    col_tarif = next((c for c in ['TARIF', 'GOL_TARIF'] if c in df.columns), 'TARIF')
    col_target = next((c for c in ['REK_AIR', 'TAGIHAN', 'TARGET'] if c in df.columns), 'REK_AIR')
    col_rayon = next((c for c in ['ZONA_NOVAK', 'PC', 'RAYON'] if c in df.columns), 'ZONA_NOVAK')

    for _, row in df.iterrows():
        nomen = clean_nomen(row.get(col_nomen))
        if not nomen: continue
        
        # Bersihkan target (Rp)
        try:
            raw_target = row.get(col_target, '0')
            target = float(str(raw_target).replace(',', '').replace(' ', '') or 0)
        except:
            target = 0
        
        data_list.append((
            nomen,
            str(row.get(col_nama, '')),
            str(row.get(col_alamat, '')),
            str(row.get(col_rayon, '')), # Menggunakan ZONA_NOVAK jika PC tidak ada
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
    # Format: TXT/CSV (Pipa |)
    df = pd.read_csv(filepath, sep='|', dtype=str, low_memory=False)
    df.columns = df.columns.str.strip().str.upper()
    
    # Hapus data lama dari file yang sama
    cursor.execute("DELETE FROM collection_harian WHERE sumber_file = ?", (filename,))
    
    data_list = []
    
    col_nomen = 'NOMEN'
    col_amt = 'AMT_COLLECT'
    col_date = 'PAY_DT'

    for _, row in df.iterrows():
        nomen = clean_nomen(row.get(col_nomen))
        if not nomen: continue
        
        try:
            raw_amt = row.get(col_amt, '0')
            amount = abs(float(str(raw_amt).replace(',', '')))
        except:
            amount = 0
        
        tgl_fix = clean_date(row.get(col_date, ''), '%d-%m-%Y')
        
        # Pastikan Nomen ada di Master (Integrity)
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
    df = pd.read_csv(filepath, dtype=str, low_memory=False)
    df.columns = df.columns.str.strip().str.upper()
    
    data_list = []
    col_nomen = 'NOMEN'
    col_read = next((c for c in ['CURR_READ_1', 'STAND_AKHIR'] if c in df.columns), 'CURR_READ_1')
    col_date = next((c for c in ['READ_DATE_1', 'TGL_BACA'] if c in df.columns), 'READ_DATE_1')

    for _, row in df.iterrows():
        nomen = clean_nomen(row.get(col_nomen))
        if not nomen: continue
        
        stand = row.get(col_read, 0)
        tgl = clean_date(row.get(col_date, ''), '%d/%m/%Y')
        
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
    df = pd.read_csv(filepath, sep=';', dtype=str, low_memory=False)
    df.columns = df.columns.str.strip().str.upper()
    
    data_list = []
    for _, row in df.iterrows():
        nomen = clean_nomen(row.get('NOMEN'))
        if not nomen: continue
        
        try:
            tagihan = float(str(row.get('TOTAL_TAGIHAN', '0')).replace(',', '.'))
        except:
            tagihan = 0
            
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
# 5. MODUL ARDEBT (TUNGGAKAN)
# ==========================================
def process_ardebt(filepath, cursor):
    # Format: CSV, Header: NOMEN, RAYON, JUMLAH (Bukan SumOfJUMLAH)
    df = pd.read_csv(filepath, dtype=str, low_memory=False)
    df.columns = df.columns.str.strip().str.upper()
    
    # 1. Bersihkan Nomen terlebih dahulu
    df['NOMEN_CLEAN'] = df['NOMEN'].apply(clean_nomen)
    df = df.dropna(subset=['NOMEN_CLEAN']) # Hapus yg nomennya kosong
    
    # 2. Pastikan kolom JUMLAH terhandle dengan benar (bisa 'JUMLAH' atau 'TAGIHAN')
    col_jumlah = next((c for c in ['JUMLAH', 'TAGIHAN', 'SALDO'] if c in df.columns), None)
    
    if not col_jumlah:
        raise ValueError(f"Kolom JUMLAH tidak ditemukan. Kolom yang ada: {list(df.columns)}")

    # Convert Jumlah ke Float
    df['JUMLAH_CLEAN'] = pd.to_numeric(df[col_jumlah], errors='coerce').fillna(0)
    
    # 3. GROUP BY NOMEN (PENTING! Agar tunggakan dijumlahkan per user)
    # Jika user punya 3 bulan tunggakan, kita mau totalnya.
    grouped = df.groupby('NOMEN_CLEAN')['JUMLAH_CLEAN'].sum().reset_index()
    
    # Ambil Rayon (ambil yang pertama aja per nomen)
    rayon_map = {}
    if 'RAYON' in df.columns:
        rayon_map = df.drop_duplicates('NOMEN_CLEAN').set_index('NOMEN_CLEAN')['RAYON'].to_dict()

    update_list = []
    insert_dummy_list = []

    for _, row in grouped.iterrows():
        nomen = str(row['NOMEN_CLEAN'])
        total_saldo = float(row['JUMLAH_CLEAN'])
        rayon = str(rayon_map.get(nomen, ''))

        # Siapkan data untuk insert dummy master jika belum ada
        insert_dummy_list.append((nomen, rayon))
        
        # Siapkan data untuk update saldo
        update_list.append((total_saldo, nomen))

    # Eksekusi Database
    # A. Pastikan Master Ada (Insert or Ignore)
    cursor.executemany('''
        INSERT OR IGNORE INTO master_pelanggan (nomen, rayon) 
        VALUES (?, ?)
    ''', insert_dummy_list)

    # B. Update Saldo Ardebt
    cursor.executemany('''
        UPDATE master_pelanggan SET saldo_ardebt = ? WHERE nomen = ?
    ''', update_list)
    
    return len(update_list)

# ==========================================
# DISPATCHER UTAMA
# ==========================================
def process_file(filepath, jenis_file):
    """
    Fungsi Utama yang dipanggil oleh app.py.
    """
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
