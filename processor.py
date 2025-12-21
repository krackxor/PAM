import pandas as pd
import sqlite3
import os
from datetime import datetime
from config import DB_PATH, FILE_TYPE_MC, FILE_TYPE_COLLECTION, FILE_TYPE_MAINBILL, FILE_TYPE_SBRS, FILE_TYPE_ARDEBT

def get_filename(filepath):
    """Mengambil nama file saja dari path lengkap"""
    return os.path.basename(filepath)

def clean_nomen(val):
    """Membersihkan format Nomen (hilangkan .0 jika dari Excel)"""
    try:
        if pd.isna(val): return None
        return str(int(float(str(val))))
    except:
        return str(val).strip()

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
    # Format: CSV (Koma), Header: PC, NOMEN, NAMA_PEL, REK_AIR
    df = pd.read_csv(filepath, dtype=str)
    
    data_list = []
    for _, row in df.iterrows():
        nomen = clean_nomen(row.get('NOMEN'))
        if not nomen: continue
        
        target = float(row.get('REK_AIR', 0) or 0)
        
        data_list.append((
            nomen,
            str(row.get('NAMA_PEL', '')),
            str(row.get('ALM1_PEL', '')),
            str(row.get('PC', '')), # PC sebagai Rayon
            str(row.get('TARIF', '')),
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
    df = pd.read_csv(filepath, sep='|', dtype=str)
    
    # Hapus data lama dari file yang sama (Re-upload protection)
    cursor.execute("DELETE FROM collection_harian WHERE sumber_file = ?", (filename,))
    
    data_list = []
    for _, row in df.iterrows():
        nomen = clean_nomen(row.get('NOMEN'))
        if not nomen: continue
        
        # Bersihkan Nilai Uang (SAP pakai minus)
        try:
            raw_amt = row.get('AMT_COLLECT', '0')
            amount = abs(float(raw_amt))
        except:
            amount = 0
        
        # Format Tanggal 01-12-2025 -> 2025-12-01
        tgl_fix = clean_date(row.get('PAY_DT', ''), '%d-%m-%Y')
        
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
    # Format: CSV (Koma), Header: Nomen, Curr_Read_1, Read_date_1
    df = pd.read_csv(filepath, dtype=str)
    
    data_list = []
    for _, row in df.iterrows():
        nomen = clean_nomen(row.get('Nomen'))
        if not nomen: continue
        
        stand = row.get('Curr_Read_1', 0)
        # Format Tanggal 11/12/2025 -> 2025-12-11
        tgl = clean_date(row.get('Read_date_1', ''), '%d/%m/%Y')
        
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
    # Format: TXT (Titik Koma ;), Header: NOMEN;TOTAL_TAGIHAN;BILL_CYCLE
    df = pd.read_csv(filepath, sep=';', dtype=str)
    
    data_list = []
    for _, row in df.iterrows():
        nomen = clean_nomen(row.get('NOMEN'))
        if not nomen: continue
        
        try:
            tagihan = float(row.get('TOTAL_TAGIHAN', '0').replace(',', '.'))
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
    # Format: CSV, Header: NOMEN, RAYON, SumOfJUMLAH
    df = pd.read_csv(filepath, dtype=str)
    
    data_list = []
    for _, row in df.iterrows():
        nomen = clean_nomen(row.get('NOMEN'))
        if not nomen: continue
        
        saldo = float(row.get('SumOfJUMLAH', 0) or 0)
        
        # Buat dummy master jika belum ada
        cursor.execute("INSERT OR IGNORE INTO master_pelanggan (nomen, rayon) VALUES (?, ?)", 
                       (nomen, row.get('RAYON')))
        
        data_list.append((saldo, nomen))

    cursor.executemany('''
        UPDATE master_pelanggan SET saldo_ardebt = ? WHERE nomen = ?
    ''', data_list)
    return len(data_list)

# ==========================================
# DISPATCHER UTAMA
# ==========================================
def process_file(filepath, jenis_file):
    """
    Fungsi Utama yang dipanggil oleh app.py.
    Hanya bertugas membuka koneksi DB dan mengarahkan ke fungsi spesifik.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    filename = get_filename(filepath)
    
    print(f"[PROCESSOR] Memulai proses {jenis_file}: {filename}")
    rows_affected = 0

    try:
        # Arahkan ke fungsi sesuai jenis file
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

        # Commit & Log Audit
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
