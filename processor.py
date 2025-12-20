import pandas as pd
import sqlite3
import os
from datetime import datetime
from config import DB_PATH

def get_filename(filepath):
    """Mengambil nama file saja dari path lengkap"""
    return os.path.basename(filepath)

def clean_nomen(val):
    """Membersihkan format Nomen (hilangkan .0 jika dari Excel)"""
    try:
        return str(int(float(str(val))))
    except:
        return str(val).strip()

def process_file(filepath, jenis_file):
    """
    Fungsi Utama memproses file Excel/TXT/CSV ke Database SQLite.
    
    Args:
        filepath (str): Lokasi file yang diupload
        jenis_file (str): 'MC', 'COLLECTION', 'MAINBILL', 'SBRS', 'ARDEBT'
    """
    # Buka koneksi database manual (karena ini berjalan di background process)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    filename = get_filename(filepath)
    
    print(f"[PROCESSOR] Memulai proses {jenis_file}: {filename}")

    try:
        # ==========================================
        # 1. FILE MC (MASTER TARGET BULANAN)
        # ==========================================
        if jenis_file == 'MC':
            # Format: CSV (Koma), Header: PC, NOMEN, NAMA_PEL, REK_AIR
            df = pd.read_csv(filepath, dtype=str)
            
            data_list = []
            for _, row in df.iterrows():
                # Bersihkan data
                nomen = clean_nomen(row.get('NOMEN'))
                target = float(row.get('REK_AIR', 0) or 0) # REK_AIR adalah Target Uang
                
                data_list.append((
                    nomen,
                    str(row.get('NAMA_PEL', '')),
                    str(row.get('ALM1_PEL', '')),
                    str(row.get('PC', '')), # PC sebagai Rayon di file MC
                    str(row.get('TARIF', '')),
                    target,
                    datetime.now().strftime('%Y-%m') # Periode saat ini
                ))
            
            # UPSERT: Masukkan data baru, atau Update jika Nomen sudah ada
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

        # ==========================================
        # 2. FILE ARDEBT (DATA TUNGGAKAN)
        # ==========================================
        elif jenis_file == 'ARDEBT':
            # Format: CSV, Header: NOMEN, RAYON, SumOfJUMLAH
            df = pd.read_csv(filepath, dtype=str)
            
            data_list = []
            for _, row in df.iterrows():
                nomen = clean_nomen(row.get('NOMEN'))
                saldo = float(row.get('SumOfJUMLAH', 0) or 0)
                
                # Kita perlu memastikan Nomen ada di master dulu.
                # Gunakan INSERT OR IGNORE untuk membuat 'wadah' jika MC belum upload
                cursor.execute("INSERT OR IGNORE INTO master_pelanggan (nomen, rayon) VALUES (?, ?)", 
                               (nomen, row.get('RAYON')))
                
                data_list.append((saldo, nomen))

            # Update kolom saldo_ardebt
            cursor.executemany('''
                UPDATE master_pelanggan SET saldo_ardebt = ? WHERE nomen = ?
            ''', data_list)

        # ==========================================
        # 3. FILE COLLECTION (TRANSAKSI HARIAN)
        # ==========================================
        elif jenis_file == 'COLLECTION':
            # Format: TXT (Pipa |), Header: NOMEN|AMT_COLLECT|PAY_DT
            df = pd.read_csv(filepath, sep='|', dtype=str)
            
            # Hapus data lama dari file yang sama (RE-UPLOAD SAFE)
            cursor.execute("DELETE FROM collection_harian WHERE sumber_file = ?", (filename,))
            
            data_list = []
            for _, row in df.iterrows():
                nomen = clean_nomen(row.get('NOMEN'))
                
                # Bersihkan Nilai Uang (SAP pakai minus untuk pembayaran)
                try:
                    raw_amount = row.get('AMT_COLLECT', 0)
                    amount = abs(float(raw_amount)) # Absolutkan (-5000 jadi 5000)
                except:
                    amount = 0
                
                # Format Tanggal: 01-12-2025 -> 2025-12-01 (Database Standard)
                tgl_raw = row.get('PAY_DT', '')
                try:
                    tgl_fix = datetime.strptime(tgl_raw, '%d-%m-%Y').strftime('%Y-%m-%d')
                except:
                    tgl_fix = tgl_raw # Fallback jika format beda
                
                # Pastikan Nomen ada di Master (Integrity check)
                # Jika tidak ada di master, collection tetap dicatat tapi tanpa detail nama
                cursor.execute("INSERT OR IGNORE INTO master_pelanggan (nomen) VALUES (?)", (nomen,))

                data_list.append((
                    nomen,
                    tgl_fix,
                    amount,
                    filename
                ))
            
            cursor.executemany('''
                INSERT INTO collection_harian (nomen, tgl_bayar, jumlah_bayar, sumber_file) 
                VALUES (?, ?, ?, ?)
            ''', data_list)

        # ==========================================
        # 4. FILE MAINBILL (TAGIHAN FINAL)
        # ==========================================
        elif jenis_file == 'MAINBILL':
            # Format: TXT/CSV (Titik Koma ;), Header: NOMEN;TOTAL_TAGIHAN;BILL_CYCLE
            df = pd.read_csv(filepath, sep=';', dtype=str)
            
            data_list = []
            for _, row in df.iterrows():
                nomen = clean_nomen(row.get('NOMEN'))
                
                # Parsing Tagihan
                try:
                    tagihan = float(row.get('TOTAL_TAGIHAN', 0).replace(',', '.'))
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

        # ==========================================
        # 5. FILE SBRS (BACA METER)
        # ==========================================
        elif jenis_file == 'SBRS':
            # Format: CSV (Koma), Header: Nomen, Curr_Read_1, Read_date_1
            df = pd.read_csv(filepath, dtype=str)
            
            data_list = []
            for _, row in df.iterrows():
                nomen = clean_nomen(row.get('Nomen'))
                stand = row.get('Curr_Read_1', 0)
                tgl = row.get('Read_date_1', '')
                
                data_list.append(('SUDAH BACA', stand, tgl, nomen))
                
            cursor.executemany('''
                INSERT INTO operasional (nomen, status_baca, stand_akhir, tgl_baca) VALUES (?, ?, ?, ?)
                ON CONFLICT(nomen) DO UPDATE SET 
                    status_baca=excluded.status_baca, 
                    stand_akhir=excluded.stand_akhir, 
                    tgl_baca=excluded.tgl_baca
            ''', data_list)

        # Simpan perubahan dan catat di Audit Log
        conn.commit()
        
        # Log Audit Sederhana
        cursor.execute("INSERT INTO audit_log (aktivitas, detail) VALUES (?, ?)", 
                       (f"Upload {jenis_file}", f"File: {filename}, Rows: {len(data_list)}"))
        conn.commit()
        
        print(f"[SUCCESS] Berhasil memproses {len(data_list)} baris data.")

    except Exception as e:
        print(f"[ERROR] Gagal memproses file: {e}")
        conn.rollback()
        raise e # Lempar error agar ditangkap oleh app.py untuk Flash Message
    finally:
        conn.close()
