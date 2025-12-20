import sqlite3
import os
import json
from datetime import datetime
import pandas as pd

DB_NAME = 'data/sunter.db'

class DatabaseManager:
    def __init__(self):
        self.ensure_db_exists()

    def get_connection(self):
        """Membuka koneksi dengan konfigurasi Foreign Key & Row Factory"""
        conn = sqlite3.connect(DB_NAME)
        conn.execute("PRAGMA foreign_keys = ON") # Aktifkan Foreign Key
        conn.row_factory = sqlite3.Row         # Return hasil sebagai dict/row object
        return conn

    def ensure_db_exists(self):
        """Inisialisasi Skema Database (Tabel-tabel)"""
        if not os.path.exists('data'):
            os.makedirs('data')
            
        conn = self.get_connection()
        c = conn.cursor()
        
        # ==========================================
        # 1. TABEL MASTER PELANGGAN
        # ==========================================
        c.execute('''
            CREATE TABLE IF NOT EXISTS master_pelanggan (
                nomen TEXT PRIMARY KEY,
                nama TEXT,
                alamat TEXT,
                rayon TEXT,       -- PC (MC) / RAYON (ARDEBT)
                tarif TEXT,
                target_mc REAL DEFAULT 0,    -- Dari File MC (Kolom REK_AIR)
                saldo_ardebt REAL DEFAULT 0, -- Dari File ARDEBT (Kolom SumOfJUMLAH)
                periode TEXT,                -- Periode Data (Format YYYY-MM)
                data_json TEXT,              -- Tambahan untuk menyimpan raw data fleksibel
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        # Index untuk pencarian cepat
        c.execute('CREATE INDEX IF NOT EXISTS idx_master_rayon ON master_pelanggan(rayon)')

        # ==========================================
        # 2. TABEL COLLECTION HARIAN
        # ==========================================
        c.execute('''
            CREATE TABLE IF NOT EXISTS collection_harian (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nomen TEXT,
                tgl_bayar TEXT,      -- Format YYYY-MM-DD
                jumlah_bayar REAL,   -- Dari AMT_COLLECT (Nilai Absolut)
                sumber_file TEXT,    -- Nama file asal
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(nomen) REFERENCES master_pelanggan(nomen) ON DELETE CASCADE
            )
        ''')
        c.execute('CREATE INDEX IF NOT EXISTS idx_coll_tgl ON collection_harian(tgl_bayar)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_coll_nomen ON collection_harian(nomen)')

        # ==========================================
        # 3. TABEL OPERASIONAL (SBRS & MAINBILL)
        # ==========================================
        c.execute('''
            CREATE TABLE IF NOT EXISTS operasional (
                nomen TEXT PRIMARY KEY,
                status_baca TEXT DEFAULT 'BELUM',
                stand_akhir REAL DEFAULT 0,
                tgl_baca TEXT,
                tagihan_final REAL DEFAULT 0,
                cycle TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(nomen) REFERENCES master_pelanggan(nomen) ON DELETE CASCADE
            )
        ''')

        # ==========================================
        # 4. TABEL ANALISA MANUAL (WORKBENCH)
        # ==========================================
        c.execute('''
            CREATE TABLE IF NOT EXISTS analisa_manual (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nomen TEXT,
                jenis_anomali TEXT,   -- Zero Usage, Extreme, Stand Negatif, dll.
                catatan TEXT,         -- Hasil cek lapangan / analisa tim
                rekomendasi TEXT,     -- Ganti Meter, Tagih Susulan, Monitoring, dll.
                status TEXT DEFAULT 'Open', -- Open, Progress, Closed
                user_editor TEXT DEFAULT 'System',
                tgl_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(nomen) REFERENCES master_pelanggan(nomen) ON DELETE CASCADE
            )
        ''')

        # ==========================================
        # 5. TABEL AUDIT TRAIL (LOG AKTIVITAS)
        # ==========================================
        c.execute('''
            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                aktivitas TEXT,       -- Contoh: "Upload File MC"
                detail TEXT,          -- Detail aktivitas
                user TEXT DEFAULT 'Admin',
                waktu TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()

    # =========================================================
    # FITUR ANALYTICS (DIPERLUKAN UNTUK DASHBOARD)
    # =========================================================

    def get_collection_summary(self):
        """Mengambil ringkasan header (MC, Undue, Realisasi)"""
        conn = self.get_connection()
        
        try:
            # 1. Target MC (Bulan ini)
            mc_df = pd.read_sql("SELECT SUM(target_mc) as total_mc FROM master_pelanggan", conn)
            total_mc = mc_df['total_mc'].iloc[0] or 0
            
            # 2. Ardebt (Undue awal)
            ardebt_df = pd.read_sql("SELECT SUM(saldo_ardebt) as total_ardebt FROM master_pelanggan", conn)
            total_undue = ardebt_df['total_ardebt'].iloc[0] or 0
            
            # 3. Realisasi Collection (Total)
            coll_df = pd.read_sql("SELECT SUM(jumlah_bayar) as total_bayar FROM collection_harian", conn)
            total_coll = coll_df['total_bayar'].iloc[0] or 0
        finally:
            conn.close()
        
        # Hitung Persentase
        coll_rate = (total_coll / total_mc * 100) if total_mc > 0 else 0
        
        return {
            "mc_bulan_ini": total_mc,
            "mb_undue": total_undue,
            "coll_current": total_coll, # Asumsi sementara semua current
            "coll_undue": 0,
            "coll_rate": round(coll_rate, 2),
            "total_coll": total_coll
        }

    def get_daily_collection_table(self):
        """Menghasilkan data tabel harian dengan akumulasi dan varians."""
        conn = self.get_connection()
        
        query = '''
            SELECT 
                tgl_bayar,
                COUNT(DISTINCT nomen) as jml_cust,
                SUM(jumlah_bayar) as total_coll
            FROM collection_harian
            GROUP BY tgl_bayar
            ORDER BY tgl_bayar ASC
        '''
        try:
            df = pd.read_sql(query, conn)
        finally:
            conn.close()

        if df.empty:
            return []

        # Ambil Total MC untuk hitung %
        summary = self.get_collection_summary()
        total_mc = summary['mc_bulan_ini']

        # Proses Data Frame untuk kolom-kolom hitungan
        df['kumulatif'] = df['total_coll'].cumsum()
        df['var_h_min_1'] = df['total_coll'].diff().fillna(0)
        df['persen_mc'] = df['kumulatif'].apply(lambda x: (x / total_mc * 100) if total_mc > 0 else 0)
        
        results = []
        for _, row in df.iterrows():
            results.append({
                "tgl": row['tgl_bayar'],
                "jml_cust": int(row['jml_cust']),
                "mc_target": total_mc,
                "mb_undue": summary['mb_undue'],
                "coll_current": row['total_coll'],
                "coll_undue": 0,
                "total_coll": row['total_coll'],
                "persen_mc": round(row['persen_mc'], 2),
                "kumulatif": row['kumulatif'],
                "var_h1": row['var_h_min_1']
            })
            
        return results

    def get_breakdown_stats(self, category_col):
        """Breakdown berdasarkan Rayon, Tarif, dll."""
        conn = self.get_connection()
        
        # Mapping kolom input ke kolom DB
        col_db = category_col
        if category_col == 'pc': col_db = 'rayon'
        
        # Validasi sederhana untuk mencegah SQL Injection via parameter
        if col_db not in ['rayon', 'tarif', 'pc']:
            col_db = 'rayon'

        query = f'''
            SELECT 
                m.{col_db} as kategori,
                COUNT(DISTINCT m.nomen) as total_plg,
                SUM(m.target_mc) as target,
                SUM(c.jumlah_bayar) as realisasi
            FROM master_pelanggan m
            LEFT JOIN collection_harian c ON m.nomen = c.nomen
            GROUP BY m.{col_db}
        '''
        
        try:
            df = pd.read_sql(query, conn)
            df = df.fillna(0)
            df['persen'] = df.apply(lambda x: (x['realisasi'] / x['target'] * 100) if x['target'] > 0 else 0, axis=1)
            return df.to_dict('records')
        except:
            return []
        finally:
            conn.close()

    def get_customer_details(self, limit=100):
        """Detail Pelanggan untuk tabel bawah (Top Payer)"""
        conn = self.get_connection()
        query = '''
            SELECT 
                m.nomen, m.nama, m.rayon, m.tarif,
                m.target_mc, m.saldo_ardebt,
                IFNULL(SUM(c.jumlah_bayar), 0) as total_bayar,
                MAX(c.tgl_bayar) as tgl_bayar_terakhir
            FROM master_pelanggan m
            LEFT JOIN collection_harian c ON m.nomen = c.nomen
            GROUP BY m.nomen
            HAVING total_bayar > 0
            ORDER BY total_bayar DESC
            LIMIT ?
        '''
        try:
            # Gunakan cursor agar parameter limit aman
            cursor = conn.cursor()
            cursor.execute(query, (limit,))
            # Convert sqlite3.Row objects to dicts
            rows = [dict(row) for row in cursor.fetchall()]
            return rows
        finally:
            conn.close()

# Instance global
db = DatabaseManager()
