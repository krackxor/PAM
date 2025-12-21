import sqlite3
from flask import g
from config import DB_PATH

def get_db():
    """
    Membuka koneksi ke database SQLite.
    Menggunakan g object dari Flask untuk menyimpan koneksi selama request berlangsung.
    """
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DB_PATH)
        # Mengaktifkan Foreign Key constraint agar relasi antar tabel terjaga
        db.execute("PRAGMA foreign_keys = ON")
        # Mengembalikan hasil query sebagai Row object (bisa akses kolom pakai nama, misal: row['nomen'])
        db.row_factory = sqlite3.Row 
    return db

def close_db(e=None):
    """Menutup koneksi database jika terbuka."""
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

def init_db(app):
    """
    Inisialisasi tabel-tabel database.
    Dijalankan sekali saat aplikasi start.
    """
    with app.app_context():
        db = get_db()
        cursor = db.cursor()

        # ==========================================
        # 1. TABEL MASTER PELANGGAN
        # ==========================================
        # Gabungan data dari File MC (Target) & File ARDEBT (Tunggakan)
        # Nomen adalah kunci utama.
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS master_pelanggan (
                nomen TEXT PRIMARY KEY,
                nama TEXT,
                alamat TEXT,
                rayon TEXT,       -- PC (MC) / RAYON (ARDEBT)
                tarif TEXT,
                target_mc REAL DEFAULT 0,    -- Dari File MC (Kolom REK_AIR)
                saldo_ardebt REAL DEFAULT 0, -- Dari File ARDEBT (Kolom SumOfJUMLAH)
                periode TEXT,                -- Periode Data (Format YYYY-MM)
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        # Index untuk pencarian cepat berdasarkan Rayon (Filter Dashboard)
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_master_rayon ON master_pelanggan(rayon)')

        # ==========================================
        # 2. TABEL COLLECTION HARIAN
        # ==========================================
        # Data Transaksi Harian dari File Collection (.txt)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS collection_harian (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nomen TEXT,
                tgl_bayar TEXT,     -- Format YYYY-MM-DD
                jumlah_bayar REAL,  -- Dari AMT_COLLECT (Nilai Absolut)
                sumber_file TEXT,   -- Nama file asal untuk tracing jika ada double upload
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(nomen) REFERENCES master_pelanggan(nomen) ON DELETE CASCADE
            )
        ''')
        # Index untuk report harian dan pencarian per pelanggan
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_coll_tgl ON collection_harian(tgl_bayar)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_coll_nomen ON collection_harian(nomen)')

        # ==========================================
        # 3. TABEL OPERASIONAL SIKLUS
        # ==========================================
        # Data Progresif dari SBRS (Baca Meter) & MainBill (Tagihan Final)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS operasional (
                nomen TEXT PRIMARY KEY,
                status_baca TEXT DEFAULT 'BELUM', -- SUDAH BACA / BELUM
                stand_akhir INTEGER DEFAULT 0,    -- Dari SBRS (Curr_Read_1)
                tgl_baca TEXT,                    -- Dari SBRS (Read_date_1)
                tagihan_final REAL DEFAULT 0,     -- Dari MainBill (TOTAL_TAGIHAN)
                cycle TEXT,                       -- Dari SBRS/MainBill
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(nomen) REFERENCES master_pelanggan(nomen) ON DELETE CASCADE
            )
        ''')

        # ==========================================
        # 4. TABEL ANALISA MANUAL (WORKBENCH)
        # ==========================================
        # Tempat menyimpan hasil kerja user/tim (Menu Analisa Manual)
        cursor.execute('''
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
        # Mencatat history upload dan perubahan data (Syarat Wajib: Audit)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                aktivitas TEXT,       -- Contoh: "Upload File MC", "Simpan Analisa"
                detail TEXT,          -- Contoh: "NamaFile.csv" atau "Nomen: 6012345"
                user TEXT DEFAULT 'Admin',
                waktu TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        db.commit()
