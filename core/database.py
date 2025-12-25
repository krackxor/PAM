"""
Database Management Module
Handles database connection, initialization, and schema
"""

import sqlite3
import os
from flask import g

DB_PATH = os.path.join('database', 'sunter.db')

def get_db():
    """Get database connection"""
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys = ON")
    return db

def close_db(exception):
    """Close database connection"""
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

def init_db(app):
    """Initialize database schema"""
    with app.app_context():
        db = get_db()
        cursor = db.cursor()
        
        # Master Pelanggan
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS master_pelanggan (
                nomen TEXT PRIMARY KEY,
                nama TEXT,
                alamat TEXT,
                rayon TEXT,
                pc TEXT,
                ez TEXT,
                pcez TEXT,
                block TEXT,
                zona_novak TEXT,
                tarif TEXT,
                target_mc REAL DEFAULT 0,
                kubikasi REAL DEFAULT 0,
                periode TEXT,
                periode_bulan INTEGER,
                periode_tahun INTEGER,
                upload_id INTEGER,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Collection Harian
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS collection_harian (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nomen TEXT,
                tgl_bayar TEXT,
                jumlah_bayar REAL DEFAULT 0,
                volume_air REAL DEFAULT 0,
                tipe_bayar TEXT DEFAULT 'current',
                bill_period TEXT,
                periode_bulan INTEGER,
                periode_tahun INTEGER,
                upload_id INTEGER,
                sumber_file TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(nomen) REFERENCES master_pelanggan(nomen),
                UNIQUE(nomen, tgl_bayar, jumlah_bayar, bill_period)
            )
        ''')
        
        # Master Bayar
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS master_bayar (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nomen TEXT,
                tgl_bayar TEXT,
                jumlah_bayar REAL DEFAULT 0,
                periode_bulan INTEGER,
                periode_tahun INTEGER,
                upload_id INTEGER,
                periode TEXT,
                sumber_file TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(nomen) REFERENCES master_pelanggan(nomen)
            )
        ''')
        
        # MainBill
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS mainbill (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nomen TEXT,
                tgl_tagihan TEXT,
                total_tagihan REAL DEFAULT 0,
                pcezbk TEXT,
                tarif TEXT,
                periode_bulan INTEGER,
                periode_tahun INTEGER,
                upload_id INTEGER,
                periode TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(nomen) REFERENCES master_pelanggan(nomen)
            )
        ''')
        
        # Ardebt
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ardebt (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nomen TEXT,
                saldo_tunggakan REAL DEFAULT 0,
                periode_bulan INTEGER,
                periode_tahun INTEGER,
                upload_id INTEGER,
                periode TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(nomen) REFERENCES master_pelanggan(nomen)
            )
        ''')
        
        # SBRS Data
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sbrs_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nomen TEXT NOT NULL,
                nama TEXT,
                alamat TEXT,
                rayon TEXT,
                readmethod TEXT,
                skip_status TEXT,
                trouble_status TEXT,
                spm_status TEXT,
                stand_awal REAL,
                stand_akhir REAL,
                volume REAL,
                analisa_tindak_lanjut TEXT,
                tag1 TEXT,
                tag2 TEXT,
                periode_bulan INTEGER NOT NULL,
                periode_tahun INTEGER NOT NULL,
                upload_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (upload_id) REFERENCES upload_metadata(id)
            )
        ''')
        
        # Upload Metadata
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS upload_metadata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_type TEXT NOT NULL,
                file_name TEXT NOT NULL,
                periode_bulan INTEGER NOT NULL,
                periode_tahun INTEGER NOT NULL,
                upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_count INTEGER,
                status TEXT DEFAULT 'success'
            )
        ''')
        
        # Analisa Manual
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS analisa_manual (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nomen TEXT,
                jenis_anomali TEXT,
                deskripsi TEXT,
                status TEXT DEFAULT 'pending',
                priority TEXT DEFAULT 'medium',
                assigned_to TEXT,
                due_date TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        ''')
        
        # Analisa Comments
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS analisa_comments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                analisa_id INTEGER NOT NULL,
                user TEXT NOT NULL,
                comment TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (analisa_id) REFERENCES analisa_manual(id)
            )
        ''')
        
        # Analisa Activity
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS analisa_activity (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                analisa_id INTEGER NOT NULL,
                action TEXT NOT NULL,
                user TEXT NOT NULL,
                icon TEXT DEFAULT 'circle',
                created_at TEXT NOT NULL,
                FOREIGN KEY (analisa_id) REFERENCES analisa_manual(id)
            )
        ''')
        
        # Indexes
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_coll_tgl ON collection_harian(tgl_bayar)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_coll_nomen ON collection_harian(nomen)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_master_rayon ON master_pelanggan(rayon)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_mb_nomen ON master_bayar(nomen)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_sbrs_nomen ON sbrs_data(nomen)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_sbrs_periode ON sbrs_data(periode_bulan, periode_tahun)')
        
        db.commit()
        print("✅ Database schema initialized")
