#!/usr/bin/env python3
"""
Sunter Dashboard - Database Creator
Run: python3 create_database.py
"""

import sqlite3
import os

def create_database():
    """Create Sunter Dashboard database with all tables"""
    
    # Create database folder
    os.makedirs('database', exist_ok=True)
    
    db_path = 'database/sunter.db'
    
    # Check if exists
    if os.path.exists(db_path):
        response = input(f"⚠️  Database already exists at {db_path}\nDelete and recreate? (y/n): ")
        if response.lower() != 'y':
            print("❌ Cancelled")
            return
        os.remove(db_path)
        print("✓ Old database deleted")
    
    print("\n" + "="*60)
    print("Creating Sunter Dashboard Database")
    print("="*60)
    
    # Connect
    db = sqlite3.connect(db_path)
    cursor = db.cursor()
    
    # 1. Master Pelanggan (MC) - INDUK
    print("\n1. Creating master_pelanggan table...")
    cursor.execute("""
    CREATE TABLE master_pelanggan (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nomen TEXT NOT NULL,
        nama TEXT,
        alamat TEXT,
        rayon TEXT,
        target_mc REAL DEFAULT 0,
        pcez TEXT,
        tgl_catat TEXT,
        periode_bulan INTEGER NOT NULL,
        periode_tahun INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(nomen, periode_bulan, periode_tahun)
    )
    """)
    
    cursor.execute("CREATE INDEX idx_mc_nomen ON master_pelanggan(nomen)")
    cursor.execute("CREATE INDEX idx_mc_periode ON master_pelanggan(periode_bulan, periode_tahun)")
    cursor.execute("CREATE INDEX idx_mc_rayon ON master_pelanggan(rayon)")
    cursor.execute("CREATE INDEX idx_mc_pcez ON master_pelanggan(pcez)")
    print("   ✓ master_pelanggan created with 4 indexes")
    
    # 2. Belum Bayar (MB)
    print("\n2. Creating belum_bayar table...")
    cursor.execute("""
    CREATE TABLE belum_bayar (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nomen TEXT NOT NULL,
        nama TEXT,
        alamat TEXT,
        rayon TEXT,
        total_tagihan REAL DEFAULT 0,
        tgl_bayar TEXT,
        periode_bulan INTEGER NOT NULL,
        periode_tahun INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(nomen, periode_bulan, periode_tahun)
    )
    """)
    
    cursor.execute("CREATE INDEX idx_mb_nomen ON belum_bayar(nomen)")
    cursor.execute("CREATE INDEX idx_mb_periode ON belum_bayar(periode_bulan, periode_tahun)")
    print("   ✓ belum_bayar created with 2 indexes")
    
    # 3. Collection Harian
    print("\n3. Creating collection_harian table...")
    cursor.execute("""
    CREATE TABLE collection_harian (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nomen TEXT NOT NULL,
        pay_dt TEXT,
        volume REAL DEFAULT 0,
        current REAL DEFAULT 0,
        tunggakan REAL DEFAULT 0,
        total REAL DEFAULT 0,
        periode_bulan INTEGER NOT NULL,
        periode_tahun INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(nomen, pay_dt, periode_bulan, periode_tahun)
    )
    """)
    
    cursor.execute("CREATE INDEX idx_coll_nomen ON collection_harian(nomen)")
    cursor.execute("CREATE INDEX idx_coll_periode ON collection_harian(periode_bulan, periode_tahun)")
    cursor.execute("CREATE INDEX idx_coll_pay_dt ON collection_harian(pay_dt)")
    print("   ✓ collection_harian created with 3 indexes")
    
    # 4. Mainbill
    print("\n4. Creating mainbill table...")
    cursor.execute("""
    CREATE TABLE mainbill (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nomen TEXT NOT NULL,
        freeze_dt TEXT,
        tagihan REAL DEFAULT 0,
        cycle INTEGER,
        periode_bulan INTEGER NOT NULL,
        periode_tahun INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(nomen, freeze_dt, periode_bulan, periode_tahun)
    )
    """)
    
    cursor.execute("CREATE INDEX idx_mainbill_nomen ON mainbill(nomen)")
    cursor.execute("CREATE INDEX idx_mainbill_periode ON mainbill(periode_bulan, periode_tahun)")
    cursor.execute("CREATE INDEX idx_mainbill_cycle ON mainbill(cycle)")
    print("   ✓ mainbill created with 3 indexes")
    
    # 5. SBRS
    print("\n5. Creating sbrs table...")
    cursor.execute("""
    CREATE TABLE sbrs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        rayon TEXT NOT NULL,
        cmr_rd_date TEXT,
        total_pelanggan INTEGER DEFAULT 0,
        sudah_bayar INTEGER DEFAULT 0,
        belum_bayar INTEGER DEFAULT 0,
        persen_bayar REAL DEFAULT 0,
        total_tagihan REAL DEFAULT 0,
        total_collection REAL DEFAULT 0,
        achievement REAL DEFAULT 0,
        periode_bulan INTEGER NOT NULL,
        periode_tahun INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(rayon, periode_bulan, periode_tahun)
    )
    """)
    
    cursor.execute("CREATE INDEX idx_sbrs_rayon ON sbrs(rayon)")
    cursor.execute("CREATE INDEX idx_sbrs_periode ON sbrs(periode_bulan, periode_tahun)")
    print("   ✓ sbrs created with 2 indexes")
    
    # 6. Ardebt (UNIVERSAL - NO PERIODE)
    print("\n6. Creating ardebt table (UNIVERSAL)...")
    cursor.execute("""
    CREATE TABLE ardebt (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nomen TEXT NOT NULL UNIQUE,
        nama TEXT,
        total_piutang REAL DEFAULT 0,
        periode_bill TEXT,
        umur_piutang INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    cursor.execute("CREATE INDEX idx_ardebt_nomen ON ardebt(nomen)")
    cursor.execute("CREATE INDEX idx_ardebt_periode_bill ON ardebt(periode_bill)")
    print("   ✓ ardebt created with 2 indexes")
    
    # 7. Upload History
    print("\n7. Creating upload_history table...")
    cursor.execute("""
    CREATE TABLE upload_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        filename TEXT NOT NULL,
        file_type TEXT NOT NULL,
        periode_bulan INTEGER,
        periode_tahun INTEGER,
        rows_processed INTEGER DEFAULT 0,
        status TEXT DEFAULT 'success',
        error_message TEXT,
        uploaded_by TEXT,
        uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    cursor.execute("CREATE INDEX idx_upload_file_type ON upload_history(file_type)")
    cursor.execute("CREATE INDEX idx_upload_periode ON upload_history(periode_bulan, periode_tahun)")
    cursor.execute("CREATE INDEX idx_upload_at ON upload_history(uploaded_at)")
    print("   ✓ upload_history created with 3 indexes")
    
    # Commit
    db.commit()
    
    # Verify
    print("\n" + "="*60)
    print("Verifying tables...")
    print("="*60)
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = cursor.fetchall()
    
    print(f"\n✅ Created {len(tables)} tables:")
    for table in tables:
        if table[0] != 'sqlite_sequence':
            cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
            count = cursor.fetchone()[0]
            print(f"   ✓ {table[0]:25} ({count} rows)")
    
    db.close()
    
    # File info
    file_size = os.path.getsize(db_path)
    print("\n" + "="*60)
    print("✅ SUCCESS!")
    print("="*60)
    print(f"Location: {db_path}")
    print(f"Size: {file_size:,} bytes ({file_size/1024:.1f} KB)")
    print("\nDatabase ready to use!")
    print("="*60)

if __name__ == '__main__':
    try:
        create_database()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
