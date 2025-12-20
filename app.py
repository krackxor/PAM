import os
import sqlite3
from flask import Flask, render_template, g

# --- KONFIGURASI ---
app = Flask(__name__)
app.config['SECRET_KEY'] = 'kunci-rahasia-sunter-dashboard-123' # Ganti nanti
DB_FOLDER = os.path.join(os.getcwd(), 'database')
DB_PATH = os.path.join(DB_FOLDER, 'sunter.db')

# Pastikan folder database ada
if not os.path.exists(DB_FOLDER):
    os.makedirs(DB_FOLDER)

# --- FUNGSI DATABASE (SQLite) ---
def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row 
    return db

@app.teardown_appcontext
def close_db(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

def init_db():
    """Membuat tabel jika belum ada (Auto-setup saat pertama kali run)"""
    with app.app_context():
        db = get_db()
        cursor = db.cursor()
        
        # 1. Tabel Master Pelanggan
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS master_pelanggan (
                nomen TEXT PRIMARY KEY,
                nama TEXT,
                rayon TEXT,
                tarif TEXT,
                target_mc REAL DEFAULT 0,
                saldo_ardebt REAL DEFAULT 0
            )
        ''')
        
        # 2. Tabel Collection
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS collection_harian (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nomen TEXT,
                tgl_bayar TEXT,
                jumlah_bayar REAL,
                FOREIGN KEY(nomen) REFERENCES master_pelanggan(nomen)
            )
        ''')

        # 3. Tabel Analisa Manual (Penting untuk user)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS analisa_manual (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nomen TEXT,
                jenis_anomali TEXT,
                catatan TEXT,
                status TEXT DEFAULT 'Open',
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        db.commit()
        print(f"✅ Database siap di: {DB_PATH}")

# --- ROUTING (HALAMAN) ---

@app.route('/')
def index():
    # Contoh query data ringkasan untuk Dashboard
    db = get_db()
    
    # Hitung Total Pelanggan
    cust_count = db.execute('SELECT COUNT(*) as total FROM master_pelanggan').fetchone()['total']
    
    # Hitung Total Collection (Gabungan Rayon 34 & 35 / SUNTER)
    total_coll = db.execute('SELECT SUM(jumlah_bayar) as total FROM collection_harian').fetchone()['total'] or 0
    
    return render_template('index.html', cust_count=cust_count, total_coll=total_coll)

@app.route('/collection')
def collection():
    return "Halaman Collection (Excel Style)"

# --- MAIN EXECUTION ---
if __name__ == '__main__':
    # Cek & Buat DB saat aplikasi start
    if not os.path.exists(DB_PATH):
        print("⚡ Database belum ada, menginisialisasi...")
        init_db()
    
    print("🚀 Sistem SUNTER DASHBOARD Berjalan...")
    print("👉 Buka di browser: http://localhost:5000")
    
    # Debug=True agar auto-reload saat edit coding
    app.run(host='0.0.0.0', port=5000, debug=True)
