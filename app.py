import os
import sqlite3
import pandas as pd
from flask import Flask, render_template, g, request, session, redirect, url_for, flash, jsonify
from werkzeug.utils import secure_filename
from datetime import datetime

# --- KONFIGURASI ---
app = Flask(__name__)
# Secret key tetap ada biar flash message jalan, tapi tidak dipakai untuk login
app.config['SECRET_KEY'] = 'DEV-MODE-NO-LOGIN' 
app.config['UPLOAD_FOLDER'] = 'uploads'
DB_FOLDER = os.path.join(os.getcwd(), 'database')
DB_PATH = os.path.join(DB_FOLDER, 'sunter.db')

# Buat folder sistem
for folder in [app.config['UPLOAD_FOLDER'], DB_FOLDER]:
    if not os.path.exists(folder):
        os.makedirs(folder)

# --- DATABASE ENGINE ---
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
    with app.app_context():
        db = get_db()
        cursor = db.cursor()
        
        # 1. Master Pelanggan
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS master_pelanggan (
                nomen TEXT PRIMARY KEY,
                nama TEXT,
                rayon TEXT,
                tarif TEXT,
                target_mc REAL DEFAULT 0
            )
        ''')
        
        # 2. Collection Harian
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS collection_harian (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nomen TEXT,
                tgl_bayar TEXT,
                jumlah_bayar REAL,
                sumber_file TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # 3. Analisa Manual (CRUD)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS analisa_manual (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nomen TEXT,
                jenis_anomali TEXT,
                analisa_tim TEXT,
                kesimpulan TEXT,
                rekomendasi TEXT,
                status TEXT DEFAULT 'Open',
                user_editor TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # 4. Audit Trail
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user TEXT,
                aktivitas TEXT,
                waktu TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        db.commit()

def catat_audit(user, aktivitas):
    db = get_db()
    db.execute('INSERT INTO audit_log (user, aktivitas) VALUES (?, ?)', (user, aktivitas))
    db.commit()

# --- ROUTING UTAMA (LANGSUNG DASHBOARD) ---

@app.route('/')
def index():
    # TIDAK ADA CEK LOGIN LAGI
    
    db = get_db()
    # KPI GLOBAL
    kpi = {}
    try:
        kpi['cust'] = db.execute('SELECT COUNT(*) as t FROM master_pelanggan').fetchone()['t']
        kpi['coll'] = db.execute('SELECT SUM(jumlah_bayar) as t FROM collection_harian').fetchone()['t'] or 0
        kpi['anomali'] = db.execute('SELECT COUNT(*) as t FROM analisa_manual WHERE status != "Closed"').fetchone()['t']
    except:
        kpi = {'cust': 0, 'coll': 0, 'anomali': 0}
    
    return render_template('index.html', kpi=kpi)

# Jika user iseng buka /login, lempar balik ke dashboard
@app.route('/login')
def login():
    return redirect(url_for('index'))

@app.route('/logout')
def logout():
    return redirect(url_for('index'))
