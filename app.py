from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
import os
import sqlite3
from datetime import datetime

# Import modul buatan sendiri
from config import UPLOAD_FOLDER
from database import init_db, get_db, close_db
from processor import process_file

app = Flask(__name__)
app.secret_key = 'sunter_dashboard_secret_key' # Diperlukan untuk flash message

# 1. SETUP DATABASE
# Menutup koneksi database otomatis saat request selesai
app.teardown_appcontext(close_db)

# Inisialisasi database (Buat tabel) saat aplikasi pertama kali dijalankan
init_db(app)

# --- ROUTES (ALUR WEBSITE) ---

@app.route('/')
def index():
    """
    Halaman Utama Dashboard (Single Page).
    Mengambil data ringkasan dan grafik untuk ditampilkan.
    """
    db = get_db()
    
    # --- A. QUERY STATISTIK UTAMA (RINGKASAN) ---
    
    # 1. Ambil Data Master (Target MC & Saldo Ardebt & Jumlah Pelanggan)
    # Kita menggunakan COALESCE agar jika data kosong, hasilnya 0 (bukan None)
    master_query = db.execute('''
        SELECT 
            COUNT(nomen) as total_cust, 
            SUM(target_mc) as total_mc, 
            SUM(saldo_ardebt) as total_debt 
        FROM master_pelanggan
    ''').fetchone()
    
    # 2. Ambil Realisasi Collection (Total uang masuk dari semua file collection)
    coll_query = db.execute('''
        SELECT SUM(jumlah_bayar) as total_coll 
        FROM collection_harian
    ''').fetchone()

    # Bersihkan nilai (Handle jika None)
    total_pelanggan = master_query['total_cust'] or 0
    target_mc = master_query['total_mc'] or 0
    saldo_ardebt = master_query['total_debt'] or 0
    realisasi_coll = coll_query['total_coll'] or 0
    
    # Hitung Persentase (Collection Rate)
    # Hindari pembagian dengan nol
    if target_mc > 0:
        coll_rate = (realisasi_coll / target_mc) * 100
    else:
        coll_rate = 0

    # Bungkus dalam dictionary 'stats' untuk dikirim ke HTML
    stats = {
        'total_pelanggan': total_pelanggan,
        'target_mc': target_mc,
        'saldo_ardebt': saldo_ardebt,
        'realisasi_coll': realisasi_coll,
        'coll_rate': coll_rate
    }

    # --- B. QUERY UNTUK GRAFIK & TABEL (Collection Harian) ---
    
    # Mengambil data collection dikelompokkan per tanggal
    daily_chart_query = db.execute('''
        SELECT 
            tgl_bayar, 
            SUM(jumlah_bayar) as harian
        FROM collection_harian 
        GROUP BY tgl_bayar 
        ORDER BY tgl_bayar ASC
    ''').fetchall()

    # Format data agar mudah dibaca Chart.js & Tabel HTML
    chart_data = []
    kumulatif = 0 # Variabel bantu untuk hitung kumulatif
    
    for row in daily_chart_query:
        harian = row['harian']
        kumulatif += harian # Tambahkan harian ke total berjalan
        
        # Hitung % progress harian terhadap target MC
        persen_progress = 0
        if target_mc > 0:
            persen_progress = (kumulatif / target_mc) * 100

        chart_data.append({
            'tgl': row['tgl_bayar'],
            'total': harian,          # Untuk grafik garis/batang harian
            'kumulatif': kumulatif,   # Untuk kolom kumulatif di tabel
            'progress': persen_progress # Untuk kolom % di tabel
        })

    # Render Template HTML dengan membawa data stats & chart
    return render_template('dashboard.html', stats=stats, chart_data=chart_data)

@app.route('/upload', methods=['POST'])
def upload_file():
    """
    Route khusus untuk menerima file Upload dari Modal di Dashboard.
    """
    # 1. Cek apakah ada file yang dikirim
    if 'file' not in request.files:
        flash('Tidak ada file yang dipilih', 'error')
        return redirect(url_for('index'))
    
    file = request.files['file']
    jenis_file = request.form.get('jenis_file') # MC, COLLECTION, ARDEBT, dll
    
    # 2. Cek nama file
    if file.filename == '':
        flash('Nama file kosong', 'error')
        return redirect(url_for('index'))

    # 3. Simpan & Proses
    if file:
        # Pastikan folder upload ada
        if not os.path.exists(UPLOAD_FOLDER):
            os.makedirs(UPLOAD_FOLDER)
            
        filepath = os.path.join(UPLOAD_FOLDER, file.filename)
        file.save(filepath)
        
        try:
            # Panggil fungsi 'otak' di processor.py
            process_file(filepath, jenis_file)
            flash(f'Berhasil memproses file {jenis_file}!', 'success')
        except Exception as e:
            flash(f'Gagal memproses file: {str(e)}', 'error')
            print(f"Error: {e}")
        
        return redirect(url_for('index'))

@app.route('/api/analisa', methods=['POST'])
def simpan_analisa():
    """
    (Opsional) Endpoint API jika nanti ingin menyimpan Analisa Manual
    tanpa reload halaman (AJAX).
    """
    data = request.json
    db = get_db()
    try:
        db.execute('''
            INSERT INTO analisa_manual (nomen, jenis_anomali, catatan, rekomendasi, status)
            VALUES (?, ?, ?, ?, ?)
        ''', (data['nomen'], data['jenis'], data['catatan'], data['rekomendasi'], 'Open'))
        db.commit()
        return jsonify({'status': 'success', 'message': 'Analisa tersimpan'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

# Jalankan Aplikasi
if __name__ == '__main__':
    app.run(debug=True, port=5000)
