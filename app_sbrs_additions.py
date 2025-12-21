# ========================================
# TAMBAHAN KODE UNTUK APP.PY
# Sisipkan di bagian yang sesuai
# ========================================

# ========================================
# 1. INIT DATABASE - Tambah tabel baru
# ========================================
def init_db():
    """Initialize database dengan tabel tambahan untuk SBRS dan tracking periode"""
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    
    # Tabel existing...
    # (copy dari app.py yang ada)
    
    # TABEL BARU: Upload Metadata
    conn.execute('''
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
    
    # TABEL BARU: SBRS Data
    conn.execute('''
        CREATE TABLE IF NOT EXISTS sbrs_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nomen TEXT NOT NULL,
            nama TEXT,
            alamat TEXT,
            rayon TEXT,
            
            -- Data Pembacaan
            readmethod TEXT,
            skip_status TEXT,
            trouble_status TEXT,
            spm_status TEXT,
            
            -- Stand & Volume
            stand_awal REAL,
            stand_akhir REAL,
            volume REAL,
            
            -- Analisa
            analisa_tindak_lanjut TEXT,
            tag1 TEXT,
            tag2 TEXT,
            
            -- Metadata
            periode_bulan INTEGER NOT NULL,
            periode_tahun INTEGER NOT NULL,
            upload_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            FOREIGN KEY (upload_id) REFERENCES upload_metadata(id)
        )
    ''')
    
    # Update tabel existing - tambah kolom periode
    # Cek dulu apakah kolom sudah ada
    try:
        conn.execute('ALTER TABLE master_pelanggan ADD COLUMN periode_bulan INTEGER')
        conn.execute('ALTER TABLE master_pelanggan ADD COLUMN periode_tahun INTEGER')
        conn.execute('ALTER TABLE master_pelanggan ADD COLUMN upload_id INTEGER')
    except sqlite3.OperationalError:
        pass  # Kolom sudah ada
    
    try:
        conn.execute('ALTER TABLE collection_harian ADD COLUMN periode_bulan INTEGER')
        conn.execute('ALTER TABLE collection_harian ADD COLUMN periode_tahun INTEGER')
        conn.execute('ALTER TABLE collection_harian ADD COLUMN upload_id INTEGER')
    except sqlite3.OperationalError:
        pass
    
    conn.commit()
    conn.close()


# ========================================
# 2. ROUTE UPLOAD - Tambah parameter bulan/tahun
# ========================================
@app.route('/upload', methods=['POST'])
def upload_file():
    """Upload file dengan metadata bulan/tahun"""
    
    if 'file' not in request.files:
        flash('Tidak ada file yang dipilih', 'danger')
        return redirect(url_for('index'))
    
    file = request.files['file']
    file_type = request.form.get('file_type')
    periode_bulan = int(request.form.get('periode_bulan', 0))
    periode_tahun = int(request.form.get('periode_tahun', 0))
    
    if not periode_bulan or not periode_tahun:
        flash('❌ Pilih bulan dan tahun terlebih dahulu!', 'danger')
        return redirect(url_for('index'))
    
    if file.filename == '':
        flash('Nama file kosong', 'danger')
        return redirect(url_for('index'))
    
    # Save file
    filename = secure_filename(file.filename)
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    file.save(filepath)
    
    # Simpan metadata upload
    conn = get_db()
    cursor = conn.execute('''
        INSERT INTO upload_metadata (file_type, file_name, periode_bulan, periode_tahun, row_count)
        VALUES (?, ?, ?, ?, 0)
    ''', (file_type, filename, periode_bulan, periode_tahun))
    upload_id = cursor.lastrowid
    conn.commit()
    
    # Process berdasarkan tipe
    if file_type == 'SBRS':
        return process_sbrs(filepath, upload_id, periode_bulan, periode_tahun)
    elif file_type == 'MC':
        return process_mc_with_periode(filepath, upload_id, periode_bulan, periode_tahun)
    # dst...


# ========================================
# 3. PROCESS SBRS - Handler baru
# ========================================
def process_sbrs(filepath, upload_id, periode_bulan, periode_tahun):
    """Process file SBRS dengan multi-bulan"""
    
    try:
        # Baca file SBRS
        # Coba berbagai engine
        df = None
        for engine in ['xlrd', 'openpyxl', None]:
            try:
                df = pd.read_excel(filepath, engine=engine)
                print(f"✅ SBRS dibaca dengan engine: {engine}")
                break
            except:
                continue
        
        if df is None:
            flash('❌ Tidak bisa membaca file SBRS. Install xlrd atau convert ke .xlsx', 'danger')
            return redirect(url_for('index'))
        
        print(f"📊 SBRS: {len(df)} rows, {len(df.columns)} columns")
        
        # Mapping kolom SBRS
        kolom_map = {
            'cmr_account': 'nomen',
            'cmr_name': 'nama',
            'cmr_address': 'alamat'
        }
        
        df = df.rename(columns=kolom_map)
        
        # Filter rayon 34/35 (dari PC atau parsing alamat)
        # Asumsi PC column ada
        if 'PC' in df.columns:
            # PC 98 = rayon 34, PC 96/92 = rayon 35 (example, adjust accordingly)
            df['rayon'] = df['PC'].apply(lambda x: '34' if x == 98 else ('35' if x in [96, 92] else None))
            df = df[df['rayon'].notna()]
        
        # Tentukan bulan yang akan diproses
        # Jika periode_bulan = 12 (Desember), cari kolom _des25
        bulan_code = {
            10: 'okt', 11: 'nov', 12: 'des',
            1: 'jan', 2: 'feb', 3: 'mar',
            4: 'apr', 5: 'mei', 6: 'jun',
            7: 'jul', 8: 'ags', 9: 'sep'
        }
        
        month_suffix = bulan_code.get(periode_bulan, 'des')
        year_suffix = str(periode_tahun)[-2:]  # '25' dari 2025
        
        col_suffix = f'_{month_suffix}{year_suffix}'
        
        # Extract data untuk bulan yang dipilih
        records = []
        for _, row in df.iterrows():
            record = {
                'nomen': str(row.get('nomen', '')).strip(),
                'nama': row.get('nama'),
                'alamat': row.get('alamat'),
                'rayon': row.get('rayon'),
                'readmethod': row.get(f'readmethod{col_suffix}'),
                'skip_status': row.get(f'skip{col_suffix}'),
                'trouble_status': row.get(f'trouble{col_suffix}'),
                'spm_status': row.get(f'spm{col_suffix}'),
                'stand_akhir': row.get(f'sbstand{col_suffix}'),
                'volume': row.get(f'vol{col_suffix}'),
                'analisa_tindak_lanjut': row.get(f'Analisa/Tindak Lanjut{col_suffix}'),
                'tag1': row.get('Tag1'),
                'tag2': row.get('Tag2'),
                'periode_bulan': periode_bulan,
                'periode_tahun': periode_tahun,
                'upload_id': upload_id
            }
            records.append(record)
        
        # Insert ke database
        conn = get_db()
        
        for rec in records:
            if not rec['nomen'] or rec['nomen'] == 'nan':
                continue
            
            conn.execute('''
                INSERT INTO sbrs_data (
                    nomen, nama, alamat, rayon,
                    readmethod, skip_status, trouble_status, spm_status,
                    stand_akhir, volume,
                    analisa_tindak_lanjut, tag1, tag2,
                    periode_bulan, periode_tahun, upload_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                rec['nomen'], rec['nama'], rec['alamat'], rec['rayon'],
                rec['readmethod'], rec['skip_status'], rec['trouble_status'], rec['spm_status'],
                rec['stand_akhir'], rec['volume'],
                rec['analisa_tindak_lanjut'], rec['tag1'], rec['tag2'],
                rec['periode_bulan'], rec['periode_tahun'], rec['upload_id']
            ))
        
        # Update row_count di metadata
        conn.execute('UPDATE upload_metadata SET row_count = ? WHERE id = ?', (len(records), upload_id))
        
        conn.commit()
        
        flash(f'✅ SBRS ({bulan_code[periode_bulan].upper()} {periode_tahun}): {len(records):,} data berhasil diupload!', 'success')
        return redirect(url_for('index'))
        
    except Exception as e:
        print(f"❌ Error process SBRS: {e}")
        import traceback
        traceback.print_exc()
        flash(f'❌ Error: {str(e)}', 'danger')
        return redirect(url_for('index'))


# ========================================
# 4. API BARU - Anomali dari SBRS
# ========================================
@app.route('/api/sbrs_anomali')
def api_sbrs_anomali():
    """Deteksi anomali dari data SBRS"""
    db = get_db()
    periode_bulan = request.args.get('bulan', type=int)
    periode_tahun = request.args.get('tahun', type=int)
    
    where_clause = ""
    params = []
    
    if periode_bulan and periode_tahun:
        where_clause = "WHERE periode_bulan = ? AND periode_tahun = ?"
        params = [periode_bulan, periode_tahun]
    
    try:
        # Skip
        skip = db.execute(f'''
            SELECT nomen, nama, rayon, skip_status, readmethod
            FROM sbrs_data
            {where_clause} AND skip_status IS NOT NULL
            ORDER BY nomen
            LIMIT 100
        ''', params).fetchall()
        
        # Trouble
        trouble = db.execute(f'''
            SELECT nomen, nama, rayon, trouble_status, spm_status
            FROM sbrs_data
            {where_clause} AND trouble_status IS NOT NULL
            ORDER BY nomen
            LIMIT 100
        ''', params).fetchall()
        
        # Photo Entry
        photo = db.execute(f'''
            SELECT nomen, nama, rayon, readmethod
            FROM sbrs_data
            {where_clause} AND readmethod = 'PE'
            ORDER BY nomen
            LIMIT 100
        ''', params).fetchall()
        
        # Data Lama
        data_lama = db.execute(f'''
            SELECT nomen, nama, rayon, readmethod
            FROM sbrs_data
            {where_clause} AND readmethod = 'DL'
            ORDER BY nomen
            LIMIT 100
        ''', params).fetchall()
        
        result = {
            'skip': [dict(row) for row in skip],
            'trouble': [dict(row) for row in trouble],
            'photo_entry': [dict(row) for row in photo],
            'data_lama': [dict(row) for row in data_lama],
            'skip_count': len(skip),
            'trouble_count': len(trouble),
            'photo_count': len(photo),
            'data_lama_count': len(data_lama)
        }
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ========================================
# 5. API - History Multi Periode
# ========================================
@app.route('/api/history_multi_periode/<nomen>')
def api_history_multi_periode(nomen):
    """History kubikasi dan SBRS multi periode"""
    db = get_db()
    
    try:
        # History MC
        mc_history = db.execute('''
            SELECT 
                periode_bulan,
                periode_tahun,
                kubikasi,
                target_mc
            FROM master_pelanggan
            WHERE nomen = ?
            ORDER BY periode_tahun DESC, periode_bulan DESC
            LIMIT 12
        ''', (nomen,)).fetchall()
        
        # History SBRS
        sbrs_history = db.execute('''
            SELECT 
                periode_bulan,
                periode_tahun,
                readmethod,
                skip_status,
                trouble_status,
                volume,
                analisa_tindak_lanjut
            FROM sbrs_data
            WHERE nomen = ?
            ORDER BY periode_tahun DESC, periode_bulan DESC
            LIMIT 12
        ''', (nomen,)).fetchall()
        
        result = {
            'mc_history': [dict(row) for row in mc_history],
            'sbrs_history': [dict(row) for row in sbrs_history]
        }
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ========================================
# 6. API - Profil Gabungan MC + SBRS
# ========================================
@app.route('/api/profil_lengkap/<nomen>')
def api_profil_lengkap(nomen):
    """Profil lengkap: MC + SBRS + Collection + Analisa"""
    db = get_db()
    periode_bulan = request.args.get('bulan', type=int, default=12)
    periode_tahun = request.args.get('tahun', type=int, default=2025)
    
    try:
        # Data MC
        mc = db.execute('''
            SELECT * FROM master_pelanggan
            WHERE nomen = ? AND periode_bulan = ? AND periode_tahun = ?
        ''', (nomen, periode_bulan, periode_tahun)).fetchone()
        
        # Data SBRS
        sbrs = db.execute('''
            SELECT * FROM sbrs_data
            WHERE nomen = ? AND periode_bulan = ? AND periode_tahun = ?
        ''', (nomen, periode_bulan, periode_tahun)).fetchone()
        
        # Collection
        collection = db.execute('''
            SELECT * FROM collection_harian
            WHERE nomen = ?
            ORDER BY tgl_bayar DESC
            LIMIT 10
        ''', (nomen,)).fetchall()
        
        # Analisa Manual
        analisa = db.execute('''
            SELECT * FROM analisa_manual
            WHERE nomen = ?
            ORDER BY updated_at DESC
        ''', (nomen,)).fetchall()
        
        result = {
            'mc': dict(mc) if mc else None,
            'sbrs': dict(sbrs) if sbrs else None,
            'collection': [dict(c) for c in collection],
            'analisa': [dict(a) for a in analisa]
        }
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
