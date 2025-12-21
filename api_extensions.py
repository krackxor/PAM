# API ENDPOINTS BARU UNTUK FITUR TAMBAHAN
# Tambahkan ke app.py

# ==========================================
# API: METER - ANOMALI DETECTION
# ==========================================

@app.route('/api/meter_anomali')
def api_meter_anomali():
    """Deteksi anomali pencatatan meter"""
    db = get_db()
    
    try:
        # 1. Pemakaian Extreme (> 200% dari rata-rata)
        extreme = db.execute('''
            SELECT m.nomen, m.nama, m.kubikasi, m.rayon
            FROM master_pelanggan m
            WHERE m.kubikasi > (SELECT AVG(kubikasi) * 2 FROM master_pelanggan WHERE rayon IN ('34', '35'))
            AND m.rayon IN ('34', '35')
            ORDER BY m.kubikasi DESC
            LIMIT 100
        ''').fetchall()
        
        # 2. Zero Usage (kubikasi = 0)
        zero_usage = db.execute('''
            SELECT m.nomen, m.nama, m.alamat, m.rayon
            FROM master_pelanggan m
            WHERE m.kubikasi = 0
            AND m.rayon IN ('34', '35')
            ORDER BY m.nomen
            LIMIT 100
        ''').fetchall()
        
        # 3. Pemakaian Turun (TODO: butuh history data)
        # Untuk sekarang, placeholder
        
        result = {
            'extreme': [dict(row) for row in extreme],
            'zero_usage': [dict(row) for row in zero_usage],
            'extreme_count': len(extreme),
            'zero_count': len(zero_usage),
            'turun_count': 0,  # Placeholder
            'stand_negatif_count': 0,  # Placeholder
            'salah_catat_count': 0,  # Placeholder
            'rebill_count': 0,  # Placeholder
            'estimasi_count': 0  # Placeholder
        }
        
        return jsonify(result)
        
    except Exception as e:
        print(f"Error meter anomali: {e}")
        return jsonify({'error': str(e)}), 500


# ==========================================
# API: HISTORY - DATA KRONOLOGIS
# ==========================================

@app.route('/api/history_kubikasi')
def api_history_kubikasi():
    """History kubikasi per nomen (multi periode)"""
    nomen = request.args.get('nomen')
    
    if not nomen:
        return jsonify({'error': 'Parameter nomen required'}), 400
    
    db = get_db()
    
    try:
        # Untuk sekarang, ambil data dari MC (single periode)
        # TODO: Extend untuk multi-periode jika ada data history
        
        data = db.execute('''
            SELECT 
                nomen,
                nama,
                kubikasi,
                periode,
                updated_at
            FROM master_pelanggan
            WHERE nomen = ?
        ''', (nomen,)).fetchone()
        
        if data:
            return jsonify(dict(data))
        else:
            return jsonify({'error': 'Nomen not found'}), 404
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/history_pembayaran')
def api_history_pembayaran():
    """History pembayaran per nomen"""
    nomen = request.args.get('nomen')
    
    if not nomen:
        return jsonify({'error': 'Parameter nomen required'}), 400
    
    db = get_db()
    
    try:
        history = db.execute('''
            SELECT 
                tgl_bayar,
                jumlah_bayar,
                tipe_bayar,
                bill_period,
                sumber_file
            FROM collection_harian
            WHERE nomen = ?
            ORDER BY tgl_bayar DESC
        ''', (nomen,)).fetchall()
        
        return jsonify([dict(row) for row in history])
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ==========================================
# API: ANALISA MANUAL - CRUD
# ==========================================

@app.route('/api/analisa_list')
def api_analisa_list():
    """List semua analisa manual"""
    db = get_db()
    
    try:
        status_filter = request.args.get('status', 'all')
        
        if status_filter == 'all':
            analisa = db.execute('''
                SELECT 
                    a.id,
                    a.nomen,
                    m.nama,
                    m.rayon,
                    a.jenis_anomali,
                    a.status,
                    a.updated_at
                FROM analisa_manual a
                LEFT JOIN master_pelanggan m ON a.nomen = m.nomen
                ORDER BY a.updated_at DESC
            ''').fetchall()
        else:
            analisa = db.execute('''
                SELECT 
                    a.id,
                    a.nomen,
                    m.nama,
                    m.rayon,
                    a.jenis_anomali,
                    a.status,
                    a.updated_at
                FROM analisa_manual a
                LEFT JOIN master_pelanggan m ON a.nomen = m.nomen
                WHERE a.status = ?
                ORDER BY a.updated_at DESC
            ''', (status_filter,)).fetchall()
        
        return jsonify([dict(row) for row in analisa])
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/analisa_detail/<int:analisa_id>')
def api_analisa_detail(analisa_id):
    """Detail analisa manual"""
    db = get_db()
    
    try:
        analisa = db.execute('''
            SELECT 
                a.*,
                m.nama,
                m.alamat,
                m.rayon,
                m.kubikasi,
                m.target_mc
            FROM analisa_manual a
            LEFT JOIN master_pelanggan m ON a.nomen = m.nomen
            WHERE a.id = ?
        ''', (analisa_id,)).fetchone()
        
        if analisa:
            return jsonify(dict(analisa))
        else:
            return jsonify({'error': 'Analisa not found'}), 404
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/analisa_save', methods=['POST'])
def api_analisa_save():
    """Simpan atau update analisa manual"""
    db = get_db()
    
    try:
        data = request.json
        analisa_id = data.get('id')
        
        if analisa_id:
            # Update existing
            db.execute('''
                UPDATE analisa_manual
                SET jenis_anomali = ?,
                    analisa_tim = ?,
                    kesimpulan = ?,
                    rekomendasi = ?,
                    status = ?,
                    user_editor = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (
                data.get('jenis_anomali'),
                data.get('analisa_tim'),
                data.get('kesimpulan'),
                data.get('rekomendasi'),
                data.get('status'),
                data.get('user_editor', 'Admin'),
                analisa_id
            ))
            db.commit()
            return jsonify({'success': True, 'id': analisa_id})
        else:
            # Insert new
            cursor = db.execute('''
                INSERT INTO analisa_manual 
                (nomen, jenis_anomali, analisa_tim, kesimpulan, rekomendasi, status, user_editor)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                data.get('nomen'),
                data.get('jenis_anomali'),
                data.get('analisa_tim'),
                data.get('kesimpulan'),
                data.get('rekomendasi'),
                data.get('status', 'Open'),
                data.get('user_editor', 'Admin')
            ))
            db.commit()
            return jsonify({'success': True, 'id': cursor.lastrowid})
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/profil_pelanggan/<nomen>')
def api_profil_pelanggan(nomen):
    """Profil lengkap pelanggan untuk analisa"""
    db = get_db()
    
    try:
        # Data master
        master = db.execute('''
            SELECT * FROM master_pelanggan WHERE nomen = ?
        ''', (nomen,)).fetchone()
        
        if not master:
            return jsonify({'error': 'Nomen not found'}), 404
        
        # History pembayaran
        payments = db.execute('''
            SELECT tgl_bayar, jumlah_bayar, tipe_bayar
            FROM collection_harian
            WHERE nomen = ?
            ORDER BY tgl_bayar DESC
            LIMIT 12
        ''', (nomen,)).fetchall()
        
        # Analisa terkait
        analisa = db.execute('''
            SELECT id, jenis_anomali, status, updated_at
            FROM analisa_manual
            WHERE nomen = ?
            ORDER BY updated_at DESC
        ''', (nomen,)).fetchall()
        
        result = {
            'master': dict(master),
            'payments': [dict(p) for p in payments],
            'analisa': [dict(a) for a in analisa],
            'payment_count': len(payments),
            'analisa_count': len(analisa)
        }
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
