"""
Collection API Endpoints
Handles collection analytics and reporting
"""

from flask import jsonify, request
from datetime import datetime

def register_collection_routes(app, get_db):
    """Register collection routes"""
    
    @app.route('/api/collection/daily')
    def collection_daily():
        """Get daily collection data"""
        try:
            periode_bulan = request.args.get('bulan', type=int)
            periode_tahun = request.args.get('tahun', type=int)
            
            if not periode_bulan or not periode_tahun:
                return jsonify({'error': 'Missing bulan or tahun'}), 400
            
            db = get_db()
            cursor = db.cursor()
            
            cursor.execute('''
                SELECT 
                    tgl_bayar,
                    COUNT(DISTINCT nomen) as jumlah_transaksi,
                    SUM(CASE WHEN tipe_bayar = 'current' THEN jumlah_bayar ELSE 0 END) as current,
                    SUM(CASE WHEN tipe_bayar = 'tunggakan' THEN jumlah_bayar ELSE 0 END) as tunggakan,
                    SUM(jumlah_bayar) as total
                FROM collection_harian
                WHERE periode_bulan = ? AND periode_tahun = ?
                GROUP BY tgl_bayar
                ORDER BY tgl_bayar
            ''', (periode_bulan, periode_tahun))
            
            rows = cursor.fetchall()
            
            data = []
            for row in rows:
                data.append({
                    'date': row['tgl_bayar'],
                    'transactions': row['jumlah_transaksi'],
                    'current': row['current'],
                    'tunggakan': row['tunggakan'],
                    'total': row['total']
                })
            
            return jsonify(data)
            
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/collection/by-rayon')
    def collection_by_rayon():
        """Get collection grouped by rayon"""
        try:
            periode_bulan = request.args.get('bulan', type=int)
            periode_tahun = request.args.get('tahun', type=int)
            
            if not periode_bulan or not periode_tahun:
                return jsonify({'error': 'Missing bulan or tahun'}), 400
            
            db = get_db()
            cursor = db.cursor()
            
            cursor.execute('''
                SELECT 
                    m.rayon,
                    COUNT(DISTINCT c.nomen) as jumlah_pelanggan,
                    SUM(c.jumlah_bayar) as total_collection,
                    SUM(m.target_mc) as total_target,
                    ROUND(SUM(c.jumlah_bayar) * 100.0 / NULLIF(SUM(m.target_mc), 0), 2) as percentage
                FROM collection_harian c
                JOIN master_pelanggan m ON c.nomen = m.nomen
                WHERE c.periode_bulan = ? AND c.periode_tahun = ?
                AND m.periode_bulan = ? AND m.periode_tahun = ?
                GROUP BY m.rayon
                ORDER BY m.rayon
            ''', (periode_bulan, periode_tahun, periode_bulan, periode_tahun))
            
            rows = cursor.fetchall()
            
            data = []
            for row in rows:
                data.append({
                    'rayon': row['rayon'],
                    'pelanggan': row['jumlah_pelanggan'],
                    'collection': row['total_collection'],
                    'target': row['total_target'],
                    'percentage': row['percentage'] or 0
                })
            
            return jsonify(data)
            
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/collection/top-payers')
    def collection_top_payers():
        """Get top 10 payers"""
        try:
            periode_bulan = request.args.get('bulan', type=int)
            periode_tahun = request.args.get('tahun', type=int)
            limit = request.args.get('limit', 10, type=int)
            
            if not periode_bulan or not periode_tahun:
                return jsonify({'error': 'Missing bulan or tahun'}), 400
            
            db = get_db()
            cursor = db.cursor()
            
            cursor.execute('''
                SELECT 
                    c.nomen,
                    m.nama,
                    m.rayon,
                    SUM(c.jumlah_bayar) as total_bayar,
                    COUNT(*) as jumlah_transaksi
                FROM collection_harian c
                JOIN master_pelanggan m ON c.nomen = m.nomen
                WHERE c.periode_bulan = ? AND c.periode_tahun = ?
                GROUP BY c.nomen, m.nama, m.rayon
                ORDER BY total_bayar DESC
                LIMIT ?
            ''', (periode_bulan, periode_tahun, limit))
            
            rows = cursor.fetchall()
            
            data = []
            for row in rows:
                data.append({
                    'nomen': row['nomen'],
                    'nama': row['nama'],
                    'rayon': row['rayon'],
                    'total': row['total_bayar'],
                    'transactions': row['jumlah_transaksi']
                })
            
            return jsonify(data)
            
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    print("✅ Collection routes registered")
