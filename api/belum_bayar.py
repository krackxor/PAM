"""
Belum Bayar API Endpoints
Handles unpaid customers data
"""

from flask import jsonify, request

def register_belum_bayar_routes(app, get_db):
    """Register belum bayar routes"""
    
    @app.route('/api/belum-bayar/list')
    def get_belum_bayar_list():
        """Get list of customers who haven't paid"""
        try:
            periode_bulan = request.args.get('bulan', type=int)
            periode_tahun = request.args.get('tahun', type=int)
            
            if not periode_bulan or not periode_tahun:
                return jsonify({'error': 'Missing bulan or tahun'}), 400
            
            db = get_db()
            cursor = db.cursor()
            
            # Get all customers from master_pelanggan
            cursor.execute('''
                SELECT 
                    m.nomen,
                    m.nama,
                    m.alamat,
                    m.rayon,
                    m.target_mc as tagihan,
                    m.tarif
                FROM master_pelanggan m
                WHERE m.periode_bulan = ? AND m.periode_tahun = ?
                AND m.nomen NOT IN (
                    SELECT DISTINCT nomen 
                    FROM collection_harian 
                    WHERE periode_bulan = ? AND periode_tahun = ?
                )
                ORDER BY m.rayon, m.nomen
            ''', (periode_bulan, periode_tahun, periode_bulan, periode_tahun))
            
            rows = cursor.fetchall()
            
            data = []
            for row in rows:
                data.append({
                    'nomen': row['nomen'],
                    'nama': row['nama'],
                    'alamat': row['alamat'],
                    'rayon': row['rayon'],
                    'tagihan': row['tagihan'],
                    'tarif': row['tarif'],
                    'durasi_bulan': 1  # Default 1 bulan, bisa dikembangkan dengan history
                })
            
            return jsonify(data)
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/belum-bayar/summary')
    def get_belum_bayar_summary():
        """Get summary statistics of unpaid customers"""
        try:
            periode_bulan = request.args.get('bulan', type=int)
            periode_tahun = request.args.get('tahun', type=int)
            
            if not periode_bulan or not periode_tahun:
                return jsonify({'error': 'Missing bulan or tahun'}), 400
            
            db = get_db()
            cursor = db.cursor()
            
            # Total customers
            cursor.execute('''
                SELECT COUNT(*) as total
                FROM master_pelanggan
                WHERE periode_bulan = ? AND periode_tahun = ?
            ''', (periode_bulan, periode_tahun))
            
            total_customers = cursor.fetchone()['total']
            
            # Unpaid customers
            cursor.execute('''
                SELECT COUNT(*) as unpaid, SUM(target_mc) as unpaid_amount
                FROM master_pelanggan m
                WHERE m.periode_bulan = ? AND m.periode_tahun = ?
                AND m.nomen NOT IN (
                    SELECT DISTINCT nomen 
                    FROM collection_harian 
                    WHERE periode_bulan = ? AND periode_tahun = ?
                )
            ''', (periode_bulan, periode_tahun, periode_bulan, periode_tahun))
            
            unpaid_row = cursor.fetchone()
            unpaid_count = unpaid_row['unpaid'] or 0
            unpaid_amount = unpaid_row['unpaid_amount'] or 0
            
            # Calculate percentage
            unpaid_percentage = (unpaid_count / total_customers * 100) if total_customers > 0 else 0
            avg_unpaid = (unpaid_amount / unpaid_count) if unpaid_count > 0 else 0
            
            return jsonify({
                'total_customers': total_customers,
                'unpaid_count': unpaid_count,
                'unpaid_amount': unpaid_amount,
                'unpaid_percentage': round(unpaid_percentage, 2),
                'avg_unpaid': round(avg_unpaid, 2)
            })
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/belum-bayar/by-rayon')
    def get_belum_bayar_by_rayon():
        """Get unpaid customers grouped by rayon"""
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
                    COUNT(*) as count,
                    SUM(m.target_mc) as total_amount
                FROM master_pelanggan m
                WHERE m.periode_bulan = ? AND m.periode_tahun = ?
                AND m.nomen NOT IN (
                    SELECT DISTINCT nomen 
                    FROM collection_harian 
                    WHERE periode_bulan = ? AND periode_tahun = ?
                )
                GROUP BY m.rayon
                ORDER BY count DESC
            ''', (periode_bulan, periode_tahun, periode_bulan, periode_tahun))
            
            rows = cursor.fetchall()
            
            data = []
            for row in rows:
                data.append({
                    'rayon': row['rayon'],
                    'count': row['count'],
                    'total_amount': row['total_amount']
                })
            
            return jsonify(data)
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            return jsonify({'error': str(e)}), 500
    
    print("✅ Belum Bayar routes registered")
