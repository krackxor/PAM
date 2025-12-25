"""
KPI API Endpoints
Handles KPI calculation and retrieval
"""

from flask import jsonify, request

def register_kpi_routes(app, get_db):
    """Register KPI routes"""
    
    @app.route('/api/kpi')
    def get_kpi():
        """Get KPI data"""
        try:
            periode_bulan = request.args.get('bulan', type=int)
            periode_tahun = request.args.get('tahun', type=int)
            
            if not periode_bulan or not periode_tahun:
                return jsonify({'error': 'Missing bulan or tahun'}), 400
            
            db = get_db()
            cursor = db.cursor()
            
            # Target MC
            cursor.execute('''
                SELECT COALESCE(SUM(target_mc), 0) as target_mc
                FROM master_pelanggan
                WHERE periode_bulan = ? AND periode_tahun = ?
            ''', (periode_bulan, periode_tahun))
            
            target_mc = cursor.fetchone()['target_mc']
            
            # Collection (current + tunggakan)
            cursor.execute('''
                SELECT 
                    COALESCE(SUM(CASE WHEN tipe_bayar = 'current' THEN jumlah_bayar ELSE 0 END), 0) as collection_current,
                    COALESCE(SUM(CASE WHEN tipe_bayar = 'tunggakan' THEN jumlah_bayar ELSE 0 END), 0) as collection_tunggakan
                FROM collection_harian
                WHERE periode_bulan = ? AND periode_tahun = ?
            ''', (periode_bulan, periode_tahun))
            
            row = cursor.fetchone()
            collection_current = row['collection_current']
            collection_tunggakan = row['collection_tunggakan']
            collection_total = collection_current + collection_tunggakan
            
            # Collection %
            collection_pct = (collection_current / target_mc * 100) if target_mc > 0 else 0
            
            # Tunggakan %
            tunggakan_pct = (collection_tunggakan / collection_total * 100) if collection_total > 0 else 0
            
            # Jumlah pelanggan
            cursor.execute('''
                SELECT COUNT(DISTINCT nomen) as jumlah_pelanggan
                FROM master_pelanggan
                WHERE periode_bulan = ? AND periode_tahun = ?
            ''', (periode_bulan, periode_tahun))
            
            jumlah_pelanggan = cursor.fetchone()['jumlah_pelanggan']
            
            # Average payment
            avg_payment = collection_total / jumlah_pelanggan if jumlah_pelanggan > 0 else 0
            
            return jsonify({
                'target_mc': target_mc,
                'collection_current': collection_current,
                'collection_tunggakan': collection_tunggakan,
                'collection_total': collection_total,
                'collection_pct': round(collection_pct, 2),
                'tunggakan_pct': round(tunggakan_pct, 2),
                'jumlah_pelanggan': jumlah_pelanggan,
                'avg_payment': round(avg_payment, 2)
            })
            
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/kpi/trend')
    def get_kpi_trend():
        """Get KPI trend over time"""
        try:
            db = get_db()
            cursor = db.cursor()
            
            cursor.execute('''
                SELECT 
                    periode_bulan,
                    periode_tahun,
                    SUM(target_mc) as target_mc,
                    (SELECT COALESCE(SUM(jumlah_bayar), 0) 
                     FROM collection_harian c 
                     WHERE c.periode_bulan = m.periode_bulan 
                     AND c.periode_tahun = m.periode_tahun
                     AND c.tipe_bayar = 'current') as collection
                FROM master_pelanggan m
                GROUP BY periode_bulan, periode_tahun
                ORDER BY periode_tahun, periode_bulan
            ''')
            
            rows = cursor.fetchall()
            
            trend = []
            for row in rows:
                target = row['target_mc']
                collection = row['collection']
                pct = (collection / target * 100) if target > 0 else 0
                
                trend.append({
                    'periode': f"{row['periode_bulan']}/{row['periode_tahun']}",
                    'target_mc': target,
                    'collection': collection,
                    'percentage': round(pct, 2)
                })
            
            return jsonify(trend)
            
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    print("✅ KPI routes registered")
