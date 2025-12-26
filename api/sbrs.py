"""
SBRS API Endpoints
Handles SBRS (Summary By Rayon Status) data
"""

from flask import jsonify, request

def register_sbrs_routes(app, get_db):
    """Register SBRS routes"""
    
    @app.route('/api/sbrs/data')
    def get_sbrs_data():
        """Get SBRS summary data by rayon"""
        try:
            periode_bulan = request.args.get('bulan', type=int)
            periode_tahun = request.args.get('tahun', type=int)
            
            if not periode_bulan or not periode_tahun:
                return jsonify({'error': 'Missing bulan or tahun'}), 400
            
            db = get_db()
            cursor = db.cursor()
            
            # Get summary by rayon
            cursor.execute("""
                SELECT 
                    m.rayon,
                    COUNT(DISTINCT m.nomen) as total_pelanggan,
                    COUNT(DISTINCT c.nomen) as sudah_bayar,
                    SUM(m.target_mc) as total_tagihan,
                    COALESCE(SUM(c.total), 0) as total_collection
                FROM master_pelanggan m
                LEFT JOIN collection_harian c 
                    ON m.nomen = c.nomen 
                    AND c.periode_bulan = m.periode_bulan 
                    AND c.periode_tahun = m.periode_tahun
                WHERE m.periode_bulan = ? AND m.periode_tahun = ?
                GROUP BY m.rayon
                ORDER BY m.rayon
            """, (periode_bulan, periode_tahun))
            
            rows = cursor.fetchall()
            
            data = []
            for row in rows:
                total_pelanggan = row['total_pelanggan']
                sudah_bayar = row['sudah_bayar'] or 0
                belum_bayar = total_pelanggan - sudah_bayar
                
                data.append({
                    'rayon': row['rayon'],
                    'total_pelanggan': total_pelanggan,
                    'sudah_bayar': sudah_bayar,
                    'belum_bayar': belum_bayar,
                    'persen_bayar': round((sudah_bayar / total_pelanggan * 100) if total_pelanggan > 0 else 0, 2),
                    'total_tagihan': row['total_tagihan'] or 0,
                    'total_collection': row['total_collection'] or 0,
                    'achievement': round((row['total_collection'] / row['total_tagihan'] * 100) if row['total_tagihan'] > 0 else 0, 2)
                })
            
            return jsonify(data)
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            return jsonify({'error': str(e)}), 500
    
    print("✅ SBRS routes registered")
