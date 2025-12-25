"""
Anomaly Detection API Endpoints
Handles SBRS data anomaly analysis and summary
"""

from flask import jsonify, request
import traceback

def register_anomaly_routes(app, get_db):
    """Register all anomaly detection routes"""
    
    @app.route('/api/anomaly/summary')
    def api_anomaly_summary():
        """Summary count for each anomaly type - FIXED VERSION"""
        db = get_db()
        
        try:
            # Ambil periode terakhir dari SBRS
            periode_query = """
                SELECT periode_bulan, periode_tahun 
                FROM sbrs_data 
                WHERE periode_bulan IS NOT NULL AND periode_tahun IS NOT NULL
                ORDER BY periode_tahun DESC, periode_bulan DESC 
                LIMIT 1
            """
            periode_row = db.execute(periode_query).fetchone()
            
            if not periode_row:
                return jsonify({
                    'periode': None,
                    'anomalies': {}
                })
            
            periode_bulan = periode_row[0]
            periode_tahun = periode_row[1]
            
            # Format periode untuk display
            bulan_names = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'Mei', 'Jun', 
                          'Jul', 'Ags', 'Sep', 'Okt', 'Nov', 'Des']
            periode_label = f"{bulan_names[periode_bulan]} {periode_tahun}"
            
            # Query untuk hitung setiap anomali
            anomalies = {}
            
            # 1. PEMAKAIAN EXTREME (>100 m3 atau >3x avg)
            extreme_query = """
                SELECT COUNT(DISTINCT nomen) as count,
                       SUM(volume) as total_kubikasi,
                       AVG(volume) as avg_kubikasi
                FROM sbrs_data
                WHERE periode_bulan = ? AND periode_tahun = ?
                AND (volume > 100 OR volume > (
                    SELECT AVG(volume) * 3 FROM sbrs_data 
                    WHERE periode_bulan = ? AND periode_tahun = ?
                ))
            """
            extreme = db.execute(extreme_query, (periode_bulan, periode_tahun, periode_bulan, periode_tahun)).fetchone()
            anomalies['extreme'] = {
                'count': extreme[0] or 0,
                'total_kubikasi': extreme[1] or 0,
                'avg': extreme[2] or 0
            }
            
            # 2. PEMAKAIAN TURUN (turun >50% dari periode sebelumnya)
            turun_query = """
                SELECT COUNT(*) as count
                FROM sbrs_data s1
                LEFT JOIN sbrs_data s2 ON s1.nomen = s2.nomen 
                WHERE s1.periode_bulan = ? AND s1.periode_tahun = ?
                AND (
                    (s1.periode_bulan = 1 AND s2.periode_bulan = 12 AND s2.periode_tahun = s1.periode_tahun - 1)
                    OR (s1.periode_bulan > 1 AND s2.periode_bulan = s1.periode_bulan - 1 AND s2.periode_tahun = s1.periode_tahun)
                )
                AND s2.volume > 0
                AND s1.volume < (s2.volume * 0.5)
            """
            turun = db.execute(turun_query, (periode_bulan, periode_tahun)).fetchone()
            anomalies['turun'] = {'count': turun[0] or 0}
            
            # 3. ZERO USAGE (volume = 0)
            zero_query = """
                SELECT COUNT(DISTINCT nomen) as count
                FROM sbrs_data
                WHERE periode_bulan = ? AND periode_tahun = ?
                AND volume = 0
            """
            zero = db.execute(zero_query, (periode_bulan, periode_tahun)).fetchone()
            anomalies['zero'] = {'count': zero[0] or 0}
            
            # 4. STAND NEGATIF (volume < 0)
            negatif_query = """
                SELECT COUNT(DISTINCT nomen) as count,
                       SUM(volume) as total_negatif
                FROM sbrs_data
                WHERE periode_bulan = ? AND periode_tahun = ?
                AND volume < 0
            """
            negatif = db.execute(negatif_query, (periode_bulan, periode_tahun)).fetchone()
            anomalies['negatif'] = {
                'count': negatif[0] or 0,
                'total': negatif[1] or 0
            }
            
            # 5. SALAH CATAT (stand_akhir < stand_awal)
            salah_query = """
                SELECT COUNT(DISTINCT nomen) as count
                FROM sbrs_data
                WHERE periode_bulan = ? AND periode_tahun = ?
                AND stand_akhir < stand_awal
            """
            salah = db.execute(salah_query, (periode_bulan, periode_tahun)).fetchone()
            anomalies['salah_catat'] = {'count': salah[0] or 0}
            
            return jsonify({
                'periode': periode_label,
                'periode_bulan': periode_bulan,
                'periode_tahun': periode_tahun,
                'anomalies': anomalies
            })
            
        except Exception as e:
            traceback.print_exc()
            return jsonify({'error': str(e)}), 500

    @app.route('/api/anomaly/detail/<anomaly_type>')
    def api_anomaly_detail(anomaly_type):
        """Detail data untuk jenis anomali tertentu"""
        db = get_db()
        try:
            periode_query = "SELECT periode_bulan, periode_tahun FROM sbrs_data ORDER BY periode_tahun DESC, periode_bulan DESC LIMIT 1"
            periode_row = db.execute(periode_query).fetchone()
            if not periode_row: 
                return jsonify({'data': []})
            
            p_bulan, p_tahun = periode_row[0], periode_row[1]
            
            if anomaly_type == 'zero':
                query = "SELECT nomen, nama, alamat, volume FROM sbrs_data WHERE periode_bulan=? AND periode_tahun=? AND volume=0 LIMIT 100"
            elif anomaly_type == 'negatif':
                query = "SELECT nomen, nama, alamat, volume FROM sbrs_data WHERE periode_bulan=? AND periode_tahun=? AND volume < 0 LIMIT 100"
            elif anomaly_type == 'extreme':
                query = """
                    SELECT nomen, nama, alamat, volume FROM sbrs_data 
                    WHERE periode_bulan=? AND periode_tahun=? AND volume > 100 LIMIT 100
                """
            else:
                return jsonify({'error': 'Jenis anomali tidak dikenal'}), 400
                
            rows = db.execute(query, (p_bulan, p_tahun)).fetchall()
            return jsonify({'data': [dict(row) for row in rows]})
            
        except Exception as e:
            return jsonify({'error': str(e)}), 500

    print("✅ Anomaly Detection routes registered")
