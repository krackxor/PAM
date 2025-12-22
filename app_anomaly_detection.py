# app_anomaly_detection.py
# FIXED VERSION - All queries use periode_bulan/periode_tahun and volume

"""
ANOMALY DETECTION SYSTEM - SBRS ANALYSIS
Deteksi 7 jenis anomali dengan history tracking multi-periode

FIXED:
- All queries use periode_bulan & periode_tahun (not Bill_Period)
- All queries use volume (not SB_Stand)  
- All queries use nomen (not cmr_account)
"""

from flask import jsonify
import traceback

def register_anomaly_routes(app, get_db):
    """Register semua route untuk anomaly detection"""
    
    @app.route('/api/anomaly/summary')
    def api_anomaly_summary():
        """Summary count untuk setiap jenis anomali - FIXED VERSION"""
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
            
            # 6. REBILL (ada flag rebill di spm_status)
            rebill_query = """
                SELECT COUNT(DISTINCT nomen) as count
                FROM sbrs_data
                WHERE periode_bulan = ? AND periode_tahun = ?
                AND (spm_status LIKE '%REBILL%' OR spm_status LIKE '%rebill%')
            """
            rebill = db.execute(rebill_query, (periode_bulan, periode_tahun)).fetchone()
            anomalies['rebill'] = {
                'count': rebill[0] or 0,
                'total': 0
            }
            
            # 7. ESTIMASI (readmethod != 'ACTUAL' atau skip_status tidak kosong)
            estimasi_query = """
                SELECT COUNT(DISTINCT nomen) as count
                FROM sbrs_data
                WHERE periode_bulan = ? AND periode_tahun = ?
                AND (readmethod != 'ACTUAL' OR (skip_status IS NOT NULL AND skip_status != ''))
            """
            estimasi = db.execute(estimasi_query, (periode_bulan, periode_tahun)).fetchone()
            anomalies['estimasi'] = {'count': estimasi[0] or 0}
            
            return jsonify({
                'periode': periode_label,
                'periode_bulan': periode_bulan,
                'periode_tahun': periode_tahun,
                'anomalies': anomalies
            })
            
        except Exception as e:
            print(f"Error anomaly summary: {e}")
            traceback.print_exc()
            return jsonify({
                'error': str(e),
                'traceback': traceback.format_exc()
            }), 500
    
    
    @app.route('/api/anomaly/detail/<anomaly_type>')
    def api_anomaly_detail(anomaly_type):
        """Detail data untuk jenis anomali tertentu - FIXED VERSION"""
        db = get_db()
        
        try:
            # Ambil periode terakhir
            periode_query = """
                SELECT periode_bulan, periode_tahun 
                FROM sbrs_data 
                WHERE periode_bulan IS NOT NULL AND periode_tahun IS NOT NULL
                ORDER BY periode_tahun DESC, periode_bulan DESC 
                LIMIT 1
            """
            periode_row = db.execute(periode_query).fetchone()
            
            if not periode_row:
                return jsonify({'data': []})
            
            periode_bulan = periode_row[0]
            periode_tahun = periode_row[1]
            
            # Query berdasarkan tipe anomali
            if anomaly_type == 'extreme':
                query = """
                    SELECT s.nomen, s.nama, s.alamat, s.rayon, s.volume,
                           s.stand_awal, s.stand_akhir, s.readmethod
                    FROM sbrs_data s
                    WHERE s.periode_bulan = ? AND s.periode_tahun = ?
                    AND (s.volume > 100 OR s.volume > (
                        SELECT AVG(volume) * 3 FROM sbrs_data 
                        WHERE periode_bulan = ? AND periode_tahun = ?
                    ))
                    ORDER BY s.volume DESC
                    LIMIT 100
                """
                rows = db.execute(query, (periode_bulan, periode_tahun, periode_bulan, periode_tahun)).fetchall()
                
            elif anomaly_type == 'turun':
                query = """
                    SELECT s1.nomen, s1.nama, s1.alamat, s1.rayon,
                           s1.volume as volume_sekarang,
                           s2.volume as volume_lalu,
                           ROUND((s1.volume - s2.volume) * 100.0 / s2.volume, 2) as persen_perubahan
                    FROM sbrs_data s1
                    INNER JOIN sbrs_data s2 ON s1.nomen = s2.nomen
                    WHERE s1.periode_bulan = ? AND s1.periode_tahun = ?
                    AND (
                        (s1.periode_bulan = 1 AND s2.periode_bulan = 12 AND s2.periode_tahun = s1.periode_tahun - 1)
                        OR (s1.periode_bulan > 1 AND s2.periode_bulan = s1.periode_bulan - 1 AND s2.periode_tahun = s1.periode_tahun)
                    )
                    AND s2.volume > 0
                    AND s1.volume < (s2.volume * 0.5)
                    ORDER BY persen_perubahan ASC
                    LIMIT 100
                """
                rows = db.execute(query, (periode_bulan, periode_tahun)).fetchall()
                
            elif anomaly_type == 'zero':
                query = """
                    SELECT nomen, nama, alamat, rayon, volume,
                           readmethod, skip_status, trouble_status
                    FROM sbrs_data
                    WHERE periode_bulan = ? AND periode_tahun = ?
                    AND volume = 0
                    ORDER BY nomen
                    LIMIT 100
                """
                rows = db.execute(query, (periode_bulan, periode_tahun)).fetchall()
                
            elif anomaly_type == 'negatif':
                query = """
                    SELECT nomen, nama, alamat, rayon, volume,
                           stand_awal, stand_akhir
                    FROM sbrs_data
                    WHERE periode_bulan = ? AND periode_tahun = ?
                    AND volume < 0
                    ORDER BY volume ASC
                    LIMIT 100
                """
                rows = db.execute(query, (periode_bulan, periode_tahun)).fetchall()
                
            elif anomaly_type == 'salah_catat':
                query = """
                    SELECT nomen, nama, alamat, rayon,
                           stand_awal, stand_akhir, volume
                    FROM sbrs_data
                    WHERE periode_bulan = ? AND periode_tahun = ?
                    AND stand_akhir < stand_awal
                    ORDER BY (stand_awal - stand_akhir) DESC
                    LIMIT 100
                """
                rows = db.execute(query, (periode_bulan, periode_tahun)).fetchall()
                
            elif anomaly_type == 'rebill':
                query = """
                    SELECT nomen, nama, alamat, rayon, volume,
                           spm_status
                    FROM sbrs_data
                    WHERE periode_bulan = ? AND periode_tahun = ?
                    AND (spm_status LIKE '%REBILL%' OR spm_status LIKE '%rebill%')
                    ORDER BY nomen
                    LIMIT 100
                """
                rows = db.execute(query, (periode_bulan, periode_tahun)).fetchall()
                
            elif anomaly_type == 'estimasi':
                query = """
                    SELECT nomen, nama, alamat, rayon, volume,
                           readmethod, skip_status
                    FROM sbrs_data
                    WHERE periode_bulan = ? AND periode_tahun = ?
                    AND (readmethod != 'ACTUAL' OR (skip_status IS NOT NULL AND skip_status != ''))
                    ORDER BY nomen
                    LIMIT 100
                """
                rows = db.execute(query, (periode_bulan, periode_tahun)).fetchall()
            else:
                return jsonify({'error': 'Invalid anomaly type'}), 400
            
            # Convert to dict
            data = []
            for row in rows:
                data.append(dict(row))
            
            return jsonify({'data': data})
            
        except Exception as e:
            print(f"Error anomaly detail: {e}")
            traceback.print_exc()
            return jsonify({
                'error': str(e),
                'traceback': traceback.format_exc()
            }), 500
    
    
    print("✅ Anomaly Detection Routes registered")
