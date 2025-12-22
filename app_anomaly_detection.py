# app_anomaly_detection.py
# API Extensions untuk Deteksi Anomali Meter dengan History Tracking

"""
ANOMALY DETECTION SYSTEM - SBRS ANALYSIS
Deteksi 7 jenis anomali dengan history tracking multi-periode
"""

from flask import jsonify
import traceback

def register_anomaly_routes(app, get_db):
    """Register semua route untuk anomaly detection"""
    
    @app.route('/api/anomaly/summary')
    def api_anomaly_summary():
        """Summary count untuk setiap jenis anomali"""
        db = get_db()
        
        try:
            # Ambil periode terakhir dari SBRS
            periode_query = "SELECT DISTINCT Bill_Period FROM sbrs_data ORDER BY Bill_Period DESC LIMIT 1"
            periode_row = db.execute(periode_query).fetchone()
            
            if not periode_row:
                return jsonify({
                    'periode': None,
                    'anomalies': {}
                })
            
            periode = periode_row[0]
            
            # Query untuk hitung setiap anomali
            anomalies = {}
            
            # 1. PEMAKAIAN EXTREME (>100 m3 atau >3x avg)
            extreme_query = """
                SELECT COUNT(DISTINCT cmr_account) as count,
                       SUM(SB_Stand) as total_kubikasi,
                       AVG(SB_Stand) as avg_kubikasi
                FROM sbrs_data
                WHERE Bill_Period = ?
                AND (SB_Stand > 100 OR SB_Stand > (SELECT AVG(SB_Stand) * 3 FROM sbrs_data WHERE Bill_Period = ?))
            """
            extreme = db.execute(extreme_query, (periode, periode)).fetchone()
            anomalies['extreme'] = {
                'count': extreme[0] or 0,
                'total_kubikasi': extreme[1] or 0,
                'avg': extreme[2] or 0
            }
            
            # 2. PEMAKAIAN TURUN (turun >50% dari periode sebelumnya)
            turun_query = """
                SELECT COUNT(*) as count
                FROM (
                    SELECT s1.cmr_account, s1.SB_Stand, s2.SB_Stand as prev_stand
                    FROM sbrs_data s1
                    LEFT JOIN sbrs_data s2 ON s1.cmr_account = s2.cmr_account 
                        AND s2.Bill_Period = (SELECT MAX(Bill_Period) FROM sbrs_data WHERE Bill_Period < s1.Bill_Period)
                    WHERE s1.Bill_Period = ?
                    AND s2.SB_Stand > 0
                    AND s1.SB_Stand < (s2.SB_Stand * 0.5)
                )
            """
            turun = db.execute(turun_query, (periode,)).fetchone()
            anomalies['turun'] = {'count': turun[0] or 0}
            
            # 3. ZERO USAGE (SB_Stand = 0)
            zero_query = """
                SELECT COUNT(DISTINCT cmr_account) as count
                FROM sbrs_data
                WHERE Bill_Period = ?
                AND SB_Stand = 0
            """
            zero = db.execute(zero_query, (periode,)).fetchone()
            anomalies['zero'] = {'count': zero[0] or 0}
            
            # 4. STAND NEGATIF (SB_Stand < 0)
            negatif_query = """
                SELECT COUNT(DISTINCT cmr_account) as count,
                       SUM(SB_Stand) as total_negatif
                FROM sbrs_data
                WHERE Bill_Period = ?
                AND SB_Stand < 0
            """
            negatif = db.execute(negatif_query, (periode,)).fetchone()
            anomalies['negatif'] = {
                'count': negatif[0] or 0,
                'total': negatif[1] or 0
            }
            
            # 5. SALAH CATAT (cmr_reading < cmr_prev_read)
            salah_query = """
                SELECT COUNT(DISTINCT cmr_account) as count
                FROM sbrs_data
                WHERE Bill_Period = ?
                AND cmr_reading < cmr_prev_read
            """
            salah = db.execute(salah_query, (periode,)).fetchone()
            anomalies['salah_catat'] = {'count': salah[0] or 0}
            
            # 6. REBILL (ada flag rebill atau Bill_Amount negatif)
            rebill_query = """
                SELECT COUNT(DISTINCT cmr_account) as count,
                       SUM(Bill_Amount) as total_rebill
                FROM sbrs_data
                WHERE Bill_Period = ?
                AND (Bill_Amount < 0 OR cmr_chg_spcl_msg LIKE '%REBILL%')
            """
            rebill = db.execute(rebill_query, (periode,)).fetchone()
            anomalies['rebill'] = {
                'count': rebill[0] or 0,
                'total': rebill[1] or 0
            }
            
            # 7. ESTIMASI (Read_Method != 'ACTUAL' atau cmr_skip_code tidak kosong)
            estimasi_query = """
                SELECT COUNT(DISTINCT cmr_account) as count
                FROM sbrs_data
                WHERE Bill_Period = ?
                AND (Read_Method != 'ACTUAL' OR cmr_skip_code IS NOT NULL AND cmr_skip_code != '')
            """
            estimasi = db.execute(estimasi_query, (periode,)).fetchone()
            anomalies['estimasi'] = {'count': estimasi[0] or 0}
            
            return jsonify({
                'periode': periode,
                'anomalies': anomalies
            })
            
        except Exception as e:
            print(f"Error anomaly summary: {e}")
            traceback.print_exc()
            return jsonify({'error': str(e)}), 500
    
    
    @app.route('/api/anomaly/<anomaly_type>')
    def api_anomaly_detail(anomaly_type):
        """Detail list untuk setiap jenis anomali"""
        db = get_db()
        
        try:
            # Ambil periode terakhir
            periode_query = "SELECT DISTINCT Bill_Period FROM sbrs_data ORDER BY Bill_Period DESC LIMIT 1"
            periode_row = db.execute(periode_query).fetchone()
            
            if not periode_row:
                return jsonify([])
            
            periode = periode_row[0]
            
            # Query berbeda untuk setiap tipe anomali
            if anomaly_type == 'extreme':
                query = """
                    SELECT cmr_account, cmr_name, cmr_address, cmr_route,
                           cmr_prev_read, cmr_reading, SB_Stand,
                           Tariff, Bill_Amount, Read_Method
                    FROM sbrs_data
                    WHERE Bill_Period = ?
                    AND (SB_Stand > 100 OR SB_Stand > (SELECT AVG(SB_Stand) * 3 FROM sbrs_data WHERE Bill_Period = ?))
                    ORDER BY SB_Stand DESC
                """
                params = (periode, periode)
                
            elif anomaly_type == 'turun':
                query = """
                    SELECT s1.cmr_account, s1.cmr_name, s1.cmr_address, s1.cmr_route,
                           s1.cmr_prev_read, s1.cmr_reading, s1.SB_Stand,
                           s2.SB_Stand as prev_period_stand,
                           s1.Tariff, s1.Bill_Amount, s1.Read_Method
                    FROM sbrs_data s1
                    LEFT JOIN sbrs_data s2 ON s1.cmr_account = s2.cmr_account 
                        AND s2.Bill_Period = (SELECT MAX(Bill_Period) FROM sbrs_data WHERE Bill_Period < s1.Bill_Period)
                    WHERE s1.Bill_Period = ?
                    AND s2.SB_Stand > 0
                    AND s1.SB_Stand < (s2.SB_Stand * 0.5)
                    ORDER BY s1.SB_Stand DESC
                """
                params = (periode,)
                
            elif anomaly_type == 'zero':
                query = """
                    SELECT cmr_account, cmr_name, cmr_address, cmr_route,
                           cmr_prev_read, cmr_reading, SB_Stand,
                           cmr_skip_code, cmr_trbl1_code,
                           Tariff, Bill_Amount, Read_Method
                    FROM sbrs_data
                    WHERE Bill_Period = ?
                    AND SB_Stand = 0
                    ORDER BY cmr_account
                """
                params = (periode,)
                
            elif anomaly_type == 'negatif':
                query = """
                    SELECT cmr_account, cmr_name, cmr_address, cmr_route,
                           cmr_prev_read, cmr_reading, SB_Stand,
                           cmr_skip_code, cmr_trbl1_code,
                           Tariff, Bill_Amount, Read_Method
                    FROM sbrs_data
                    WHERE Bill_Period = ?
                    AND SB_Stand < 0
                    ORDER BY SB_Stand ASC
                """
                params = (periode,)
                
            elif anomaly_type == 'salah_catat':
                query = """
                    SELECT cmr_account, cmr_name, cmr_address, cmr_route,
                           cmr_prev_read, cmr_reading, SB_Stand,
                           cmr_mtr_num, Meter_Make_1,
                           Tariff, Bill_Amount, Read_Method
                    FROM sbrs_data
                    WHERE Bill_Period = ?
                    AND cmr_reading < cmr_prev_read
                    ORDER BY cmr_account
                """
                params = (periode,)
                
            elif anomaly_type == 'rebill':
                query = """
                    SELECT cmr_account, cmr_name, cmr_address, cmr_route,
                           cmr_prev_read, cmr_reading, SB_Stand,
                           cmr_chg_spcl_msg,
                           Tariff, Bill_Amount, Read_Method
                    FROM sbrs_data
                    WHERE Bill_Period = ?
                    AND (Bill_Amount < 0 OR cmr_chg_spcl_msg LIKE '%REBILL%')
                    ORDER BY Bill_Amount ASC
                """
                params = (periode,)
                
            elif anomaly_type == 'estimasi':
                query = """
                    SELECT cmr_account, cmr_name, cmr_address, cmr_route,
                           cmr_prev_read, cmr_reading, SB_Stand,
                           cmr_skip_code, Read_Method,
                           Tariff, Bill_Amount
                    FROM sbrs_data
                    WHERE Bill_Period = ?
                    AND (Read_Method != 'ACTUAL' OR cmr_skip_code IS NOT NULL AND cmr_skip_code != '')
                    ORDER BY cmr_account
                """
                params = (periode,)
            else:
                return jsonify({'error': 'Invalid anomaly type'}), 400
            
            rows = db.execute(query, params).fetchall()
            
            return jsonify([dict(row) for row in rows])
            
        except Exception as e:
            print(f"Error anomaly detail: {e}")
            traceback.print_exc()
            return jsonify({'error': str(e)}), 500
    
    
    @app.route('/api/anomaly/history/<cmr_account>')
    def api_anomaly_history(cmr_account):
        """History lengkap untuk satu pelanggan (multi-periode)"""
        db = get_db()
        
        try:
            # Query semua periode untuk pelanggan ini
            query = """
                SELECT 
                    cmr_account,
                    cmr_name,
                    cmr_address,
                    cmr_rd_date,
                    cmr_route,
                    cmr_mrid,
                    Meter_Make_1,
                    cmr_mtr_num,
                    cmr_prev_read,
                    cmr_reading,
                    cmr_skip_code,
                    cmr_trbl1_code,
                    cmr_chg_spcl_msg,
                    SB_Stand,
                    Tariff,
                    Bill_Period,
                    Bill_Due_Date,
                    Bill_Amount,
                    Read_Method
                FROM sbrs_data
                WHERE cmr_account = ?
                ORDER BY Bill_Period DESC
            """
            
            rows = db.execute(query, (cmr_account,)).fetchall()
            
            if not rows:
                return jsonify({'error': 'Customer not found'}), 404
            
            # Convert to list of dict
            history = [dict(row) for row in rows]
            
            # Calculate statistics
            kubikasi_list = [row['SB_Stand'] for row in history if row['SB_Stand']]
            avg_kubikasi = sum(kubikasi_list) / len(kubikasi_list) if kubikasi_list else 0
            max_kubikasi = max(kubikasi_list) if kubikasi_list else 0
            min_kubikasi = min(kubikasi_list) if kubikasi_list else 0
            
            # Detect anomalies in history
            anomalies_found = []
            for row in history:
                if row['SB_Stand'] and row['SB_Stand'] > 100:
                    anomalies_found.append('EXTREME')
                if row['SB_Stand'] == 0:
                    anomalies_found.append('ZERO')
                if row['SB_Stand'] and row['SB_Stand'] < 0:
                    anomalies_found.append('NEGATIF')
                if row['cmr_reading'] and row['cmr_prev_read'] and row['cmr_reading'] < row['cmr_prev_read']:
                    anomalies_found.append('SALAH CATAT')
                if row['Bill_Amount'] and row['Bill_Amount'] < 0:
                    anomalies_found.append('REBILL')
                if row['Read_Method'] != 'ACTUAL':
                    anomalies_found.append('ESTIMASI')
            
            return jsonify({
                'customer_info': {
                    'cmr_account': history[0]['cmr_account'],
                    'cmr_name': history[0]['cmr_name'],
                    'cmr_address': history[0]['cmr_address'],
                    'cmr_route': history[0]['cmr_route'],
                    'Meter_Make_1': history[0]['Meter_Make_1'],
                    'cmr_mtr_num': history[0]['cmr_mtr_num']
                },
                'statistics': {
                    'avg_kubikasi': round(avg_kubikasi, 2),
                    'max_kubikasi': max_kubikasi,
                    'min_kubikasi': min_kubikasi,
                    'total_periods': len(history),
                    'anomalies_count': len(set(anomalies_found)),
                    'anomaly_types': list(set(anomalies_found))
                },
                'history': history
            })
            
        except Exception as e:
            print(f"Error anomaly history: {e}")
            traceback.print_exc()
            return jsonify({'error': str(e)}), 500
    
    
    @app.route('/api/anomaly/chart/<cmr_account>')
    def api_anomaly_chart(cmr_account):
        """Data untuk chart history kubikasi"""
        db = get_db()
        
        try:
            query = """
                SELECT Bill_Period, SB_Stand, Bill_Amount, Read_Method
                FROM sbrs_data
                WHERE cmr_account = ?
                ORDER BY Bill_Period ASC
            """
            
            rows = db.execute(query, (cmr_account,)).fetchall()
            
            return jsonify([dict(row) for row in rows])
            
        except Exception as e:
            print(f"Error chart data: {e}")
            return jsonify({'error': str(e)}), 500

    print("✅ Anomaly Detection Routes registered")
