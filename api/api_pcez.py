"""
Collection API - PCEZ Performance Endpoint
Monitoring performance per PCEZ
"""

from flask import jsonify, request

def register_pcez_route(app, get_db):
    """Register PCEZ performance route"""
    
    @app.route('/api/collection/pcez')
    def get_pcez_performance():
        """
        Get PCEZ performance data
        
        Query params:
        - bulan: periode_bulan (required)
        - tahun: periode_tahun (required)
        - period: daily/weekly/monthly (optional, default: monthly)
        """
        try:
            periode_bulan = request.args.get('bulan', type=int)
            periode_tahun = request.args.get('tahun', type=int)
            period = request.args.get('period', 'monthly')
            
            if not periode_bulan or not periode_tahun:
                return jsonify({'error': 'Missing bulan or tahun'}), 400
            
            db = get_db()
            cursor = db.cursor()
            
            # Query based on period
            if period == 'daily':
                # Group by PCEZ and date
                query = """
                    SELECT 
                        COALESCE(m.pcez, 'Unknown') as pcez,
                        DATE(c.tanggal_bayar) as date,
                        COUNT(DISTINCT m.nomen) as nomen,
                        SUM(m.target_mc) as nominal,
                        SUM(c.total) as bayar
                    FROM master_pelanggan m
                    LEFT JOIN collection_harian c 
                        ON m.nomen = c.nomen 
                        AND c.periode_bulan = m.periode_bulan 
                        AND c.periode_tahun = m.periode_tahun
                    WHERE m.periode_bulan = ? AND m.periode_tahun = ?
                    GROUP BY m.pcez, DATE(c.tanggal_bayar)
                    ORDER BY date DESC, pcez
                """
            elif period == 'weekly':
                # Group by PCEZ and week
                query = """
                    SELECT 
                        COALESCE(m.pcez, 'Unknown') as pcez,
                        strftime('%W', c.tanggal_bayar) as week,
                        COUNT(DISTINCT m.nomen) as nomen,
                        SUM(m.target_mc) as nominal,
                        SUM(c.total) as bayar
                    FROM master_pelanggan m
                    LEFT JOIN collection_harian c 
                        ON m.nomen = c.nomen 
                        AND c.periode_bulan = m.periode_bulan 
                        AND c.periode_tahun = m.periode_tahun
                    WHERE m.periode_bulan = ? AND m.periode_tahun = ?
                    GROUP BY m.pcez, strftime('%W', c.tanggal_bayar)
                    ORDER BY week DESC, pcez
                """
            else:  # monthly (default)
                # Group by PCEZ only (monthly summary)
                query = """
                    SELECT 
                        COALESCE(m.pcez, 'Unknown') as pcez,
                        COUNT(DISTINCT m.nomen) as nomen,
                        SUM(m.target_mc) as nominal,
                        COALESCE(SUM(c.total), 0) as bayar,
                        COALESCE(SUM(c.tunggakan), 0) as tunggakan
                    FROM master_pelanggan m
                    LEFT JOIN collection_harian c 
                        ON m.nomen = c.nomen 
                        AND c.periode_bulan = m.periode_bulan 
                        AND c.periode_tahun = m.periode_tahun
                    WHERE m.periode_bulan = ? AND m.periode_tahun = ?
                    GROUP BY m.pcez
                    ORDER BY pcez
                """
            
            cursor.execute(query, (periode_bulan, periode_tahun))
            rows = cursor.fetchall()
            
            data = []
            for row in rows:
                pcez = row['pcez'] if row['pcez'] else 'Unknown'
                nomen = row['nomen'] or 0
                nominal = row['nominal'] or 0
                
                # Bayar = collection + tunggakan (for monthly)
                if period == 'monthly':
                    bayar = (row['bayar'] or 0) + (row['tunggakan'] or 0)
                else:
                    bayar = row['bayar'] or 0
                
                # Calculate metrics
                performance = (bayar / nominal * 100) if nominal > 0 else 0
                selisih = nominal - bayar
                pct_selisih = (selisih / nominal * 100) if nominal > 0 else 0
                
                item = {
                    'pcez': pcez,
                    'nomen': nomen,
                    'nominal': nominal,
                    'bayar': bayar,
                    'performance': round(performance, 2),
                    'selisih': selisih,
                    'pct_selisih': round(pct_selisih, 2)
                }
                
                # Add date/week if daily/weekly
                if period == 'daily' and 'date' in row.keys():
                    item['date'] = row['date']
                elif period == 'weekly' and 'week' in row.keys():
                    item['week'] = row['week']
                
                data.append(item)
            
            return jsonify(data)
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            return jsonify({'error': str(e)}), 500
    
    print("✅ PCEZ performance route registered")
