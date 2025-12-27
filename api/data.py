"""
API Routes untuk Home, Collection, Belum Bayar
File: api/data.py

CRITICAL: Semua query HARUS include periode_bulan dan periode_tahun!
"""

from flask import jsonify, request


def register_data_routes(app, get_db):
    """Register routes untuk Home, Collection, Belum Bayar"""
    
    # =========================================
    # HOME / DASHBOARD
    # =========================================
    
    @app.route('/api/home/stats', methods=['GET'])
    def home_stats():
        """
        Get statistics untuk Home/Dashboard
        
        Query params (optional):
        - periode_bulan: int (1-12)
        - periode_tahun: int (2020-2030)
        
        If not provided, auto-get latest periode
        """
        try:
            db = get_db()
            cursor = db.cursor()
            
            # Get periode from query params
            periode_bulan = request.args.get('periode_bulan', type=int)
            periode_tahun = request.args.get('periode_tahun', type=int)
            
            # Auto-get latest periode if not specified
            if not periode_bulan or not periode_tahun:
                cursor.execute("""
                    SELECT periode_bulan, periode_tahun
                    FROM master_pelanggan
                    ORDER BY periode_tahun DESC, periode_bulan DESC
                    LIMIT 1
                """)
                result = cursor.fetchone()
                if not result:
                    return jsonify({'error': 'No data available'}), 404
                periode_bulan = result['periode_bulan']
                periode_tahun = result['periode_tahun']
            
            print(f"📊 Getting Home stats for periode {periode_bulan:02d}/{periode_tahun}")
            
            # 1. Total Pelanggan
            cursor.execute("""
                SELECT COUNT(*) as total
                FROM master_pelanggan
                WHERE periode_bulan = ? AND periode_tahun = ?
            """, (periode_bulan, periode_tahun))
            total_pelanggan = cursor.fetchone()['total']
            
            # 2. Total Target
            cursor.execute("""
                SELECT SUM(target_mc) as total_target
                FROM master_pelanggan
                WHERE periode_bulan = ? AND periode_tahun = ?
            """, (periode_bulan, periode_tahun))
            total_target = cursor.fetchone()['total_target'] or 0
            
            # 3. Total Collection (pembayaran)
            cursor.execute("""
                SELECT 
                    SUM(jumlah_bayar) as total_bayar,
                    COUNT(DISTINCT nomen) as unique_bayar
                FROM collection_harian
                WHERE periode_bulan = ? AND periode_tahun = ?
            """, (periode_bulan, periode_tahun))
            collection_data = cursor.fetchone()
            total_bayar = collection_data['total_bayar'] or 0
            unique_bayar = collection_data['unique_bayar'] or 0
            
            # 4. Belum Bayar (MC yang tidak ada di Collection)
            cursor.execute("""
                SELECT COUNT(DISTINCT m.nomen) as belum_bayar
                FROM master_pelanggan m
                LEFT JOIN collection_harian c 
                    ON m.nomen = c.nomen 
                    AND m.periode_bulan = c.periode_bulan 
                    AND m.periode_tahun = c.periode_tahun
                WHERE m.periode_bulan = ? 
                  AND m.periode_tahun = ?
                  AND c.nomen IS NULL
            """, (periode_bulan, periode_tahun))
            belum_bayar = cursor.fetchone()['belum_bayar']
            
            # 5. Total Tunggakan (dari Ardebt)
            cursor.execute("""
                SELECT SUM(saldo_tunggakan) as total_tunggakan
                FROM ardebt
                WHERE periode_bulan = ? AND periode_tahun = ?
            """, (periode_bulan, periode_tahun))
            total_tunggakan = cursor.fetchone()['total_tunggakan'] or 0
            
            # 6. By Rayon
            cursor.execute("""
                SELECT 
                    m.rayon,
                    COUNT(m.nomen) as total,
                    COUNT(c.nomen) as sudah_bayar,
                    SUM(m.target_mc) as target,
                    SUM(c.jumlah_bayar) as realisasi
                FROM master_pelanggan m
                LEFT JOIN collection_harian c 
                    ON m.nomen = c.nomen 
                    AND m.periode_bulan = c.periode_bulan 
                    AND m.periode_tahun = c.periode_tahun
                WHERE m.periode_bulan = ? 
                  AND m.periode_tahun = ?
                GROUP BY m.rayon
                ORDER BY m.rayon
            """, (periode_bulan, periode_tahun))
            by_rayon = [dict(row) for row in cursor.fetchall()]
            
            # Calculate percentages
            pct_bayar = (unique_bayar / total_pelanggan * 100) if total_pelanggan > 0 else 0
            pct_realisasi = (total_bayar / total_target * 100) if total_target > 0 else 0
            
            result = {
                'periode': f"{periode_bulan:02d}/{periode_tahun}",
                'periode_bulan': periode_bulan,
                'periode_tahun': periode_tahun,
                'total_pelanggan': total_pelanggan,
                'sudah_bayar': unique_bayar,
                'belum_bayar': belum_bayar,
                'pct_bayar': round(pct_bayar, 2),
                'total_target': float(total_target),
                'total_realisasi': float(total_bayar),
                'pct_realisasi': round(pct_realisasi, 2),
                'total_tunggakan': float(total_tunggakan),
                'by_rayon': by_rayon
            }
            
            print(f"✅ Total: {total_pelanggan:,} | Bayar: {unique_bayar:,} | Belum: {belum_bayar:,}")
            
            return jsonify(result)
            
        except Exception as e:
            import traceback
            print(f"❌ Error: {e}")
            print(traceback.format_exc())
            return jsonify({
                'error': str(e),
                'traceback': traceback.format_exc()
            }), 500
    
    
    # =========================================
    # COLLECTION
    # =========================================
    
    @app.route('/api/collection', methods=['GET'])
    def collection_list():
        """
        Get list collection dengan info MC
        
        Query params:
        - periode_bulan: int (optional)
        - periode_tahun: int (optional)
        - limit: int (default: 100)
        - offset: int (default: 0)
        """
        try:
            db = get_db()
            cursor = db.cursor()
            
            periode_bulan = request.args.get('periode_bulan', type=int)
            periode_tahun = request.args.get('periode_tahun', type=int)
            limit = request.args.get('limit', 100, type=int)
            offset = request.args.get('offset', 0, type=int)
            
            # Auto-get latest periode if not specified
            if not periode_bulan or not periode_tahun:
                cursor.execute("""
                    SELECT periode_bulan, periode_tahun
                    FROM master_pelanggan
                    ORDER BY periode_tahun DESC, periode_bulan DESC
                    LIMIT 1
                """)
                result = cursor.fetchone()
                if not result:
                    return jsonify({'error': 'No data available'}), 404
                periode_bulan = result['periode_bulan']
                periode_tahun = result['periode_tahun']
            
            print(f"📋 Getting Collection for periode {periode_bulan:02d}/{periode_tahun}")
            
            # Query: Collection dengan JOIN ke MC (include periode!)
            cursor.execute("""
                SELECT 
                    c.nomen,
                    m.nama,
                    m.alamat,
                    m.rayon,
                    m.pc,
                    m.ez,
                    c.tgl_bayar,
                    c.jumlah_bayar,
                    c.volume_air,
                    c.tipe_bayar,
                    m.target_mc,
                    c.periode_bulan,
                    c.periode_tahun
                FROM collection_harian c
                LEFT JOIN master_pelanggan m 
                    ON c.nomen = m.nomen 
                    AND c.periode_bulan = m.periode_bulan 
                    AND c.periode_tahun = m.periode_tahun
                WHERE c.periode_bulan = ? 
                  AND c.periode_tahun = ?
                ORDER BY c.tgl_bayar DESC, c.jumlah_bayar DESC
                LIMIT ? OFFSET ?
            """, (periode_bulan, periode_tahun, limit, offset))
            
            data = [dict(row) for row in cursor.fetchall()]
            
            # Get summary
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_transaksi,
                    COUNT(DISTINCT c.nomen) as unique_pelanggan,
                    SUM(c.jumlah_bayar) as total_bayar,
                    SUM(c.volume_air) as total_volume,
                    COUNT(CASE WHEN c.tipe_bayar = 'current' THEN 1 END) as current_count,
                    COUNT(CASE WHEN c.tipe_bayar = 'tunggakan' THEN 1 END) as tunggakan_count,
                    AVG(c.jumlah_bayar) as avg_bayar
                FROM collection_harian c
                WHERE c.periode_bulan = ? AND c.periode_tahun = ?
            """, (periode_bulan, periode_tahun))
            
            summary = dict(cursor.fetchone())
            
            print(f"✅ Found {len(data)} records")
            
            return jsonify({
                'data': data,
                'summary': summary,
                'pagination': {
                    'limit': limit,
                    'offset': offset,
                    'count': len(data)
                },
                'periode': {
                    'bulan': periode_bulan,
                    'tahun': periode_tahun,
                    'label': f"{periode_bulan:02d}/{periode_tahun}"
                }
            })
            
        except Exception as e:
            import traceback
            print(f"❌ Error: {e}")
            return jsonify({
                'error': str(e),
                'traceback': traceback.format_exc()
            }), 500
    
    
    # =========================================
    # BELUM BAYAR
    # =========================================
    
    @app.route('/api/belum-bayar', methods=['GET'])
    def belum_bayar_list():
        """
        Get list pelanggan yang belum bayar
        
        Query params:
        - periode_bulan: int (optional)
        - periode_tahun: int (optional)
        - limit: int (default: 100)
        - offset: int (default: 0)
        """
        try:
            db = get_db()
            cursor = db.cursor()
            
            periode_bulan = request.args.get('periode_bulan', type=int)
            periode_tahun = request.args.get('periode_tahun', type=int)
            limit = request.args.get('limit', 100, type=int)
            offset = request.args.get('offset', 0, type=int)
            
            # Auto-get latest periode if not specified
            if not periode_bulan or not periode_tahun:
                cursor.execute("""
                    SELECT periode_bulan, periode_tahun
                    FROM master_pelanggan
                    ORDER BY periode_tahun DESC, periode_bulan DESC
                    LIMIT 1
                """)
                result = cursor.fetchone()
                if not result:
                    return jsonify({'error': 'No data available'}), 404
                periode_bulan = result['periode_bulan']
                periode_tahun = result['periode_tahun']
            
            print(f"📋 Getting Belum Bayar for periode {periode_bulan:02d}/{periode_tahun}")
            
            # Query: MC yang TIDAK ADA di Collection (LEFT JOIN dengan NULL check)
            cursor.execute("""
                SELECT 
                    m.nomen,
                    m.nama,
                    m.alamat,
                    m.rayon,
                    m.pc,
                    m.ez,
                    m.tarif,
                    m.target_mc,
                    m.kubikasi,
                    a.saldo_tunggakan,
                    a.umur_piutang,
                    mb.tgl_bayar as tgl_bayar_mb,
                    mb.jumlah_bayar as bayar_mb
                FROM master_pelanggan m
                LEFT JOIN collection_harian c 
                    ON m.nomen = c.nomen 
                    AND m.periode_bulan = c.periode_bulan 
                    AND m.periode_tahun = c.periode_tahun
                LEFT JOIN ardebt a
                    ON m.nomen = a.nomen
                    AND m.periode_bulan = a.periode_bulan
                    AND m.periode_tahun = a.periode_tahun
                LEFT JOIN master_bayar mb
                    ON m.nomen = mb.nomen
                    AND m.periode_bulan = mb.periode_bulan
                    AND m.periode_tahun = mb.periode_tahun
                WHERE m.periode_bulan = ? 
                  AND m.periode_tahun = ?
                  AND c.nomen IS NULL
                ORDER BY m.rayon, m.nomen
                LIMIT ? OFFSET ?
            """, (periode_bulan, periode_tahun, limit, offset))
            
            data = [dict(row) for row in cursor.fetchall()]
            
            # Get summary
            cursor.execute("""
                SELECT 
                    COUNT(m.nomen) as total_belum_bayar,
                    SUM(m.target_mc) as total_target,
                    SUM(a.saldo_tunggakan) as total_tunggakan
                FROM master_pelanggan m
                LEFT JOIN collection_harian c 
                    ON m.nomen = c.nomen 
                    AND m.periode_bulan = c.periode_bulan 
                    AND m.periode_tahun = c.periode_tahun
                LEFT JOIN ardebt a
                    ON m.nomen = a.nomen
                    AND m.periode_bulan = a.periode_bulan
                    AND m.periode_tahun = a.periode_tahun
                WHERE m.periode_bulan = ? 
                  AND m.periode_tahun = ?
                  AND c.nomen IS NULL
            """, (periode_bulan, periode_tahun))
            
            summary_row = cursor.fetchone()
            summary = {
                'total_belum_bayar': summary_row['total_belum_bayar'],
                'total_target': float(summary_row['total_target'] or 0),
                'total_tunggakan': float(summary_row['total_tunggakan'] or 0)
            }
            
            # By rayon
            cursor.execute("""
                SELECT 
                    m.rayon,
                    COUNT(m.nomen) as total,
                    SUM(m.target_mc) as total_target
                FROM master_pelanggan m
                LEFT JOIN collection_harian c 
                    ON m.nomen = c.nomen 
                    AND m.periode_bulan = c.periode_bulan 
                    AND m.periode_tahun = c.periode_tahun
                WHERE m.periode_bulan = ? 
                  AND m.periode_tahun = ?
                  AND c.nomen IS NULL
                GROUP BY m.rayon
                ORDER BY m.rayon
            """, (periode_bulan, periode_tahun))
            
            by_rayon = [dict(row) for row in cursor.fetchall()]
            
            print(f"✅ Found {len(data)} belum bayar records")
            
            return jsonify({
                'data': data,
                'summary': summary,
                'by_rayon': by_rayon,
                'pagination': {
                    'limit': limit,
                    'offset': offset,
                    'count': len(data)
                },
                'periode': {
                    'bulan': periode_bulan,
                    'tahun': periode_tahun,
                    'label': f"{periode_bulan:02d}/{periode_tahun}"
                }
            })
            
        except Exception as e:
            import traceback
            print(f"❌ Error: {e}")
            return jsonify({
                'error': str(e),
                'traceback': traceback.format_exc()
            }), 500
    
    
    # =========================================
    # PERIODES
    # =========================================
    
    @app.route('/api/periodes', methods=['GET'])
    def get_periodes():
        """Get all available periodes for dropdown"""
        try:
            db = get_db()
            cursor = db.cursor()
            
            cursor.execute("""
                SELECT DISTINCT periode_bulan, periode_tahun
                FROM master_pelanggan
                ORDER BY periode_tahun DESC, periode_bulan DESC
            """)
            
            periodes = [
                {
                    'bulan': row['periode_bulan'],
                    'tahun': row['periode_tahun'],
                    'label': f"{row['periode_bulan']:02d}/{row['periode_tahun']}"
                }
                for row in cursor.fetchall()
            ]
            
            return jsonify({'periodes': periodes})
            
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    
    print("✅ Data routes registered (Home, Collection, Belum Bayar)")
