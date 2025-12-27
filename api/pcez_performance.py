"""
API: Monitoring Performance per PCEZ

Endpoint: /api/collection/performance/pcez
Method: GET
Params: 
  - bulan (required): 1-12
  - tahun (required): YYYY
  
Returns:
  - Performance metrics grouped by PC (Pembagian Cabang) and EZ (Ekonomi Zone)
  - Includes: target, realisasi, tunggakan, collection rate, dll
"""

from flask import jsonify, request


def register_pcez_performance_routes(app, get_db):
    """Register PCEZ Performance Monitoring routes"""
    
    @app.route('/api/collection/performance/pcez', methods=['GET'])
    def get_pcez_performance():
        """
        Get Collection Performance per PCEZ
        
        Query params:
        - bulan: 1-12 (required)
        - tahun: YYYY (required)
        
        Returns aggregated performance by PC and EZ:
        - Total pelanggan
        - Target (dari MC)
        - Realisasi (dari Collection)
        - Tunggakan (dari ARDEBT)
        - Collection Rate (%)
        - Outstanding (%)
        """
        try:
            # Get params
            bulan = request.args.get('bulan', type=int)
            tahun = request.args.get('tahun', type=int)
            
            if not bulan or not tahun:
                return jsonify({
                    'error': 'Parameter bulan dan tahun required',
                    'example': '/api/collection/performance/pcez?bulan=12&tahun=2025'
                }), 400
            
            if bulan < 1 or bulan > 12:
                return jsonify({'error': 'Bulan harus antara 1-12'}), 400
            
            db = get_db()
            cursor = db.cursor()
            
            print(f"\n{'='*70}")
            print(f"PCEZ PERFORMANCE MONITORING - {bulan:02d}/{tahun}")
            print(f"{'='*70}")
            
            # Main query: Aggregate by PC and EZ
            query = """
            WITH mc_data AS (
                SELECT 
                    m.nomen,
                    m.rayon,
                    m.target_mc,
                    COALESCE(a.pc, 'UNKNOWN') as pc,
                    COALESCE(a.ez, 'UNKNOWN') as ez
                FROM master_pelanggan m
                LEFT JOIN ardebt a 
                    ON m.nomen = a.nomen 
                    AND m.periode_bulan = a.periode_bulan 
                    AND m.periode_tahun = a.periode_tahun
                WHERE m.periode_bulan = ? AND m.periode_tahun = ?
            ),
            collection_data AS (
                SELECT 
                    c.nomen,
                    SUM(c.jumlah_bayar) as total_bayar,
                    SUM(CASE WHEN c.tipe_bayar = 'current' THEN c.jumlah_bayar ELSE 0 END) as bayar_current,
                    SUM(CASE WHEN c.tipe_bayar = 'tunggakan' THEN c.jumlah_bayar ELSE 0 END) as bayar_tunggakan,
                    SUM(c.volume_air) as total_volume
                FROM collection_harian c
                WHERE c.periode_bulan = ? AND c.periode_tahun = ?
                GROUP BY c.nomen
            ),
            tunggakan_data AS (
                SELECT 
                    nomen,
                    SUM(saldo_tunggakan) as total_tunggakan
                FROM ardebt
                WHERE periode_bulan = ? AND periode_tahun = ?
                GROUP BY nomen
            )
            SELECT 
                mc.pc,
                mc.ez,
                COUNT(DISTINCT mc.nomen) as total_pelanggan,
                SUM(mc.target_mc) as total_target,
                SUM(COALESCE(c.total_bayar, 0)) as total_realisasi,
                SUM(COALESCE(c.bayar_current, 0)) as realisasi_current,
                SUM(COALESCE(c.bayar_tunggakan, 0)) as realisasi_tunggakan,
                SUM(COALESCE(c.total_volume, 0)) as total_volume,
                SUM(COALESCE(t.total_tunggakan, 0)) as total_outstanding,
                COUNT(DISTINCT CASE WHEN c.nomen IS NOT NULL THEN mc.nomen END) as pelanggan_bayar,
                COUNT(DISTINCT mc.rayon) as total_rayon
            FROM mc_data mc
            LEFT JOIN collection_data c ON mc.nomen = c.nomen
            LEFT JOIN tunggakan_data t ON mc.nomen = t.nomen
            GROUP BY mc.pc, mc.ez
            ORDER BY mc.pc, mc.ez
            """
            
            cursor.execute(query, (bulan, tahun, bulan, tahun, bulan, tahun))
            results = cursor.fetchall()
            
            # Process results
            pcez_list = []
            total_summary = {
                'total_pelanggan': 0,
                'total_target': 0,
                'total_realisasi': 0,
                'total_outstanding': 0,
                'pelanggan_bayar': 0
            }
            
            for row in results:
                target = float(row['total_target'] or 0)
                realisasi = float(row['total_realisasi'] or 0)
                outstanding = float(row['total_outstanding'] or 0)
                pelanggan = row['total_pelanggan']
                pelanggan_bayar = row['pelanggan_bayar']
                
                # Calculate rates
                collection_rate = (realisasi / target * 100) if target > 0 else 0
                outstanding_rate = (outstanding / target * 100) if target > 0 else 0
                paying_rate = (pelanggan_bayar / pelanggan * 100) if pelanggan > 0 else 0
                
                pcez_item = {
                    'pc': row['pc'],
                    'ez': row['ez'],
                    'pcez': f"{row['pc']}/{row['ez']}",
                    'total_pelanggan': pelanggan,
                    'pelanggan_bayar': pelanggan_bayar,
                    'pelanggan_tidak_bayar': pelanggan - pelanggan_bayar,
                    'paying_rate': round(paying_rate, 2),
                    'total_rayon': row['total_rayon'],
                    'target': round(target, 2),
                    'realisasi': round(realisasi, 2),
                    'realisasi_current': round(float(row['realisasi_current'] or 0), 2),
                    'realisasi_tunggakan': round(float(row['realisasi_tunggakan'] or 0), 2),
                    'outstanding': round(outstanding, 2),
                    'total_volume': round(float(row['total_volume'] or 0), 2),
                    'collection_rate': round(collection_rate, 2),
                    'outstanding_rate': round(outstanding_rate, 2),
                    'gap': round(target - realisasi, 2)
                }
                
                pcez_list.append(pcez_item)
                
                # Accumulate totals
                total_summary['total_pelanggan'] += pelanggan
                total_summary['total_target'] += target
                total_summary['total_realisasi'] += realisasi
                total_summary['total_outstanding'] += outstanding
                total_summary['pelanggan_bayar'] += pelanggan_bayar
            
            # Calculate summary rates
            total_summary['collection_rate'] = round(
                (total_summary['total_realisasi'] / total_summary['total_target'] * 100)
                if total_summary['total_target'] > 0 else 0,
                2
            )
            total_summary['outstanding_rate'] = round(
                (total_summary['total_outstanding'] / total_summary['total_target'] * 100)
                if total_summary['total_target'] > 0 else 0,
                2
            )
            total_summary['paying_rate'] = round(
                (total_summary['pelanggan_bayar'] / total_summary['total_pelanggan'] * 100)
                if total_summary['total_pelanggan'] > 0 else 0,
                2
            )
            total_summary['gap'] = round(
                total_summary['total_target'] - total_summary['total_realisasi'],
                2
            )
            
            # Round summary values
            total_summary['total_target'] = round(total_summary['total_target'], 2)
            total_summary['total_realisasi'] = round(total_summary['total_realisasi'], 2)
            total_summary['total_outstanding'] = round(total_summary['total_outstanding'], 2)
            
            # Get top performers
            top_performers = sorted(
                pcez_list,
                key=lambda x: x['collection_rate'],
                reverse=True
            )[:5]
            
            # Get worst performers
            worst_performers = sorted(
                pcez_list,
                key=lambda x: x['collection_rate']
            )[:5]
            
            print(f"✅ PCEZ Performance calculated: {len(pcez_list)} PCEZ groups")
            print(f"   Total Collection Rate: {total_summary['collection_rate']}%")
            
            return jsonify({
                'success': True,
                'periode': {
                    'bulan': bulan,
                    'tahun': tahun,
                    'label': f"{bulan:02d}/{tahun}"
                },
                'summary': total_summary,
                'data': pcez_list,
                'top_performers': top_performers,
                'worst_performers': worst_performers,
                'metadata': {
                    'total_pcez': len(pcez_list),
                    'query_time': 'calculated'
                }
            })
            
        except Exception as e:
            import traceback
            print(f"\n❌ ERROR in PCEZ Performance:")
            print(traceback.format_exc())
            
            return jsonify({
                'error': str(e),
                'traceback': traceback.format_exc()
            }), 500
    
    
    @app.route('/api/collection/performance/pcez/<pc>/<ez>', methods=['GET'])
    def get_pcez_detail(pc, ez):
        """
        Get detailed performance for specific PCEZ
        
        Path params:
        - pc: Pembagian Cabang
        - ez: Ekonomi Zone
        
        Query params:
        - bulan: 1-12 (required)
        - tahun: YYYY (required)
        """
        try:
            bulan = request.args.get('bulan', type=int)
            tahun = request.args.get('tahun', type=int)
            
            if not bulan or not tahun:
                return jsonify({'error': 'Parameter bulan dan tahun required'}), 400
            
            db = get_db()
            cursor = db.cursor()
            
            # Get pelanggan list for this PCEZ
            query = """
            SELECT 
                m.nomen,
                m.nama,
                m.alamat,
                m.rayon,
                m.target_mc,
                COALESCE(SUM(c.jumlah_bayar), 0) as total_bayar,
                COALESCE(SUM(c.volume_air), 0) as total_volume,
                COALESCE(t.saldo_tunggakan, 0) as tunggakan,
                CASE WHEN SUM(c.jumlah_bayar) > 0 THEN 'BAYAR' ELSE 'TIDAK BAYAR' END as status
            FROM master_pelanggan m
            LEFT JOIN ardebt a 
                ON m.nomen = a.nomen 
                AND m.periode_bulan = a.periode_bulan 
                AND m.periode_tahun = a.periode_tahun
            LEFT JOIN collection_harian c 
                ON m.nomen = c.nomen 
                AND m.periode_bulan = c.periode_bulan 
                AND m.periode_tahun = c.periode_tahun
            LEFT JOIN (
                SELECT nomen, SUM(saldo_tunggakan) as saldo_tunggakan
                FROM ardebt
                WHERE periode_bulan = ? AND periode_tahun = ?
                GROUP BY nomen
            ) t ON m.nomen = t.nomen
            WHERE m.periode_bulan = ? AND m.periode_tahun = ?
                AND a.pc = ? AND a.ez = ?
            GROUP BY m.nomen
            ORDER BY m.rayon, m.nomen
            """
            
            cursor.execute(query, (bulan, tahun, bulan, tahun, pc, ez))
            pelanggan_list = [dict(row) for row in cursor.fetchall()]
            
            # Calculate summary
            summary = {
                'pc': pc,
                'ez': ez,
                'pcez': f"{pc}/{ez}",
                'total_pelanggan': len(pelanggan_list),
                'pelanggan_bayar': sum(1 for p in pelanggan_list if p['status'] == 'BAYAR'),
                'total_target': sum(p['target_mc'] for p in pelanggan_list),
                'total_realisasi': sum(p['total_bayar'] for p in pelanggan_list),
                'total_tunggakan': sum(p['tunggakan'] for p in pelanggan_list)
            }
            
            summary['collection_rate'] = round(
                (summary['total_realisasi'] / summary['total_target'] * 100)
                if summary['total_target'] > 0 else 0,
                2
            )
            
            return jsonify({
                'success': True,
                'periode': {'bulan': bulan, 'tahun': tahun},
                'summary': summary,
                'pelanggan': pelanggan_list
            })
            
        except Exception as e:
            import traceback
            return jsonify({
                'error': str(e),
                'traceback': traceback.format_exc()
            }), 500
    
    
    @app.route('/api/collection/performance/pc', methods=['GET'])
    def get_pc_summary():
        """
        Get performance summary grouped by PC only (all EZ combined)
        
        Query params:
        - bulan: 1-12 (required)
        - tahun: YYYY (required)
        """
        try:
            bulan = request.args.get('bulan', type=int)
            tahun = request.args.get('tahun', type=int)
            
            if not bulan or not tahun:
                return jsonify({'error': 'Parameter bulan dan tahun required'}), 400
            
            db = get_db()
            cursor = db.cursor()
            
            # Aggregate by PC only
            query = """
            WITH mc_data AS (
                SELECT 
                    m.nomen,
                    m.target_mc,
                    COALESCE(a.pc, 'UNKNOWN') as pc
                FROM master_pelanggan m
                LEFT JOIN ardebt a 
                    ON m.nomen = a.nomen 
                    AND m.periode_bulan = a.periode_bulan 
                    AND m.periode_tahun = a.periode_tahun
                WHERE m.periode_bulan = ? AND m.periode_tahun = ?
            ),
            collection_data AS (
                SELECT 
                    nomen,
                    SUM(jumlah_bayar) as total_bayar
                FROM collection_harian
                WHERE periode_bulan = ? AND periode_tahun = ?
                GROUP BY nomen
            )
            SELECT 
                mc.pc,
                COUNT(DISTINCT mc.nomen) as total_pelanggan,
                SUM(mc.target_mc) as total_target,
                SUM(COALESCE(c.total_bayar, 0)) as total_realisasi,
                COUNT(DISTINCT CASE WHEN c.nomen IS NOT NULL THEN mc.nomen END) as pelanggan_bayar
            FROM mc_data mc
            LEFT JOIN collection_data c ON mc.nomen = c.nomen
            GROUP BY mc.pc
            ORDER BY mc.pc
            """
            
            cursor.execute(query, (bulan, tahun, bulan, tahun))
            results = cursor.fetchall()
            
            pc_list = []
            for row in results:
                target = float(row['total_target'] or 0)
                realisasi = float(row['total_realisasi'] or 0)
                
                pc_list.append({
                    'pc': row['pc'],
                    'total_pelanggan': row['total_pelanggan'],
                    'pelanggan_bayar': row['pelanggan_bayar'],
                    'target': round(target, 2),
                    'realisasi': round(realisasi, 2),
                    'collection_rate': round((realisasi / target * 100) if target > 0 else 0, 2),
                    'gap': round(target - realisasi, 2)
                })
            
            return jsonify({
                'success': True,
                'periode': {'bulan': bulan, 'tahun': tahun},
                'data': pc_list
            })
            
        except Exception as e:
            import traceback
            return jsonify({
                'error': str(e),
                'traceback': traceback.format_exc()
            }), 500
    
    print("✅ PCEZ Performance routes registered:")
    print("   - GET /api/collection/performance/pcez")
    print("   - GET /api/collection/performance/pcez/<pc>/<ez>")
    print("   - GET /api/collection/performance/pc")
