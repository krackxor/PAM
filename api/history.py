"""
History API Endpoints
Handles upload history and data tracking
"""

from flask import jsonify, request

def register_history_routes(app, get_db):
    """Register history routes"""
    
    @app.route('/api/history/uploads')
    def history_uploads():
        """Get upload history"""
        try:
            file_type = request.args.get('file_type')
            limit = request.args.get('limit', 50, type=int)
            
            db = get_db()
            cursor = db.cursor()
            
            query = 'SELECT * FROM upload_metadata WHERE 1=1'
            params = []
            
            if file_type:
                query += ' AND file_type = ?'
                params.append(file_type)
            
            query += ' ORDER BY upload_date DESC LIMIT ?'
            params.append(limit)
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            data = []
            for row in rows:
                data.append({
                    'id': row['id'],
                    'file_type': row['file_type'],
                    'file_name': row['file_name'],
                    'periode_bulan': row['periode_bulan'],
                    'periode_tahun': row['periode_tahun'],
                    'upload_date': row['upload_date'],
                    'row_count': row['row_count'],
                    'status': row['status']
                })
            
            return jsonify(data)
            
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/history/stats')
    def history_stats():
        """Get upload statistics"""
        try:
            db = get_db()
            cursor = db.cursor()
            
            # Total uploads by type
            cursor.execute('''
                SELECT 
                    file_type,
                    COUNT(*) as total_uploads,
                    SUM(row_count) as total_rows,
                    MAX(upload_date) as last_upload
                FROM upload_metadata
                GROUP BY file_type
            ''')
            
            rows = cursor.fetchall()
            
            by_type = {}
            for row in rows:
                by_type[row['file_type']] = {
                    'uploads': row['total_uploads'],
                    'rows': row['total_rows'],
                    'last_upload': row['last_upload']
                }
            
            # Recent activity
            cursor.execute('''
                SELECT 
                    DATE(upload_date) as date,
                    COUNT(*) as uploads
                FROM upload_metadata
                WHERE upload_date >= date('now', '-30 days')
                GROUP BY DATE(upload_date)
                ORDER BY date DESC
            ''')
            
            recent_rows = cursor.fetchall()
            recent_activity = []
            for row in recent_rows:
                recent_activity.append({
                    'date': row['date'],
                    'uploads': row['uploads']
                })
            
            # Success rate
            cursor.execute('''
                SELECT 
                    status,
                    COUNT(*) as count
                FROM upload_metadata
                GROUP BY status
            ''')
            
            status_rows = cursor.fetchall()
            status_stats = {}
            total = 0
            for row in status_rows:
                status_stats[row['status']] = row['count']
                total += row['count']
            
            success_rate = (status_stats.get('success', 0) / total * 100) if total > 0 else 0
            
            return jsonify({
                'by_type': by_type,
                'recent_activity': recent_activity,
                'status_stats': status_stats,
                'success_rate': round(success_rate, 2),
                'total_uploads': total
            })
            
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/history/periods')
    def history_periods():
        """Get list of available periods"""
        try:
            db = get_db()
            cursor = db.cursor()
            
            cursor.execute('''
                SELECT DISTINCT 
                    periode_bulan,
                    periode_tahun
                FROM upload_metadata
                WHERE status = 'success'
                ORDER BY periode_tahun DESC, periode_bulan DESC
            ''')
            
            rows = cursor.fetchall()
            
            periods = []
            for row in rows:
                periods.append({
                    'bulan': row['periode_bulan'],
                    'tahun': row['periode_tahun'],
                    'label': f"{row['periode_bulan']}/{row['periode_tahun']}"
                })
            
            return jsonify(periods)
            
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/history/upload/<int:upload_id>')
    def history_upload_detail(upload_id):
        """Get detailed info for specific upload"""
        try:
            db = get_db()
            cursor = db.cursor()
            
            cursor.execute('''
                SELECT * FROM upload_metadata
                WHERE id = ?
            ''', (upload_id,))
            
            row = cursor.fetchone()
            
            if not row:
                return jsonify({'error': 'Upload not found'}), 404
            
            data = {
                'id': row['id'],
                'file_type': row['file_type'],
                'file_name': row['file_name'],
                'periode_bulan': row['periode_bulan'],
                'periode_tahun': row['periode_tahun'],
                'upload_date': row['upload_date'],
                'row_count': row['row_count'],
                'status': row['status']
            }
            
            return jsonify(data)
            
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    print("✅ History routes registered")
