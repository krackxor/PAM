# app_analisa_api.py
# Backend API for Analisa Manual Features

"""
ANALISA MANUAL SYSTEM
- Create, Read, Update, Delete analisa
- Status tracking (pending, in_progress, completed, rejected)
- Comments and activity log
- Assignment and collaboration
"""

from flask import jsonify, request
from datetime import datetime
import traceback

def register_analisa_routes(app, get_db):
    """Register all analisa routes"""
    
    @app.route('/api/analisa/list')
    def api_analisa_list():
        """Get all analisa with filters"""
        db = get_db()
        
        try:
            status_filter = request.args.get('status', 'all')
            jenis_filter = request.args.get('jenis', 'all')
            
            query = '''
                SELECT 
                    a.id,
                    a.nomen,
                    m.nama,
                    m.alamat,
                    m.rayon,
                    m.tarif,
                    m.kubikasi,
                    a.jenis_anomali,
                    a.deskripsi,
                    a.status,
                    a.priority,
                    a.assigned_to,
                    a.due_date,
                    a.created_at,
                    a.updated_at
                FROM analisa_manual a
                LEFT JOIN master_pelanggan m ON a.nomen = m.nomen
                WHERE 1=1
            '''
            
            params = []
            
            if status_filter != 'all':
                query += ' AND a.status = ?'
                params.append(status_filter)
            
            if jenis_filter != 'all':
                query += ' AND a.jenis_anomali = ?'
                params.append(jenis_filter)
            
            query += ' ORDER BY a.created_at DESC'
            
            rows = db.execute(query, params).fetchall()
            
            return jsonify([dict(row) for row in rows])
            
        except Exception as e:
            print(f"Error analisa list: {e}")
            traceback.print_exc()
            return jsonify({'error': str(e)}), 500
    
    
    @app.route('/api/analisa/detail/<int:analisa_id>')
    def api_analisa_detail(analisa_id):
        """Get analisa detail"""
        db = get_db()
        
        try:
            row = db.execute('''
                SELECT 
                    a.*,
                    m.nama,
                    m.alamat,
                    m.rayon,
                    m.tarif,
                    m.kubikasi
                FROM analisa_manual a
                LEFT JOIN master_pelanggan m ON a.nomen = m.nomen
                WHERE a.id = ?
            ''', (analisa_id,)).fetchone()
            
            if not row:
                return jsonify({'error': 'Analisa not found'}), 404
            
            return jsonify(dict(row))
            
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    
    @app.route('/api/analisa/create', methods=['POST'])
    def api_analisa_create():
        """Create new analisa"""
        db = get_db()
        
        try:
            data = request.get_json()
            
            # Validate required fields
            required = ['nomen', 'jenis_anomali', 'deskripsi']
            for field in required:
                if field not in data:
                    return jsonify({'error': f'Missing field: {field}'}), 400
            
            # Insert
            cursor = db.execute('''
                INSERT INTO analisa_manual (
                    nomen,
                    jenis_anomali,
                    deskripsi,
                    status,
                    priority,
                    assigned_to,
                    due_date,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                data['nomen'],
                data['jenis_anomali'],
                data['deskripsi'],
                data.get('status', 'pending'),
                data.get('priority', 'medium'),
                data.get('assigned_to', ''),
                data.get('due_date', ''),
                datetime.now().isoformat(),
                datetime.now().isoformat()
            ))
            
            db.commit()
            
            analisa_id = cursor.lastrowid
            
            # Log activity
            db.execute('''
                INSERT INTO analisa_activity (
                    analisa_id, action, user, created_at
                ) VALUES (?, ?, ?, ?)
            ''', (analisa_id, 'Created', 'System', datetime.now().isoformat()))
            
            db.commit()
            
            return jsonify({
                'success': True,
                'id': analisa_id,
                'message': 'Analisa created successfully'
            })
            
        except Exception as e:
            print(f"Error create analisa: {e}")
            traceback.print_exc()
            return jsonify({'error': str(e)}), 500
    
    
    @app.route('/api/analisa/update/<int:analisa_id>', methods=['PUT'])
    def api_analisa_update(analisa_id):
        """Update analisa"""
        db = get_db()
        
        try:
            data = request.get_json()
            
            # Build update query dynamically
            update_fields = []
            params = []
            
            allowed_fields = ['jenis_anomali', 'deskripsi', 'status', 'priority', 'assigned_to', 'due_date']
            
            for field in allowed_fields:
                if field in data:
                    update_fields.append(f'{field} = ?')
                    params.append(data[field])
            
            if not update_fields:
                return jsonify({'error': 'No fields to update'}), 400
            
            # Add updated_at
            update_fields.append('updated_at = ?')
            params.append(datetime.now().isoformat())
            
            # Add analisa_id
            params.append(analisa_id)
            
            query = f'''
                UPDATE analisa_manual 
                SET {', '.join(update_fields)}
                WHERE id = ?
            '''
            
            db.execute(query, params)
            db.commit()
            
            # Log activity
            db.execute('''
                INSERT INTO analisa_activity (
                    analisa_id, action, user, created_at
                ) VALUES (?, ?, ?, ?)
            ''', (analisa_id, 'Updated', 'System', datetime.now().isoformat()))
            
            db.commit()
            
            return jsonify({
                'success': True,
                'message': 'Analisa updated successfully'
            })
            
        except Exception as e:
            print(f"Error update analisa: {e}")
            traceback.print_exc()
            return jsonify({'error': str(e)}), 500
    
    
    @app.route('/api/analisa/update-status/<int:analisa_id>', methods=['PUT'])
    def api_analisa_update_status(analisa_id):
        """Update analisa status only"""
        db = get_db()
        
        try:
            data = request.get_json()
            new_status = data.get('status')
            
            if not new_status:
                return jsonify({'error': 'Status required'}), 400
            
            db.execute('''
                UPDATE analisa_manual 
                SET status = ?, updated_at = ?
                WHERE id = ?
            ''', (new_status, datetime.now().isoformat(), analisa_id))
            
            db.commit()
            
            # Log activity
            db.execute('''
                INSERT INTO analisa_activity (
                    analisa_id, action, user, icon, created_at
                ) VALUES (?, ?, ?, ?, ?)
            ''', (analisa_id, f'Status changed to {new_status}', 'System', 'check-circle', datetime.now().isoformat()))
            
            db.commit()
            
            return jsonify({
                'success': True,
                'message': 'Status updated successfully'
            })
            
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    
    @app.route('/api/analisa/delete/<int:analisa_id>', methods=['DELETE'])
    def api_analisa_delete(analisa_id):
        """Delete analisa"""
        db = get_db()
        
        try:
            # Delete comments first
            db.execute('DELETE FROM analisa_comments WHERE analisa_id = ?', (analisa_id,))
            
            # Delete activity
            db.execute('DELETE FROM analisa_activity WHERE analisa_id = ?', (analisa_id,))
            
            # Delete analisa
            db.execute('DELETE FROM analisa_manual WHERE id = ?', (analisa_id,))
            
            db.commit()
            
            return jsonify({
                'success': True,
                'message': 'Analisa deleted successfully'
            })
            
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    
    @app.route('/api/analisa/comment/<int:analisa_id>', methods=['POST'])
    def api_analisa_add_comment(analisa_id):
        """Add comment to analisa"""
        db = get_db()
        
        try:
            data = request.get_json()
            comment = data.get('comment')
            
            if not comment:
                return jsonify({'error': 'Comment required'}), 400
            
            db.execute('''
                INSERT INTO analisa_comments (
                    analisa_id, user, comment, created_at
                ) VALUES (?, ?, ?, ?)
            ''', (analisa_id, 'System User', comment, datetime.now().isoformat()))
            
            db.commit()
            
            # Log activity
            db.execute('''
                INSERT INTO analisa_activity (
                    analisa_id, action, user, icon, created_at
                ) VALUES (?, ?, ?, ?, ?)
            ''', (analisa_id, 'Added comment', 'System User', 'comment', datetime.now().isoformat()))
            
            db.commit()
            
            return jsonify({
                'success': True,
                'message': 'Comment added successfully'
            })
            
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    
    @app.route('/api/analisa/comments/<int:analisa_id>')
    def api_analisa_get_comments(analisa_id):
        """Get comments for analisa"""
        db = get_db()
        
        try:
            rows = db.execute('''
                SELECT * FROM analisa_comments
                WHERE analisa_id = ?
                ORDER BY created_at DESC
            ''', (analisa_id,)).fetchall()
            
            return jsonify([dict(row) for row in rows])
            
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    
    @app.route('/api/analisa/activity/<int:analisa_id>')
    def api_analisa_get_activity(analisa_id):
        """Get activity log for analisa"""
        db = get_db()
        
        try:
            rows = db.execute('''
                SELECT * FROM analisa_activity
                WHERE analisa_id = ?
                ORDER BY created_at DESC
            ''', (analisa_id,)).fetchall()
            
            return jsonify([dict(row) for row in rows])
            
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    
    @app.route('/api/analisa/stats')
    def api_analisa_stats():
        """Get analisa statistics"""
        db = get_db()
        
        try:
            stats = {}
            
            # Total
            total = db.execute('SELECT COUNT(*) as count FROM analisa_manual').fetchone()
            stats['total'] = total['count'] if total else 0
            
            # By status
            by_status = db.execute('''
                SELECT status, COUNT(*) as count
                FROM analisa_manual
                GROUP BY status
            ''').fetchall()
            
            stats['by_status'] = {row['status']: row['count'] for row in by_status}
            
            # By priority
            by_priority = db.execute('''
                SELECT priority, COUNT(*) as count
                FROM analisa_manual
                GROUP BY priority
            ''').fetchall()
            
            stats['by_priority'] = {row['priority']: row['count'] for row in by_priority}
            
            # By jenis
            by_jenis = db.execute('''
                SELECT jenis_anomali, COUNT(*) as count
                FROM analisa_manual
                GROUP BY jenis_anomali
            ''').fetchall()
            
            stats['by_jenis'] = {row['jenis_anomali']: row['count'] for row in by_jenis}
            
            return jsonify(stats)
            
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    
    @app.route('/api/customer/search')
    def api_customer_search():
        """Search customer by nomen"""
        db = get_db()
        
        try:
            nomen = request.args.get('nomen')
            
            if not nomen:
                return jsonify({'error': 'Nomen required'}), 400
            
            row = db.execute('''
                SELECT * FROM master_pelanggan
                WHERE nomen = ?
            ''', (nomen,)).fetchone()
            
            if not row:
                return jsonify({'error': 'Customer not found'}), 404
            
            return jsonify(dict(row))
            
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    print("✅ Analisa Manual Routes registered")


def init_analisa_tables(db):
    """Initialize analisa tables"""
    
    # analisa_manual table
    db.execute('''
        CREATE TABLE IF NOT EXISTS analisa_manual (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nomen TEXT NOT NULL,
            jenis_anomali TEXT NOT NULL,
            deskripsi TEXT,
            status TEXT DEFAULT 'pending',
            priority TEXT DEFAULT 'medium',
            assigned_to TEXT,
            due_date TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    ''')
    
    # analisa_comments table
    db.execute('''
        CREATE TABLE IF NOT EXISTS analisa_comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            analisa_id INTEGER NOT NULL,
            user TEXT NOT NULL,
            comment TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (analisa_id) REFERENCES analisa_manual(id)
        )
    ''')
    
    # analisa_activity table
    db.execute('''
        CREATE TABLE IF NOT EXISTS analisa_activity (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            analisa_id INTEGER NOT NULL,
            action TEXT NOT NULL,
            user TEXT NOT NULL,
            icon TEXT DEFAULT 'circle',
            created_at TEXT NOT NULL,
            FOREIGN KEY (analisa_id) REFERENCES analisa_manual(id)
        )
    ''')
    
    db.commit()
    print("✅ Analisa tables initialized")
