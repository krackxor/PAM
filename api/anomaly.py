"""
Analisa Manual API Endpoints
Handles manual analysis and case management
"""

from flask import jsonify, request
from datetime import datetime

def register_analisa_routes(app, get_db):
    """Register analisa manual routes"""
    
    @app.route('/api/analisa/list')
    def analisa_list():
        """Get list of manual analysis cases"""
        try:
            status = request.args.get('status')  # pending, in_progress, resolved
            priority = request.args.get('priority')  # low, medium, high
            
            db = get_db()
            cursor = db.cursor()
            
            query = 'SELECT * FROM analisa_manual WHERE 1=1'
            params = []
            
            if status:
                query += ' AND status = ?'
                params.append(status)
            
            if priority:
                query += ' AND priority = ?'
                params.append(priority)
            
            query += ' ORDER BY created_at DESC'
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            data = []
            for row in rows:
                data.append({
                    'id': row['id'],
                    'nomen': row['nomen'],
                    'jenis_anomali': row['jenis_anomali'],
                    'deskripsi': row['deskripsi'],
                    'status': row['status'],
                    'priority': row['priority'],
                    'assigned_to': row['assigned_to'],
                    'due_date': row['due_date'],
                    'created_at': row['created_at'],
                    'updated_at': row['updated_at']
                })
            
            return jsonify(data)
            
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/analisa/create', methods=['POST'])
    def analisa_create():
        """Create new analysis case"""
        try:
            data = request.get_json()
            
            nomen = data.get('nomen')
            jenis_anomali = data.get('jenis_anomali')
            deskripsi = data.get('deskripsi')
            priority = data.get('priority', 'medium')
            assigned_to = data.get('assigned_to')
            due_date = data.get('due_date')
            
            if not nomen or not jenis_anomali:
                return jsonify({'error': 'Missing required fields'}), 400
            
            db = get_db()
            cursor = db.cursor()
            
            now = datetime.now().isoformat()
            
            cursor.execute('''
                INSERT INTO analisa_manual 
                (nomen, jenis_anomali, deskripsi, status, priority, assigned_to, due_date, created_at, updated_at)
                VALUES (?, ?, ?, 'pending', ?, ?, ?, ?, ?)
            ''', (nomen, jenis_anomali, deskripsi, priority, assigned_to, due_date, now, now))
            
            analisa_id = cursor.lastrowid
            
            # Add activity log
            cursor.execute('''
                INSERT INTO analisa_activity (analisa_id, action, user, icon, created_at)
                VALUES (?, 'created', ?, 'plus-circle', ?)
            ''', (analisa_id, assigned_to or 'system', now))
            
            db.commit()
            
            return jsonify({
                'success': True,
                'id': analisa_id,
                'message': 'Analysis case created'
            })
            
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/analisa/<int:analisa_id>/update', methods=['PUT'])
    def analisa_update(analisa_id):
        """Update analysis case"""
        try:
            data = request.get_json()
            
            status = data.get('status')
            priority = data.get('priority')
            assigned_to = data.get('assigned_to')
            due_date = data.get('due_date')
            
            db = get_db()
            cursor = db.cursor()
            
            updates = []
            params = []
            
            if status:
                updates.append('status = ?')
                params.append(status)
            
            if priority:
                updates.append('priority = ?')
                params.append(priority)
            
            if assigned_to:
                updates.append('assigned_to = ?')
                params.append(assigned_to)
            
            if due_date:
                updates.append('due_date = ?')
                params.append(due_date)
            
            if not updates:
                return jsonify({'error': 'No fields to update'}), 400
            
            now = datetime.now().isoformat()
            updates.append('updated_at = ?')
            params.append(now)
            
            params.append(analisa_id)
            
            query = f"UPDATE analisa_manual SET {', '.join(updates)} WHERE id = ?"
            cursor.execute(query, params)
            
            # Add activity log
            action = f"updated: {', '.join(data.keys())}"
            cursor.execute('''
                INSERT INTO analisa_activity (analisa_id, action, user, icon, created_at)
                VALUES (?, ?, ?, 'edit', ?)
            ''', (analisa_id, action, data.get('user', 'system'), now))
            
            db.commit()
            
            return jsonify({
                'success': True,
                'message': 'Analysis case updated'
            })
            
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/analisa/<int:analisa_id>/comments')
    def analisa_comments(analisa_id):
        """Get comments for analysis case"""
        try:
            db = get_db()
            cursor = db.cursor()
            
            cursor.execute('''
                SELECT * FROM analisa_comments
                WHERE analisa_id = ?
                ORDER BY created_at DESC
            ''', (analisa_id,))
            
            rows = cursor.fetchall()
            
            data = []
            for row in rows:
                data.append({
                    'id': row['id'],
                    'user': row['user'],
                    'comment': row['comment'],
                    'created_at': row['created_at']
                })
            
            return jsonify(data)
            
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/analisa/<int:analisa_id>/comments/add', methods=['POST'])
    def analisa_add_comment(analisa_id):
        """Add comment to analysis case"""
        try:
            data = request.get_json()
            
            user = data.get('user')
            comment = data.get('comment')
            
            if not user or not comment:
                return jsonify({'error': 'Missing user or comment'}), 400
            
            db = get_db()
            cursor = db.cursor()
            
            now = datetime.now().isoformat()
            
            cursor.execute('''
                INSERT INTO analisa_comments (analisa_id, user, comment, created_at)
                VALUES (?, ?, ?, ?)
            ''', (analisa_id, user, comment, now))
            
            # Add activity log
            cursor.execute('''
                INSERT INTO analisa_activity (analisa_id, action, user, icon, created_at)
                VALUES (?, 'commented', ?, 'message-circle', ?)
            ''', (analisa_id, user, now))
            
            db.commit()
            
            return jsonify({
                'success': True,
                'message': 'Comment added'
            })
            
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/analisa/<int:analisa_id>/activity')
    def analisa_activity(analisa_id):
        """Get activity log for analysis case"""
        try:
            db = get_db()
            cursor = db.cursor()
            
            cursor.execute('''
                SELECT * FROM analisa_activity
                WHERE analisa_id = ?
                ORDER BY created_at DESC
            ''', (analisa_id,))
            
            rows = cursor.fetchall()
            
            data = []
            for row in rows:
                data.append({
                    'id': row['id'],
                    'action': row['action'],
                    'user': row['user'],
                    'icon': row['icon'],
                    'created_at': row['created_at']
                })
            
            return jsonify(data)
            
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    print("✅ Analisa routes registered")
