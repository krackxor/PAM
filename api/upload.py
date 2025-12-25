"""
Upload API Endpoints
Handles file uploads and delegates to processors
"""

import os
from datetime import datetime
from flask import jsonify, request
from werkzeug.utils import secure_filename

from processors.mc_processor import MCProcessor
from processors.collection_processor import CollectionProcessor
from processors.sbrs_processor import SBRSProcessor
from processors.mb_processor import MBProcessor
from processors.mainbill_processor import MainBillProcessor
from processors.ardebt_processor import ArdebtProcessor
from processors.auto_detect import auto_detect_periode

ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls', 'txt'}

def allowed_file(filename):
    """Check if file extension is allowed"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def register_upload_routes(app, get_db):
    """Register upload routes"""
    
    @app.route('/api/upload', methods=['POST'])
    def upload_file():
        """Handle file upload"""
        try:
            # Validate request
            if 'file' not in request.files:
                return jsonify({'error': 'No file uploaded'}), 400
            
            file = request.files['file']
            file_type = request.form.get('fileType')
            
            if not file or file.filename == '':
                return jsonify({'error': 'No file selected'}), 400
            
            if not file_type:
                return jsonify({'error': 'File type not specified'}), 400
            
            if not allowed_file(file.filename):
                return jsonify({'error': 'Invalid file type'}), 400
            
            # Save file
            filename = secure_filename(file.filename)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            unique_filename = f"{timestamp}_{filename}"
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], unique_filename)
            file.save(filepath)
            
            # Auto-detect periode
            periode_info = auto_detect_periode(filepath, file_type)
            periode_bulan = periode_info.get('bulan')
            periode_tahun = periode_info.get('tahun')
            
            if not periode_bulan or not periode_tahun:
                return jsonify({
                    'error': 'Cannot detect periode from file',
                    'suggestion': 'Please include PERIODE column in your file'
                }), 400
            
            # Get database connection
            db = get_db()
            cursor = db.cursor()
            
            # Create upload metadata
            cursor.execute('''
                INSERT INTO upload_metadata 
                (file_type, file_name, periode_bulan, periode_tahun, row_count, status)
                VALUES (?, ?, ?, ?, 0, 'processing')
            ''', (file_type, filename, periode_bulan, periode_tahun))
            
            upload_id = cursor.lastrowid
            db.commit()
            
            # Process file based on type
            processor_map = {
                'mc': MCProcessor,
                'collection': CollectionProcessor,
                'sbrs': SBRSProcessor,
                'mb': MBProcessor,
                'mainbill': MainBillProcessor,
                'ardebt': ArdebtProcessor
            }
            
            if file_type not in processor_map:
                return jsonify({'error': f'Unsupported file type: {file_type}'}), 400
            
            ProcessorClass = processor_map[file_type]
            processor = ProcessorClass(filepath, upload_id, periode_bulan, periode_tahun, db)
            row_count = processor.execute()
            
            # Update metadata
            cursor.execute('''
                UPDATE upload_metadata 
                SET row_count = ?, status = 'success'
                WHERE id = ?
            ''', (row_count, upload_id))
            db.commit()
            
            return jsonify({
                'success': True,
                'message': f'{file_type.upper()} file processed successfully',
                'upload_id': upload_id,
                'row_count': row_count,
                'periode': f"{periode_bulan}/{periode_tahun}"
            })
            
        except Exception as e:
            # Update status to failed
            try:
                cursor.execute('''
                    UPDATE upload_metadata 
                    SET status = 'failed'
                    WHERE id = ?
                ''', (upload_id,))
                db.commit()
            except:
                pass
            
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/upload/history')
    def upload_history():
        """Get upload history"""
        try:
            db = get_db()
            cursor = db.cursor()
            
            cursor.execute('''
                SELECT 
                    id,
                    file_type,
                    file_name,
                    periode_bulan,
                    periode_tahun,
                    upload_date,
                    row_count,
                    status
                FROM upload_metadata
                ORDER BY upload_date DESC
                LIMIT 50
            ''')
            
            rows = cursor.fetchall()
            
            history = []
            for row in rows:
                history.append({
                    'id': row['id'],
                    'file_type': row['file_type'],
                    'file_name': row['file_name'],
                    'periode': f"{row['periode_bulan']}/{row['periode_tahun']}",
                    'upload_date': row['upload_date'],
                    'row_count': row['row_count'],
                    'status': row['status']
                })
            
            return jsonify(history)
            
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    print("✅ Upload routes registered")
