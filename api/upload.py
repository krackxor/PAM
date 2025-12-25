import os
import pandas as pd
from flask import jsonify, request, current_app
from werkzeug.utils import secure_filename
from processors import ProcessorFactory
from datetime import datetime

def register_upload_routes(app, get_db):
    
    @app.route('/api/upload', methods=['POST'])
    def upload_file():
        if 'file' not in request.files:
            return jsonify({'error': 'No file part'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No selected file'}), 400

        filename = secure_filename(file.filename)
        # Mengambil UPLOAD_FOLDER dari config.py
        upload_folder = current_app.config.get('UPLOAD_FOLDER', 'uploads')
        
        if not os.path.exists(upload_folder):
            os.makedirs(upload_folder)
            
        filepath = os.path.join(upload_folder, filename)
        file.save(filepath)

        try:
            # --- LOGIKA INTERNAL AUTO DETECT (Content & Filename) ---
            # Membaca header file untuk menentukan tipe
            if filename.lower().endswith('.csv'):
                df_sample = pd.read_csv(filepath, nrows=5)
            elif filename.lower().endswith(('.xls', '.xlsx')):
                df_sample = pd.read_excel(filepath, nrows=5)
            else:
                return jsonify({'error': 'Format file tidak didukung'}), 400
            
            cols = [c.upper().strip() for c in df_sample.columns]
            filename_upper = filename.upper()
            detected_type = None

            # A. Deteksi Tipe Berdasarkan Kolom & Nama
            if 'SBRS' in filename_upper or 'CMR_ACCOUNT' in cols or 'SB_STAND' in cols:
                detected_type = 'SBRS'
            elif 'COLLECTION' in filename_upper or 'AMT_COLLECT' in cols or 'PAY_DT' in cols:
                detected_type = 'COLLECTION'
            elif 'MC' in filename_upper or 'ZONA_NOVAK' in cols:
                detected_type = 'MC'
            elif 'MB' in filename_upper or 'TGL_BAYAR' in cols:
                detected_type = 'MB'
            elif 'ARDEBT' in filename_upper or 'SALDO' in cols:
                detected_type = 'ARDEBT'
            elif 'MAINBILL' in filename_upper or 'TOTAL_TAGIHAN' in cols:
                detected_type = 'MAINBILL'

            if not detected_type:
                return jsonify({'error': 'Sistem tidak mengenali tipe file ini secara otomatis.'}), 400

            # B. Deteksi Periode (Logika Bisnis PDAM)
            # Default ke bulan/tahun sekarang
            now = datetime.now()
            bulan, tahun = now.month, now.year
            
            # Khusus MC, MB, ARDEBT: Offset +1 bulan sesuai aturan bisnis
            if detected_type in ['MC', 'MB', 'ARDEBT']:
                bulan += 1
                if bulan > 12:
                    bulan = 1
                    tahun += 1

            # --- EKSEKUSI PROCESSOR ---
            db = get_db()
            # Mendapatkan class processor dari ProcessorFactory
            processor = ProcessorFactory.get_processor(detected_type, db)
            
            if not processor:
                return jsonify({'error': f'Processor untuk {detected_type} tidak ditemukan'}), 400
            
            # Memproses file ke database
            result = processor.process(filepath, bulan, tahun)
            
            return jsonify({
                'success': True,
                'message': f'Berhasil: File {detected_type} terdeteksi untuk periode {bulan}/{tahun}',
                'details': result
            })

        except Exception as e:
            if os.path.exists(filepath):
                os.remove(filepath)
            return jsonify({'error': f'Gagal memproses file: {str(e)}'}), 500

    print("✅ Upload API with Internal Auto-Detect registered")
