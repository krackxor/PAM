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
        upload_folder = current_app.config.get('UPLOAD_FOLDER', 'uploads')
        
        if not os.path.exists(upload_folder):
            os.makedirs(upload_folder)
            
        filepath = os.path.join(upload_folder, filename)
        file.save(filepath)

        try:
            # --- LOGIKA AUTO DETECT LANGSUNG DI SINI ---
            # Baca sedikit data untuk deteksi kolom
            if filename.endswith('.csv'):
                df_sample = pd.read_csv(filepath, nrows=5)
            else:
                df_sample = pd.read_excel(filepath, nrows=5)
            
            cols = [c.upper().strip() for c in df_sample.columns]
            filename_upper = filename.upper()
            detected_type = None

            # 1. Deteksi Tipe Berdasarkan Nama File & Kolom
            if 'SBRS' in filename_upper or 'CMR_ACCOUNT' in cols or 'SB_STAND' in cols:
                detected_type = 'SBRS'
            elif 'COLLECTION' in filename_upper or 'AMT_COLLECT' in cols or 'PAY_DT' in cols:
                detected_type = 'COLLECTION'
            elif 'MC' in filename_upper or 'MASTER' in filename_upper or 'ZONA_NOVAK' in cols:
                detected_type = 'MC'
            elif 'MB' in filename_upper or 'TGL_BAYAR' in cols:
                detected_type = 'MB'
            elif 'ARDEBT' in filename_upper or 'SALDO' in cols:
                detected_type = 'ARDEBT'
            elif 'MAINBILL' in filename_upper or 'TOTAL_TAGIHAN' in cols:
                detected_type = 'MAINBILL'

            if not detected_type:
                return jsonify({'error': 'Gagal mendeteksi tipe file otomatis. Pastikan nama file atau kolom sesuai.'}), 400

            # 2. Deteksi Periode (Default ke bulan/tahun sekarang)
            now = datetime.now()
            bulan = now.month
            tahun = now.year

            # --- PROSES KE DATABASE ---
            db = get_db()
            processor = ProcessorFactory.get_processor(detected_type, db)
            
            if not processor:
                return jsonify({'error': f'Processor untuk tipe {detected_type} tidak ditemukan'}), 400
            
            # Jalankan proses (Gunakan bulan/tahun hasil deteksi atau default)
            result = processor.process(filepath, bulan, tahun)
            
            return jsonify({
                'success': True,
                'message': f'Berhasil mendeteksi dan mengunggah data {detected_type}',
                'detected_as': detected_type,
                'periode': f"{bulan}/{tahun}",
                'details': result
            })

        except Exception as e:
            if os.path.exists(filepath):
                os.remove(filepath)
            return jsonify({'error': f'Gagal memproses file: {str(e)}'}), 500

    print("✅ Upload API (Internal Auto-Detect) registered")
