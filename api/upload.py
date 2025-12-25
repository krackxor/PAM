import os
from flask import jsonify, request, current_app
from werkzeug.utils import secure_filename
from processors import ProcessorFactory
# Pastikan file auto_detect_periode.py ada di folder yang bisa diakses (misal root atau modul processors)
from auto_detect_periode import auto_detect_periode

def register_upload_routes(app, get_db):
    
    @app.route('/api/upload', methods=['POST'])
    def upload_file():
        if 'file' not in request.files:
            return jsonify({'error': 'No file part'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No selected file'}), 400

        # Ambil file_type dari form jika ada (manual), jika tidak ada pakai None
        file_type = request.form.get('file_type')
        if file_type == "": file_type = None

        if file:
            filename = secure_filename(file.filename)
            upload_folder = current_app.config['UPLOAD_FOLDER']
            
            if not os.path.exists(upload_folder):
                os.makedirs(upload_folder)
                
            filepath = os.path.join(upload_folder, filename)
            file.save(filepath)

            try:
                # 1. JALANKAN AUTO-DETECT (Gunakan script Anda)
                # Ini akan mendeteksi Tipe File dan Periode sekaligus
                detection = auto_detect_periode(filepath, filename, file_type)
                
                if not detection:
                    return jsonify({'error': 'Sistem gagal mendeteksi tipe file atau periode. Gunakan format file yang sesuai.'}), 400
                
                # Gunakan hasil deteksi
                detected_type = detection['file_type']
                bulan = detection['periode_bulan']
                tahun = detection['periode_tahun']
                label = detection['periode_label']

                # 2. PROSES KE DATABASE
                db = get_db()
                processor = ProcessorFactory.get_processor(detected_type, db)
                
                if not processor:
                    return jsonify({'error': f'Processor untuk tipe {detected_type} tidak ditemukan'}), 400
                
                # Jalankan proses dengan periode hasil deteksi
                result = processor.process(filepath, bulan, tahun)
                
                return jsonify({
                    'success': True,
                    'message': f'Berhasil mengunggah data {detected_type} periode {label}',
                    'details': result,
                    'detection_method': detection['method']
                })

            except Exception as e:
                import traceback
                traceback.print_exc()
                return jsonify({'error': f'Gagal memproses file: {str(e)}'}), 500
            finally:
                # Opsional: hapus file setelah diproses agar hemat storage
                # if os.path.exists(filepath): os.remove(filepath)
                pass

    print("✅ Upload API (Auto-Detect Content Version) registered")
