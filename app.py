# ========================
# File: app.py
# Deskripsi: Inisialisasi Aplikasi dan Pendaftaran Blueprint
# ========================

from flask import Flask, redirect, url_for

# 1. Import Blueprint yang akan kita buat
from collections import collections_bp 

def create_app():
    app = Flask(__name__)
    app.config['SECRET_KEY'] = 'kunci_rahasia_anda' # Ganti dengan kunci yang kuat

    # 2. Daftarkan Blueprint Koleksi
    # Semua rute di collections_bp akan diawali dengan /collections
    app.register_blueprint(collections_bp, url_prefix='/collections')

    # Rute default (root /)
    @app.route('/')
    def index():
        # Arahkan pengguna langsung ke halaman daftar koleksi
        return redirect(url_for('collections.list_collections'))

    return app

if __name__ == '__main__':
    app = create_app()
    app.run(debug=True)
