"""
MB (Master Bayar) File Processor
Handles MB file processing
"""

import pandas as pd
from processors.base import BaseProcessor
from core.helpers import clean_date, clean_nomen

class MBProcessor(BaseProcessor):
    """MB file processor"""
    
    def __init__(self, db):
        # Inisialisasi hanya dengan koneksi database sesuai BaseProcessor yang baru
        super().__init__(db)

    def process(self, filepath, periode_bulan, periode_tahun):
        """Process MB file dengan parameter dinamis dari Auto-Detect"""
        
        # 1. Baca file menggunakan method dari BaseProcessor
        self.read_file(filepath)
        
        # 2. Pemetaan kolom (Column mapping)
        col_map = {
            'NO_PLGGN': 'nomen',
            'NOPEL': 'nomen',
            'NOMEN': 'nomen',
            'TGL_BAYAR': 'tgl_bayar',
            'TANGGAL': 'tgl_bayar',
            'JML_BAYAR': 'jumlah_bayar',
            'JUMLAH': 'jumlah_bayar',
            'NOMINAL': 'jumlah_bayar',
            'PERIODE': 'periode'
        }
        
        # Rename kolom yang ditemukan di file
        self.df = self.df.rename(columns={k: v for k, v in col_map.items() if k in self.df.columns})
        
        # 3. Validasi
        if 'nomen' not in self.df.columns or 'tgl_bayar' not in self.df.columns:
            raise Exception('MB: Memerlukan kolom nomen dan tgl_bayar')
        
        # 4. Pembersihan Data
        # Membersihkan kolom nomen menggunakan helper dari base class
        self.clean_nomen_column()
        
        # Pembersihan tanggal
        self.df['tgl_bayar'] = self.df['tgl_bayar'].apply(clean_date)
        
        # 5. Pengisian Nilai Default & Konversi Numerik
        if 'jumlah_bayar' not in self.df.columns:
            self.df['jumlah_bayar'] = 0
        else:
            self.df['jumlah_bayar'] = pd.to_numeric(self.df['jumlah_bayar'], errors='coerce').fillna(0)
        
        # Jika kolom periode tidak ada di file, gunakan label dari hasil deteksi
        if 'periode' not in self.df.columns:
            self.df['periode'] = f"{periode_bulan:02d}/{periode_tahun}"
        
        # 6. Penambahan Metadata dari hasil Auto-Detect
        self.df['periode_bulan'] = periode_bulan
        self.df['periode_tahun'] = periode_tahun
        self.df['sumber_file'] = 'mb'
        
        # 7. Simpan ke database 'master_bayar'
        # Catatan: Kolom upload_id diisi null karena menggunakan sistem Auto-Detect
        cols_db = ['nomen', 'tgl_bayar', 'jumlah_bayar', 'periode_bulan', 'periode_tahun', 
                   'periode', 'sumber_file']
        
        # Simpan ke tabel master_bayar menggunakan koneksi db dari base class
        self.df[cols_db].to_sql('master_bayar', self.db, if_exists='append', index=False)
        
        return len(self.df)
