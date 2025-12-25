"""
MainBill File Processor
Handles MainBill file processing
"""

import pandas as pd
from processors.base import BaseProcessor
from core.helpers import clean_date, clean_nomen

class MainBillProcessor(BaseProcessor):
    """MainBill file processor"""
    
    def __init__(self, db):
        # Inisialisasi hanya dengan koneksi database sesuai BaseProcessor yang baru
        super().__init__(db)

    def process(self, filepath, periode_bulan, periode_tahun):
        """Process MainBill file dengan parameter dinamis dari Auto-Detect"""
        
        # 1. Baca file menggunakan method dari BaseProcessor
        self.read_file(filepath)
        
        # 2. Pemetaan kolom (Column mapping)
        col_map = {
            'NOMEN': 'nomen',
            'NO_PLGGN': 'nomen',
            'NOPEL': 'nomen',
            'TGL_TAGIHAN': 'tgl_tagihan',
            'TANGGAL': 'tgl_tagihan',
            'TOTAL_TAGIHAN': 'total_tagihan',
            'NOMINAL': 'total_tagihan',
            'PCEZBK': 'pcezbk',
            'TARIF': 'tarif',
            'KODETARIF': 'tarif',
            'PERIODE': 'periode'
        }
        
        # Rename kolom yang ditemukan di file
        self.df = self.df.rename(columns={k: v for k, v in col_map.items() if k in self.df.columns})
        
        # 3. Validasi kolom wajib
        if 'nomen' not in self.df.columns:
            raise Exception('MainBill: Memerlukan kolom nomen')
        
        # 4. Pembersihan Data
        # Membersihkan kolom nomen menggunakan helper dari base class
        self.clean_nomen_column()
        
        # Penanganan Tanggal
        if 'tgl_tagihan' in self.df.columns:
            self.df['tgl_tagihan'] = self.df['tgl_tagihan'].apply(clean_date)
        else:
            self.df['tgl_tagihan'] = ''
        
        # 5. Pengisian Nilai Default & Konversi Numerik
        if 'total_tagihan' not in self.df.columns:
            self.df['total_tagihan'] = 0
        else:
            self.df['total_tagihan'] = pd.to_numeric(self.df['total_tagihan'], errors='coerce').fillna(0)
        
        for col in ['pcezbk', 'tarif']:
            if col not in self.df.columns:
                self.df[col] = ''
        
        # Jika kolom periode tidak ada di file, gunakan label dari hasil deteksi
        if 'periode' not in self.df.columns:
            self.df['periode'] = f"{periode_bulan:02d}/{periode_tahun}"
        
        # 6. Penambahan Metadata dari hasil Auto-Detect
        self.df['periode_bulan'] = periode_bulan
        self.df['periode_tahun'] = periode_tahun
        
        # 7. Simpan ke database 'mainbill'
        # Catatan: Kolom upload_id dihilangkan atau diisi null jika tidak digunakan dalam Auto-Detect
        cols_db = ['nomen', 'tgl_tagihan', 'total_tagihan', 'pcezbk', 'tarif',
                   'periode_bulan', 'periode_tahun', 'periode']
        
        # Simpan ke tabel mainbill menggunakan koneksi db dari base class
        self.df[cols_db].to_sql('mainbill', self.db, if_exists='append', index=False)
        
        return len(self.df)
