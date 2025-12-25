"""
Collection File Processor
Handles collection_harian processing
"""

import pandas as pd
from processors.base import BaseProcessor
from core.helpers import clean_date, clean_nomen

class CollectionProcessor(BaseProcessor):
    """Collection file processor"""
    
    def __init__(self, db):
        # Inisialisasi hanya dengan koneksi database sesuai BaseProcessor yang baru
        super().__init__(db)

    def process(self, filepath, periode_bulan, periode_tahun):
        """Process collection file dengan parameter dinamis dari Auto-Detect"""
        
        # 1. Baca file menggunakan method dari BaseProcessor
        self.read_file(filepath)
        
        # 2. Pemetaan kolom (Map columns)
        col_map = {
            'NO_PLGGN': 'nomen',
            'NOPEL': 'nomen',
            'TGL_BAYAR': 'tgl_bayar',
            'TANGGAL': 'tgl_bayar',
            'JML_BAYAR': 'jumlah_bayar',
            'JUMLAH': 'jumlah_bayar',
            'VOLUME_AIR': 'volume_air',
            'VOLUME': 'volume_air',
            'BILL_PERIOD': 'bill_period',
            'PERIODE': 'bill_period'
        }
        
        # Rename kolom yang ditemukan di file
        self.df = self.df.rename(columns={k: v for k, v in col_map.items() if k in self.df.columns})
        
        # 3. Validasi kolom wajib
        if 'nomen' not in self.df.columns and 'NOMEN' not in self.df.columns:
            # Jika rename gagal, coba pakai kolom asli jika ada
             if 'CMR_ACCOUNT' in self.df.columns:
                 self.df = self.df.rename(columns={'CMR_ACCOUNT': 'nomen'})
        
        if 'nomen' not in self.df.columns or 'tgl_bayar' not in self.df.columns:
            raise Exception('Collection: Kolom nomen dan tgl_bayar wajib tersedia.')
        
        # 4. Pembersihan Data
        # Membersihkan kolom nomen menggunakan helper
        self.df['nomen'] = self.df['nomen'].apply(clean_nomen)
        self.df = self.df.dropna(subset=['nomen'])
        self.df = self.df[self.df['nomen'] != '']
        
        # Membersihkan format tanggal
        self.df['tgl_bayar'] = self.df['tgl_bayar'].apply(clean_date)
        
        # 5. Pengisian Nilai Default & Konversi Numerik
        if 'jumlah_bayar' not in self.df.columns:
            self.df['jumlah_bayar'] = 0
        else:
            self.df['jumlah_bayar'] = pd.to_numeric(self.df['jumlah_bayar'], errors='coerce').fillna(0)
        
        if 'volume_air' not in self.df.columns:
            self.df['volume_air'] = 0
        else:
            self.df['volume_air'] = pd.to_numeric(self.df['volume_air'], errors='coerce').fillna(0)
        
        if 'bill_period' not in self.df.columns:
            self.df['bill_period'] = ''
        
        # 6. Klasifikasi Tipe Pembayaran
        self.df['tipe_bayar'] = self.df.apply(
            lambda row: 'tunggakan' if row['jumlah_bayar'] > 0 and row['volume_air'] == 0 else 'current',
            axis=1
        )
        
        # 7. Penambahan Metadata dari hasil Auto-Detect
        self.df['periode_bulan'] = periode_bulan
        self.df['periode_tahun'] = periode_tahun
        self.df['sumber_file'] = 'collection'
        
        # 8. Simpan ke Database menggunakan INSERT OR IGNORE
        cols_db = ['nomen', 'tgl_bayar', 'jumlah_bayar', 'volume_air', 'tipe_bayar', 
                   'bill_period', 'periode_bulan', 'periode_tahun', 'sumber_file']
        
        # Gunakan cursor dari BaseProcessor
        for _, row in self.df[cols_db].iterrows():
            self.cursor.execute('''
                INSERT OR IGNORE INTO collection_harian 
                (nomen, tgl_bayar, jumlah_bayar, volume_air, tipe_bayar, bill_period, 
                 periode_bulan, periode_tahun, sumber_file)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', tuple(row))
        
        self.db.commit()
        return len(self.df)
