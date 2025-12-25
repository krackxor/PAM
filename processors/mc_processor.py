"""
MC (Master Customer) File Processor
Handles MC file processing and database insertion
"""

import pandas as pd
from processors.base import BaseProcessor
from core.helpers import parse_zona_novak, clean_nomen

class MCProcessor(BaseProcessor):
    """MC file processor"""
    
    def __init__(self, db):
        # Inisialisasi hanya dengan koneksi database sesuai BaseProcessor yang baru
        super().__init__(db)

    def process(self, filepath, periode_bulan, periode_tahun):
        """Process MC file dengan parameter dinamis dari Auto-Detect"""
        
        # 1. Baca file menggunakan method dari BaseProcessor
        self.read_file(filepath)
        
        # 2. Validasi kolom wajib sebelum rename
        if 'ZONA_NOVAK' not in self.df.columns or 'NOMEN' not in self.df.columns:
            raise Exception('MC: Memerlukan kolom ZONA_NOVAK dan NOMEN')
        
        # 3. Pemetaan kolom (Column mapping)
        rename_dict = {'NOMEN': 'nomen'}
        
        # Nama Pelanggan
        if 'NAMA_PEL' in self.df.columns:
            rename_dict['NAMA_PEL'] = 'nama'
        elif 'NAMA' in self.df.columns:
            rename_dict['NAMA'] = 'nama'
        
        # Alamat
        if 'ALM1_PEL' in self.df.columns:
            rename_dict['ALM1_PEL'] = 'alamat'
        elif 'ALAMAT' in self.df.columns:
            rename_dict['ALAMAT'] = 'alamat'
        
        # Tarif
        if 'TARIF' in self.df.columns:
            rename_dict['TARIF'] = 'tarif'
        elif 'KODETARIF' in self.df.columns:
            rename_dict['KODETARIF'] = 'tarif'
        
        # Target MC (Nominal Rupiah)
        if 'NOMINAL' in self.df.columns:
            rename_dict['NOMINAL'] = 'target_mc'
        elif 'REK_AIR' in self.df.columns:
            rename_dict['REK_AIR'] = 'target_mc'
        
        # Kubikasi (Volume)
        if 'KUBIK' in self.df.columns:
            rename_dict['KUBIK'] = 'kubikasi'
        elif 'KUBIKASI' in self.df.columns:
            rename_dict['KUBIKASI'] = 'kubikasi'
        
        self.df = self.df.rename(columns=rename_dict)
        
        # 4. Pembersihan Data
        # Bersihkan nomen menggunakan helper
        self.df['nomen'] = self.df['nomen'].apply(clean_nomen)
        self.df = self.df.dropna(subset=['nomen'])
        self.df = self.df[self.df['nomen'] != '']
        
        # 5. Parsing ZONA_NOVAK
        self.df['zona_novak'] = self.df['ZONA_NOVAK'].astype(str).str.strip()
        zona_parsed = self.df['zona_novak'].apply(parse_zona_novak)
        
        self.df['rayon'] = zona_parsed.apply(lambda x: x['rayon'])
        self.df['pc'] = zona_parsed.apply(lambda x: x['pc'])
        self.df['ez'] = zona_parsed.apply(lambda x: x['ez'])
        self.df['block'] = zona_parsed.apply(lambda x: x['block'])
        self.df['pcez'] = zona_parsed.apply(lambda x: x['pcez'])
        
        # Filter khusus Rayon 34 dan 35 (Sunter)
        self.df = self.df[self.df['rayon'].isin(['34', '35'])]
        
        if len(self.df) == 0:
            raise Exception('Data Rayon 34/35 tidak ditemukan dalam file ini.')
        
        # 6. Pengisian Nilai Default
        for col in ['nama', 'alamat', 'tarif']:
            if col not in self.df.columns:
                self.df[col] = ''
        
        if 'target_mc' not in self.df.columns:
            self.df['target_mc'] = 0
        
        if 'kubikasi' not in self.df.columns:
            self.df['kubikasi'] = 0
        else:
            self.df['kubikasi'] = pd.to_numeric(self.df['kubikasi'], errors='coerce').apply(
                lambda x: abs(float(x)) if pd.notna(x) else 0
            )
        
        # 7. Metadata dari Auto-Detect
        self.df['periode_bulan'] = periode_bulan
        self.df['periode_tahun'] = periode_tahun
        
        # 8. Simpan ke Database
        # Menggunakan 'replace' karena MC biasanya data master yang diperbarui tiap bulan
        cols_db = ['nomen', 'nama', 'alamat', 'rayon', 'pc', 'ez', 'pcez', 'block', 
                   'zona_novak', 'tarif', 'target_mc', 'kubikasi', 
                   'periode_bulan', 'periode_tahun']
        
        self.df[cols_db].to_sql('master_pelanggan', self.db, if_exists='replace', index=False)
        
        return len(self.df)
