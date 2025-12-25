"""
SBRS File Processor
Handles SBRS data with correct column mapping
"""

import pandas as pd
from processors.base import BaseProcessor
from core.helpers import clean_nomen

class SBRSProcessor(BaseProcessor):
    """SBRS file processor"""
    
    def __init__(self, db):
        # Inisialisasi hanya dengan koneksi database sesuai BaseProcessor terbaru
        super().__init__(db)

    def process(self, filepath, periode_bulan, periode_tahun):
        """Process SBRS file dengan parameter dinamis dari Auto-Detect"""
        
        # 1. Baca file menggunakan method dari BaseProcessor
        self.read_file(filepath)
        
        # 2. Pemetaan kolom (flexible search)
        col_map = {}
        
        # Nomen mapping
        for col in ['CMR_ACCOUNT', 'NOMEN', 'NO_PELANGGAN', 'ACCOUNT']:
            if col in self.df.columns:
                col_map[col] = 'nomen'
                break
        
        # Volume mapping (CRITICAL FIX)
        for col in ['SB_STAND', 'VOLUME', 'PAKAI', 'STAND', 'PEMAKAIAN']:
            if col in self.df.columns:
                col_map[col] = 'volume'
                break
        
        # Name mapping
        for col in ['CMR_NAME', 'NAMA', 'NAMA_PELANGGAN']:
            if col in self.df.columns:
                col_map[col] = 'nama'
                break
        
        # Rayon mapping
        for col in ['CMR_ROUTE', 'RAYON', 'RUTE']:
            if col in self.df.columns:
                col_map[col] = 'rayon'
                break
        
        # Additional columns
        extra_map = {
            'CMR_ADDRESS': 'alamat',
            'ALAMAT': 'alamat',
            'READMETHOD': 'readmethod',
            'SKIPSTS': 'skip_status',
            'TROUBLESTS': 'trouble_status',
            'SPMSTS': 'spm_status',
            'STAND_AWAL': 'stand_awal',
            'STAND_AKHIR': 'stand_akhir',
            'ANALISA_TINDAK_LANJUT': 'analisa_tindak_lanjut',
            'TAG1': 'tag1',
            'TAG2': 'tag2'
        }
        
        for k, v in extra_map.items():
            if k in self.df.columns:
                col_map[k] = v
        
        # Apply mapping
        self.df = self.df.rename(columns=col_map)
        
        # 3. Validasi kolom kritis
        if 'nomen' not in self.df.columns or 'volume' not in self.df.columns:
            raise Exception('SBRS: Kolom nomen (CMR_ACCOUNT) dan volume (SB_STAND) wajib tersedia.')
        
        # 4. Pembersihan Data
        # Bersihkan nomen menggunakan helper (sudah diwarisi dari BaseProcessor atau import langsung)
        self.df['nomen'] = self.df['nomen'].apply(clean_nomen)
        self.df = self.df.dropna(subset=['nomen'])
        self.df = self.df[self.df['nomen'] != '']
        
        # Konversi volume ke numerik
        self.df['volume'] = pd.to_numeric(self.df['volume'], errors='coerce').fillna(0)
        
        # Isi kolom opsional dengan nilai default
        optional_cols = ['nama', 'alamat', 'rayon', 'readmethod', 'skip_status', 
                        'trouble_status', 'spm_status', 'analisa_tindak_lanjut', 'tag1', 'tag2']
        for col in optional_cols:
            if col not in self.df.columns:
                self.df[col] = ''
        
        for col in ['stand_awal', 'stand_akhir']:
            if col not in self.df.columns:
                self.df[col] = 0
            else:
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce').fillna(0)
        
        # 5. Penambahan Metadata dari Auto-Detect
        self.df['periode_bulan'] = periode_bulan
        self.df['periode_tahun'] = periode_tahun
        
        # 6. Simpan ke Database
        # Catatan: Kolom upload_id opsional jika tidak dikelola oleh sistem Auto-Detect
        cols_db = ['nomen', 'nama', 'alamat', 'rayon', 'readmethod', 'skip_status',
                   'trouble_status', 'spm_status', 'stand_awal', 'stand_akhir', 'volume',
                   'analisa_tindak_lanjut', 'tag1', 'tag2', 
                   'periode_bulan', 'periode_tahun']
        
        # Menyaring hanya kolom yang ada di database
        final_df = self.df[[c for c in cols_db if c in self.df.columns]]
        final_df.to_sql('sbrs_data', self.db, if_exists='append', index=False)
        
        print(f"✅ SBRS diproses: {len(self.df)} baris (Periode: {periode_bulan}/{periode_tahun})")
        return len(self.df)
