"""
SBRS File Processor
Handles SBRS data with correct column mapping
"""

import pandas as pd
from processors.base import BaseProcessor

class SBRSProcessor(BaseProcessor):
    """SBRS file processor"""
    
    def process(self):
        """Process SBRS file"""
        
        # Column mapping (flexible search)
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
        
        # Validate critical columns
        if 'nomen' not in self.df.columns or 'volume' not in self.df.columns:
            raise Exception('SBRS: Need nomen and volume columns')
        
        # Clean nomen
        self.clean_nomen_column()
        
        # Convert volume to numeric
        self.df['volume'] = pd.to_numeric(self.df['volume'], errors='coerce').fillna(0)
        
        # Fill missing columns with defaults
        for col in ['nama', 'alamat', 'rayon', 'readmethod', 'skip_status', 
                    'trouble_status', 'spm_status', 'analisa_tindak_lanjut', 'tag1', 'tag2']:
            if col not in self.df.columns:
                self.df[col] = ''
        
        for col in ['stand_awal', 'stand_akhir']:
            if col not in self.df.columns:
                self.df[col] = 0
            else:
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce').fillna(0)
        
        # Add metadata
        self.add_metadata()
        
        # Save to database (dengan kolom yang benar)
        cols_db = ['nomen', 'nama', 'alamat', 'rayon', 'readmethod', 'skip_status',
                   'trouble_status', 'spm_status', 'stand_awal', 'stand_akhir', 'volume',
                   'analisa_tindak_lanjut', 'tag1', 'tag2', 
                   'periode_bulan', 'periode_tahun', 'upload_id']
        
        self.df[cols_db].to_sql('sbrs_data', self.db, if_exists='append', index=False)
        
        print(f"✅ SBRS saved with correct columns: nomen, volume (not cmr_account, SB_Stand)")
        return len(self.df)
