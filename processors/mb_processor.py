"""
MB (Master Bayar) File Processor
Handles MB file processing
"""

import pandas as pd
from processors.base import BaseProcessor
from core.helpers import clean_date

class MBProcessor(BaseProcessor):
    """MB file processor"""
    
    def process(self):
        """Process MB file"""
        
        # Column mapping
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
        
        self.df = self.df.rename(columns={k: v for k, v in col_map.items() if k in self.df.columns})
        
        # Validate
        if 'nomen' not in self.df.columns or 'tgl_bayar' not in self.df.columns:
            raise Exception('MB: Need nomen and tgl_bayar columns')
        
        # Clean data
        self.clean_nomen_column()
        self.df['tgl_bayar'] = self.df['tgl_bayar'].apply(clean_date)
        
        # Fill defaults
        if 'jumlah_bayar' not in self.df.columns:
            self.df['jumlah_bayar'] = 0
        else:
            self.df['jumlah_bayar'] = pd.to_numeric(self.df['jumlah_bayar'], errors='coerce').fillna(0)
        
        if 'periode' not in self.df.columns:
            self.df['periode'] = f"{self.periode_bulan:02d}/{self.periode_tahun}"
        
        # Add metadata
        self.add_metadata()
        self.df['sumber_file'] = 'mb'
        
        # Save to master_bayar
        cols_db = ['nomen', 'tgl_bayar', 'jumlah_bayar', 'periode_bulan', 'periode_tahun', 
                   'upload_id', 'periode', 'sumber_file']
        
        self.df[cols_db].to_sql('master_bayar', self.db, if_exists='append', index=False)
        
        return len(self.df)
