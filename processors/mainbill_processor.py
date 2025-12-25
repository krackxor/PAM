"""
MainBill File Processor
Handles MainBill file processing
"""

import pandas as pd
from processors.base import BaseProcessor
from core.helpers import clean_date

class MainBillProcessor(BaseProcessor):
    """MainBill file processor"""
    
    def process(self):
        """Process MainBill file"""
        
        # Column mapping
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
        
        self.df = self.df.rename(columns={k: v for k, v in col_map.items() if k in self.df.columns})
        
        # Validate
        if 'nomen' not in self.df.columns:
            raise Exception('MainBill: Need nomen column')
        
        # Clean data
        self.clean_nomen_column()
        
        # Date handling
        if 'tgl_tagihan' in self.df.columns:
            self.df['tgl_tagihan'] = self.df['tgl_tagihan'].apply(clean_date)
        else:
            self.df['tgl_tagihan'] = ''
        
        # Fill defaults
        if 'total_tagihan' not in self.df.columns:
            self.df['total_tagihan'] = 0
        else:
            self.df['total_tagihan'] = pd.to_numeric(self.df['total_tagihan'], errors='coerce').fillna(0)
        
        for col in ['pcezbk', 'tarif']:
            if col not in self.df.columns:
                self.df[col] = ''
        
        if 'periode' not in self.df.columns:
            self.df['periode'] = f"{self.periode_bulan:02d}/{self.periode_tahun}"
        
        # Add metadata
        self.add_metadata()
        
        # Save to mainbill
        cols_db = ['nomen', 'tgl_tagihan', 'total_tagihan', 'pcezbk', 'tarif',
                   'periode_bulan', 'periode_tahun', 'upload_id', 'periode']
        
        self.df[cols_db].to_sql('mainbill', self.db, if_exists='append', index=False)
        
        return len(self.df)
