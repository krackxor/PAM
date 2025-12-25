"""
Collection File Processor
Handles collection_harian processing
"""

import pandas as pd
from processors.base import BaseProcessor
from core.helpers import clean_date

class CollectionProcessor(BaseProcessor):
    """Collection file processor"""
    
    def process(self):
        """Process collection file"""
        
        # Map columns
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
        
        self.df = self.df.rename(columns={k: v for k, v in col_map.items() if k in self.df.columns})
        
        # Validate
        if 'nomen' not in self.df.columns or 'tgl_bayar' not in self.df.columns:
            raise Exception('Collection: Need nomen and tgl_bayar columns')
        
        # Clean data
        self.clean_nomen_column()
        self.df['tgl_bayar'] = self.df['tgl_bayar'].apply(clean_date)
        
        # Fill defaults
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
        
        # Classify payment type
        self.df['tipe_bayar'] = self.df.apply(
            lambda row: 'tunggakan' if row['jumlah_bayar'] > 0 and row['volume_air'] == 0 else 'current',
            axis=1
        )
        
        # Add metadata
        self.add_metadata()
        self.df['sumber_file'] = 'collection'
        
        # Save to database
        cols_db = ['nomen', 'tgl_bayar', 'jumlah_bayar', 'volume_air', 'tipe_bayar', 
                   'bill_period', 'periode_bulan', 'periode_tahun', 'upload_id', 'sumber_file']
        
        # Use INSERT OR IGNORE for duplicates
        cursor = self.db.cursor()
        for _, row in self.df[cols_db].iterrows():
            cursor.execute('''
                INSERT OR IGNORE INTO collection_harian 
                (nomen, tgl_bayar, jumlah_bayar, volume_air, tipe_bayar, bill_period, 
                 periode_bulan, periode_tahun, upload_id, sumber_file)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', tuple(row))
        
        self.db.commit()
        return len(self.df)
