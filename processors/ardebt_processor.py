"""
Ardebt File Processor
Handles Ardebt (AR Debt) file processing
"""

import pandas as pd
from processors.base import BaseProcessor

class ArdebtProcessor(BaseProcessor):
    """Ardebt file processor"""
    
    def process(self):
        """Process Ardebt file"""
        
        # Column mapping
        col_map = {
            'NOMEN': 'nomen',
            'NO_PLGGN': 'nomen',
            'NOPEL': 'nomen',
            'SALDO_TUNGGAKAN': 'saldo_tunggakan',
            'SALDO': 'saldo_tunggakan',
            'TUNGGAKAN': 'saldo_tunggakan',
            'PERIODE': 'periode'
        }
        
        self.df = self.df.rename(columns={k: v for k, v in col_map.items() if k in self.df.columns})
        
        # Validate
        if 'nomen' not in self.df.columns:
            raise Exception('Ardebt: Need nomen column')
        
        # Clean data
        self.clean_nomen_column()
        
        # Fill defaults
        if 'saldo_tunggakan' not in self.df.columns:
            self.df['saldo_tunggakan'] = 0
        else:
            self.df['saldo_tunggakan'] = pd.to_numeric(self.df['saldo_tunggakan'], errors='coerce').fillna(0)
        
        if 'periode' not in self.df.columns:
            self.df['periode'] = f"{self.periode_bulan:02d}/{self.periode_tahun}"
        
        # Add metadata
        self.add_metadata()
        
        # Save to ardebt
        cols_db = ['nomen', 'saldo_tunggakan', 'periode_bulan', 'periode_tahun', 
                   'upload_id', 'periode']
        
        self.df[cols_db].to_sql('ardebt', self.db, if_exists='append', index=False)
        
        return len(self.df)
