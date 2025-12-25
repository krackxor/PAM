"""
MC (Master Customer) File Processor
Handles MC file processing and database insertion
"""

from processors.base import BaseProcessor
from core.helpers import parse_zona_novak

class MCProcessor(BaseProcessor):
    """MC file processor"""
    
    def process(self):
        """Process MC file"""
        
        # Validate required columns
        if 'ZONA_NOVAK' not in self.df.columns or 'NOMEN' not in self.df.columns:
            raise Exception('MC: Need ZONA_NOVAK and NOMEN columns')
        
        # Rename columns
        rename_dict = {'NOMEN': 'nomen'}
        
        # Name column
        if 'NAMA_PEL' in self.df.columns:
            rename_dict['NAMA_PEL'] = 'nama'
        elif 'NAMA' in self.df.columns:
            rename_dict['NAMA'] = 'nama'
        
        # Address column
        if 'ALM1_PEL' in self.df.columns:
            rename_dict['ALM1_PEL'] = 'alamat'
        elif 'ALAMAT' in self.df.columns:
            rename_dict['ALAMAT'] = 'alamat'
        
        # Tariff column
        if 'TARIF' in self.df.columns:
            rename_dict['TARIF'] = 'tarif'
        elif 'KODETARIF' in self.df.columns:
            rename_dict['KODETARIF'] = 'tarif'
        
        # Target MC column
        if 'NOMINAL' in self.df.columns:
            rename_dict['NOMINAL'] = 'target_mc'
        elif 'REK_AIR' in self.df.columns:
            rename_dict['REK_AIR'] = 'target_mc'
        
        # Kubikasi column
        if 'KUBIK' in self.df.columns:
            rename_dict['KUBIK'] = 'kubikasi'
        elif 'KUBIKASI' in self.df.columns:
            rename_dict['KUBIKASI'] = 'kubikasi'
        
        self.df = self.df.rename(columns=rename_dict)
        
        # Clean nomen
        self.clean_nomen_column()
        
        # Parse zona
        self.df['zona_novak'] = self.df['ZONA_NOVAK'].astype(str).str.strip()
        zona_parsed = self.df['zona_novak'].apply(parse_zona_novak)
        
        self.df['rayon'] = zona_parsed.apply(lambda x: x['rayon'])
        self.df['pc'] = zona_parsed.apply(lambda x: x['pc'])
        self.df['ez'] = zona_parsed.apply(lambda x: x['ez'])
        self.df['block'] = zona_parsed.apply(lambda x: x['block'])
        self.df['pcez'] = zona_parsed.apply(lambda x: x['pcez'])
        
        # Filter rayon 34/35
        self.df = self.df[self.df['rayon'].isin(['34', '35'])]
        
        if len(self.df) == 0:
            raise Exception('No Rayon 34/35 data found')
        
        # Fill missing columns
        for col in ['nama', 'alamat', 'tarif']:
            if col not in self.df.columns:
                self.df[col] = ''
        
        if 'target_mc' not in self.df.columns:
            self.df['target_mc'] = 0
        
        if 'kubikasi' not in self.df.columns:
            self.df['kubikasi'] = 0
        else:
            self.df['kubikasi'] = self.df['kubikasi'].apply(
                lambda x: abs(float(x)) if pd.notna(x) else 0
            )
        
        # Add metadata
        self.add_metadata()
        
        # Save to database
        cols_db = ['nomen', 'nama', 'alamat', 'rayon', 'pc', 'ez', 'pcez', 'block', 
                   'zona_novak', 'tarif', 'target_mc', 'kubikasi', 
                   'periode_bulan', 'periode_tahun', 'upload_id']
        
        self.df[cols_db].to_sql('master_pelanggan', self.db, if_exists='replace', index=False)
        
        return len(self.df)
