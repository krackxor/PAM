"""
Base Processor Class
Parent class for all file processors
"""

import pandas as pd
from abc import ABC, abstractmethod
from core.helpers import clean_nomen, clean_date

class BaseProcessor(ABC):
    """Base class for file processors"""
    
    def __init__(self, filepath, upload_id, periode_bulan, periode_tahun, db):
        self.filepath = filepath
        self.upload_id = upload_id
        self.periode_bulan = periode_bulan
        self.periode_tahun = periode_tahun
        self.db = db
        self.df = None
    
    def read_file(self):
        """Read file based on extension"""
        try:
            if self.filepath.endswith('.csv'):
                self.df = pd.read_csv(self.filepath, dtype=str)
            elif self.filepath.endswith(('.xls', '.xlsx')):
                self.df = pd.read_excel(self.filepath)
            elif self.filepath.endswith('.txt'):
                # Try pipe delimiter first
                try:
                    self.df = pd.read_csv(self.filepath, sep='|', dtype=str)
                except:
                    self.df = pd.read_csv(self.filepath, dtype=str)
            else:
                raise Exception(f'Unsupported file format: {self.filepath}')
            
            # Normalize column names
            self.df.columns = self.df.columns.str.upper().str.strip()
            
            print(f"✅ File read: {len(self.df)} rows, {len(self.df.columns)} columns")
            return True
            
        except Exception as e:
            print(f"❌ Error reading file: {e}")
            raise
    
    def validate_columns(self, required_columns):
        """Validate required columns exist"""
        missing = [col for col in required_columns if col not in self.df.columns]
        if missing:
            raise Exception(f"Missing required columns: {missing}")
        return True
    
    def clean_nomen_column(self):
        """Clean nomen column"""
        if 'nomen' in self.df.columns:
            self.df['nomen'] = self.df['nomen'].apply(clean_nomen)
            self.df = self.df.dropna(subset=['nomen'])
            self.df = self.df[self.df['nomen'] != '']
            self.df = self.df[self.df['nomen'].str.lower() != 'nan']
    
    def add_metadata(self):
        """Add metadata columns"""
        self.df['periode_bulan'] = self.periode_bulan
        self.df['periode_tahun'] = self.periode_tahun
        self.df['upload_id'] = self.upload_id
    
    @abstractmethod
    def process(self):
        """Process file (must be implemented by child classes)"""
        pass
    
    def execute(self):
        """Execute processing pipeline"""
        try:
            # Read file
            self.read_file()
            
            # Process (implemented by child)
            row_count = self.process()
            
            print(f"✅ Processing complete: {row_count} rows")
            return row_count
            
        except Exception as e:
            print(f"❌ Processing failed: {e}")
            raise
