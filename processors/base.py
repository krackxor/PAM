"""
Base Processor Class
Parent class for all file processors
"""

import pandas as pd
from abc import ABC, abstractmethod
from core.helpers import clean_nomen

class BaseProcessor(ABC):
    """Base class for file processors"""
    
    def __init__(self, db):
        """
        Inisialisasi hanya dengan koneksi database.
        Parameter lain seperti filepath dan periode dikirim saat pemanggilan process()
        untuk mendukung deteksi otomatis (Auto-Detect).
        """
        self.db = db
        self.df = None
        # Cursor untuk mempermudah eksekusi query pada class anak
        self.cursor = db.cursor()
    
    def read_file(self, filepath):
        """Membaca file berdasarkan ekstensi (CSV, Excel, atau TXT)"""
        try:
            if filepath.endswith('.csv'):
                self.df = pd.read_csv(filepath, dtype=str)
            elif filepath.endswith(('.xls', '.xlsx')):
                self.df = pd.read_excel(filepath)
            elif filepath.endswith('.txt'):
                # Mencoba separator pipa (|) terlebih dahulu
                try:
                    self.df = pd.read_csv(filepath, sep='|', dtype=str)
                except:
                    self.df = pd.read_csv(filepath, dtype=str)
            else:
                raise Exception(f'Format file tidak didukung: {filepath}')
            
            # Normalisasi nama kolom menjadi huruf kapital dan hapus spasi tambahan
            self.df.columns = self.df.columns.str.upper().str.strip()
            
            print(f"✅ File berhasil dibaca: {len(self.df)} baris, {len(self.df.columns)} kolom")
            return True
            
        except Exception as e:
            print(f"❌ Gagal membaca file: {e}")
            raise
    
    def validate_columns(self, required_columns):
        """Validasi apakah kolom yang dibutuhkan tersedia di dalam file"""
        missing = [col for col in required_columns if col not in self.df.columns]
        if missing:
            raise Exception(f"Kolom wajib tidak ditemukan: {missing}")
        return True
    
    def clean_nomen_column(self):
        """Membersihkan kolom nomen (menggunakan NOMEN atau CMR_ACCOUNT)"""
        # Mendeteksi nama kolom yang tersedia
        nomen_col = 'NOMEN' if 'NOMEN' in self.df.columns else 'CMR_ACCOUNT'
        
        if nomen_col in self.df.columns:
            from core.helpers import clean_nomen
            self.df[nomen_col] = self.df[nomen_col].apply(clean_nomen)
            # Hapus baris yang nomen-nya kosong atau NaN
            self.df = self.df.dropna(subset=[nomen_col])
            self.df = self.df[self.df[nomen_col] != '']
    
    @abstractmethod
    def process(self, filepath, periode_bulan, periode_tahun):
        """
        Metode utama untuk memproses file. 
        Wajib diimplementasikan oleh class anak (SBRSProcessor, MCProcessor, dll).
        """
        pass
