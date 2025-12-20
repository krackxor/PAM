import os

# ==========================================
# 1. PATH CONFIGURATION (PENGATURAN FOLDER)
# ==========================================

# Mendapatkan direktori root project (tempat file ini berada)
BASE_DIR = os.path.abspath(os.path.dirname(__file__))

# Folder utama penyimpanan data
DATA_DIR = os.path.join(BASE_DIR, 'data')

# Folder khusus untuk menampung file hasil upload
UPLOAD_FOLDER = os.path.join(DATA_DIR, 'uploads')

# Lokasi absolut file Database SQLite
DB_PATH = os.path.join(DATA_DIR, 'sunter.db')

# ==========================================
# 2. SECURITY & APP CONFIG (KEAMANAN)
# ==========================================

# Secret Key wajib ada untuk fitur Flash Message & Session Flask
# Di server production, ganti ini dengan string acak yang panjang
SECRET_KEY = os.environ.get('SECRET_KEY') or 'kunci_rahasia_sunter_dashboard_operasional_2025'

# ==========================================
# 3. UPLOAD CONFIGURATION (ATURAN UPLOAD)
# ==========================================

# Daftar ekstensi file yang diizinkan sistem
# - txt: Format standar SAP untuk Collection & MainBill
# - csv, xls, xlsx: Format standar laporan MC, SBRS, Ardebt
ALLOWED_EXTENSIONS = {'txt', 'csv', 'xls', 'xlsx'}

# Batas maksimal ukuran file upload (Contoh: 50 MB)
# Mencegah server crash jika user upload file terlalu besar
MAX_CONTENT_LENGTH = 50 * 1024 * 1024 

# ==========================================
# 4. CONSTANTS (KAMUS TIPE FILE)
# ==========================================
# Gunakan variabel ini di processor.py agar tidak salah ketik string manual

FILE_TYPE_MC = 'MC'
FILE_TYPE_COLLECTION = 'COLLECTION'
FILE_TYPE_MAINBILL = 'MAINBILL'
FILE_TYPE_SBRS = 'SBRS'
FILE_TYPE_ARDEBT = 'ARDEBT'

# Mapping label untuk tampilan di Dropdown HTML (Opsional)
FILE_TYPE_LABELS = {
    FILE_TYPE_MC: 'MC (Master Customer - Target)',
    FILE_TYPE_COLLECTION: 'Collection (Transaksi Harian)',
    FILE_TYPE_MAINBILL: 'MainBill (Tagihan Final)',
    FILE_TYPE_SBRS: 'SBRS (Baca Meter)',
    FILE_TYPE_ARDEBT: 'Ardebt (Saldo Tunggakan)'
}

# ==========================================
# 5. AUTO-INITIALIZATION (PEMBUATAN FOLDER)
# ==========================================

def init_environment():
    """
    Fungsi ini otomatis dijalankan saat file config di-import.
    Memastikan folder 'data' dan 'data/uploads' benar-benar ada.
    """
    if not os.path.exists(DATA_DIR):
        try:
            os.makedirs(DATA_DIR)
            print(f"[INFO] Membuat folder data: {DATA_DIR}")
        except OSError as e:
            print(f"[ERROR] Gagal membuat folder data: {e}")

    if not os.path.exists(UPLOAD_FOLDER):
        try:
            os.makedirs(UPLOAD_FOLDER)
            print(f"[INFO] Membuat folder upload: {UPLOAD_FOLDER}")
        except OSError as e:
            print(f"[ERROR] Gagal membuat folder upload: {e}")

# Jalankan inisialisasi folder secara langsung
init_environment()
