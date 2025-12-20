# 📋 DOKUMENTASI SISTEM SUNTER DASHBOARD

## 🔍 PERBEDAAN FIELD ANTAR FILE

### 1. FIELD NOMEN (Nomor Pelanggan)
| File Type | Field Name | Keterangan |
|-----------|-----------|------------|
| **MC** | `NOTAGIHAN` | Master Customer - Data Induk |
| **MB** (MainBill) | `NOTAGIHAN` | Tagihan Bulanan |
| **DAILY** (Collection) | `NOTAG` | Transaksi Harian |

**Solusi Sistem:**
- Semua field ini di-mapping ke kolom `nomen` di database
- Nomen dari MC menjadi **data induk** (master_pelanggan)
- Nomen dari DAILY & MB akan di-link ke master

---

### 2. FIELD TANGGAL BAYAR

| File Type | Field Name | Format | Keterangan |
|-----------|-----------|--------|------------|
| **MC** | ❌ TIDAK ADA | - | MC hanya master, tanpa tanggal bayar |
| **MB** (MainBill) | `TGL_BAYAR` | DD-MM-YYYY | Tanggal tagihan/jatuh tempo |
| **DAILY** (Collection) | `PAY_DT` | DD-MM-YYYY | Tanggal pembayaran aktual |

**Solusi Sistem:**
- Sistem otomatis convert semua format ke `YYYY-MM-DD`
- Mendukung format: `DD-MM-YYYY` dan `DD/MM/YYYY`

---

### 3. FIELD ZONA_NOVAK (Parsing Otomatis)

**Format:** `350960217` (9 digit)

**Breakdown:**
```
Posisi 1-2   : Rayon (35)
Posisi 3-5   : PC (096)
Posisi 6-7   : EZ (02)
Posisi 8-9   : Block (17)
PCEZ         : PC/EZ (096/02)
```

**Contoh Parsing:**
| ZONA_NOVAK | Rayon | PC | EZ | Block | PCEZ |
|------------|-------|----|----|-------|------|
| 350960217 | 35 | 096 | 02 | 17 | 096/02 |
| 340450512 | 34 | 045 | 05 | 12 | 045/05 |

**Catatan Penting:**
- ✅ Sistem hanya memproses Rayon **34** dan **35**
- ✅ ZONA_NOVAK dari file MC yang sudah di-filter
- ✅ Nomen MC = data induk untuk semua field ini

---

## 📂 STRUKTUR DATABASE

### Tabel: `master_pelanggan` (Data Induk)
Sumber: **File MC**

| Kolom | Sumber Field | Contoh |
|-------|-------------|--------|
| nomen | NOTAGIHAN | 6012345678 |
| nama | NAMA_PEL | PT ABC |
| alamat | ALM1_PEL | Jl. Sunter... |
| zona_novak | ZONA_NOVAK | 350960217 |
| rayon | ZONA_NOVAK[0:2] | 35 |
| pc | ZONA_NOVAK[2:5] | 096 |
| ez | ZONA_NOVAK[5:7] | 02 |
| block | ZONA_NOVAK[7:9] | 17 |
| pcez | PC/EZ | 096/02 |
| tarif | TARIF | I-B |
| target_mc | REK_AIR | 50000000 |

---

### Tabel: `collection_harian` (Transaksi)
Sumber: **File DAILY (Collection)**

| Kolom | Sumber Field | Contoh |
|-------|-------------|--------|
| nomen | NOTAG | 6012345678 |
| tgl_bayar | PAY_DT | 2025-12-15 |
| jumlah_bayar | AMT_COLLECT | 45000000 |
| sumber_file | filename | coll_20251215.txt |

**Catatan:**
- `AMT_COLLECT` sering minus di SAP → sistem auto `abs()`
- Tanggal auto-convert ke format `YYYY-MM-DD`

---

### Tabel: `mainbill` (Tagihan)
Sumber: **File MB (MainBill)**

| Kolom | Sumber Field | Contoh |
|-------|-------------|--------|
| nomen | NOTAGIHAN | 6012345678 |
| tgl_bayar | TGL_BAYAR | 2025-12-20 |
| tagihan | TAGIHAN | 52000000 |
| periode | auto | 2025-12 |

---

## 🚀 CARA PENGGUNAAN SISTEM

### Step 1: Upload Master (MC) - **WAJIB PERTAMA**
1. Klik tombol **"Upload Data"**
2. Pilih jenis: **"MASTER (TARGET)"**
3. Upload file MC (.csv)
4. Sistem akan:
   - Parse ZONA_NOVAK otomatis
   - Filter Rayon 34 & 35
   - Simpan sebagai data induk

**File MC wajib punya kolom:**
- `ZONA_NOVAK`
- `NOTAGIHAN`
- `NAMA_PEL` (optional tapi sangat direkomendasikan)
- `REK_AIR` (target)

---

### Step 2: Upload Collection (DAILY)
1. Pilih jenis: **"COLLECTION (HARIAN)"**
2. Upload file DAILY (.txt atau .csv)
3. Sistem akan:
   - Mapping `NOTAG` → `nomen`
   - Mapping `PAY_DT` → `tgl_bayar`
   - Mapping `AMT_COLLECT` → `jumlah_bayar`
   - Auto-link ke master pelanggan

**File DAILY wajib punya kolom:**
- `NOTAG`
- `PAY_DT`
- `AMT_COLLECT`

---

### Step 3: Upload MainBill (MB) - Optional
1. Pilih jenis: **"MAINBILL"**
2. Upload file MB (.txt atau .csv)
3. Sistem akan mapping field otomatis

**File MB wajib punya kolom:**
- `NOTAGIHAN`
- `TGL_BAYAR`
- `TAGIHAN`

---

## ⚙️ FORMAT FILE YANG DIDUKUNG

### File MC (Master)
- **Format:** CSV (koma `,`)
- **Encoding:** UTF-8 atau Latin-1
- **Delimiter:** `,` atau `;`
- **Extension:** `.csv` atau `.xlsx`

### File DAILY (Collection)
- **Format:** TXT atau CSV
- **Delimiter:** `|` (pipe) atau `,` (koma)
- **Encoding:** UTF-8
- **Extension:** `.txt` atau `.csv`

### File MB (MainBill)
- **Format:** TXT atau CSV
- **Delimiter:** `;` (titik koma) atau `,`
- **Encoding:** UTF-8
- **Extension:** `.txt` atau `.csv`

---

## 🔧 TROUBLESHOOTING

### Problem: "Format MC salah! Wajib ada kolom ZONA_NOVAK dan NOTAGIHAN"
**Solusi:**
- Cek file Excel/CSV Anda
- Pastikan header baris pertama ada `ZONA_NOVAK` dan `NOTAGIHAN`
- Jangan ada merge cell di header

---

### Problem: "Tidak ada data Rayon 34/35 dalam file MC"
**Solusi:**
- File MC Anda mungkin untuk wilayah lain
- Pastikan ZONA_NOVAK dimulai dengan `34` atau `35`
- Contoh valid: `340450512`, `350960217`

---

### Problem: "Nama pelanggan tidak muncul"
**Solusi:**
- File MC harus punya kolom `NAMA_PEL` atau `NAMA`
- Upload ulang file MC yang lengkap
- Jika MC tidak ada nama, sistem akan tampilkan "Belum Ada Nama"

---

### Problem: "Collection tidak masuk ke database"
**Solusi:**
- Pastikan sudah upload MC terlebih dahulu
- Cek kolom `NOTAG` di file DAILY tidak kosong
- Format tanggal `PAY_DT` harus valid (DD-MM-YYYY)

---

## 📊 FITUR DASHBOARD

### Tab 1: Ringkasan
- **KPI Cards:** Total Pelanggan, Target MC, Realisasi, Collection Rate
- **Chart:** Tren Collection harian vs kumulatif
- **Pie Chart:** Komposisi Rayon 34 vs 35

### Tab 2: Collection (Full Data)
- **Tabel Excel-like:** Semua transaksi bulan berjalan
- **Filter:** Per tanggal, rayon, PCEZ
- **Export:** Excel (coming soon)
- **Unlimited rows:** Tidak ada batasan jumlah data

### Tab 3: Analisa Manual
- **Form Input:** Catat anomali per pelanggan
- **Tracking:** Status (Open/Progress/Closed)
- **History:** Audit trail lengkap

---

## 🎯 BEST PRACTICES

### ✅ DO (Lakukan)
1. **Selalu upload MC dulu** sebelum upload Collection
2. **Update MC setiap bulan** untuk data target terbaru
3. **Upload DAILY setiap hari** untuk monitoring real-time
4. **Filter Rayon** di dashboard untuk analisa spesifik
5. **Backup database** secara berkala

### ❌ DON'T (Jangan)
1. Jangan upload Collection sebelum MC
2. Jangan edit database manual via SQLite Browser
3. Jangan upload file yang sama 2x (akan duplikat)
4. Jangan hapus folder `database/` atau `uploads/`

---

## 📞 SUPPORT

Jika ada error atau pertanyaan:
1. Cek log error di terminal/console
2. Screenshot error message
3. Kirim sample file (baris pertama saja)
4. Hubungi tim developer

---

**Version:** 2.0 (December 2025)  
**Last Updated:** December 21, 2025  
**Author:** Claude + Tim IT PAM Jaya Sunter

# 🌊 SUNTER DASHBOARD - Monitoring Operasional PAM JAYA

Dashboard monitoring collection dan operasional untuk wilayah Sunter (Rayon 34 & 35).

---

## 🚀 QUICK START

### 1. Instalasi Dependencies
```bash
pip install -r requirements.txt
```

### 2. Jalankan Aplikasi
```bash
python app.py
```

### 3. Akses Dashboard
Buka browser dan akses: **http://localhost:5000**

---

## 📦 FILE YANG DIBUTUHKAN

### File 1: MC (Master Customer) - **WAJIB UPLOAD PERTAMA**
```
Format: CSV
Kolom Wajib:
- ZONA_NOVAK (contoh: 350960217)
- NOTAGIHAN (contoh: 6012345678)
- NAMA_PEL (nama pelanggan)
- REK_AIR (target collection)
```

### File 2: DAILY (Collection Harian)
```
Format: TXT atau CSV
Delimiter: | (pipe)
Kolom Wajib:
- NOTAG (nomen pelanggan)
- PAY_DT (tanggal bayar, format: DD-MM-YYYY)
- AMT_COLLECT (jumlah bayar)
```

### File 3: MB (MainBill) - Optional
```
Format: TXT atau CSV
Delimiter: ; (titik koma)
Kolom Wajib:
- NOTAGIHAN (nomen pelanggan)
- TGL_BAYAR (tanggal bayar)
- TAGIHAN (jumlah tagihan)
```

---

## 🎯 MAPPING FIELD (IMPORTANT!)

### Nomen Pelanggan
| File | Field Name |
|------|------------|
| MC | `NOTAGIHAN` |
| MB | `NOTAGIHAN` |
| DAILY | `NOTAG` |

→ Semua di-mapping ke kolom `nomen` di database

### Tanggal Bayar
| File | Field Name |
|------|------------|
| MC | ❌ Tidak ada |
| MB | `TGL_BAYAR` |
| DAILY | `PAY_DT` |

→ Semua di-convert ke format `YYYY-MM-DD`

### ZONA_NOVAK Parsing
```
Format: 350960217 (9 digit)
├─ Rayon: 35 (digit 1-2)
├─ PC: 096 (digit 3-5)
├─ EZ: 02 (digit 6-7)
├─ Block: 17 (digit 8-9)
└─ PCEZ: 096/02
```

---

## 📊 FITUR UTAMA

✅ **Single Page Dashboard** - Semua fitur dalam 1 halaman  
✅ **Global Filter** - Area (SUNTER/34/35), Periode, PCEZ  
✅ **Unlimited Data** - Tidak ada batasan jumlah row  
✅ **Real-time KPI** - Collection rate, target, realisasi  
✅ **Excel-like Table** - Tabel collection dengan scroll horizontal  
✅ **Analisa Manual** - Form untuk tracking anomali  
✅ **Auto Parsing** - ZONA_NOVAK otomatis dipecah jadi Rayon/PC/EZ  
✅ **Multi-format Support** - CSV, TXT, Excel  

---

## 🔧 TROUBLESHOOTING

### Error: "Format MC salah! Wajib ada kolom ZONA_NOVAK dan NOTAGIHAN"
**Solusi:**
- Buka file MC di Excel
- Pastikan header baris pertama ada `ZONA_NOVAK` dan `NOTAGIHAN`
- Save as CSV (UTF-8)

### Error: "Tidak ada data Rayon 34/35 dalam file MC"
**Solusi:**
- Cek kolom ZONA_NOVAK dimulai dengan `34` atau `35`
- File mungkin untuk wilayah lain

### Nama Pelanggan Tidak Muncul
**Solusi:**
- Pastikan file MC punya kolom `NAMA_PEL` atau `NAMA`
- Upload ulang file MC yang lengkap

### Collection Tidak Masuk
**Solusi:**
- Upload file MC terlebih dahulu
- Cek format tanggal `PAY_DT` di file DAILY (DD-MM-YYYY)
- Pastikan kolom `NOTAG` tidak kosong

---

## 📁 STRUKTUR FOLDER

```
sunter-dashboard/
├── app.py                  # Aplikasi utama
├── requirements.txt        # Dependencies
├── DOCUMENTATION.md        # Dokumentasi lengkap
├── README.md              # File ini
├── uploads/               # Folder upload (auto-created)
├── database/              # Folder database (auto-created)
│   └── sunter.db          # SQLite database
└── templates/             # HTML templates
    └── index.html         # Dashboard utama
```

---

## 🗃️ DATABASE SCHEMA

### Table: master_pelanggan
```sql
nomen          TEXT PRIMARY KEY  -- dari NOTAGIHAN (MC)
nama           TEXT               -- dari NAMA_PEL (MC)
alamat         TEXT               -- dari ALM1_PEL (MC)
zona_novak     TEXT               -- dari ZONA_NOVAK (MC)
rayon          TEXT               -- parsed dari ZONA_NOVAK
pc             TEXT               -- parsed dari ZONA_NOVAK
ez             TEXT               -- parsed dari ZONA_NOVAK
block          TEXT               -- parsed dari ZONA_NOVAK
pcez           TEXT               -- format: PC/EZ
tarif          TEXT               -- dari TARIF (MC)
target_mc      REAL               -- dari REK_AIR (MC)
```

### Table: collection_harian
```sql
id             INTEGER PRIMARY KEY
nomen          TEXT               -- dari NOTAG (DAILY)
tgl_bayar      TEXT               -- dari PAY_DT (DAILY)
jumlah_bayar   REAL               -- dari AMT_COLLECT (DAILY)
sumber_file    TEXT               -- nama file upload
```

### Table: mainbill
```sql
nomen          TEXT PRIMARY KEY   -- dari NOTAGIHAN (MB)
tgl_bayar      TEXT               -- dari TGL_BAYAR (MB)
tagihan        REAL               -- dari TAGIHAN (MB)
periode        TEXT               -- format: YYYY-MM
```

---

## 🎓 CARA PAKAI STEP-BY-STEP

### Langkah 1: Upload Master (MC)
1. Klik **"Upload Data"**
2. Pilih radio button **"MASTER (TARGET)"**
3. Klik **"Choose File"** → Pilih file MC (.csv)
4. Klik **"Upload"**
5. Tunggu notifikasi sukses

**Hasil:** Data master pelanggan tersimpan, lengkap dengan parsing ZONA_NOVAK

---

### Langkah 2: Upload Collection (DAILY)
1. Klik **"Upload Data"**
2. Pilih radio button **"COLLECTION (HARIAN)"**
3. Klik **"Choose File"** → Pilih file DAILY (.txt)
4. Klik **"Upload"**
5. Tunggu notifikasi sukses

**Hasil:** Transaksi pembayaran masuk ke database dan langsung tampil di tabel

---

### Langkah 3: Monitoring Dashboard
1. Tab **"Ringkasan"** → Lihat KPI dan chart
2. Tab **"Collection"** → Lihat detail transaksi
3. Tab **"Analisa Manual"** → Input analisa anomali

---

### Langkah 4: Filter Data
1. Di header, pilih dropdown **Area** (SUNTER/34/35)
2. Sistem auto-filter semua data
3. KPI, chart, dan tabel update otomatis

---

## 💡 TIPS & BEST PRACTICES

### ✅ DO (Lakukan)
- Upload MC setiap awal bulan untuk update target
- Upload DAILY setiap hari untuk monitoring real-time
- Backup database (copy file `sunter.db`) setiap minggu
- Gunakan filter Rayon untuk analisa spesifik
- Catat anomali di menu "Analisa Manual"

### ❌ DON'T (Jangan)
- Jangan upload Collection sebelum MC (akan error)
- Jangan edit database manual via DB Browser
- Jangan hapus folder `database/` atau `uploads/`
- Jangan upload file yang sama 2x (akan duplikat)
- Jangan ubah format ZONA_NOVAK di Excel

---

## 🔐 SECURITY NOTES

- Dashboard ini untuk **internal use only**
- Tidak ada fitur login (sesuai request)
- Jalankan di **localhost** atau **intranet** saja
- Jangan expose ke internet publik
- Backup database secara berkala

---

## 📌 CHANGELOG

### Version 2.0 (December 21, 2025)
- ✅ Fix field mapping: MC (NOTAGIHAN), DAILY (NOTAG)
- ✅ Fix tanggal: MB (TGL_BAYAR), DAILY (PAY_DT)
- ✅ Auto parsing ZONA_NOVAK ke Rayon/PC/EZ/Block
- ✅ Filter otomatis Rayon 34 & 35
- ✅ Unlimited rows (hapus LIMIT 100)
- ✅ Support multiple file formats

### Version 1.0 (December 2025)
- Initial release
- Basic dashboard & upload

---

## 📞 SUPPORT

**Developer Contact:**
- Email: support@pamjaya-sunter.com
- Phone: 021-XXXXXXX
- Office: Gedung PAM Jaya, Sunter

**Documentation:**
- File lengkap: `DOCUMENTATION.md`
- Field mapping: Lihat tabel di atas
- Sample files: Minta ke tim IT

---

## ⚖️ LICENSE

Proprietary - PAM Jaya Internal Use Only

---

**Last Updated:** December 21, 2025  
**Version:** 2.0  
**Status:** Production Ready ✅
