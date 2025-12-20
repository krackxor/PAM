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


# 🗺️ PANDUAN MAPPING FIELD - SUNTER DASHBOARD

> **Dokumen ini dibuat berdasarkan ANALISA FILE AKTUAL** dari sistem PAM Jaya Sunter

---

## 📋 DAFTAR ISI

1. [File MC (Master Customer)](#file-mc)
2. [File Collection Daily](#file-collection)
3. [File MainBill](#file-mainbill)
4. [Relasi Antar File](#relasi)
5. [Contoh Kasus](#contoh)

---

<a name="file-mc"></a>
## 📄 1. FILE MC (Master Customer)

### Informasi File
- **Nama File:** `MC1125_AB_Sunter.xls`
- **Format:** Excel (.xls) - Microsoft Excel 97-2003
- **Role:** **DATA INDUK** - Wajib diupload pertama!

### Kolom Wajib

| Kolom | Tipe | Keterangan | Mapping DB |
|-------|------|------------|------------|
| `ZONA_NOVAK` | TEXT (9 digit) | Kode lokasi lengkap | → zona_novak, rayon, pc, ez, block, pcez |
| `NOTAGIHAN` | TEXT | **KEY UTAMA** - Nomor pelanggan | → **nomen** (PRIMARY KEY) |
| `NAMA_PEL` | TEXT | Nama pelanggan lengkap | → nama |
| `ALM1_PEL` | TEXT | Alamat pelanggan | → alamat |
| `REK_AIR` | NUMERIC | Target collection/tagihan | → target_mc |
| `TARIF` | TEXT | Kode tarif pelanggan | → tarif |

### Parsing ZONA_NOVAK

**Format:** `350960217` (9 digit)

```
Contoh: 3 5 0 9 6 0 2 1 7
        │ │ └─┬─┘ └─┬─┘ │ │
        │ │   │     │   │ │
        └─┴───┼─────┼───┼─┴─── ZONA_NOVAK (full)
          │   │     │   │
          │   │     │   └────── Block: 17
          │   │     └────────── EZ: 02
          │   └──────────────── PC: 096
          └──────────────────── Rayon: 35

PCEZ = PC/EZ = 096/02
```

**Kode Python:**
```python
zona = "350960217"
rayon = zona[0:2]   # "35"
pc    = zona[2:5]   # "096"
ez    = zona[5:7]   # "02"
block = zona[7:9]   # "17"
pcez  = f"{pc}/{ez}" # "096/02"
```

### Filter Rayon
✅ Sistem hanya memproses Rayon **34** dan **35**  
❌ Rayon lain akan di-skip

---

<a name="file-collection"></a>
## 📄 2. FILE COLLECTION DAILY (Transaksi Harian)

### Informasi File
- **Nama File:** `Collection-2025-12-01_sd_2025-12-02-02122025042156.txt`
- **Format:** TXT (text file)
- **Delimiter:** `|` (pipe)
- **Encoding:** UTF-8

### Kolom Yang Digunakan

| Kolom | Tipe | Keterangan | Mapping DB |
|-------|------|------------|------------|
| `NOTAG` | TEXT | **KEY** - Link ke MC.NOTAGIHAN | → **nomen** (FOREIGN KEY) |
| `PAY_DT` | DATE | Tanggal bayar (DD-MM-YYYY) | → tgl_bayar (YYYY-MM-DD) |
| `AMT_COLLECT` | NUMERIC | Jumlah bayar (**MINUS!**) | → jumlah_bayar (absolute) |
| `RAYON` | TEXT | Rayon pelanggan | → rayon_check (validasi) |
| `NOMEN` | TEXT | Nomor induk (beda dgn NOTAG) | ❌ Tidak dipakai |

### ⚠️ CATATAN PENTING

#### 1. NOTAG vs NOMEN
```
NOTAG  = 011270295227  ← INI YANG DIPAKAI (link ke MC.NOTAGIHAN)
NOMEN  = 40061003       ← Nomor lain, tidak dipakai
```

**Mengapa NOTAG?**
- NOTAG di DAILY = NOTAGIHAN di MC
- NOTAG adalah nomor tagihan yang sama antar sistem
- NOMEN adalah ID internal yang berbeda

#### 2. AMT_COLLECT Selalu Minus
```
File asli:   AMT_COLLECT = -846184
Database:    jumlah_bayar = 846184  (di-abs())
```

#### 3. Format Tanggal
```
File asli:   PAY_DT = 01-12-2025 (DD-MM-YYYY)
Database:    tgl_bayar = 2025-12-01 (YYYY-MM-DD)
```

### Contoh Data
```
NOMEN    |RAYON|NOTAG        |AMT_COLLECT|PAY_DT     
40061003 |61   |011270295227 |-846184    |01-12-2025
         └─────┴─────────────┴───────────┴──────────
         Skip   KEY (pakai)  Abs()      Convert
```

---

<a name="file-mainbill"></a>
## 📄 3. FILE MAINBILL (Tagihan Bulanan)

### Informasi File
- **Nama File:** `MainBill-12-12-2025_sd_13-12-2025-13122025043303.txt`
- **Format:** TXT (text file)
- **Delimiter:** `;` (semicolon/titik koma)
- **Encoding:** UTF-8

### Kolom Yang Digunakan

| Kolom | Tipe | Keterangan | Mapping DB |
|-------|------|------------|------------|
| `NOMEN` | TEXT | **KEY** - Nomor pelanggan | → **nomen** (FOREIGN KEY) |
| `TOTAL_TAGIHAN` | NUMERIC | Total tagihan | → tagihan |
| `FREEZE_DT` | DATE | Tanggal tagihan (DD-MM-YYYY) | → tgl_bayar (YYYY-MM-DD) |
| `PCEZBK` | TEXT | Kode PC/EZ/Block (7 digit) | → pcezbk |
| `CC` | TEXT | Rayon (untuk validasi) | → rayon_check |
| `TARIF` | TEXT | Kode tarif | → tarif_check |

### ⚠️ CATATAN PENTING

#### 1. NOMEN di MainBill
```
MB.NOMEN = 60578518  ← Link ke MC.NOTAGIHAN
```

**Asumsi:** 
- NOMEN di MainBill = NOTAGIHAN di MC
- Jika tidak cocok, perlu mapping manual

#### 2. Format PCEZBK
```
File asli:   PCEZBK = 1510224  (7 digit)
Parse:       PC = 151, EZ = 02, Block = 24
```

Berbeda dengan ZONA_NOVAK di MC!

---

<a name="relasi"></a>
## 🔗 4. RELASI ANTAR FILE

### Diagram Relasi

```
┌─────────────────────────┐
│  FILE MC (MASTER)       │
│  ✅ DATA INDUK          │
├─────────────────────────┤
│ NOTAGIHAN (PK)         │ ←─────┐
│ ZONA_NOVAK             │        │
│ NAMA_PEL               │        │
│ REK_AIR (target)       │        │
└─────────────────────────┘        │
                                   │
        ┌──────────────────────────┼────────────────────┐
        │                          │                    │
        ↓ LINK                     ↓ LINK              ↓ LINK
┌───────────────────┐    ┌──────────────────┐   ┌──────────────────┐
│ FILE DAILY        │    │ FILE MAINBILL    │   │ FILE SBRS        │
├───────────────────┤    ├──────────────────┤   ├──────────────────┤
│ NOTAG (FK)        │    │ NOMEN (FK)       │   │ Nomen (FK)       │
│ PAY_DT            │    │ TOTAL_TAGIHAN    │   │ Curr_Read_1      │
│ AMT_COLLECT       │    │ FREEZE_DT        │   │ Read_date_1      │
│ RAYON             │    │ PCEZBK           │   │ ...              │
└───────────────────┘    └──────────────────┘   └──────────────────┘
```

### Mapping Tabel

| Tabel Database | Sumber File | Key Field | Foreign Key Ke |
|----------------|-------------|-----------|----------------|
| `master_pelanggan` | MC | NOTAGIHAN → nomen | - (PRIMARY) |
| `collection_harian` | DAILY | NOTAG → nomen | master_pelanggan.nomen |
| `mainbill` | MB | NOMEN → nomen | master_pelanggan.nomen |

---

<a name="contoh"></a>
## 💡 5. CONTOH KASUS NYATA

### Kasus 1: Upload MC (Master)

**File MC baris pertama:**
```csv
ZONA_NOVAK,NOTAGIHAN,NAMA_PEL,ALM1_PEL,REK_AIR,TARIF
350960217,250450123456,PT ABC,Jl Sunter Raya,50000000,I-B
```

**Hasil di Database:**
```sql
INSERT INTO master_pelanggan VALUES (
    nomen = '250450123456',        -- dari NOTAGIHAN
    nama = 'PT ABC',               -- dari NAMA_PEL
    alamat = 'Jl Sunter Raya',     -- dari ALM1_PEL
    zona_novak = '350960217',      -- raw
    rayon = '35',                  -- parsed [0:2]
    pc = '096',                    -- parsed [2:5]
    ez = '02',                     -- parsed [5:7]
    block = '17',                  -- parsed [7:9]
    pcez = '096/02',               -- PC/EZ
    tarif = 'I-B',                 -- dari TARIF
    target_mc = 50000000           -- dari REK_AIR
);
```

---

### Kasus 2: Upload Collection DAILY

**File DAILY baris pertama:**
```
NOMEN|RAYON|NOTAG|AMT_COLLECT|PAY_DT
40061003|35|250450123456|-45000000|15-12-2025
```

**Proses:**
1. Ambil `NOTAG` = `250450123456` ← INI YANG JADI KEY
2. Skip `NOMEN` = `40061003` (tidak dipakai)
3. Abs `AMT_COLLECT` = `45000000` (hilangkan minus)
4. Convert tanggal `15-12-2025` → `2025-12-15`

**Hasil di Database:**
```sql
INSERT INTO collection_harian VALUES (
    nomen = '250450123456',        -- dari NOTAG (link ke MC)
    tgl_bayar = '2025-12-15',      -- dari PAY_DT (converted)
    jumlah_bayar = 45000000,       -- dari AMT_COLLECT (abs)
    rayon_check = '35'             -- dari RAYON (validasi)
);
```

**Query Join:**
```sql
SELECT 
    m.nama,
    m.target_mc,
    c.jumlah_bayar,
    c.tgl_bayar
FROM collection_harian c
LEFT JOIN master_pelanggan m ON c.nomen = m.nomen
WHERE c.nomen = '250450123456';
```

---

### Kasus 3: Upload MainBill

**File MB baris pertama:**
```
NOMEN;TOTAL_TAGIHAN;FREEZE_DT;PCEZBK;CC
250450123456;52000000;20-12-2025;1510224;35
```

**Hasil di Database:**
```sql
INSERT INTO mainbill VALUES (
    nomen = '250450123456',        -- dari NOMEN (link ke MC)
    tagihan = 52000000,            -- dari TOTAL_TAGIHAN
    tgl_bayar = '2025-12-20',      -- dari FREEZE_DT (converted)
    pcezbk = '1510224',            -- dari PCEZBK
    rayon_check = '35'             -- dari CC (validasi)
);
```

---

## ✅ CHECKLIST VALIDASI

### Sebelum Upload MC
- [ ] File punya kolom `ZONA_NOVAK`
- [ ] File punya kolom `NOTAGIHAN`
- [ ] File punya kolom `NAMA_PEL` (agar nama tidak kosong)
- [ ] ZONA_NOVAK dimulai dengan 34 atau 35
- [ ] Format file .xls atau .csv

### Sebelum Upload Collection DAILY
- [ ] File MC sudah diupload terlebih dahulu
- [ ] File punya kolom `NOTAG` (bukan NOMEN!)
- [ ] File punya kolom `PAY_DT`
- [ ] File punya kolom `AMT_COLLECT`
- [ ] Delimiter = `|` (pipe)
- [ ] Format file .txt

### Sebelum Upload MainBill
- [ ] File MC sudah diupload terlebih dahulu
- [ ] File punya kolom `NOMEN`
- [ ] File punya kolom `TOTAL_TAGIHAN`
- [ ] File punya kolom `FREEZE_DT`
- [ ] Delimiter = `;` (titik koma)
- [ ] Format file .txt

---

## 🐛 TROUBLESHOOTING

### Error: "Field NOTAG tidak ditemukan"
**Penyebab:** File Collection bukan format DAILY yang benar
**Solusi:** Pastikan file punya kolom `NOTAG` (bukan `NOMEN`)

### Error: "Tidak ada data matching antara Collection dan MC"
**Penyebab:** NOTAG di DAILY tidak cocok dengan NOTAGIHAN di MC
**Solusi:** 
1. Cek apakah MC sudah diupload
2. Bandingkan nilai NOTAG vs NOTAGIHAN di kedua file
3. Pastikan tidak ada spasi atau karakter tersembunyi

### Error: "AMT_COLLECT = 0 di database"
**Penyebab:** Parsing field salah atau nilai memang 0
**Solusi:** Cek nilai asli di file, pastikan kolom `AMT_COLLECT` ada

---

## 📞 KONTAK DEVELOPER

**Tim IT PAM Jaya Sunter**
- Email: support@pamjaya-sunter.com
- Ext: 1234

---

**Last Updated:** December 21, 2025  
**Version:** 2.0 (Berdasarkan File Aktual)  
**Author:** Claude + Tim Analisa Data
