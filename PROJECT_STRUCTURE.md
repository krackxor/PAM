# 📁 PROJECT STRUCTURE - SUNTER DASHBOARD

Complete directory structure and file organization.

---

## 🗂️ DIRECTORY TREE

```
sunter-dashboard/
│
├── app.py                          # Main application (entry point)
├── config.py                       # Configuration management
├── requirements.txt                # Python dependencies
├── README.md                       # Project documentation
├── .gitignore                      # Git ignore rules
│
├── core/                           # Core business logic
│   ├── __init__.py
│   ├── database.py                 # Database connection & initialization
│   ├── models.py                   # Data models (optional)
│   └── helpers.py                  # Helper functions (formatRupiah, etc)
│
├── processors/                     # File processing modules
│   ├── __init__.py
│   ├── base.py                     # Base processor class (DRY)
│   ├── mc_processor.py             # MC file processor
│   ├── collection_processor.py     # Collection processor
│   ├── sbrs_processor.py           # SBRS processor (FIXED columns)
│   ├── mb_processor.py             # MB processor
│   ├── mainbill_processor.py       # MainBill processor
│   ├── ardebt_processor.py         # Ardebt processor
│   └── auto_detect.py              # Auto-detect periode
│
├── api/                            # API endpoints (modular)
│   ├── __init__.py
│   ├── kpi.py                      # KPI endpoints
│   ├── collection.py               # Collection endpoints
│   ├── anomaly.py                  # Anomaly detection endpoints
│   ├── analisa.py                  # Analisa manual endpoints
│   ├── upload.py                   # Upload endpoints
│   └── history.py                  # History endpoints
│
├── static/                         # Static files
│   ├── css/
│   │   ├── main.css                # Main styles (600+ lines)
│   │   └── responsive.css          # Responsive styles (500+ lines)
│   ├── js/
│   │   ├── main.js                 # Main JavaScript utilities
│   │   ├── anomaly.js              # Anomaly detection JS
│   │   ├── collection.js           # Collection dashboard JS
│   │   └── charts.js               # Chart utilities
│   └── images/                     # Images (optional)
│
├── templates/                      # HTML templates
│   ├── base.html                   # Base template (responsive)
│   ├── index.html                  # Main dashboard (FIXED columns)
│   ├── collection_dashboard.html   # Collection page (optional)
│   ├── anomaly.html                # Anomaly page (optional)
│   └── components/                 # Reusable components
│       ├── kpi_cards.html          # KPI cards component
│       ├── sbrs_table.html         # SBRS table component
│       ├── header.html             # Header component
│       ├── modals.html             # Modal dialogs
│       └── tables.html             # Table components
│
├── uploads/                        # Uploaded files (auto-created)
│   ├── mc/
│   ├── collection/
│   ├── sbrs/
│   └── temp/
│
├── database/                       # SQLite database (auto-created)
│   ├── sunter.db                   # Main database
│   └── backups/                    # Database backups (optional)
│
└── tests/                          # Unit tests (future)
    ├── __init__.py
    ├── test_processors.py          # Processor tests
    ├── test_api.py                 # API tests
    └── test_helpers.py             # Helper tests
```

---

## 📄 FILE DESCRIPTIONS

### **Root Level**

| File | Purpose | Lines |
|------|---------|-------|
| `app.py` | Main Flask application entry point | ~150 |
| `config.py` | Configuration management (dev/prod) | ~70 |
| `requirements.txt` | Python dependencies | ~10 |

### **core/** - Core Business Logic

| File | Purpose | Lines |
|------|---------|-------|
| `database.py` | Database schema & connection management | ~200 |
| `helpers.py` | Utility functions (formatRupiah, clean_nomen, etc) | ~100 |
| `models.py` | Data models (optional, future) | - |

### **processors/** - File Processors

| File | Purpose | Lines |
|------|---------|-------|
| `base.py` | Base processor class (DRY principle) | ~100 |
| `mc_processor.py` | Process MC files | ~120 |
| `collection_processor.py` | Process collection files | ~100 |
| `sbrs_processor.py` | Process SBRS files (FIXED columns) | ~120 |
| `mb_processor.py` | Process MB files | ~80 |
| `mainbill_processor.py` | Process MainBill files | ~80 |
| `ardebt_processor.py` | Process Ardebt files | ~70 |
| `auto_detect.py` | Auto-detect file periode | ~150 |

### **api/** - API Endpoints

| File | Purpose | Endpoints | Lines |
|------|---------|-----------|-------|
| `kpi.py` | KPI metrics | `/api/kpi`, `/api/kpi/trend` | ~150 |
| `collection.py` | Collection data | `/api/collection/*` | ~200 |
| `anomaly.py` | Anomaly detection | `/api/anomaly/*` | ~250 |
| `analisa.py` | Manual analysis | `/api/analisa/*` | ~300 |
| `upload.py` | File uploads | `/api/upload` | ~200 |
| `history.py` | Upload history | `/api/history/*` | ~150 |

### **static/** - Static Files

| File | Purpose | Lines |
|------|---------|-------|
| `css/main.css` | Core styles (design system) | ~600 |
| `css/responsive.css` | Responsive components | ~500 |
| `js/main.js` | Main utilities (toast, modal, etc) | ~450 |
| `js/charts.js` | Chart helpers | ~150 |
| `js/collection.js` | Collection-specific JS | ~150 |
| `js/anomaly.js` | Anomaly-specific JS | ~150 |

### **templates/** - HTML Templates

| File | Purpose | Lines |
|------|---------|-------|
| `base.html` | Base template with navbar | ~120 |
| `index.html` | Main dashboard | ~200 |
| `components/kpi_cards.html` | KPI cards component | ~150 |
| `components/sbrs_table.html` | SBRS table component | ~200 |

---

## 🎯 MODULE RESPONSIBILITIES

### **app.py** - Application Entry Point
```python
# Responsibilities:
- Initialize Flask app
- Load configuration
- Register API routes
- Define UI routes
- Start server
```

### **config.py** - Configuration
```python
# Responsibilities:
- Environment-specific configs (dev/prod)
- Database paths
- Upload settings
- Security settings
```

### **core/database.py** - Database Management
```python
# Responsibilities:
- Database schema definition
- Connection management
- init_db() function
- get_db() function
```

### **core/helpers.py** - Utilities
```python
# Responsibilities:
- formatRupiah()
- format_number()
- clean_nomen()
- parse_zona_novak()
- Template filters
```

### **processors/base.py** - Base Processor
```python
# Responsibilities:
- Common file reading logic
- Column validation
- Metadata addition
- DRY principle implementation
```

### **processors/*_processor.py** - Specific Processors
```python
# Each processor:
- Extends BaseProcessor
- Implements process() method
- Handles file-specific logic
- Maps columns to database schema
```

### **api/*.py** - API Endpoints
```python
# Each API module:
- Defines related endpoints
- Handles request validation
- Queries database
- Returns JSON responses
```

---

## 🔄 DATA FLOW

### **1. File Upload Flow**
```
User uploads file
    ↓
app.py → /upload route
    ↓
api/upload.py
    ↓
auto_detect.py (detect periode)
    ↓
Specific processor (mc_processor.py, etc)
    ↓
base.py (common logic)
    ↓
core/database.py (save to DB)
    ↓
Response to user
```

### **2. Dashboard Load Flow**
```
User opens dashboard
    ↓
app.py → / route
    ↓
Render templates/index.html
    ↓
JavaScript loads data
    ↓
api/kpi.py (fetch KPI data)
    ↓
core/database.py (query DB)
    ↓
Return JSON
    ↓
Update UI with Chart.js
```

### **3. SBRS Table Flow**
```
User clicks "Load SBRS"
    ↓
JavaScript calls API
    ↓
api/anomaly.py or custom endpoint
    ↓
Query sbrs_data table
    ↓
Return JSON with correct columns:
    - nomen (NOT cmr_account)
    - volume (NOT SB_Stand)
    - nama (NOT cmr_name)
    - rayon (NOT cmr_route)
    ↓
JavaScript renders table
    ↓
DataTables displays data
```

---

## 🚀 GETTING STARTED

### **1. Install Dependencies**
```bash
pip install -r requirements.txt
```

### **2. Initialize Database**
```bash
python -c "from flask import Flask; from core.database import init_db; app = Flask(__name__); app.app_context().push(); init_db(app)"
```

### **3. Run Application**
```bash
# Development
python app.py

# Production
gunicorn -w 4 -b 0.0.0.0:5000 app:app
```

### **4. Access Dashboard**
```
http://localhost:5000
```

---

## 📋 FILE COUNTS

| Directory | Files | Lines |
|-----------|-------|-------|
| Root | 3 | ~230 |
| core/ | 3 | ~300 |
| processors/ | 8 | ~820 |
| api/ | 6 | ~1,250 |
| static/ | 6 | ~2,000 |
| templates/ | 6 | ~870 |
| **TOTAL** | **32** | **~5,470** |

---

## ✅ STRUCTURE BENEFITS

### **1. Modularity**
- Easy to find files
- Clear responsibilities
- Independent modules

### **2. Scalability**
- Add new processors easily
- Add new API endpoints easily
- Add new templates easily

### **3. Maintainability**
- Single responsibility principle
- DRY (Don't Repeat Yourself)
- Consistent naming

### **4. Testability**
- Each module can be tested independently
- Clear input/output
- Mock dependencies easily

---

## 🔍 FINDING FILES

### **Need to add new file processor?**
→ `processors/` directory

### **Need to add new API endpoint?**
→ `api/` directory

### **Need to update styles?**
→ `static/css/` directory

### **Need to add JavaScript functionality?**
→ `static/js/` directory

### **Need to create new page?**
→ `templates/` directory

### **Need to change database schema?**
→ `core/database.py`

### **Need to add utility function?**
→ `core/helpers.py`

### **Need to change configuration?**
→ `config.py`

---

**Structure Version:** 2.0  
**Last Updated:** December 2024  
**Status:** ✅ Production Ready

