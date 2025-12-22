# 💧 PAM JAYA SUNTER Dashboard

> **Professional Water Management System for PAM JAYA Rayon 34 & 35**

[![Version](https://img.shields.io/badge/version-3.0.0-blue.svg)](https://github.com/pamjaya/sunter-dashboard)
[![Python](https://img.shields.io/badge/python-3.8+-brightgreen.svg)](https://www.python.org/)
[![Flask](https://img.shields.io/badge/flask-3.0.0-green.svg)](https://flask.palletsprojects.com/)
[![License](https://img.shields.io/badge/license-MIT-orange.svg)](LICENSE)

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Installation](#installation)
- [Usage](#usage)
- [API Documentation](#api-documentation)
- [Database Schema](#database-schema)
- [Development](#development)
- [Deployment](#deployment)
- [Contributing](#contributing)
- [License](#license)

## 🎯 Overview

**PAM JAYA SUNTER Dashboard** adalah sistem manajemen data air minum yang dirancang khusus untuk monitoring dan analisis operasional Rayon 34 & 35 PAM JAYA. Sistem ini menyediakan dashboard real-time, deteksi anomali otomatis, dan tools analisis manual untuk meningkatkan efisiensi operasional.

### Key Highlights

- 📊 **Real-time Dashboard** - KPI monitoring dengan visualisasi interaktif
- 🔍 **Anomaly Detection** - Deteksi otomatis 7 jenis anomali meter
- 📁 **Multi-file Upload** - Auto-detect tipe file & periode
- 📈 **Multi-period Tracking** - History data lengkap multi-periode
- 🛠️ **Manual Analysis** - Collaborative analysis tools
- 🎨 **Modern UI/UX** - Responsive design dengan Glassmorphism

## ✨ Features

### 1. Dashboard & KPI Monitoring
- Real-time collection rate tracking
- Target vs actual comparison
- Rayon breakdown analysis
- Daily trend charts
- Customer statistics

### 2. Anomaly Detection System
- **Extreme Usage** - Pemakaian >100m³
- **Usage Drop** - Penurunan >50%
- **Zero Usage** - Tidak ada pemakaian
- **Negative Reading** - Stand negatif
- **Incorrect Recording** - Salah catat
- **Rebill Cases** - Koreksi tagihan
- **Estimated Reading** - Non-ACTUAL

### 3. Data Management
- MC (Master Customer) management
- Collection transaction tracking
- SBRS (Meter Reading) integration
- MB (Master Bayar) history
- MainBill processing
- Ardebt (Tunggakan) monitoring

### 4. Analysis Tools
- Manual analysis workbench
- Customer profile viewer
- Payment history tracker
- Multi-period comparison
- Export to Excel/CSV

### 5. File Processing
- Auto-detect file type
- Auto-detect periode
- Multi-file batch upload
- Validation & error handling
- Upload history tracking

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     Presentation Layer                   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │  HTML/Jinja2 │  │   JavaScript │  │   Bootstrap  │  │
│  │  Templates   │  │   Chart.js   │  │   Tailwind   │  │
│  └──────────────┘  └──────────────┘  └──────────────┘  │
└─────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────┐
│                      API Layer (Flask)                   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │  Dashboard   │  │  Customers   │  │  Collections │  │
│  │  Endpoints   │  │  CRUD API    │  │  API         │  │
│  └──────────────┘  └──────────────┘  └──────────────┘  │
└─────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────┐
│                    Service Layer                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │  Customer    │  │  Collection  │  │   Anomaly    │  │
│  │  Service     │  │  Service     │  │   Detector   │  │
│  └──────────────┘  └──────────────┘  └──────────────┘  │
└─────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────┐
│                    Data Layer                            │
│  ┌──────────────────────────────────────────────────┐   │
│  │         Database Manager (SQLite)                 │   │
│  │  - Connection pooling                             │   │
│  │  - Transaction management                         │   │
│  │  - Query optimization                             │   │
│  └──────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────┐
│                   Database (SQLite)                      │
│  customers │ collections │ meter_readings │ analyses     │
└─────────────────────────────────────────────────────────┘
```

### Technology Stack

| Layer | Technology |
|-------|-----------|
| **Backend** | Python 3.8+, Flask 3.0 |
| **Database** | SQLite 3 |
| **Frontend** | HTML5, JavaScript ES6+, Bootstrap 5, Chart.js |
| **Data Processing** | Pandas, NumPy |
| **File Handling** | openpyxl, xlrd, chardet |
| **Testing** | pytest, pytest-flask |
| **Code Quality** | black, flake8, pylint |

## 🚀 Installation

### Prerequisites

- Python 3.8 or higher
- pip (Python package manager)
- Git (for version control)

### Step 1: Clone Repository

```bash
git clone https://github.com/pamjaya/sunter-dashboard.git
cd sunter-dashboard
```

### Step 2: Create Virtual Environment

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# Linux/Mac
python3 -m venv venv
source venv/bin/activate
```

### Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 4: Configure Environment

```bash
# Copy example environment file
cp .env.example .env

# Edit .env with your settings
# Set SECRET_KEY, DATABASE_PATH, etc.
```

### Step 5: Initialize Database

```bash
# Using Flask CLI
flask init-db

# Or run the script
python -c "from main_app import create_app; app = create_app(); app.cli('init-db')"
```

### Step 6: Run Application

```bash
# Development server
python main_app.py

# Or using Flask CLI
flask run --host=0.0.0.0 --port=5000
```

Visit: `http://localhost:5000`

## 📚 Usage

### 1. Upload Master Customer (MC)

1. Click **UPLOAD** button
2. Select **MC** file type
3. Choose periode (month/year)
4. Select your `.xlsx` or `.csv` file
5. Click **UPLOAD**

### 2. Upload Collection Data

1. Select **COLLECTION** file type
2. Upload daily transaction file
3. System auto-detects payment type (Current/Undue)

### 3. Upload SBRS (Meter Reading)

1. Select **SBRS** file type
2. Upload meter reading file
3. System detects anomalies automatically

### 4. View Dashboard

- Monitor KPI in real-time
- View rayon breakdown
- Check daily trends
- Export reports

### 5. Anomaly Detection

1. Go to **METER** tab
2. Click anomaly category
3. View detailed list
4. Click row for full history
5. Create manual analysis if needed

### 6. Manual Analysis

1. Go to **ANALISA** tab
2. Click **Create New Analysis**
3. Enter customer NOMEN
4. Select anomaly type
5. Add description
6. Assign to team member
7. Track progress

## 📡 API Documentation

### Dashboard KPI

```http
GET /api/kpi_data
```

**Response:**
```json
{
  "total_pelanggan": 5000,
  "total_kubikasi": 125000,
  "target": {
    "total_nomen": 5000,
    "total_nominal": 2500000000,
    "sudah_bayar_nomen": 4500,
    "sudah_bayar_nominal": 2250000000
  },
  "collection_rate": 90.5
}
```

### Customer Search

```http
GET /api/customers/search?q=12345&rayon=34
```

**Response:**
```json
{
  "results": [
    {
      "nomen": "123456",
      "name": "PT Example",
      "address": "Jl. Sunter No.1",
      "rayon": "34"
    }
  ]
}
```

### Anomaly Detection

```http
GET /api/anomaly/summary
```

**Response:**
```json
{
  "periode": "Desember 2025",
  "anomalies": {
    "extreme": {"count": 45},
    "zero": {"count": 120},
    "turun": {"count": 67}
  }
}
```

For complete API documentation, see [API.md](docs/API.md)

## 🗄️ Database Schema

### Main Tables

```sql
-- Customers (Master Pelanggan)
customers (
  nomen PRIMARY KEY,
  name, address, rayon,
  target_mc, kubikasi,
  periode_month, periode_year
)

-- Collections (Transactions)
collections (
  id PRIMARY KEY,
  nomen FOREIGN KEY,
  payment_date, amount,
  payment_type, periode_month
)

-- Meter Readings (SBRS)
meter_readings (
  id PRIMARY KEY,
  nomen, volume,
  read_method, skip_status,
  periode_month, periode_year
)

-- Manual Analyses
manual_analyses (
  id PRIMARY KEY,
  nomen FOREIGN KEY,
  anomaly_type, status,
  priority, assigned_to
)
```

For complete schema, see [DATABASE_SCHEMA.md](docs/DATABASE_SCHEMA.md)

## 👨‍💻 Development

### Project Structure

```
sunter-dashboard/
├── app/                    # Application modules
│   ├── api/               # API endpoints
│   ├── services/          # Business logic
│   ├── models/            # Data models
│   └── utils/             # Utilities
├── static/                # Static assets
├── templates/             # HTML templates
├── tests/                 # Unit tests
├── database/              # Database files
├── uploads/               # Uploaded files
└── logs/                  # Application logs
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=app --cov-report=html

# Run specific test file
pytest tests/test_services.py
```

### Code Quality

```bash
# Format code
black app/ tests/

# Lint code
flake8 app/ tests/

# Type checking
mypy app/
```

### Database Management

```bash
# Create backup
flask backup-db

# Show statistics
flask db-stats

# Optimize database
flask optimize-db
```

## 🚢 Deployment

### Production with Gunicorn

```bash
# Install gunicorn
pip install gunicorn

# Run with 4 workers
gunicorn -w 4 -b 0.0.0.0:8000 "main_app:create_app()"
```

### Production with Waitress (Windows)

```bash
# Install waitress
pip install waitress

# Run server
waitress-serve --host=0.0.0.0 --port=8000 main_app:app
```

### Docker Deployment

```bash
# Build image
docker build -t sunter-dashboard .

# Run container
docker run -d -p 8000:8000 sunter-dashboard
```

For detailed deployment guide, see [DEPLOYMENT.md](docs/DEPLOYMENT.md)

## 🤝 Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details.

### Development Workflow

1. Fork the repository
2. Create feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit changes (`git commit -m 'Add AmazingFeature'`)
4. Push to branch (`git push origin feature/AmazingFeature`)
5. Open Pull Request

## 📄 License

This project is licensed under the MIT License - see [LICENSE](LICENSE) file for details.

## 👥 Team

- **Project Lead**: PAM JAYA IT Team
- **Backend Development**: [Your Name]
- **Frontend Development**: [Your Name]
- **Database Design**: [Your Name]

## 📞 Support

For support, email: support@pamjaya.co.id

## 🙏 Acknowledgments

- PAM JAYA for project support
- Flask community for excellent framework
- Bootstrap team for UI components
- Chart.js for visualization

---

**Made with ❤️ by PAM JAYA IT Team**

**Version**: 3.0.0 | **Last Updated**: December 23, 2025
