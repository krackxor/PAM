#!/bin/bash

echo "════════════════════════════════════════════════════════════════"
echo "  PAM DSS V3.0 - Deployment Script"
echo "════════════════════════════════════════════════════════════════"
echo ""

# Warna untuk output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function untuk print dengan warna
print_success() { echo -e "${GREEN}✅ $1${NC}"; }
print_info() { echo -e "${BLUE}ℹ️  $1${NC}"; }
print_warning() { echo -e "${YELLOW}⚠️  $1${NC}"; }
print_error() { echo -e "${RED}❌ $1${NC}"; }

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
   print_warning "Script ini sebaiknya dijalankan dengan sudo untuk instalasi package"
fi

echo ""
print_info "Starting deployment process..."
echo ""

# ═══════════════════════════════════════════════════════════════════
# STEP 1: Install Dependencies
# ═══════════════════════════════════════════════════════════════════
echo "─────────────────────────────────────────────────────────────────"
echo "STEP 1: Installing System Dependencies"
echo "─────────────────────────────────────────────────────────────────"

# Check Python
if command -v python3 &> /dev/null; then
    print_success "Python3 is installed: $(python3 --version)"
else
    print_info "Installing Python3..."
    sudo apt update
    sudo apt install -y python3 python3-pip
fi

# Check Node.js
if command -v node &> /dev/null; then
    print_success "Node.js is installed: $(node --version)"
else
    print_info "Installing Node.js..."
    curl -fsSL https://deb.nodesource.com/setup_18.x | sudo -E bash -
    sudo apt install -y nodejs
fi

# Check npm
if command -v npm &> /dev/null; then
    print_success "npm is installed: $(npm --version)"
fi

echo ""

# ═══════════════════════════════════════════════════════════════════
# STEP 2: Setup Backend
# ═══════════════════════════════════════════════════════════════════
echo "─────────────────────────────────────────────────────────────────"
echo "STEP 2: Setting up Backend (Flask)"
echo "─────────────────────────────────────────────────────────────────"

# Create .env if not exists
if [ ! -f ".env" ]; then
    print_info "Creating .env file..."
    cat > .env << 'EOF'
# .env file
MONGO_URI="mongodb+srv://telmovidtmid:tmid@cluster0.exs4okw.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
MONGO_DB_NAME="PAM_DSS_DB"

# 🚨 DUMMY SECRET KEY: HARUS DIGANTI UNTUK PRODUKSI!
SECRET_KEY="1609825b0451458e65870020d2d31215f9b5c3d7e4a1a6f0b4d4a821e25e1a3b9d4e5f6e"

# DAFTAR PENGGUNA DAN PERANNYA (Format: USERNAME:PASSWORD:IS_ADMIN(True/False))
USER_LIST="heru:rahasiaadmin:True,romi:romi:False,muji:muji:False"
EOF
    print_success "Created .env file"
    print_warning "IMPORTANT: Edit .env and update MONGO_URI with your credentials!"
else
    print_success ".env file already exists"
fi

# Install Python dependencies
print_info "Installing Python dependencies..."
pip3 install -r requirements.txt --break-system-packages
print_success "Python dependencies installed"

# Initialize database
print_info "Initializing database..."
python3 -c "from utils import init_db; init_db()" || print_warning "Database initialization warning (check if MongoDB is accessible)"

echo ""

# ═══════════════════════════════════════════════════════════════════
# STEP 3: Setup Frontend
# ═══════════════════════════════════════════════════════════════════
echo "─────────────────────────────────────────────────────────────────"
echo "STEP 3: Setting up Frontend (React + Vite)"
echo "─────────────────────────────────────────────────────────────────"

# Create package.json
if [ ! -f "package.json" ]; then
    print_info "Creating package.json..."
    cat > package.json << 'EOF'
{
  "name": "pam-dss-dashboard",
  "version": "3.0.0",
  "type": "module",
  "scripts": {
    "dev": "vite",
    "build": "vite build",
    "preview": "vite preview"
  },
  "dependencies": {
    "lucide-react": "^0.294.0",
    "react": "^18.2.0",
    "react-dom": "^18.2.0"
  },
  "devDependencies": {
    "@types/react": "^18.2.43",
    "@types/react-dom": "^18.2.17",
    "@vitejs/plugin-react": "^4.2.1",
    "autoprefixer": "^10.4.16",
    "postcss": "^8.4.32",
    "tailwindcss": "^3.4.0",
    "vite": "^5.0.0"
  }
}
EOF
    print_success "Created package.json"
fi

# Create vite.config.js
if [ ! -f "vite.config.js" ]; then
    print_info "Creating vite.config.js..."
    cat > vite.config.js << 'EOF'
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    host: '0.0.0.0',
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:5000',
        changeOrigin: true
      }
    }
  }
})
EOF
    print_success "Created vite.config.js"
fi

# Create Tailwind config
if [ ! -f "tailwind.config.js" ]; then
    print_info "Creating tailwind.config.js..."
    cat > tailwind.config.js << 'EOF'
/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {},
  },
  plugins: [],
}
EOF
    print_success "Created tailwind.config.js"
fi

# Create PostCSS config
if [ ! -f "postcss.config.js" ]; then
    cat > postcss.config.js << 'EOF'
export default {
  plugins: {
    tailwindcss: {},
    autoprefixer: {},
  },
}
EOF
    print_success "Created postcss.config.js"
fi

# Create src directory structure
mkdir -p src

# Move App.jsx to src
if [ -f "App.jsx" ] && [ ! -f "src/App.jsx" ]; then
    mv App.jsx src/App.jsx
    print_success "Moved App.jsx to src/"
fi

# Create main.jsx
if [ ! -f "src/main.jsx" ]; then
    cat > src/main.jsx << 'EOF'
import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App.jsx'
import './index.css'

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
)
EOF
    print_success "Created src/main.jsx"
fi

# Create index.css
if [ ! -f "src/index.css" ]; then
    cat > src/index.css << 'EOF'
@tailwind base;
@tailwind components;
@tailwind utilities;
EOF
    print_success "Created src/index.css"
fi

# Create index.html
if [ ! -f "index.html" ]; then
    cat > index.html << 'EOF'
<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>PAM DSS Dashboard V3.0</title>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/main.jsx"></script>
  </body>
</html>
EOF
    print_success "Created index.html"
fi

# Install npm dependencies
print_info "Installing npm dependencies (this may take a few minutes)..."
npm install
print_success "npm dependencies installed"

echo ""

# ═══════════════════════════════════════════════════════════════════
# STEP 4: Setup Systemd Services (Optional)
# ═══════════════════════════════════════════════════════════════════
echo "─────────────────────────────────────────────────────────────────"
echo "STEP 4: Setup Systemd Services (Optional)"
echo "─────────────────────────────────────────────────────────────────"

read -p "Do you want to setup systemd services for auto-start? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    # Backend service
    sudo tee /etc/systemd/system/pam-dss-backend.service > /dev/null << EOF
[Unit]
Description=PAM DSS Backend API
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$(pwd)
Environment="PATH=$(pwd)/venv/bin"
ExecStart=/usr/bin/python3 app.py
Restart=always

[Install]
WantedBy=multi-user.target
EOF

    sudo systemctl daemon-reload
    sudo systemctl enable pam-dss-backend
    print_success "Backend service configured"
    
    print_info "To start backend service: sudo systemctl start pam-dss-backend"
    print_info "To check status: sudo systemctl status pam-dss-backend"
fi

echo ""

# ═══════════════════════════════════════════════════════════════════
# STEP 5: Setup Nginx (Optional)
# ═══════════════════════════════════════════════════════════════════
echo "─────────────────────────────────────────────────────────────────"
echo "STEP 5: Setup Nginx Reverse Proxy (Optional)"
echo "─────────────────────────────────────────────────────────────────"

read -p "Do you want to setup Nginx reverse proxy? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    # Install nginx if not present
    if ! command -v nginx &> /dev/null; then
        print_info "Installing Nginx..."
        sudo apt install -y nginx
    fi
    
    # Create nginx config
    sudo tee /etc/nginx/sites-available/pam-dss > /dev/null << 'EOF'
server {
    listen 80;
    server_name _;

    # Frontend
    location / {
        proxy_pass http://127.0.0.1:5173;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_cache_bypass $http_upgrade;
    }

    # Backend API
    location /api {
        proxy_pass http://127.0.0.1:5000;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }
}
EOF

    sudo ln -sf /etc/nginx/sites-available/pam-dss /etc/nginx/sites-enabled/
    sudo nginx -t && sudo systemctl reload nginx
    print_success "Nginx configured"
fi

echo ""

# ═══════════════════════════════════════════════════════════════════
# FINAL SUMMARY
# ═══════════════════════════════════════════════════════════════════
echo "════════════════════════════════════════════════════════════════"
echo "  ✅ Deployment Complete!"
echo "════════════════════════════════════════════════════════════════"
echo ""
print_success "PAM DSS V3.0 is ready to run!"
echo ""
echo "📋 Quick Start Commands:"
echo "─────────────────────────────────────────────────────────────────"
echo ""
echo "1️⃣  Start Backend (Flask API):"
echo "   python3 app.py"
echo "   → Server will run on http://174.138.16.241:5000"
echo ""
echo "2️⃣  Start Frontend (Development):"
echo "   npm run dev"
echo "   → Dashboard will run on http://174.138.16.241:5173"
echo ""
echo "3️⃣  Build for Production:"
echo "   npm run build"
echo "   → Built files will be in ./dist/"
echo ""
echo "─────────────────────────────────────────────────────────────────"
echo "📚 Documentation: See README.md for detailed usage"
echo "🔧 Configuration: Edit .env for database settings"
echo "🐛 Troubleshooting: Check logs with 'journalctl -u pam-dss-backend'"
echo "─────────────────────────────────────────────────────────────────"
echo ""
print_warning "REMEMBER: Update .env with your actual MongoDB credentials!"
echo ""
