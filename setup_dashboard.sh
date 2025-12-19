#!/bin/bash

echo "🚀 Menyiapkan Dashboard Frontend di VPS..."

# 1. Buat package.json (Konfigurasi Project)
cat > package.json <<EOF
{
  "name": "pam-dashboard",
  "version": "1.0.0",
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

# 2. Buat Config Vite (Host 0.0.0.0 agar bisa diakses public)
cat > vite.config.js <<EOF
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

# 3. Setup Tailwind CSS
cat > tailwind.config.js <<EOF
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

cat > postcss.config.js <<EOF
export default {
  plugins: {
    tailwindcss: {},
    autoprefixer: {},
  },
}
EOF

# 4. Buat Struktur Folder src
mkdir -p src

# 5. Pindahkan App.jsx ke dalam src (jika ada di root)
if [ -f "App.jsx" ]; then
    mv App.jsx src/App.jsx
    echo "✅ File App.jsx ditemukan dan dipindahkan ke folder src/."
elif [ -f "src/App.jsx" ]; then
    echo "✅ File App.jsx sudah ada di folder src/."
else
    echo "⚠️ App.jsx tidak ditemukan. Pastikan Anda mengupload App.jsx ke folder ini."
fi

# 6. Buat Entry Point (main.jsx & index.css)
cat > src/main.jsx <<EOF
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

cat > src/index.css <<EOF
@tailwind base;
@tailwind components;
@tailwind utilities;
EOF

# 7. Buat index.html
cat > index.html <<EOF
<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>PAM DSS Dashboard</title>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/main.jsx"></script>
  </body>
</html>
EOF

echo "📦 Menginstall dependencies (npm install)..."
npm install

echo "✅ Setup Selesai!"
echo "👉 Sekarang jalankan: npm run dev"
echo "👉 Akses di browser: http://174.138.16.241:5173"
