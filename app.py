"""
SUNTER DASHBOARD - Main Application
Simplified entry point - delegates to modular components
"""

import os
from flask import Flask, render_template, redirect, url_for
from werkzeug.utils import secure_filename

# Configuration
from config import get_config

# Core imports
from core.database import init_db, get_db, close_db
from core.helpers import register_helpers

# API module imports
from api.kpi import register_kpi_routes
from api.collection import register_collection_routes
from api.anomaly import register_anomaly_routes
from api.analisa import register_analisa_routes
from api.upload import register_upload_routes
from api.history import register_history_routes

# Get configuration
config_class = get_config()

# Initialize Flask app
app = Flask(__name__)
app.config.from_object(config_class)

# Initialize config (create folders, etc)
config_class.init_app(app)

# Register database teardown
app.teardown_appcontext(close_db)

# Register template helpers (formatRupiah, etc)
register_helpers(app)

# Register API blueprints/routes
register_kpi_routes(app, get_db)
register_collection_routes(app, get_db)
register_anomaly_routes(app, get_db)
register_analisa_routes(app, get_db)
register_upload_routes(app, get_db)
register_history_routes(app, get_db)

# ==========================================
# MAIN ROUTES (UI)
# ==========================================

@app.route('/')
def index():
    """Main dashboard"""
    return render_template('index.html')

@app.route('/collection_dashboard')
def collection_dashboard():
    """Collection analytics dashboard"""
    return render_template('collection_dashboard.html')

@app.route('/menu')
def menu():
    """Navigation menu"""
    return render_template('menu.html')

@app.route('/login')
@app.route('/logout')
def auth_bypass():
    """Bypass authentication (for now)"""
    return redirect(url_for('index'))

# ==========================================
# ERROR HANDLERS
# ==========================================

@app.errorhandler(404)
def not_found(e):
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_error(e):
    return render_template('500.html'), 500

# ==========================================
# MAIN ENTRY POINT
# ==========================================

if __name__ == '__main__':
    # Initialize database if not exists
    DB_PATH = os.path.join('database', 'sunter.db')
    if not os.path.exists(DB_PATH):
        init_db(app)
        print("✅ Database initialized")
    
    print("=" * 60)
    print("🚀 SUNTER DASHBOARD STARTING")
    print("=" * 60)
    print("📌 Modular Architecture:")
    print("   ✓ Core: Database, Helpers")
    print("   ✓ Processors: MC, Collection, SBRS, MB, MainBill, Ardebt")
    print("   ✓ API: KPI, Collection, Anomaly, Analisa, Upload, History")
    print("=" * 60)
    print("🌐 Server: http://localhost:5000")
    print("=" * 60)
    
    app.run(host='0.0.0.0', port=5000, debug=True)
