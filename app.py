"""
SUNTER DASHBOARD - Main Application
Entry point dengan mobile-first design
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
from api.sbrs import register_sbrs_routes
from api.belum_bayar import register_belum_bayar_routes
from api.pcez_performance import register_pcez_performance_routes

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
register_sbrs_routes(app, get_db)
register_belum_bayar_routes(app, get_db)
register_pcez_performance_routes(app, get_db)

# ==========================================
# MAIN ROUTES (UI) - Mobile First
# ==========================================

@app.route('/')
def index():
    """Main dashboard"""
    return render_template('index.html')

@app.route('/collection')
def collection_page():
    """Collection page"""
    return render_template('collection.html')

@app.route('/belum-bayar')
def belum_bayar_page():
    """Unpaid customers page"""
    return render_template('belum_bayar.html')

@app.route('/upload')
def upload_page():
    """Upload page"""
    return render_template('upload.html')

@app.route('/menu')
def menu():
    """Menu page"""
    return render_template('menu.html')

@app.route('/collection_dashboard')
def collection_dashboard():
    """Redirect old collection_dashboard to new collection page"""
    return redirect(url_for('collection_page'))

@app.route('/anomaly')
def anomaly():
    """Anomaly detection page"""
    return render_template('anomaly.html')

@app.route('/analisa')
def analisa():
    """Analisa page"""
    return render_template('analisa.html')

@app.route('/history')
def history():
    """History page"""
    return render_template('history.html')

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
    print("🚀 SUNTER DASHBOARD - MOBILE APP")
    print("=" * 60)
    print("📱 Mobile-First Design with Bottom Navigation")
    print("=" * 60)
    print("📌 Features:")
    print("   ✓ Collection Dashboard (Tables + Charts)")
    print("   ✓ Belum Bayar (Unpaid Customers)")
    print("   ✓ Bottom Navigation")
    print("   ✓ Swipe & Touch Optimized")
    print("=" * 60)
    print("🌐 Server: http://0.0.0.0:5000")
    print("=" * 60)
    
    app.run(host='0.0.0.0', port=5000, debug=True)
