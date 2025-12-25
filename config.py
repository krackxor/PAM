"""
Application Configuration
Centralized configuration management
"""

import os
from pathlib import Path

# Base Directory
BASE_DIR = Path(__file__).parent

# Flask Configuration
class Config:
    """Base configuration"""
    SECRET_KEY = os.environ.get('55rytrhtfht6tu8tykmgvwdq243ty5ri68p9iu880dvfq343654jluyo84ewr3rfef') or '55rytrhtfht6tu8tykmgvwdq243ty5ri68p9iu880dvfq343654jluyo84ewr3rfef'
    
    # Database
    DATABASE_PATH = os.environ.get('DATABASE_PATH') or BASE_DIR / 'database' / 'sunter.db'
    
    # Upload Settings
    UPLOAD_FOLDER = BASE_DIR / 'uploads'
    MAX_CONTENT_LENGTH = 100 * 1024 * 1024  # 100MB max file size
    ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls', 'txt'}
    
    # Pagination
    ITEMS_PER_PAGE = 50
    
    # Session
    PERMANENT_SESSION_LIFETIME = 3600  # 1 hour
    
    # API
    API_RATE_LIMIT = 100  # requests per minute
    
    @staticmethod
    def init_app(app):
        """Initialize application with config"""
        # Create directories if not exist
        os.makedirs(Config.UPLOAD_FOLDER, exist_ok=True)
        os.makedirs(Config.DATABASE_PATH.parent, exist_ok=True)


class DevelopmentConfig(Config):
    """Development configuration"""
    DEBUG = True
    TESTING = False


class ProductionConfig(Config):
    """Production configuration"""
    DEBUG = False
    TESTING = False
    
    # Override with stronger secret key in production
    SECRET_KEY = os.environ.get('SECRET_KEY')
    if not SECRET_KEY:
        raise ValueError("SECRET_KEY must be set in production")


class TestingConfig(Config):
    """Testing configuration"""
    TESTING = True
    DATABASE_PATH = BASE_DIR / 'database' / 'test.db'


# Configuration mapping
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}


def get_config(env=None):
    """Get configuration based on environment"""
    env = env or os.environ.get('FLASK_ENV', 'development')
    return config.get(env, config['default'])
