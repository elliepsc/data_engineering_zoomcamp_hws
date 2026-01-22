"""Test configuration"""
from pipeline.config import Config
import pytest

def test_config_database_url():
    """Config MUST generate valid database URL"""
    cfg = Config()
    url = cfg.database_url
    
    assert 'postgresql://' in url
    assert cfg.DB_NAME in url

def test_config_validates_port():
    """Config MUST validate port is integer"""
    import os
    
    # Save original
    original = os.environ.get('POSTGRES_PORT')
    
    try:
        # Test invalid port
        os.environ['POSTGRES_PORT'] = 'invalid'
        
        with pytest.raises(ValueError, match="must be integer"):
            Config()  # Create NEW instance with invalid port
    
    finally:
        # Restore (always execute, even if test fails)
        if original:
            os.environ['POSTGRES_PORT'] = original
        else:
            os.environ.pop('POSTGRES_PORT', None)