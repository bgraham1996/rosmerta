"""
Shared database configuration — reads from .env
"""
import os
from dotenv import load_dotenv

load_dotenv()

def get_db_config(no_db=False):
    """Return PostgreSQL connection dict from environment variables."""
    if no_db:
        return None
    return {
        'host': os.getenv('DB_HOST', '10.0.0.1'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'dbname': os.getenv('DB_NAME', 'stocks'),
        'user': os.getenv('DB_USER', 'stock_user'),
        'password': os.getenv('DB_PASS', ''),
    }

