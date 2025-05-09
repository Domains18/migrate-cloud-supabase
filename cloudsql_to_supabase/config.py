import os
from pathlib import Path
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('cloudsql_to_supabase')

# Load environment variables
load_dotenv()

# Database configurations
CLOUDSQL_USER = os.getenv("CLOUDSQL_USER")
CLOUDSQL_HOST = os.getenv("CLOUDSQL_HOST")
CLOUDSQL_DB = os.getenv("CLOUDSQL_DB")
CLOUDSQL_PORT = int(os.getenv("CLOUDSQL_PORT", 5432))
CLOUDSQL_SSL_MODE = os.getenv("CLOUDSQL_SSL_MODE", "prefer")
CLOUDSQL_SCHEMA = os.getenv("CLOUDSQL_SCHEMA", "public")

SUPABASE_USER = os.getenv("SUPABASE_USER", "postgres")
SUPABASE_HOST = os.getenv("SUPABASE_HOST")
SUPABASE_DB = os.getenv("SUPABASE_DB", "postgres")
SUPABASE_PASSWORD = os.getenv("SUPABASE_PASSWORD")
SUPABASE_PORT = int(os.getenv("SUPABASE_PORT", 5432))
SUPABASE_SSL_MODE = os.getenv("SUPABASE_SSL_MODE", "require")
SUPABASE_SCHEMA = os.getenv("SUPABASE_SCHEMA", "public")

# Output file paths
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "."))
OUTPUT_DUMP = OUTPUT_DIR / os.getenv("OUTPUT_DUMP", "backup.sql")
CLEANED_DUMP = OUTPUT_DIR / os.getenv("CLEANED_DUMP", "cleaned_backup.sql")

# Validation function
def validate_config():
    """Validate that the required environment variables are set."""
    required_vars = {
        "CLOUDSQL_USER": CLOUDSQL_USER,
        "CLOUDSQL_HOST": CLOUDSQL_HOST,
        "CLOUDSQL_DB": CLOUDSQL_DB,
        "SUPABASE_HOST": SUPABASE_HOST,
        "SUPABASE_PASSWORD": SUPABASE_PASSWORD,
    }
    
    missing = [var for var, value in required_vars.items() if not value]
    
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    
    # Create output directory if it doesn't exist
    OUTPUT_DIR.mkdir(exist_ok=True)
