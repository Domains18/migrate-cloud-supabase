from dotenv import load_dotenv
import os


load_dotenv()

CLOUDSQL_USER = os.getenv("CLOUDSQL_USER")
CLOUDSQL_HOST = os.getenv("CLOUDSQL_HOST")
CLOUDSQL_DB = os.getenv("CLOUDSQL_DB")
SUPABASE_USER = os.getenv("SUPABASE_USER")
SUPABASE_HOST = os.getenv("SUPABASE_HOST")
SUPABASE_DB = os.getenv("SUPABASE_DB", "postgres")
SUPABASE_PASSWORD = os.getenv("SUPABASE_PASSWORD")
SUPABASE_PORT = int(os.getenv("SUPABASE_PORT", 5432))
OUTPUT_DUMP = "backup.sql"
CLEANED_DUMP = "cleaned_backup.sql"


def validate_config():
    """
    Validate the configuration values.
    """
    required_vars = [
        "CLOUDSQL_USER",
        "CLOUDSQL_HOST",
        "CLOUDSQL_DB",
        "SUPABASE_USER",
        "SUPABASE_HOST",
        "SUPABASE_DB",
        "SUPABASE_PASSWORD",
        "SUPABASE_PORT",
    ]
    for var in required_vars:
        if not os.getenv(var):
            raise ValueError(f"Environment variable {var} is not set.")
    if not SUPABASE_PORT or SUPABASE_PORT <= 0:
        raise ValueError("SUPABASE_PORT must be a positive integer.")
    