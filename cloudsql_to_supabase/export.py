from pathlib import Path
import getpass
import os
import logging
from . import config, utils

logger = logging.getLogger('cloudsql_to_supabase.export')

def export_cloudsql(password: str = None, schema_only: bool = False, schema: str = None) -> Path:
    """
    Export a CloudSQL PostgreSQL database to a dump file.
    
    Args:
        password: CloudSQL database password. If None, will prompt.
        schema_only: If True, only export schema, not data
        schema: Specific schema to export. If None, uses the one from config
        
    Returns:
        Path to the created dump file
    """
    config.validate_config()
    schema = schema or config.CLOUDSQL_SCHEMA
    logger.info(f"Starting export from CloudSQL database: {config.CLOUDSQL_DB}, schema: {schema}")
    
    # Build pg_dump command
    cmd_parts = [
        f"pg_dump -U {config.CLOUDSQL_USER}",
        f"-h {config.CLOUDSQL_HOST}",
        f"-p {config.CLOUDSQL_PORT}",
        f"-d {config.CLOUDSQL_DB}",
        f"--sslmode={config.CLOUDSQL_SSL_MODE}",
        "-F p",  # Plain text format
    ]
    
    # Add schema if not public
    if schema != "public":
        cmd_parts.append(f"-n {schema}")
    
    if schema_only:
        cmd_parts.append("--schema-only")
    
    cmd_parts.append(f"-f {config.OUTPUT_DUMP}")
    cmd = " ".join(cmd_parts)
    
    # Set up environment with password
    env = os.environ.copy()
    if password is None:
        env['PGPASSWORD'] = getpass.getpass("Enter Cloud SQL password: ")
    else:
        env['PGPASSWORD'] = password
    
    try:
        utils.run_command(cmd, env)
        logger.info(f"Export completed successfully, saved to {config.OUTPUT_DUMP}")
        return Path(config.OUTPUT_DUMP)
    except Exception as e:
        logger.error(f"Export failed: {e}")
        raise
