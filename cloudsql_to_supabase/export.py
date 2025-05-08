from . import config, utils
import getpass
import os
from pathlib import Path
import getpass
import logging
from . import config, utils


logger = logging.getLogger('cloud_to_supabase.export')


def export_cloudsql(password: str = None, schema_only: bool = False):
    
    """
    Export a CloudSQL PostgreSQL database to a dump file.
    
    Args:
        password: CloudSQL database password. If None, will prompt.
        schema_only: If True, only export schema, not data
        
    Returns:
        Path to the created dump file
    """
    
    config.validate_config()
    logger.info(f'starting export from cloudsql database: {config.CLOUDSQL_DB}')
    
    
    cmd_parts = [
        f"pg_dump -U {config.CLOUDSQL_USER}",
        f"-h {config.CLOUDSQL_HOST}",
        f"-p {config.CLOUDSQL_PORT}",
        f"-d {config.CLOUDSQL_DB}",
        f"--sslmode={config.CLOUDSQL_SSL_MODE}",
        "-F p",  # Plain text format
        f"-f {config.OUTPUT_DUMP}"
    ]
    
    
    if schema_only: 
        cmd_parts.append("--schema-ony")
        
        
    cmd = " ".join(cmd_parts)
    
    env = os.environ.copy()
    if password is None:
        env['PGPASSWORD'] = getpass.getpass('enter cloud sql password: ')
    else:
        env['PGPASSWORD'] = password
        
        
    try:
        utils.run_command(cmd, env)
        logger.info(f'export completed succesfully, saved to {config.OUTPUT_DUMP}')
        return Path(config.OUTPUT_DUMP)
    except Exception as e:
        logger.error(f"export failed: {e}")
        raise