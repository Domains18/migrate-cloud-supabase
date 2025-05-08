import os
import logging
from pathlib import Path
from typing import Optional
from . import config, utils



logger = logging.getLogger('cloudsql_to_supabase.import')



def import_to_supabase(input_file: Optional[Path] = None, password: Optional[str] = None) -> None:
    """
        import cleaned sql dump into supabase
        
        Args:
            input_file: Path to the sql dump file to import. if None, uses the default.
            password: supabase database password. if none uses from config
            
    """
    config.validate_config()
    dump_file = input_file or Path(config.CLEANED_DUMP)
    if not dump_file.exists():
        raise FileNotFoundError(f"Dump not found: {dump_file}")
    
    logger.info(f'importing data into supabase database: {config.SUPABASE_DB}')
    
    env = os.environ.copy()
    env['PGPASSWORD'] = password or config.SUPABASE_PASSWORD
    
    
    cmd = (
        f"psql -h {config.SUPABASE_HOST} "
        f"-p {config.SUPABASE_PORT} "
        f"-U {config.SUPABASE_USER} "
        f"-d {config.SUPABASE_DB} "
        f"--set ON_ERROR_STOP=on "  # Stop on first error
        f"--single-transaction "     # Run as a single transaction
        f"--sslmode={config.SUPABASE_SSL_MODE} "
        f"-f {dump_file}"
    )
    
    
    try:
        utils.run_command(cmd, env)
        logger.info("import completed successfully")
    except Exception as e:
        logger.error(f'import failed: {e}')
        raise