import os
import logging
from pathlib import Path
from typing import Optional
from . import config, utils

logger = logging.getLogger('cloudsql_to_supabase.import')

def import_to_supabase(input_file: Optional[Path] = None, password: Optional[str] = None, schema: Optional[str] = None) -> None:
    """
    Import a cleaned SQL dump file into Supabase

    Args:
        input_file: Path to the SQL dump file to import. If None, uses the default.
        password: Supabase database password. If None, uses from config.
        schema: Target schema to import into. If None, uses the one from config.
    """
    config.validate_config()
    dump_file = input_file or Path(config.CLEANED_DUMP)
    target_schema = schema or config.SUPABASE_SCHEMA

    if not dump_file.exists():
        raise FileNotFoundError(f"Dump file not found: {dump_file}")

    logger.info(f"Importing file {dump_file} into Supabase database: {config.SUPABASE_DB}, schema: {target_schema}")

    # Set up environment with password
    env = os.environ.copy()
    env['PGPASSWORD'] = password or config.SUPABASE_PASSWORD

    # Create schema if it doesn't exist (and it's not 'public')
    if target_schema != "public":
        create_schema_cmd = (
            f"psql -h {config.SUPABASE_HOST} "
            f"-p {config.SUPABASE_PORT} "
            f"-U {config.SUPABASE_USER} "
            f"-d {config.SUPABASE_DB} "
            f"-c \"CREATE SCHEMA IF NOT EXISTS {target_schema};\""
        )
        try:
            logger.info(f"Creating schema if it doesn't exist: {target_schema}")
            utils.run_command(create_schema_cmd, env)
        except Exception as e:
            logger.warning(f"Failed to create schema, it may already exist: {e}")

    # Build psql command
    cmd = (
        f"psql -h {config.SUPABASE_HOST} "
        f"-p {config.SUPABASE_PORT} "
        f"-U {config.SUPABASE_USER} "
        f"-d {config.SUPABASE_DB} "
        f"--set ON_ERROR_STOP=on "
        f"--single-transaction "
    )

    # Set search_path if using non-public schema
    if target_schema != "public":
        cmd += f"--set search_path={target_schema} "

    # Attach SQL file
    cmd += f"-f {dump_file} "

    try:
        utils.run_command(cmd, env)
        logger.info("Import completed successfully")
    except Exception as e:
        logger.error(f"Import failed: {e}")
        raise
