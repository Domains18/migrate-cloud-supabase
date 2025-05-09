import subprocess
import shlex
from typing import Dict, Optional, List, Union
import logging

logger = logging.getLogger('cloudsql_to_supabase.utils')

def run_command(cmd: str, env: Optional[Dict] = None, show_output: bool = True) -> subprocess.CompletedProcess:
    """
    Run a shell command safely with proper logging and error handling.
    
    Args:
        cmd: Command to run
        env: Environment variables
        show_output: Whether to print command output to console
        
    Returns:
        CompletedProcess instance
    """
    # Log the command (with password redacted if present)
    safe_cmd = cmd
    if "PGPASSWORD" in str(env):
        safe_cmd = cmd.replace(env.get('PGPASSWORD', ''), '********')
    
    logger.info(f"Running command: {safe_cmd}")
    
    try:
        # Use shlex.split for safer command execution
        args = shlex.split(cmd)
        result = subprocess.run(
            args, 
            env=env, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            text=True,
            check=False  # We'll handle errors ourselves
        )
        
        if result.returncode != 0:
            logger.error(f"Command failed with exit code {result.returncode}")
            logger.error(result.stderr)
            raise RuntimeError(f"Command failed: {result.stderr}")
        
        if show_output and result.stdout:
            logger.info(result.stdout)
            
        return result
    except Exception as e:
        logger.exception(f"Error executing command: {e}")
        raise
