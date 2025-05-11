import subprocess
import shlex
from typing import Dict, Optional, List, Union
import logging
import sys 

logger = logging.getLogger('cloudsql_to_supabase.utils')

if not logger.hasHandlers():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def run_command(cmd: str, env: Optional[Dict] = None, show_output: bool = True, progress_char: str = ".") -> None:
    """
    Run a shell command safely with proper logging and real-time output streaming
    that can act as a progress indicator.

    Args:
        cmd: Command to run
        env: Environment variables
        show_output: Whether to print command output to console
        progress_char: Character to print for basic progress if no stdout/stderr.
                       Set to None or empty string to disable.
    """
    safe_cmd = cmd
    
    
    if env and "PGPASSWORD" in env and env.get('PGPASSWORD'):
        safe_cmd = cmd.replace(env['PGPASSWORD'], '********')

    logger.info(f"Running command: {safe_cmd}")

    try:
        args = shlex.split(cmd)
        
        process = subprocess.Popen(
            args,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True, 
            bufsize=1,  
            universal_newlines=True 
        )

        
        if show_output and process.stdout:
            for line in iter(process.stdout.readline, ''):
                logger.info(line.strip())
                sys.stdout.flush() 
            process.stdout.close()

        
        
        
        
        
        stderr_output = ""
        if process.stderr:
            stderr_output = process.stderr.read()
            process.stderr.close()

        process.wait() 

        if process.returncode != 0:
            error_message = f"Command failed with exit code {process.returncode}"
            logger.error(error_message)
            if stderr_output:
                logger.error("Error output:\n" + stderr_output.strip())
            raise RuntimeError(f"{error_message}\n{stderr_output.strip()}")
        else:
            if stderr_output and show_output: 
                logger.warning("Standard error output (may contain warnings):\n" + stderr_output.strip())
            logger.info(f"Command '{safe_cmd}' executed successfully.")

    except FileNotFoundError:
        logger.error(f"Error: The command '{shlex.split(cmd)[0]}' was not found.")
        raise
    except Exception as e:
        logger.exception(f"Error executing command '{safe_cmd}': {e}")
        raise


if __name__ == '__main__':
    
    print("\n--- Example 1: Listing files with details (produces stdout) ---")
    try:
        
        command_to_run = 'ls -l'
        if sys.platform == "win32":
            command_to_run = 'dir /w'
        run_command_with_progress(command_to_run)
    except Exception as e:
        print(f"Example 1 failed: {e}")

    
    print("\n--- Example 2: Sleep and echo (simulates work) ---")
    try:
        run_command_with_progress('bash -c "echo Starting task...; sleep 2; echo Task nearly done; sleep 1; echo Task complete"')
        
        
    except Exception as e:
        print(f"Example 2 failed: {e}")

    
    print("\n--- Example 3: Command that fails (produces stderr) ---")
    try:
        run_command_with_progress("ls /nonexistentdirectory")
    except RuntimeError as e:
        print(f"Example 3 failed as expected: {e}")
    except FileNotFoundError as e: 
        print(f"Example 3 failed due to command not found: {e}")


    
    print("\n--- Example 4: Command with environment variable (password redaction) ---")
    try:
        
        
        
        sensitive_env = {"MY_VAR": "some_value", "PGPASSWORD": "mysecretpassword123"}
        run_command_with_progress('bash -c "echo PGPASSWORD is $PGPASSWORD"', env=sensitive_env)
        
        
    except Exception as e:
        print(f"Example 4 failed: {e}")

    
    print("\n--- Example 5: Command with a simple progress indicator (not implemented in this version, just showing output) ---")
    
    
    
    try:
        run_command_with_progress('bash -c "sleep 3; echo Done sleeping."')
        
        
    except Exception as e:
        print(f"Example 5 failed: {e}")