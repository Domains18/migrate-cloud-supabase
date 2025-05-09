import subprocess
import shlex
from typing import Dict, Optional, List, Union
import logging
import sys # Added for flushing stdout

logger = logging.getLogger('cloudsql_to_supabase.utils')
# Configure a basic logger if no configuration is present
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
    # Redact password if PGPASSWORD is in the environment variables
    # Ensure env is not None and PGPASSWORD is in env before trying to replace
    if env and "PGPASSWORD" in env and env.get('PGPASSWORD'):
        safe_cmd = cmd.replace(env['PGPASSWORD'], '********')

    logger.info(f"Running command: {safe_cmd}")

    try:
        args = shlex.split(cmd)
        # Use Popen for real-time output streaming
        process = subprocess.Popen(
            args,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True, # Decodes stdout and stderr as text
            bufsize=1,  # Line buffered
            universal_newlines=True # Ensures text mode works across platforms
        )

        # Real-time streaming of stdout
        if show_output and process.stdout:
            for line in iter(process.stdout.readline, ''):
                logger.info(line.strip())
                sys.stdout.flush() # Ensure output is displayed immediately
            process.stdout.close()

        # Real-time streaming of stderr (or after stdout if not interleaved)
        # For fully interleaved output, a more complex select-based approach or threads would be needed.
        # This approach first prints all stdout, then all stderr if any error occurred during execution.
        # A simpler way for now is to wait and then read stderr.
        
        stderr_output = ""
        if process.stderr:
            stderr_output = process.stderr.read()
            process.stderr.close()

        process.wait() # Wait for the process to complete

        if process.returncode != 0:
            error_message = f"Command failed with exit code {process.returncode}"
            logger.error(error_message)
            if stderr_output:
                logger.error("Error output:\n" + stderr_output.strip())
            raise RuntimeError(f"{error_message}\n{stderr_output.strip()}")
        else:
            if stderr_output and show_output: # Log stderr even on success if it contains messages (e.g., warnings)
                logger.warning("Standard error output (may contain warnings):\n" + stderr_output.strip())
            logger.info(f"Command '{safe_cmd}' executed successfully.")

    except FileNotFoundError:
        logger.error(f"Error: The command '{shlex.split(cmd)[0]}' was not found.")
        raise
    except Exception as e:
        logger.exception(f"Error executing command '{safe_cmd}': {e}")
        raise

# --- Example Usage ---
if __name__ == '__main__':
    # Example 1: A command that produces continuous output
    print("\n--- Example 1: Listing files with details (produces stdout) ---")
    try:
        # On Windows, use 'dir /w'. On Linux/macOS, use 'ls -l'.
        command_to_run = 'ls -l'
        if sys.platform == "win32":
            command_to_run = 'dir /w'
        run_command_with_progress(command_to_run)
    except Exception as e:
        print(f"Example 1 failed: {e}")

    # Example 2: A command that might produce stderr output or take a moment
    print("\n--- Example 2: Sleep and echo (simulates work) ---")
    try:
        run_command_with_progress('bash -c "echo Starting task...; sleep 2; echo Task nearly done; sleep 1; echo Task complete"')
        # For windows:
        # run_command_with_progress('cmd /c "echo Starting task... & timeout /t 2 /nobreak >nul & echo Task nearly done & timeout /t 1 /nobreak >nul & echo Task complete"')
    except Exception as e:
        print(f"Example 2 failed: {e}")

    # Example 3: A command that fails
    print("\n--- Example 3: Command that fails (produces stderr) ---")
    try:
        run_command_with_progress("ls /nonexistentdirectory")
    except RuntimeError as e:
        print(f"Example 3 failed as expected: {e}")
    except FileNotFoundError as e: # Catch if ls or bash isn't found
        print(f"Example 3 failed due to command not found: {e}")


    # Example 4: Command with environment variable (password redaction test)
    print("\n--- Example 4: Command with environment variable (password redaction) ---")
    try:
        # This command will just print the env var if MY_VAR is set.
        # The actual password isn't used by 'echo' in a sensitive way,
        # but it demonstrates the redaction in logging.
        sensitive_env = {"MY_VAR": "some_value", "PGPASSWORD": "mysecretpassword123"}
        run_command_with_progress('bash -c "echo PGPASSWORD is $PGPASSWORD"', env=sensitive_env)
        # For Windows (won't directly use PGPASSWORD like bash, but shows env passing):
        # run_command_with_progress('cmd /c "echo PGPASSWORD is %PGPASSWORD%"', env=sensitive_env)
    except Exception as e:
        print(f"Example 4 failed: {e}")

    # Example 5: Command that produces no stdout for a while
    print("\n--- Example 5: Command with a simple progress indicator (not implemented in this version, just showing output) ---")
    # To implement a visual spinner for commands that don't output, you'd need threading.
    # The current version streams line by line, so if there are no lines, there's no new log output
    # until the command finishes or prints something.
    try:
        run_command_with_progress('bash -c "sleep 3; echo Done sleeping."')
        # For windows:
        # run_command_with_progress('cmd /c "timeout /t 3 /nobreak >nul & echo Done sleeping."')
    except Exception as e:
        print(f"Example 5 failed: {e}")