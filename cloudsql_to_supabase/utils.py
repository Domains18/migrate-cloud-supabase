import subprocess

def run_command(command, env=None):
    print(f"Running command: {command}")
    result = subprocess.run(command, shell=True, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode != 0:
        print(result.stderr.decode())
        raise Exception(f"Command failed with error: {result.stderr.decode()}")
    