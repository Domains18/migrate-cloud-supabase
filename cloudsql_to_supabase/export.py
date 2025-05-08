from . import config, utils
import getpass
import os


def export_cloud_sql():
    cmd = (
        f"pg_dump -U {config.CLOUDSQL_USER} -h {config.CLOUDSQL_HOST} "
        f"-d {config.CLOUDSQL_DB} --no-owner --no-privileges --no-comments "
        f"--no-acl --data-only --column-inserts > {config.OUTPUT_DUMP}"
    )
    env = os.environ.copy()
    env["PGPASSWORD"] = getpass.getpass(prompt="Enter Cloud SQL password: ")
    utils.run_command(cmd, env=env)