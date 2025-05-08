from . import config, utils
import os



def import_to_supabase():
    print("\n Importing into Supabase")
    env = os.environ.copy()
    env["PGPASSWORD"] = config.SUPABASE_PASSWORD
    cmd = (
        f"psql -h {config.SUPABASE_HOST} -p {config.SUPABASE_PORT} -U {config.SUPABASE_USER} "
        f"-d {config.SUPABASE_DB} -f {config.CLEANED_DUMP} \"sslmode=require\""
    )
    utils.run_command(cmd, env=env)