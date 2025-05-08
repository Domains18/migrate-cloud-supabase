import click
from . import export, clean, import_

@click.group()
def cli():
    """CLI for exporting, cleaning, and importing data between Cloud SQL and Supabase."""
    pass
@cli.command()
def migrate():
    """Migrate data from Cloud SQL to Supabase."""
    export.export_cloud_sql()
    clean.clean_dump_file()
    import_.import_to_supabase()
    
    
@cli.command()
def export():
    """Export data from Cloud SQL."""
    export.export_cloud_sql()