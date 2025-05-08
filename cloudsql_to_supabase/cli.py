import click
import logging
from pathlib import Path
from . import export, clean, import_, config

logger = logging.getLogger('cloudsql_to_supabase.cli')

@click.group()
@click.option('--verbose', '-v', is_flag=True, help="Enable verbose output")
def cli(verbose):
    """CloudSQL → Supabase migration tool"""
    if verbose:
        logging.getLogger('cloudsql_to_supabase').setLevel(logging.DEBUG)
    pass

@cli.command()
@click.option('--cloudsql-password', help="CloudSQL password (if not provided, will prompt)")
@click.option('--schema-only', is_flag=True, help="Only export database schema, not data")
@click.option('--skip-export', is_flag=True, help="Skip the export step (use existing dump)")
@click.option('--skip-clean', is_flag=True, help="Skip the cleaning step")
def migrate(cloudsql_password, schema_only, skip_export, skip_clean):
    """Run full migration from CloudSQL to Supabase"""
    try:
        if not skip_export:
            export.export_cloudsql(password=cloudsql_password, schema_only=schema_only)
        else:
            logger.info("Skipping export step")
            
        if not skip_clean:
            clean.clean_dump_file()
        else:
            logger.info("Skipping clean step")
            
        import_.import_to_supabase()
        click.echo("Migration completed successfully!")
        
    except Exception as e:
        logger.error(f"Migration failed: {str(e)}")
        click.echo(f"Error: {str(e)}", err=True)
        exit(1)

@cli.command()
@click.option('--cloudsql-password', help="CloudSQL password (if not provided, will prompt)")
@click.option('--schema-only', is_flag=True, help="Only export database schema, not data")
def backup(cloudsql_password, schema_only):
    """Only export CloudSQL to a dump file"""
    try:
        dump_file = export.export_cloudsql(password=cloudsql_password, schema_only=schema_only)
        click.echo(f"Backup completed successfully! File saved to: {dump_file}")
    except Exception as e:
        logger.error(f"Backup failed: {str(e)}")
        click.echo(f"Error: {str(e)}", err=True)
        exit(1)

@cli.command()
@click.option('--input-file', '-i', type=click.Path(exists=True), help="Input SQL dump file")
@click.option('--output-file', '-o', type=click.Path(), help="Output cleaned SQL file")
def clean_dump(input_file, output_file):
    """Clean a SQL dump file for Supabase compatibility"""
    try:
        input_path = Path(input_file) if input_file else None
        output_path = Path(output_file) if output_file else None
        
        result_file = clean.clean_dump_file(input_path, output_path)
        click.echo(f"Cleaning completed successfully! File saved to: {result_file}")
    except Exception as e:
        logger.error(f"Cleaning failed: {str(e)}")
        click.echo(f"Error: {str(e)}", err=True)
        exit(1)

@cli.command()
@click.option('--input-file', '-i', type=click.Path(exists=True), help="Input SQL dump file to import")
def import_db(input_file):
    """Import a cleaned SQL dump file into Supabase"""
    try:
        input_path = Path(input_file) if input_file else None
        import_.import_to_supabase(input_path)
        click.echo("Import completed successfully!")
    except Exception as e:
        logger.error(f"Import failed: {str(e)}")
        click.echo(f"Error: {str(e)}", err=True)
        exit(1)

@cli.command()
def validate():
    """Validate configuration"""
    try:
        config.validate_config()
        click.echo("Configuration is valid!")
        
        # Show current configuration
        click.echo("\nCurrent configuration:")
        click.echo(f"CloudSQL: {config.CLOUDSQL_USER}@{config.CLOUDSQL_HOST}:{config.CLOUDSQL_PORT}/{config.CLOUDSQL_DB}")
        click.echo(f"Supabase: {config.SUPABASE_USER}@{config.SUPABASE_HOST}:{config.SUPABASE_PORT}/{config.SUPABASE_DB}")
        click.echo(f"Output directory: {config.OUTPUT_DIR}")
        click.echo(f"Dump file: {config.OUTPUT_DUMP}")
        click.echo(f"Cleaned dump file: {config.CLEANED_DUMP}")
        
    except Exception as e:
        click.echo(f"Configuration error: {str(e)}", err=True)
        exit(1)