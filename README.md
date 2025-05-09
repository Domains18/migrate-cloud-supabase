# CloudSQL to Supabase Migration Tool

A robust CLI tool to safely migrate PostgreSQL databases from Google Cloud SQL to Supabase.


## Installation

### 1. Clone the repository (or create the project structure)

```bash
git clone https://github.com/Domains18/migrate-cloud-supabase.git
cd cloudsql-to-supabase
```

### 2. Set up a virtual environment (recommended)

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install the package

```bash
pip install -e .
```

### 4. Configure your environment

Copy the example `.env` file and edit it with your database details:

```bash
cp .env.example .env
```

Edit the `.env` file with your database credentials:

```env
# CloudSQL Configuration
CLOUDSQL_USER=your_cloudsql_user
CLOUDSQL_HOST=your_cloudsql_host
CLOUDSQL_DB=your_database_name
CLOUDSQL_PORT=5432
CLOUDSQL_SSL_MODE=prefer
CLOUDSQL_SCHEMA=public  # Source schema to export from

# Supabase Configuration
SUPABASE_USER=postgres
SUPABASE_HOST=your_supabase_host.supabase.co
SUPABASE_DB=postgres
SUPABASE_PASSWORD=your_supabase_password
SUPABASE_PORT=5432
SUPABASE_SSL_MODE=require
SUPABASE_SCHEMA=public  # Target schema to import into

# Output Configuration
OUTPUT_DIR=./outputs
OUTPUT_DUMP=backup.sql
CLEANED_DUMP=cleaned_backup.sql
```

## Usage

### Validate Your Configuration

Before running any migration, check that your configuration is valid:

```bash
python main.py validate
```

### Full Migration

Run a complete migration from CloudSQL to Supabase in one command:

```bash
python main.py migrate
```

You'll be prompted for your CloudSQL password during the process.

### Full Migration with Custom Schema

To migrate data from a specific CloudSQL schema to a custom Supabase schema:

```bash
python main.py migrate --source-schema=my_source_schema --target-schema=my_target_schema
```

### Using with a Manually Downloaded Dump File

If you've already downloaded a PostgreSQL dump from CloudSQL:

```bash
# First clean the dump for Supabase compatibility
python main.py clean-dump --input-file your_dump.sql --target-schema=my_schema

# Then import the cleaned dump to Supabase
python main.py import-db --input-file cleaned_backup.sql --schema=my_schema
```

### Step-by-Step Migration

You can also run each step of the migration separately:

1. Export from CloudSQL:
   ```bash
   python main.py backup

## Requirements

- Python 3.8+
- PostgreSQL client tools (psql, pg_dump)
- Access to both CloudSQL and Supabase databases

## Troubleshooting

### Common Issues

1. **"Command not found" errors**
   - Ensure that PostgreSQL client tools are installed and in your PATH
   - On Ubuntu/Debian: `sudo apt-get install postgresql-client`
   - On macOS with Homebrew: `brew install libpq` and `brew link --force libpq`

2. **Connection errors**
   - Verify your database credentials in the `.env` file
   - Check that your IP is allowed in both CloudSQL and Supabase network settings
   - Test connection manually: `psql -h your_host -U your_user -d your_db`

3. **Import errors**
   - Common Supabase import issues are fixed by the cleaning step
   - For specific table errors, you may need to customize the `clean.py` replacement rules

### Getting Help

For more detailed logs, use the `--verbose` flag with any command:

```bash
python main.py migrate --verbose
```