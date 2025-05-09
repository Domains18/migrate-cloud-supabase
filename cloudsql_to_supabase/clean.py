import re
import logging
from pathlib import Path
from typing import Optional, List # Added List for type hinting

# Assuming config is a module in the same directory or an accessible path
from . import config # If config.py is in the same directory as this script

logger = logging.getLogger('cloudsql_to_supabase.clean')

class DumpCleaner:
    def __init__(
        self,
        input_file: Optional[Path] = None,
        output_file: Optional[Path] = None,
        target_schema: Optional[str] = None,
    ) -> None:
        self.input_file = Path(input_file or config.OUTPUT_DUMP)
        self.output_file = Path(output_file or config.CLEANED_DUMP)
        self.target_schema = target_schema or config.SUPABASE_SCHEMA

        logger.info(f"Initializing DumpCleaner. Input: '{self.input_file}', Output: '{self.output_file}', Target Schema: '{self.target_schema}'")

        # List of problematic roles (typically source-specific admin/system roles)
        # whose usage should be entirely removed if they are not meant to exist in the target.
        # 'postgres' is usually the main superuser in Supabase, so it's generally NOT added here.
        self.problematic_roles_to_filter: List[str] = ["cloudsqlsuperuser", "cloudsqladmin"]
        # Add other cloud-specific roles like 'rdsadmin', etc., if applicable.

        self.problematic_role_match_pattern: Optional[str] = None
        if self.problematic_roles_to_filter:
            role_patterns = []
            for role in self.problematic_roles_to_filter:
                escaped_role = re.escape(role)
                # Matches "role_name" OR role_name (as a whole word)
                role_patterns.append(f'"{escaped_role}"')
                role_patterns.append(rf'{escaped_role}\b')
            self.problematic_role_match_pattern = rf"(?:{'|'.join(role_patterns)})"
            logger.debug(f"Problematic role regex part: {self.problematic_role_match_pattern}")


        # Patterns to skip lines entirely
        self.skip_patterns = [
            re.compile(r'^\s*(CREATE|ALTER)\s+ROLE\b', re.IGNORECASE), # Skip any role creation/alteration
            re.compile(r'^\s*COMMENT ON EXTENSION\s+(?:pg_stat_statements|plpgsql)\s*;', re.IGNORECASE), # More specific
            re.compile(r'^\s*COMMENT ON EXTENSION\b', re.IGNORECASE), # General extension comments
            re.compile(r'^\s*SET\s+(?:transaction_timeout|idle_in_transaction_session_timeout|lock_timeout|statement_timeout)\s*=\s*.*?;', re.IGNORECASE),
            re.compile(r'^\s*SET\s+default_transaction_read_only\s*=\s*on;', re.IGNORECASE), # Often in Cloud SQL read replicas
            re.compile(r'^\s*GRANT\s+pg_signal_backend\s+TO\s+cloudsqlsuperuser;', re.IGNORECASE), # Specific Cloud SQL grant
        ]

        if self.problematic_role_match_pattern:
            self.skip_patterns.extend([
                # Skip SET ROLE or SET SESSION AUTHORIZATION to problematic roles
                re.compile(r'^\s*SET\s+(?:ROLE|SESSION\s+AUTHORIZATION)\s+' + self.problematic_role_match_pattern + r'\s*;', re.IGNORECASE),

                # Skip any GRANT or REVOKE statement that mentions a problematic role.
                # This is broad but effective for preventing errors if these roles don't exist.
                # It catches grants TO the role, grants OF the role, and revokes involving the role.
                re.compile(r'^\s*(?:GRANT|REVOKE).*' + self.problematic_role_match_pattern + r'.*?;', re.IGNORECASE),

                # Skip ALTER DEFAULT PRIVILEGES for problematic roles
                re.compile(r'^\s*ALTER DEFAULT PRIVILEGES\s+FOR ROLE\s+' + self.problematic_role_match_pattern + r'\s+.*?;', re.IGNORECASE),
            ])

        # Replacement rules: list of tuples (compiled_pattern, replacement_str)
        if self.target_schema == "public":
            owner_replacement_str = 'OWNER TO public;'
        else:
            # Quote the schema name if it's not 'public' to preserve case and handle special characters.
            owner_replacement_str = f'OWNER TO "{self.target_schema}";'
        logger.debug(f"Owner replacement string: {owner_replacement_str}")

        self.replacement_rules = [
            # Matches OWNER TO any_user; or OWNER TO "any_user";
            (re.compile(r'OWNER TO (?:"[^"]+"|[^\s;]+);', re.IGNORECASE), owner_replacement_str),
            (re.compile(r'^\s*CREATE SCHEMA\s+.*?;', re.IGNORECASE), '-- Schema creation removed (Supabase extension schema used instead)'),
            (re.compile(r'^\s*ALTER SCHEMA\s+public\s+OWNER TO .*?;', re.IGNORECASE), f'-- ALTER SCHEMA public OWNER removed, will be owned by supabase admin/postgres'),
             # Remove or comment out setting search_path to only public if we are using a target schema
            (re.compile(r"^\s*SELECT pg_catalog\.set_config\('search_path', '', false\);\s*$", re.IGNORECASE), "-- SELECT pg_catalog.set_config('search_path', '', false); (emptying search_path removed)"),
        ]

        # Schema-specific replacements if target_schema is not 'public'
        if self.target_schema != "public":
            schema_replacements = [
                # Change SET search_path = public, ''; (or similar) to include the target schema first.
                # This regex handles various forms of search_path settings.
                (r"SET search_path = (?:public(?:,\s*)?)([^;]*);", rf"SET search_path = \"{self.target_schema}\", public\1;"),
                (r"CREATE TABLE public\.([\w_]+)", rf'CREATE TABLE "{self.target_schema}".\1'),
                (r"ALTER TABLE ONLY public\.([\w_]+)", rf'ALTER TABLE ONLY "{self.target_schema}".\1'),
                (r"CREATE SEQUENCE public\.([\w_]+)", rf'CREATE SEQUENCE "{self.target_schema}".\1'),
                (r"ALTER SEQUENCE public\.([\w_]+)", rf'ALTER SEQUENCE "{self.target_schema}".\1'),
                (r"CREATE VIEW public\.([\w_]+)", rf'CREATE VIEW "{self.target_schema}".\1'),
                (r"CREATE FUNCTION public\.([\w_]+)", rf'CREATE FUNCTION "{self.target_schema}".\1'),
                (r"CREATE TRIGGER ", "CREATE TRIGGER "), # No change, but ensure it's before more specific public. replacements if any
                (r"REFERENCES public\.([\w_]+)", rf'REFERENCES "{self.target_schema}".\1'),
                 # For foreign keys that might reference public schema (e.g. from extensions like auth, storage in public)
                # we generally want them to stay as public. This needs careful consideration.
                # The above rule is quite broad. If you have FKs to your own tables previously in public, they should be remapped.
                # If FKs are to Supabase's own public tables (auth.users etc.), they should NOT be remapped.
                # This often requires more specific rules or post-import adjustments.
                # For now, we are broadly remapping 'public.' to the target schema.
            ]
            for pattern_str, repl_str in schema_replacements:
                self.replacement_rules.append((re.compile(pattern_str, re.IGNORECASE), repl_str))
        else: # If target_schema is 'public'
            self.replacement_rules.append(
                (re.compile(r"SET search_path = .*?;", re.IGNORECASE), r"SET search_path = public, pg_catalog;") # Ensure a sane default
            )


    def clean_dump_file(self) -> Path:
        logger.info(f"Starting cleaning of dump file: {self.input_file}")

        if not self.input_file.exists():
            logger.error(f"Input file not found: {self.input_file}")
            raise FileNotFoundError(f"Input file not found: {self.input_file}")

        skipped_lines, modified_lines, lines_processed = 0, 0, 0

        # Create output directory if it doesn't exist
        self.output_file.parent.mkdir(parents=True, exist_ok=True)

        with self.input_file.open('r', encoding='utf-8') as infile, \
             self.output_file.open('w', encoding='utf-8') as outfile:
            for line_num, line in enumerate(infile, 1):
                lines_processed += 1
                original_line = line
                
                # Check skip patterns first
                should_skip = False
                for pattern in self.skip_patterns:
                    if pattern.search(line):
                        # logger.debug(f"Line {line_num} skipped by pattern: {pattern.pattern}\n\t{line.strip()}")
                        skipped_lines += 1
                        should_skip = True
                        break
                if should_skip:
                    outfile.write(f"-- SKIPPED LINE (by pattern): {line.strip()}\n") # Write as comment for audit
                    continue

                # Apply replacement rules
                modified_in_line = 0
                for pattern, replacement in self.replacement_rules:
                    line, count = pattern.subn(replacement, line)
                    if count > 0:
                        modified_lines += count
                        modified_in_line += count
                
                # if modified_in_line > 0:
                    # logger.debug(f"Line {line_num} modified from: {original_line.strip()} \n\tTO: {line.strip()}")

                outfile.write(line)

        logger.info(f"Cleaning completed: {lines_processed} lines processed, {skipped_lines} lines skipped, {modified_lines} total modifications applied.")
        logger.info(f"Cleaned dump saved as {self.output_file}")
        return self.output_file

def clean_dump_file(
    input_file: Optional[Path] = None,
    output_file: Optional[Path] = None,
    target_schema: Optional[str] = None,
) -> Path:
    """
    Cleans a PostgreSQL dump file to make it suitable for import into Supabase.

    Args:
        input_file: Path to the input SQL dump file.
        output_file: Path where the cleaned SQL dump file will be saved.
        target_schema: The target schema in Supabase (e.g., "public" or a custom extension schema).

    Returns:
        Path to the cleaned output file.
    """
    cleaner = DumpCleaner(input_file, output_file, target_schema)
    return cleaner.clean_dump_file()

# Example usage (for testing purposes)
if __name__ == '__main__':
    # Create a dummy config.py for the test
    with open("config.py", "w") as f:
        f.write("OUTPUT_DUMP = 'dummy_input.sql'\n")
        f.write("CLEANED_DUMP = 'dummy_cleaned_output.sql'\n")
        f.write("SUPABASE_SCHEMA = 'extensions'\n") # or 'public'

    # Create a dummy input SQL file
    dummy_sql_content = """
-- PostgreSQL database dump

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';
CREATE ROLE cloudsqlsuperuser;
ALTER ROLE cloudsqlsuperuser WITH NOSUPERUSER INHERIT NOCREATEROLE NOCREATEDB LOGIN NOREPLICATION NOBYPASSRLS;
GRANT pg_signal_backend TO cloudsqlsuperuser;
SET SESSION AUTHORIZATION cloudsqlsuperuser;
SET ROLE cloudsqlsuperuser;

CREATE SCHEMA extensions;
ALTER SCHEMA extensions OWNER TO cloudsqlsuperuser;

CREATE SCHEMA public; -- This should be commented out
ALTER SCHEMA public OWNER TO cloudsqlsuperuser; -- This should be changed or commented out

SET search_path = public, extensions;

CREATE TABLE public.my_table (id integer);
ALTER TABLE public.my_table OWNER TO cloudsqlsuperuser;

CREATE TABLE extensions.another_table (name text);
ALTER TABLE extensions.another_table OWNER TO postgres; -- Assuming postgres is the target

GRANT SELECT ON public.my_table TO cloudsqlsuperuser;
GRANT INSERT ON public.my_table TO "cloudsqlsuperuser";
GRANT cloudsqlsuperuser TO some_other_user;
REVOKE cloudsqlsuperuser FROM some_other_user;
ALTER DEFAULT PRIVILEGES FOR ROLE cloudsqlsuperuser IN SCHEMA public GRANT SELECT ON TABLES TO public;
ALTER DEFAULT PRIVILEGES FOR ROLE "cloudsqlsuperuser" GRANT SELECT ON SEQUENCES TO public;

-- End of dummy dump
    """
    with open("dummy_input.sql", "w", encoding='utf-8') as f:
        f.write(dummy_sql_content)

    # Configure basic logging for the test
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    logger.info("Running dummy cleaning process...")
    # Test with target_schema = 'extensions'
    # cleaner_ext = DumpCleaner(input_file=Path("dummy_input.sql"), output_file=Path("dummy_cleaned_ext.sql"), target_schema="extensions")
    # cleaned_path_ext = cleaner_ext.clean_dump_file()
    # print(f"Cleaned file for 'extensions' schema at: {cleaned_path_ext}")
    # with open(cleaned_path_ext, 'r') as f_ext:
    #     print("\n--- Cleaned for 'extensions' schema ---")
    #     print(f_ext.read())

    # Test with target_schema = 'public' (Supabase default for user tables)
    cleaner_public = DumpCleaner(input_file=Path("dummy_input.sql"), output_file=Path("dummy_cleaned_public.sql"), target_schema="public")
    cleaned_path_public = cleaner_public.clean_dump_file()
    print(f"Cleaned file for 'public' schema at: {cleaned_path_public}")
    with open(cleaned_path_public, 'r') as f_public:
        print("\n--- Cleaned for 'public' schema ---")
        print(f_public.read())

    # Clean up dummy files
    # Path("config.py").unlink(missing_ok=True)
    # Path("dummy_input.sql").unlink(missing_ok=True)
    # Path("dummy_cleaned_ext.sql").unlink(missing_ok=True)
    # Path("dummy_cleaned_public.sql").unlink(missing_ok=True)