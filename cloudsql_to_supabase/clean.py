import re
import logging
from pathlib import Path
from typing import Optional, List, Tuple 


from . import config 

logger = logging.getLogger('cloudsql_to_supabase.clean')

class DumpCleaner:
    
    def __init__(
        self,
        input_file: Optional[Path] = None,
        output_file: Optional[Path] = None,
        target_schema: Optional[str] = None,
        target_owner: Optional[str] = None, 
    ) -> None:
        self.input_file = Path(input_file or config.OUTPUT_DUMP)
        self.output_file = Path(output_file or config.CLEANED_DUMP)
        self.target_schema = target_schema or "public"
        self.target_owner = target_owner or "postgres"

        logger.info(
            f"Initializing DumpCleaner. Input: '{self.input_file}', Output: '{self.output_file}', "
            f"Target Schema: '{self.target_schema}', Target Owner: '{self.target_owner}'"
        )

        self.problematic_roles_to_filter: List[str] = ["cloudsqlsuperuser", "cloudsqladmin"]
        self.problematic_role_match_pattern: Optional[str] = self._build_problematic_role_pattern()

        
        self.skip_patterns: List[re.Pattern] = self._build_skip_patterns()
        self.replacement_rules: List[Tuple[re.Pattern, str]] = self._build_replacement_rules()

    def _build_problematic_role_pattern(self) -> Optional[str]:
        if not self.problematic_roles_to_filter:
            return None
        role_patterns = []
        for role in self.problematic_roles_to_filter:
            escaped_role = re.escape(role)
            role_patterns.append(f'"{escaped_role}"')
            role_patterns.append(rf'{escaped_role}\b')
        pattern = rf"(?:{'|'.join(role_patterns)})"
        logger.debug(f"Problematic role regex part: {pattern}")
        return pattern

    def _build_skip_patterns(self) -> List[re.Pattern]:
        
        raw_patterns_definitions = [
            r'^\s*(CREATE|ALTER)\s+ROLE\b',
            r'^\s*COMMENT ON EXTENSION\s+(?:pg_stat_statements|plpgsql)\s*;',
            r'^\s*COMMENT ON EXTENSION\b',
            r'^\s*SET\s+(?:transaction_timeout|idle_in_transaction_session_timeout|lock_timeout|statement_timeout)\s*=\s*.*?;',
            r'^\s*SET\s+default_transaction_read_only\s*=\s*on;',
        ]

        if self.problematic_role_match_pattern:
            raw_patterns_definitions.extend([
                r'^\s*SET\s+(?:ROLE|SESSION\s+AUTHORIZATION)\s+' + self.problematic_role_match_pattern + r'\s*;',
                r'^\s*(?:GRANT|REVOKE).*?' + self.problematic_role_match_pattern + r'.*?;',
                r'^\s*ALTER DEFAULT PRIVILEGES\s+FOR ROLE\s+' + self.problematic_role_match_pattern + r'\s+.*?;',
                r'^\s*ALTER (?:TABLE|SCHEMA|SEQUENCE|FUNCTION|VIEW|MATERIALIZED VIEW|TYPE|DOMAIN|FOREIGN DATA WRAPPER|SERVER|EVENT TRIGGER|PUBLICATION|SUBSCRIPTION).* OWNER TO ' + self.problematic_role_match_pattern + r'\s*;',
            ])
        
        compiled_patterns = []
        for p_str in raw_patterns_definitions:
            try:
                
                compiled_patterns.append(re.compile(p_str, re.IGNORECASE))
            except re.error as e:
                logger.error(f"Regex compilation FAILED for skip pattern: >>>{p_str}<<<")
                logger.error(f"Error details: {e}")
                raise  
        return compiled_patterns

    def _build_replacement_rules(self) -> List[Tuple[re.Pattern, str]]:
        owner_replacement_str = f'OWNER TO {self.target_owner};'
        

        
        raw_rules_definitions: List[Tuple[str, str]] = [
            (r'OWNER TO (?:"[^"]+"|[^\s;]+);', owner_replacement_str),
            (r'^\s*CREATE SCHEMA\s+(?!public\b)(?!"?' + re.escape(self.target_schema) + r'"?\b)[^;]+?;', '-- Removed CREATE SCHEMA for non-target, non-public schema: \\g<0>'),
            (r'^\s*CREATE SCHEMA\s+public\s*;', '-- CREATE SCHEMA public; (commented out, public schema usually exists)'),
            (r'^\s*CREATE SCHEMA\s+"?' + re.escape(self.target_schema) + r'"?\s*;', f'-- CREATE SCHEMA {self.target_schema}; (commented out, handled by import script)'),
            (r'^\s*ALTER SCHEMA\s+public\s+OWNER TO .*?;', f'-- ALTER SCHEMA public OWNER removed, will be owned by supabase admin/{self.target_owner}'),
            (r"^\s*SELECT pg_catalog\.set_config\('search_path', '', false\);\s*$", "-- SELECT pg_catalog.set_config('search_path', '', false); (emptying search_path removed to preserve command-line or explicit settings)"),
        ]

        quoted_target_schema = f'"{self.target_schema}"'

        
        print(f"Current target_schema: '{self.target_schema}'")
        print(f"Type of target_schema: {type(self.target_schema)}")
        print(f"Condition result: {self.target_schema != 'public'}")

        if self.target_schema != "public":
            schema_replacements_raw = [
                (r"^\s*SET search_path = public(?:,\s*|\s*;)(.*)$", rf"SET search_path = {quoted_target_schema}, public\1"),
                (r"\bpublic\.([\w_]+)\b", rf'{quoted_target_schema}.\1'),
                (r"ALTER TABLE ONLY public\.([\w_]+)", rf'ALTER TABLE ONLY {quoted_target_schema}.\1'),
            ]
            raw_rules_definitions.extend(schema_replacements_raw)
        else:
            raw_rules_definitions.append(
                (r"^\s*SET search_path = .*?;", r"SET search_path = public, pg_catalog;")
            )
        
        compiled_rules = []
        for p_str, repl_str in raw_rules_definitions:
            try:
                
                compiled_rules.append((re.compile(p_str, re.IGNORECASE), repl_str))
            except re.error as e:
                logger.error(f"Regex compilation FAILED for replacement pattern: >>>{p_str}<<<")
                logger.error(f"Error details: {e}")
                raise 
        return compiled_rules

    
    def clean_dump_file(self) -> Path:
        logger.info(f"Starting cleaning of dump file: {self.input_file}")

        if not self.input_file.exists():
            logger.error(f"Input file not found: {self.input_file}")
            raise FileNotFoundError(f"Input file not found: {self.input_file}")

        skipped_lines_count, total_modifications_count, lines_processed_count = 0, 0, 0

        self.output_file.parent.mkdir(parents=True, exist_ok=True)

        with self.input_file.open('r', encoding='utf-8') as infile, \
             self.output_file.open('w', encoding='utf-8') as outfile:
            for line_num, line in enumerate(infile, 1):
                lines_processed_count += 1
                processed_line = line
                skipped_by_pattern = False
                for pattern in self.skip_patterns: 
                    if pattern.search(processed_line):
                        
                        outfile.write(f"-- SKIPPED LINE (by pattern {pattern.pattern}): {processed_line.strip()}\n")
                        skipped_lines_count += 1
                        skipped_by_pattern = True
                        break
                if skipped_by_pattern:
                    continue

                for pattern, replacement in self.replacement_rules: 
                    processed_line, count = pattern.subn(replacement, processed_line)
                    if count > 0:
                        total_modifications_count += count
                
                outfile.write(processed_line)

        logger.info(
            f"Cleaning completed: {lines_processed_count} lines processed, "
            f"{skipped_lines_count} lines skipped, "
            f"{total_modifications_count} total modifications applied."
        )
        logger.info(f"Cleaned dump saved as {self.output_file}")
        return self.output_file

def clean_dump_file(
    input_file: Optional[Path] = None,
    output_file: Optional[Path] = None,
    target_schema: Optional[str] = None,
    target_owner: Optional[str] = None, 
) -> Path:
    """
    Cleans a PostgreSQL dump file to make it suitable for import.

    Args:
        input_file: Path to the input SQL dump file.
        output_file: Path where the cleaned SQL dump file will be saved.
        target_schema: The target schema for objects (e.g., "public" or "extensions").
        target_owner: The role that should own the objects (e.g., "postgres").

    Returns:
        Path to the cleaned output file.
    """
    cleaner = DumpCleaner(
        input_file=input_file,
        output_file=output_file,
        target_schema= target_schema,
        target_owner=target_owner
    )
    return cleaner.clean_dump_file()
