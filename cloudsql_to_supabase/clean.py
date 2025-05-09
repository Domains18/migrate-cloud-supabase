import re
import logging
from pathlib import Path
from typing import List, Optional
from . import config

logger = logging.getLogger('cloudsql_to_supabase.clean')

class DumpCleaner:
    def __init__(self, input_file: Path = None, output_file: Path = None) -> None:
        self.input_file = input_file or Path(config.OUTPUT_DUMP)
        self.output_file = output_file or Path(config.CLEANED_DUMP)
        self.skip_patterns = [
            r'^(CREATE|ALTER) ROLE',  # Skip role creation/alteration
            r'^COMMENT ON EXTENSION',  # Skip extension comments
        ]
        self.replacement_rules = [
            (r'OWNER TO .*?;', 'OWNER TO public;'),  # Change ownership to public
            (r'CREATE SCHEMA .*?;', '-- Schema creation removed'),  # Comment out schema creation
        ]
        
    def clean_dump_file(self) -> Path:
        """
        Clean the SQL dump file for Supabase import by removing/modifying
        incompatible statements.
        
        Returns:
            Path to the cleaned dump file
        """
        logger.info(f"Cleaning dump file: {self.input_file}")
        
        if not self.input_file.exists():
            raise FileNotFoundError(f"Input file not found: {self.input_file}")
        
        # Count of modifications for logging
        skipped_lines = 0
        modified_lines = 0
        
        with open(self.input_file, 'r') as infile, open(self.output_file, 'w') as outfile:
            for line_num, line in enumerate(infile, 1):
                # Check if line should be skipped
                if any(re.search(pattern, line) for pattern in self.skip_patterns):
                    skipped_lines += 1
                    continue
                
                # Apply replacements
                original_line = line
                for pattern, replacement in self.replacement_rules:
                    if re.search(pattern, line):
                        line = re.sub(pattern, replacement, line)
                        if line != original_line:
                            modified_lines += 1
                
                outfile.write(line)
        
        logger.info(f"Cleaning completed: {skipped_lines} lines skipped, {modified_lines} lines modified")
        logger.info(f"Cleaned dump saved as {self.output_file}")
        return self.output_file

def clean_dump_file(input_file: Optional[Path] = None, output_file: Optional[Path] = None) -> Path:
    """
    Convenience function to clean a SQL dump file
    """
    cleaner = DumpCleaner(input_file, output_file)
    return cleaner.clean_dump_file()
