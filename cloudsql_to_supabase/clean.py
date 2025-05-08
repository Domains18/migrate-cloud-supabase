import re 
import logging
from pathlib import Path
from typing import List, Optional
from . import config


logger = logging.getLogger('cloudsql_to_supabase.clean')


class DumpCleaner: 
    def __init__(self, input_file: Path = None, output_file: Path = None) -> None:
        self.input_file = input_file or Path(config.OUTPUT_DUMP)
        self.output_file = output_file or Path(config.OUTPUT_DUMP)
        self.skip_patterns = [
            r'^(CREATE|ALTER) ROLE',
            r'^COMMENT ON EXTENSION',
        ]
        
        self.replacement_rules = [
            (r'OWNER TO .*?;', 'OWNER TO public;'),
            (r'CREATE SCHEMA .*?;', '--schema creation removed')
        ]
        
    def clean_dump_file(self) -> Path:
        """
        Clean the SQL dump file for Supabase import by removing/modifying
        incompatible statements.
        
        Returns:
            Path to the cleaned dump file
        """
        
        logger.info(f"cleaning dump file: {self.input_file}")
        
        if not self.input_file.exists():
            raise FileNotFoundError(f"input file not found: {self.input_file}")
        
        skipped_lines: int = 0
        modified_lines: int = 0
        
        with open(self.input_file, 'r') as infile, open(self.output_file, 'w') as outfile:
            for line_num, line in enumerate(infile, 1):
                if any(re.search(pattern, line) for pattern in self.skip_patterns):
                    skipped_lines += 1
                    continue
                
                original_line = line
                for pattern, replacement in self.replacement_rules:
                    if re.search(pattern, line):
                        line = re.sub(pattern, replacement, line)
                        if line != original_line:
                            modified_lines += 1
                            
                outfile.write(line)
                
        logger.info(f'cleaning completed: {skipped_lines} lines skipped, {modified_lines} lines modified')
        logger.info(f'cleaned dump saved as {self.output_file}')
        
        return self.output_file



def clean_dump_file(input_file: Optional[Path] = None, output_file: Optional[Path] = None) -> Path:
    """
        convenience function to clean a sql dump file
    """
    
    cleaner = DumpCleaner(input_file, output_file)
    return cleaner.clean_dump_file()