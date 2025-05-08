from . import config


def clean_dump_file():
    print(f"Cleaning dump file: {config.OUTPUT_DUMP}")
    import re
    with open(config.OUTPUT_DUMP, 'r') as infile, open(config.CLEANED_DUMP, 'w') as outfile:
        for line in infile:
            if re.search(r'^(CREATE|ALTER)ROLE', line):
                continue
            if re.search(r'OWNER TO', line):
                line = re.sub(r'OWNER TO .*?;', 'OWNER TO public;', line)
            outfile.write(line)
    print(f"Cleaned dump file saved as: {config.CLEANED_DUMP}")