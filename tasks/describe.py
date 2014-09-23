import os
import re
import sys
import logging
import json

from glob import iglob
from collections import defaultdict

import numpy as np

from settings import CACHE_DIR, REF_DIR
from utils import set_up_logging
from utils import mkdir_p

log = set_up_logging('describe', loglevel=logging.DEBUG)


def describe_dos(options):
    if options.get('loglevel', None):
        log.setLevel(options['loglevel'])

    OUT_DIR = os.path.join(REF_DIR, 'dos')
    if not os.path.exists(OUT_DIR):
        mkdir_p(OUT_DIR)

    sql_to_dtype = {
        'VARCHAR': 'object',
        'INT': 'int64',
        'MONEY': 'float64'
    }

    sep_regx = re.compile('^-+$')

    def _parse_field_line(field_line):
        m = re.match(r'^([A-Z0-9]+)\s+(VARCHAR|INT|MONEY).*$', field_line)
        name, sql_type = m.groups()
        return name, sql_type

    def _parse_readme(fileloc):
        tables = []
        with open(fileloc, 'r') as readme:
            header = [readme.next() for i in xrange(2)]
            sys.stderr.write('\n'.join(header))
            nextline = readme.next().strip()
            while True:
                if not sep_regx.match(nextline):
                    prevline = nextline[:]
                    try:
                        nextline = readme.next().strip()
                    except StopIteration:
                        break
                elif nextline:
                    table_data = {}
                    sys.stderr.write('found the sep for {}\n'.format(prevline))
                    table_data['title'] = prevline
                    table_data['pandas_dtypes'] = {}
                    table_data['sql_types'] = {}
                    table_data['columns'] = []
                    nextline = readme.next().strip()
                    while True:
                        name, sql_type = _parse_field_line(nextline)
                        table_data['columns'].append(name)
                        table_data['pandas_dtypes'][name] = sql_to_dtype[sql_type]
                        table_data['sql_types'][name] = sql_type
                        try:
                            nextline = readme.next().strip()
                        except StopIteration:
                            break
                        else:
                            if not nextline:
                                break
                    tables.append(table_data)
                else:
                    print "uhhh"
            return tables

    readme_data = defaultdict(dict)

    for floc in iglob(os.path.join(CACHE_DIR, 'dos', '*', '*', 'readme.txt')):
        year, report_cycle = os.path.split(floc)[0].split(os.sep)[-2:]
        readme_data[year][report_cycle] = _parse_readme(floc)

    with open(os.path.join(OUT_DIR, 'dos_metadata.json'), 'w') as fout:
        json.dump(readme_data, fout, indent=2)
