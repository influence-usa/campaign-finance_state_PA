import os
import sys
import logging
import json
from glob import iglob

try:
    import pandas as pd
except ImportError:
    sys.stderr.write("Warning, python-pandas not installed, won't be able to transform summaries from campaignfinanceonline\n")

from settings import ORIG_DIR, TRANS_DIR
from utils import mkdir_p
from utils import set_up_logging
from utils import sqlize_colname

log = set_up_logging('transform', loglevel=logging.DEBUG)

with open('ref/field_codes.json', 'r') as fc_ref:
    FIELD_CODES = json.load(fc_ref)


def transform_cfo(options):
    if options.get('loglevel', None):
        log.setLevel(options['loglevel'])

    OUT_DIR = os.path.join(TRANS_DIR, 'cfo')
    if not os.path.exists(OUT_DIR):
        mkdir_p(OUT_DIR)

    CFO_ORIG = os.path.join(ORIG_DIR, 'cfo')

    REPORTING_CYCLES = [c['report_cycle_code'] for
                        c in FIELD_CODES['report_cycles']
                        if c['report_cycle_group'] in
                        ('PRIMARY', 'ELECTION', 'ANNUAL')]

    def _flatten_table(loc, cycle_list=REPORTING_CYCLES):
        t = pd.read_json(loc)
        total_col = filter(lambda c: 'Totals' in c, t.columns)[0]
        try:
            most_recent_cycle = max(filter(lambda i: i in cycle_list,
                                           t.columns))
        except:
            most_recent_cycle = max(filter(lambda i: len(i) == 1, t.columns))
        summable = t[total_col].dropna()
        most_recent = t[t[total_col].isnull()][most_recent_cycle]
        combined = pd.concat([summable, most_recent])
        combined['most_recent_cycle'] = most_recent_cycle
        return combined

    def _filer_id_from_loc(loc):
        return os.path.splitext(os.path.split(loc)[1])[0]

    for cycle in os.listdir(CFO_ORIG):
        log.info("aggregating summaries for {c}".format(c=cycle))
        json_files = [fname for fname in
                      iglob(os.path.join(CFO_ORIG, cycle, '*.json'))]
        cycle_aggs = pd.concat([_flatten_table(t) for t in json_files],
                               keys=[_filer_id_from_loc(loc)
                                     for loc in json_files])
        cycle_aggs = cycle_aggs.unstack()
        cycle_aggs.index.name = 'filer_id'
        cycle_aggs.columns = cycle_aggs.columns.map(sqlize_colname)

        log.info("collecting metadata for {c}".format(c=cycle))
        filer_locs = [floc for floc in iglob(os.path.join(
            ORIG_DIR, 'dos', 'filer', '2014', '*', '*.json'))]
        filer_txt = pd.concat([pd.DataFrame.from_records(
            json.load(open(fname, 'r'))) for fname in filer_locs])
        filer_info = filer_txt.drop_duplicates(subset=[u'FILERID'],
                                               take_last=True)
        filer_info = filer_info[[u'FILERID', u'FILERNAME', u'FILERTYPE',
                                 u'ADDRESS1', u'ADDRESS2', u'CITY',
                                 u'COUNTY',  u'STATE', u'ZIPCODE',
                                 u'OFFICE', u'PARTY', u'DISTRICT', u'PHONE']]
        filer_info = filer_info.set_index('FILERID')
        filer_info.index.name = 'filer_id'

        log.info("joining metadata and aggregates for {c}".format(c=cycle))
        agg_joined = cycle_aggs.join(filer_info, how='left')
        outloc = os.path.join(OUT_DIR, '{c}.csv'.format(c=cycle))
        agg_joined.to_csv(outloc)