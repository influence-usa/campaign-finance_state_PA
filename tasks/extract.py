import logging
import sys
import os

from lxml import etree
from glob import iglob

try:
    import pandas as pd
except ImportError:
    sys.stderr.write("Warning, python-pandas not installed, won't be able to extract summaries from campaignfinanceonline\n")

from settings import ORIG_DIR, CACHE_DIR
from utils import mkdir_p
from utils import set_up_logging

log = set_up_logging('download', loglevel=logging.DEBUG)


def extract_cfo(options):
    if options.get('loglevel', None):
        log.setLevel(options['loglevel'])

    OUT_DIR = os.path.join(ORIG_DIR, 'cfo')
    if not os.path.exists(OUT_DIR):
        mkdir_p(OUT_DIR)

    CFO_CACHE = os.path.join(CACHE_DIR, 'cfo')

    html_parser = etree.HTMLParser()

    def _chunks(l, n):
        """ Yield successive n-sized chunks from l.
        """
        for i in xrange(0, len(l), n):
            yield l[i:i+n]

    def _parse_data_tables(d):
        return pd.io.html.read_html(etree.tostring(d), skiprows=1, header=1,
                                    index_col=0)

    def _parse_year_table(y):
        return y.xpath('.//tr[1]/td[2]/span')[0].text

    def _extract_tables(pg_html):
        all_tables_container = pg_html.xpath(
            "//div[@id='ContentPlaceHolder1_divCFSummary']")[0]
        summary_tables = {_parse_year_table(y): _parse_data_tables(d)[0]
                          for y, d in
                          _chunks(all_tables_container.xpath("table"), 2)}
        return summary_tables

    for loc in iglob(os.path.join(CFO_CACHE, '*.html')):
        log.debug('opening {l}'.format(l=loc))
        filer_id = os.path.splitext(os.path.split(loc)[1])[0]
        with open(loc, 'r') as fin:
            try:
                pg_html = etree.parse(fin, parser=html_parser)
                tables = _extract_tables(pg_html)
            except Exception as e:
                log.error('parsing file {l} failed:'.format(l=loc))
                log.error(e)

            try:
                for year, table in tables.iteritems():
                    if year:
                        output_dir = os.path.join(OUT_DIR, year)
                        if not os.path.exists(output_dir):
                            mkdir_p(output_dir)
                        output_loc = os.path.join(OUT_DIR, year,
                                                  '{}.json'.format(filer_id))
                        table.dropna(axis=1, how='all').to_json(
                            path_or_buf=output_loc, orient='index')
                    else:
                        log.debug('{l} contained {y} as a year?'.format(
                            l=loc, y=year))
            except Exception as e:
                log.error('reading table dict {l} failed:'.format(l=loc))
                log.error(e)
