import os
import logging

import csv
from io import StringIO
from glob import iglob

import requests

import cookielib
from lxml import etree

from settings import CACHE_DIR
from utils import mkdir_p
from utils import set_up_logging
from utils import download_all

log = set_up_logging('download', loglevel=logging.DEBUG)


def download_dos(options):
    if options.get('loglevel', None):
        log.setLevel(options['loglevel'])

    OUT_DIR = os.path.join(CACHE_DIR, 'dos')
    if not os.path.exists(OUT_DIR):
        mkdir_p(OUT_DIR)

    initial_url = 'http://www.dos.state.pa.us/portal/server.pt/community/full_campaign_finance_export/21644'
    jar = cookielib.CookieJar()
    parser = etree.HTMLParser()

    initial_resp = requests.get(initial_url, cookies=jar)
    initial_page = etree.parse(StringIO(initial_resp.text), parser)
    main_table = initial_page.xpath(
        '//*[@id="pt-portlet-content-17448"]/table')[0]
    download_buckets = list(set([(a.text, a.attrib.get('href', '')) for a in
                                 main_table.xpath("//td[@class='listText']/a")
                                 if a.text and a.text != 'more...']))

    def _extract_js_url(e):
        try:
            val = e.attrib['onclick']
            url = val.split('(')[1].split(')')[0].replace("'", "")
        except KeyError:
            url = None
        else:
            return url

    def _get_bucket_urls(bucket):
        bucket_label, bucket_url = bucket
        year, period = bucket_label.split('_')
        download_resp = requests.get(bucket_url, cookies=jar)
        download_page = etree.parse(StringIO(download_resp.text), parser)
        atags = download_page.xpath(
            '//*[@id="outerTable"]//span[@class="listSubtitle"]/a')
        for atag in atags:
            file_info = atag.text.split()
            filename = file_info[0]
            try:
                filesize = file_info[1]
            except IndexError:
                filesize = None
            atag_url = _extract_js_url(atag)
            yield (year, period, filename, filesize, atag_url)

    def _get_response_loc_pair(dl_info):
        year, period, filename, filesize, atag_url = dl_info
        loc_dir = os.path.join(OUT_DIR, year, period)
        if not os.path.exists(loc_dir):
            mkdir_p(loc_dir)
        loc = os.path.join(loc_dir, filename)
        response = requests.get(atag_url, stream=True)
        return (response, loc)

    download_infos = []
    for bucket in download_buckets:
        for info in _get_bucket_urls(bucket):
            download_infos.append(info)

    download_all(download_infos, _get_response_loc_pair, options)


def download_cfo(options):
    if options.get('loglevel', None):
        log.setLevel(options['loglevel'])

    OUT_DIR = os.path.join(CACHE_DIR, 'cfo')
    if not os.path.exists(OUT_DIR):
        mkdir_p(OUT_DIR)

    base_url = 'https://www.campaignfinanceonline.state.pa.us/pages/CFAnnualTotals.aspx'

    def _get_response_loc_pair(dl_info):
        filer_id = dl_info
        loc = os.path.join(OUT_DIR, '{}.html'.format(filer_id))
        response = requests.get(base_url, params={'Filer': filer_id})
        return (response, loc)

    filer_ids = set([])

    for loc in iglob(os.path.join(
            CACHE_DIR, 'dos', '*', '*', '[fF]iler.[Tt]xt')):
        with open(loc, 'r') as fin:
            for row in csv.reader(fin):
                if row[0]:
                    filer_ids.add(row[0])

    download_all(list(filer_ids), _get_response_loc_pair, options)
