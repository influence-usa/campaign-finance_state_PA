import sys
import os
import json
import logging
from collections import defaultdict

sys.path.append(os.path.join(os.getcwd(), os.path.pardir))

import pandas as pd
import numpy as np

from settings import ORIG_DIR, TRANS_DIR, SCHEMA_DIR, AGG_DIR
from utils import mkdir_p
from utils import set_up_logging

log = set_up_logging('transform', loglevel=logging.DEBUG)

with open(os.path.join(SCHEMA_DIR, 'scrape', 'dos.json'), 'r') as fin:
    dos_md = json.load(fin)

dos_fields = dos_md['field_info']


def aggregate_dos(options):
    if options.get('loglevel', None):
        log.setLevel(options['loglevel'])

    OUT_DIR = os.path.join(AGG_DIR, 'dos')
    if not os.path.exists(OUT_DIR):
        mkdir_p(OUT_DIR)

    def _compute_aggregates(filer, receipt, contrib, expense, year, period):
        reporting_period_dir = os.path.join(OUT_DIR, year, period)
        if not os.path.exists(reporting_period_dir):
            mkdir_p(reporting_period_dir)

        ## Aggregation

        # ```
        # Ending Cash Balance (Report Cover Page, Item E)
        #  |
        # DIF(Total Funds - Total Expenditures)
        #  |
        #  +-- Total Funds Available (Report Cover Page, Item C)
        #  |    |
        #  |   SUM
        #  |    |
        #  |    +-- Amount brought from last report (Report Cover Page, Item A)
        #  |    |    |
        #  |    |    \-- BEGINNING from Filer.txt
        #  |    |
        #  |    \-- Total Monetary Contributions and Receipts (Report Cover Page, Item B)
        #  |         |
        #  |        SUM
        #  |         |
        #  |         +-- Total Contributions recieved, $50 or less (Schedule I, Detailed Summary, Box 1)
        #  |         |    |
        #  |         |    \-- MONETARY from Filer.txt
        #  |         |
        #  |         +-- Total Contributions recieved, $50.01 - $250 (Schedule I, Detailed Summary, Box 2)
        #  |         |    |
        #  |         |   SUM
        #  |         |    |
        #  |         |    +-- Contributions recieved from political committees (Schedule I, Part A)
        #  |         |    |     |
        #  |         |    |    SUM
        #  |         |    |     |
        #  |         |    |     +-- sum(CONTAMT1) from Contrib.txt where SECTION == 'IA'
        #  |         |    |     |
        #  |         |    |     +-- sum(CONTAMT2) from Contrib.txt where SECTION == 'IA'
        #  |         |    |     |
        #  |         |    |     +-- sum(CONTAMT3) from Contrib.txt where SECTION == 'IA'
        #  |         |    |
        #  |         |    \-- All other contributions (Schedule I, Part B)
        #  |         |          |
        #  |         |         SUM
        #  |         |          |
        #  |         |          +-- sum(CONTAMT1) from Contrib.txt where SECTION == 'IB'
        #  |         |          |
        #  |         |          +-- sum(CONTAMT2) from Contrib.txt where SECTION == 'IB'
        #  |         |          |
        #  |         |          +-- sum(CONTAMT3) from Contrib.txt where SECTION == 'IB'
        #  |         |
        #  |         +-- Total Contributions recieved, over $250 (Schedule I, Detailed Summary, Box 3)
        #  |         |    |
        #  |         |   SUM
        #  |         |    |
        #  |         |    +-- Contributions recieved from political committees (Schedule I, Part C)
        #  |         |    |     |
        #  |         |    |    SUM
        #  |         |    |     |
        #  |         |    |     +-- sum(CONTAMT1) from Contrib.txt where SECTION == 'IC'
        #  |         |    |     |
        #  |         |    |     +-- sum(CONTAMT2) from Contrib.txt where SECTION == 'IC'
        #  |         |    |     |
        #  |         |    |     +-- sum(CONTAMT3) from Contrib.txt where SECTION == 'IC'
        #  |         |    |
        #  |         |    \-- All other contributions (Schedule I, Part D)
        #  |         |          |
        #  |         |         SUM
        #  |         |          |
        #  |         |          +-- sum(CONTAMT1) from Contrib.txt where SECTION == 'ID'
        #  |         |          |
        #  |         |          +-- sum(CONTAMT2) from Contrib.txt where SECTION == 'ID'
        #  |         |          |
        #  |         |          +-- sum(CONTAMT3) from Contrib.txt where SECTION == 'ID'
        #  |         |
        #  |         \-- Total Other Receipts, Refunds, Interest Earned, etc (Schedule I, Detailed Summary, Box 4)
        #  |              |
        #  |              \-- sum(RECAMT) from Receipt.txt
        #  |
        #  \-- Total Expenditures (Report Cover Page, Item D)
        #       |
        #       \-- sum(EXPAMT) from Expense.txt
        # ```

        ### Total Funds Available (Report Cover Page, Item C)

        #### Amount brought from last report (Report Cover Page, Item A)

        amount_brought_from_last_report = filer.groupby('FILERID')[['BEGINNING', ]].sum()
        amount_brought_from_last_report.columns = ['amt_brought_fwd__RCP_A', ]
        _outloc = os.path.join(reporting_period_dir, 'amount_brought_from_last_report.csv')
        amount_brought_from_last_report.to_csv(_outloc, encoding='utf8')

        #### Total Monetary Contributions and Receipts (Report Cover Page, Item B)

        ##### Total Contributions recieved, $50 or less (Schedule I, Detailed Summary, Box 1)

        total_under_50 = filer.groupby('FILERID')[['MONETARY',]].sum()
        total_under_50.columns = ['subtotal',]
        _outloc = os.path.join(reporting_period_dir, 'total_under_50.csv')
        total_under_50.to_csv(_outloc, encoding='utf8')

        ##### Total Contributions recieved, \$50.01 - \$250 (Schedule I, Detailed Summary, Box 2)

        ###### Contributions recieved from political committees (Schedule I, Part A)

        total_50_250_pac = pd.DataFrame(contrib[contrib.SECTION == 'IA'].groupby('FILERID')[['CONTAMT1','CONTAMT2','CONTAMT3']].sum().sum(axis=1))
        total_50_250_pac.columns = ['pac',]
        _outloc = os.path.join(reporting_period_dir, 'total_50_250_pac.csv')
        total_50_250_pac.to_csv(_outloc, encoding='utf8')

        ###### All other contributions (Schedule I, Part B)

        total_50_250_other = pd.DataFrame(contrib[contrib.SECTION == 'IB'].groupby('FILERID')[['CONTAMT1','CONTAMT2','CONTAMT3']].sum().sum(axis=1))
        total_50_250_other.columns = ['other',]
        _outloc = os.path.join(reporting_period_dir, 'total_50_250_other.csv')
        total_50_250_other.to_csv(_outloc, encoding='utf8')

        ###### SUBTOTAL

        total_50_250 = total_50_250_other.join(total_50_250_pac, how='outer').fillna(0)
        total_50_250['subtotal'] = total_50_250.pac + total_50_250.other
        _outloc = os.path.join(reporting_period_dir, 'total_50_250.csv')
        total_50_250.to_csv(_outloc, encoding='utf8')

        ##### Total Contributions recieved, over $250 (Schedule I, Detailed Summary, Box 3)

        ###### Contributions recieved from political committees (Schedule I, Part C)

        total_over_250_pac = pd.DataFrame(contrib[contrib.SECTION == 'IC'].groupby('FILERID')[['CONTAMT1','CONTAMT2','CONTAMT3']].sum().sum(axis=1))
        total_over_250_pac.columns = ['pac',]
        _outloc = os.path.join(reporting_period_dir, 'total_over_250_pac.csv')
        total_over_250_pac.to_csv(_outloc, encoding='utf8')

        ###### All other contributions (Schedule I, Part D)

        total_over_250_other = pd.DataFrame(contrib[contrib.SECTION == 'ID'].groupby('FILERID')[['CONTAMT1','CONTAMT2','CONTAMT3']].sum().sum(axis=1))
        total_over_250_other.columns = ['other',]
        _outloc = os.path.join(reporting_period_dir, 'total_over_250_other.csv')
        total_over_250_other.to_csv(_outloc, encoding='utf8')

        ###### SUBTOTAL

        total_over_250 = total_over_250_other.join(total_over_250_pac, how='outer').fillna(0)
        total_over_250['subtotal'] = total_over_250['pac'] + total_over_250['other']
        _outloc = os.path.join(reporting_period_dir, 'total_over_250.csv')
        total_over_250.to_csv(_outloc, encoding='utf8')

        ##### Total Other Receipts, Refunds, Interest Earned, etc (Schedule I, Detailed Summary, Box 4)

        total_other_receipts = pd.DataFrame(receipt.groupby('FILERID')['RECAMT'].sum())
        total_other_receipts.columns = ['subtotal',]
        _outloc = os.path.join(reporting_period_dir, 'total_other_receipts.csv')
        total_other_receipts.to_csv(_outloc, encoding='utf8')

        ##### SUBTOTAL

        total_monetary_contrib_and_receipt = filer.set_index('FILERID')

        subaggregates = [('under_50__S1_DS_B1', total_under_50),
                         ('50_to_250__S1_DS_B2', total_50_250),
                         ('over_250__S1_DS_B3', total_over_250),
                         ('other_rcpt__S1_DS_B4', total_other_receipts)]

        for subagg_name, subagg in subaggregates:
            joinable = subagg[['subtotal',]]
            joinable.columns = [subagg_name,]
            total_monetary_contrib_and_receipt = total_monetary_contrib_and_receipt.join(joinable, how='left')

        total_monetary_contrib_and_receipt['total_mon__RCP_B'] = total_monetary_contrib_and_receipt[[sa[0] for sa in subaggregates]].sum(axis=1)

        _outloc = os.path.join(reporting_period_dir, 'total_monetary_contrib_and_receipt.csv')
        total_monetary_contrib_and_receipt.to_csv(_outloc, encoding='utf8')

        #### SUBTOTAL

        total_funds_available = total_monetary_contrib_and_receipt.join(amount_brought_from_last_report, how='left')
        total_funds_available['total_funds_avail__RCP_C'] = total_funds_available[['total_mon__RCP_B', 'amt_brought_fwd__RCP_A']].sum(axis=1)

        _outloc = os.path.join(reporting_period_dir, 'total_funds_available.csv')
        total_funds_available.to_csv(_outloc, encoding='utf8')

        ### Total Expenditures (Report Cover Page, Item D)

        # In[127]:

        total_expenditures = expense.groupby('FILERID')[['EXPAMT',]].sum()
        total_expenditures.columns = ['total_exp__RCP_D',]
        _outloc = os.path.join(reporting_period_dir, 'total_expenditures.csv')
        total_expenditures.to_csv(_outloc, encoding='utf8')

        ### Ending Cash Balance (Report Cover Page, Item E)

        # In[128]:

        ending_cash_balance = total_funds_available.join(total_expenditures, how='left')
        ending_cash_balance['ending_cash_bal__RCP_E'] = ending_cash_balance['total_funds_avail__RCP_C'] - ending_cash_balance['total_exp__RCP_D']
        _outloc = os.path.join(reporting_period_dir, 'ending_cash_balance.csv')
        ending_cash_balance.to_csv(_outloc, encoding='utf8')

    required_filing_types = ['receipt', 'contrib', 'expense']
    
    dirs = defaultdict(list)
    DOS_TRANS = os.path.join(TRANS_DIR, 'dos')

    filing_types = os.listdir(DOS_TRANS)
    for filing_type in filing_types:
        years = os.listdir(os.path.join(DOS_TRANS, filing_type))
        for year in years:
            periods = os.listdir(os.path.join(DOS_TRANS, filing_type, year))
            for period in periods:
                dirs[(year, period)].append(filing_type)

    for reporting_period, filing_types in dirs.iteritems():
        year, fname = reporting_period
        period = fname.split('.')[0]
        if all([rft in filing_types for rft in required_filing_types]):
            filer = pd.read_csv(os.path.join(DOS_TRANS, 'filer', year, period+'.csv'),
                                dtype=dos_fields['filer']['pandas_dtypes'])
            receipt = pd.read_csv(os.path.join(DOS_TRANS, 'receipt', year, period+'.csv'),
                                  dtype=dos_fields['receipt']['pandas_dtypes'])
            contrib = pd.read_csv(os.path.join(DOS_TRANS, 'contrib', year, period+'.csv'),
                                  dtype=dos_fields['contrib']['pandas_dtypes'])
            expense = pd.read_csv(os.path.join(DOS_TRANS, 'expense', year, period+'.csv'),
                                  dtype=dos_fields['expense']['pandas_dtypes'])
            try:
                _compute_aggregates(filer, receipt, contrib, expense, year, period)
            except Exception as e:
                log.error('error on {y}, {p}'.format(y=year, p=period))
                raise e
        else:
            missing = [rft for rft in required_filing_types if rft not in filing_types]
            for _ft in missing:
                log.error('{y}, {p} is missing {ft}'.format(y=year, p=period, ft=_ft))
