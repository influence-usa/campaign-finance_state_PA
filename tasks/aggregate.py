import sys
import os
import csv
import json
import re

sys.path.append(os.path.join(os.getcwd(),os.path.pardir))

import settings

import pandas as pd
import numpy as np

with open(os.path.join(settings.SCHEMA_DIR, 'scrape', 'dos.json'), 'r') as fin:
    dos_md = json.load(fin)

dos_fields = dos_md['field_info']

## Tables

### Filer.txt

# In[19]:

filer_columns = tables[-2]['columns']
filer_dtypes = tables[-2]['dtypes']


# In[20]:

filer_columns

# In[21]:
file_loc = os.path.join(DOS_DIR,'2014','1','Filer.txt')

filer_df = pd.read_csv(file_loc,
                       header=0,
                       names=dos_fields['filer.txt']['columns'],
                       dtype=dos_fields['filer.txt']['pandas_dtypes'])


with open(os.path.join(DOS_DIR,'2014','1','Filer.txt'),'r') as fin:
    dr = csv.DictReader(fin, fieldnames=filer_columns)
    with open(os.path.join(DOS_DIR,'2014','1','Filer.csv'),'w') as fout:
        w = csv.DictWriter(fout, fieldnames=filer_columns)
        w.writeheader()
        w.writerows([r for r in dr])


# In[27]:

filer_2014_1.sort('MONETARY', ascending=False)[['FILERID','FILERNAME','MONETARY', 'INKIND']]


# In[28]:

eg = filer_2014_1[filer_2014_1.FILERID == '7900366']
eg


# In[29]:

json.dumps(eg.to_dict(outtype='records')[0])


### Receipt.txt

# In[30]:

receipt_columns = tables[-1]['columns']
receipt_dtypes = tables[-1]['dtypes']


# In[31]:

receipt_dtypes


# In[32]:

receipt_2014_1 = pd.read_csv(os.path.join(DOS_DIR,'2014','1','Receipt.txt'),
                             header=0, names=receipt_columns, dtype=receipt_dtypes)


# In[33]:

receipt_2014_1.info()


# In[34]:

receipt_2014_1.groupby(['FILERID'])


# In[35]:

len(receipt_2014_1.FILERID.unique()) - len(receipt_2014_1.FILERID)


# In[36]:

named = receipt_2014_1.set_index('FILERID').join(
 filer_2014_1.set_index('FILERID')[['FILERNAME','FILERTYPE']])


# In[37]:

named


# In[38]:

named.groupby(['RECNAME','CYCLE','EYEAR']).sum().sort('RECAMT', ascending=False)


### Contrib.txt

# In[39]:

contrib_columns = tables[0]['columns']
contrib_dtypes = tables[0]['dtypes']


# In[40]:

contrib_2014_1 = pd.read_csv(os.path.join(DOS_DIR,'2014','1','Contrib.txt'),
                             header=0, names=contrib_columns, dtype=contrib_dtypes)


# In[41]:

contrib_2014_1.info()


# In[42]:

contrib_2014_1[contrib_2014_1.FILERID == '2002268'].groupby('SECTION')['CONTAMT1'].sum()


# 
# [MORE HERE](http://www.portal.state.pa.us/portal/server.pt/community/technical_specifications/17469#Filer.txt)

### Debt.txt

# In[43]:

debt_columns = tables[1]['columns']
debt_dtypes = tables[1]['dtypes']


# In[44]:

debt_dtypes


# In[45]:

debt_2014_1 = pd.read_csv(os.path.join(DOS_DIR,'2014','1','Debt.txt'),
                          header=0, names=debt_columns, dtype=debt_dtypes)


# In[46]:

debt_2014_1.groupby(['FILERID','CYCLE','EYEAR']).sum().sort('DBTAMT', ascending=False)


# In[47]:

filer_2014_1[filer_2014_1.FILERID == '20130304']


### Expense.txt

# In[48]:

expense_columns = tables[2]['columns']
expense_dtypes = tables[2]['dtypes']


# In[49]:

expense_2014_1 = pd.read_csv(os.path.join(DOS_DIR,'2014','1','Expense.txt'),
                             header=0, names=expense_columns, dtype=expense_dtypes)


# In[50]:

expense_2014_1


# In[51]:

expense_2014_1.EXPDESC.value_counts()


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

# In[122]:

amount_brought_from_last_report = filer_2014_1.groupby('FILERID')[['BEGINNING',]].sum()
amount_brought_from_last_report.columns = ['amt_brought_fwd__RCP_A',]
amount_brought_from_last_report


# In[99]:

amount_brought_from_last_report.describe()


# In[100]:

amount_brought_from_last_report.sort('subtotal', ascending=False).head(20)


# In[101]:

amount_brought_from_last_report.sort('subtotal').head(20)


#### Total Monetary Contributions and Receipts (Report Cover Page, Item B)

##### Total Contributions recieved, $50 or less (Schedule I, Detailed Summary, Box 1)

# In[102]:

total_under_50 = filer_2014_1.groupby('FILERID')[['MONETARY',]].sum()
total_under_50.columns = ['subtotal',]
total_under_50


##### Total Contributions recieved, \$50.01 - \$250 (Schedule I, Detailed Summary, Box 2)

###### Contributions recieved from political committees (Schedule I, Part A)

# In[87]:

total_50_250_pac = pd.DataFrame(contrib_2014_1[contrib_2014_1.SECTION == 'IA'].groupby('FILERID')[['CONTAMT1','CONTAMT2','CONTAMT3']].sum().sum(axis=1))
total_50_250_pac.columns = ['pac',]
total_50_250_pac


###### All other contributions (Schedule I, Part B)

# In[88]:

total_50_250_other = pd.DataFrame(contrib_2014_1[contrib_2014_1.SECTION == 'IB'].groupby('FILERID')[['CONTAMT1','CONTAMT2','CONTAMT3']].sum().sum(axis=1))
total_50_250_other.columns = ['other',]
total_50_250_other


###### SUBTOTAL

# In[103]:

total_50_250 = total_50_250_other.join(total_50_250_pac, how='outer').fillna(0)
total_50_250['subtotal'] = total_50_250.pac + total_50_250.other
total_50_250


##### Total Contributions recieved, over $250 (Schedule I, Detailed Summary, Box 3)

###### Contributions recieved from political committees (Schedule I, Part C)

# In[91]:

total_over_250_pac = pd.DataFrame(contrib_2014_1[contrib_2014_1.SECTION == 'IC'].groupby('FILERID')[['CONTAMT1','CONTAMT2','CONTAMT3']].sum().sum(axis=1))
total_over_250_pac.columns = ['pac',]
total_over_250_pac


###### All other contributions (Schedule I, Part D)

# In[93]:

total_over_250_other = pd.DataFrame(contrib_2014_1[contrib_2014_1.SECTION == 'ID'].groupby('FILERID')[['CONTAMT1','CONTAMT2','CONTAMT3']].sum().sum(axis=1))
total_over_250_other.columns = ['other',]
total_over_250_other


###### SUBTOTAL

# In[104]:

total_over_250 = total_over_250_other.join(total_over_250_pac, how='outer').fillna(0)
total_over_250['subtotal'] = total_over_250['pac'] + total_over_250['other']
total_over_250


##### Total Other Receipts, Refunds, Interest Earned, etc (Schedule I, Detailed Summary, Box 4)

# In[105]:

total_other_receipts = pd.DataFrame(receipt_2014_1.groupby('FILERID')['RECAMT'].sum())
total_other_receipts.columns = ['subtotal',]
total_other_receipts


##### SUBTOTAL

# In[117]:

total_monetary_contrib_and_receipt = filer_2014_1.set_index('FILERID')

subaggregates = [('under_50__S1_DS_B1', total_under_50),
                 ('50_to_250__S1_DS_B2', total_50_250),
                 ('over_250__S1_DS_B3', total_over_250),
                 ('other_rcpt__S1_DS_B4', total_other_receipts)]

for subagg_name, subagg in subaggregates:
    joinable = subagg[['subtotal',]]
    joinable.columns = [subagg_name,]
    total_monetary_contrib_and_receipt = total_monetary_contrib_and_receipt.join(joinable, how='left')
    
total_monetary_contrib_and_receipt['total_mon__RCP_B'] = total_monetary_contrib_and_receipt[[sa[0] for sa in subaggregates]].sum(axis=1)


# In[118]:

total_monetary_contrib_and_receipt.head()


#### SUBTOTAL

# In[124]:

total_funds_available = total_monetary_contrib_and_receipt.join(amount_brought_from_last_report, how='left')
total_funds_available['total_funds_avail__RCP_C'] = total_funds_available[['total_mon__RCP_B', 'amt_brought_fwd__RCP_A']].sum(axis=1)


# In[125]:

total_funds_available


### Total Expenditures (Report Cover Page, Item D)

# In[127]:

total_expenditures = expense_2014_1.groupby('FILERID')[['EXPAMT',]].sum()
total_expenditures.columns = ['total_exp__RCP_D',]
total_expenditures


### Ending Cash Balance (Report Cover Page, Item E)

# In[128]:

ending_cash_balance = total_funds_available.join(total_expenditures, how='left')
ending_cash_balance['ending_cash_bal__RCP_E'] = ending_cash_balance['total_funds_avail__RCP_C'] - ending_cash_balance['total_exp__RCP_D']
ending_cash_balance.head()


# In[ ]:



