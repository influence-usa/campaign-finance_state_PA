DROP TABLE IF EXISTS "state_cf_pa_campaignfinanceonline";
CREATE TABLE "state_cf_pa_campaignfinanceonline" (
        filer_id VARCHAR(9) NOT NULL, 
        total_expenditures FLOAT NOT NULL, 
        total_monetary_contributions_and_receipts FLOAT NOT NULL, 
        value_of_inkind_contributions_received FLOAT NOT NULL, 
        amount_brought_forward_from_last_report FLOAT NOT NULL, 
        ending_cash_balance FLOAT NOT NULL, 
        total_funds_available FLOAT NOT NULL, 
        unpaid_debts_and_obligations FLOAT NOT NULL, 
        most_recent_cycle INTEGER NOT NULL, 
        filer_name VARCHAR(114), 
        filer_type INTEGER, 
        address1 VARCHAR(47), 
        address2 VARCHAR(30), 
        city VARCHAR(19), 
        county VARCHAR(4), 
        state VARCHAR(4), 
        zipcode VARCHAR(10), 
        office VARCHAR(4), 
        party VARCHAR(4), 
        district INTEGER, 
        phone VARCHAR(10)
);

COMMIT;

\copy state_cf_pa_campaignfinanceonline from 'data/transformed/cfo/2008.csv' CSV HEADER;
\copy state_cf_pa_campaignfinanceonline from 'data/transformed/cfo/2009.csv' CSV HEADER;
\copy state_cf_pa_campaignfinanceonline from 'data/transformed/cfo/2010.csv' CSV HEADER;
\copy state_cf_pa_campaignfinanceonline from 'data/transformed/cfo/2011.csv' CSV HEADER;
\copy state_cf_pa_campaignfinanceonline from 'data/transformed/cfo/2012.csv' CSV HEADER;
\copy state_cf_pa_campaignfinanceonline from 'data/transformed/cfo/2013.csv' CSV HEADER;
\copy state_cf_pa_campaignfinanceonline from 'data/transformed/cfo/2014.csv' CSV HEADER;

COMMIT;
