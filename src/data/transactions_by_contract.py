# Import libraries
import pandas as pd
from psycopg2.extras import execute_values
import psycopg2
from config.config import config
from random import randint, random
from faker import Faker
import numpy as np
import unidecode
from datetime import date
from random import sample
import numpy_financial as npf
from dateutil.relativedelta import relativedelta

# Connect to PostgreSQL

params = config(config_db_file='database.ini')
con = psycopg2.connect(**params)
print('Python connected to PostgreSQL!')
# Create table
cur = con.cursor()

cur.execute("""SELECT * FROM accounts""")
query_results = cur.fetchall()
accounts = pd.DataFrame(query_results)

cur.execute("""SELECT * FROM amortization_table WHERE payment_date = CURRENT_DATE""")
query_results = cur.fetchall()
amort_table = pd.DataFrame(query_results)

payments_by_contract = amort_table

payments_by_contract = pd.merge(payments_by_contract,accounts,how='left', left_on = [8,9], right_on = [4,6])

#build principal payment table

transactions_principal = payments_by_contract.drop(
    columns=['0_x', '3_x', '5_x', '6_x', 7, 8, 12, '1_y', '2_y', '3_y', '4_y', '5_y', '6_y'])


transactions_principal.columns = ['contract_number', 'payment_date', 'transaction_sum', 'currency', 'contract_type',
                                  'category', 'account_id']

transactions_principal['contract_type'] = transactions_principal['contract_type'].str.strip()
transactions_principal['category'] = transactions_principal['category'].str.strip()

condition_contract_type = [(transactions_principal.contract_type == "disbursements"),
                           (transactions_principal.contract_type == "time deposits"),
                           (transactions_principal.contract_type == "attracted loans"),
                           (transactions_principal.category == "Securities"),
                           (transactions_principal.category == "Issued bonds")]
values_tr_principal = ["Loan principal repayment", "Deposit principal repayment", "Attracted loan principal payment",
                       "Securities principal repayment",
                       "Issued bond`s principal repayment"]
transactions_principal["transaction_type"] = np.select(condition_contract_type, values_tr_principal)

values_tr_interest = ["Loan interest repayment", "Deposit interest repayment", "Attracted loan interest payment",
                      "Securities interest repayment",
                      "Issued bond`s interest repayment"]
values_ledger_dt_principal = [4, 5, 6, 1, 7]
values_ledger_dt_interest = [1, 12, 12, 1, 12]
values_ledger_cr_principal = [3, 1, 1, 2, 1]
values_ledger_cr_interest = [9, 1, 1, 9, 1]

transactions_principal["transaction_sum"] = transactions_principal["transaction_sum"] * (-1)


transactions_principal["ledger_id_debit"] = np.select(condition_contract_type, values_ledger_dt_principal)
transactions_principal["legder_id_credit"] = np.select(condition_contract_type, values_ledger_cr_principal)
transactions_principal["currency"] = payments_by_contract[9]
condition_currency = [(transactions_principal.currency == "AMD"), (transactions_principal.currency == "USD"),(transactions_principal.currency == "EUR") ]
values_currency = [59,1,10]
transactions_principal["currency_id"] = np.select(condition_currency, values_currency)

condition_account_dt = [(transactions_principal.contract_type == "disbursements") & (transactions_principal.currency == "AMD"),
                        (transactions_principal.category == "Securities") & (transactions_principal.currency == "AMD"),
                        (transactions_principal.contract_type == "disbursements") & (transactions_principal.currency == "USD"),
                        (transactions_principal.category == "Securities") & (transactions_principal.currency == "USD"),
                        (transactions_principal.contract_type == "disbursements") & (transactions_principal.currency == "EUR"),
                        (transactions_principal.category == "Securities") & (transactions_principal.currency == "EUR"),
                        (transactions_principal.contract_type == "time deposits"), (transactions_principal.contract_type == "attracted loans"),
                       (transactions_principal.category == "Issued bonds")]
values_account_dt = [1,1,2,2,3,3,transactions_principal.account_id,transactions_principal.account_id,transactions_principal.account_id]

condition_account_cr = [(transactions_principal.contract_type == "disbursements") & (transactions_principal.currency == "AMD"),
                        (transactions_principal.category == "Securities") & (transactions_principal.currency == "AMD"),
                        (transactions_principal.contract_type == "disbursements") & (transactions_principal.currency == "USD"),
                        (transactions_principal.category == "Securities") & (transactions_principal.currency == "USD"),
                        (transactions_principal.contract_type == "disbursements") & (transactions_principal.currency == "EUR"),
                        (transactions_principal.category == "Securities") & (transactions_principal.currency == "EUR"),
                        (transactions_principal.contract_type == "time deposits"), (transactions_principal.contract_type == "attracted loans"),
                       (transactions_principal.category == "Issued bonds")]
values_account_cr = [transactions_principal.account_id,transactions_principal.account_id,transactions_principal.account_id,1,1,2,2,3,3]


transactions_principal["debit_account"] = np.select(condition_account_dt, values_account_dt)
transactions_principal["credit_account"] = np.select(condition_account_cr, values_account_cr)

transactions_principal = transactions_principal.drop(columns= ['contract_type','category','account_id'])

#build interest payment table

transactions_interest = payments_by_contract.drop(
    columns=['0_x', '3_x', '4_x', '6_x', 7, 8, 12, '1_y', '2_y', '3_y', '4_y', '5_y', '6_y'])

transactions_interest.columns = ['contract_number', 'payment_date', 'transaction_sum', 'currency', 'contract_type',
                                  'category', 'account_id']

transactions_interest['contract_type'] = transactions_interest['contract_type'].str.strip()
transactions_interest['category'] = transactions_interest['category'].str.strip()

transactions_interest["transaction_type"] = np.select(condition_contract_type, values_tr_interest)

transactions_interest["transaction_sum"] = transactions_interest["transaction_sum"] * (-1)


transactions_interest["ledger_id_debit"] = np.select(condition_contract_type, values_ledger_dt_interest)
transactions_interest["legder_id_credit"] = np.select(condition_contract_type, values_ledger_cr_interest)
transactions_interest["currency"] = payments_by_contract[9]
transactions_interest["currency_id"] = np.select(condition_currency, values_currency)

transactions_interest["debit_account"] = np.select(condition_account_dt, values_account_dt)
transactions_interest["credit_account"] = np.select(condition_account_cr, values_account_cr)

transactions_interest = transactions_interest.drop(columns= ['contract_type','category','account_id'])

transactions_total = pd.concat([transactions_principal, transactions_interest])

np_transactions_total = transactions_total.to_numpy()

tpl_transactions_total = list(map(tuple, np_transactions_total))




execute_values(cur,
               "INSERT INTO transaction_log (contract_number,date,transaction_sum,currency,transaction_type, ledger_id_debit, ledger_id_credit, currency_id, debit_account, credit_account) VALUES %s",
               tpl_transactions_total)


con.commit()
con.close()