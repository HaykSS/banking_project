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



#######______transaction_log_____################

cur.execute("""SELECT * FROM accounts""")
query_results = cur.fetchall()
xaccounts = pd.DataFrame(query_results)

accounts_amd = xaccounts[xaccounts[6] == "AMD"]
accounts_usd = xaccounts[xaccounts[6] == "USD"]
accounts_eur = xaccounts[xaccounts[6] == "EUR"]

accountsId_amd = accounts_amd[0].to_numpy()
accountsId_usd = accounts_usd[0].to_numpy()
accountsId_eur = accounts_eur[0].to_numpy()


class transactions(object):

    def __init__(self):
        tr_type = ["Transfer", "Insurance", "Brokerage services", "Settlement operations", "Cards service"]
        self.transaction_type = np.random.choice(tr_type, p=[0.4, 0.1, 0.15, 0.2, 0.15])
        self.ledger_id_debit = 4 if self.transaction_type == "Transfer" else 1
        self.legder_id_credit = 4 if self.transaction_type == "Transfer" else 10
        self.currency = np.random.choice(["AMD", "USD", "EUR"], p=[0.45, 0.45, 0.1])
        self.currency_id = 59 if self.currency == "AMD" else 1 if self.currency == "USD" else 10
        self.transaction_sum = randint(1000, 150000)
        self.debit_account = np.random.choice(accountsId_amd[1:]) if self.transaction_type == "Transfer" and self.currency == "AMD" else np.random.choice(
            accountsId_usd[1:]) if self.transaction_type == "Transfer" and self.currency == "USD" else np.random.choice(
            accountsId_eur[1:]) if self.transaction_type == "Transfer" and self.currency == "EUR" else 1
        self.credit_account = np.random.choice(accountsId_amd[1:]) if self.currency == "AMD" else np.random.choice(
            accountsId_usd[1:]) if self.currency == "USD" else np.random.choice(accountsId_eur[1:])
        self.date = date.today() - pd.DateOffset(months=0)
        self.status = ""



transactions_data = [vars(transactions()) for i in range(300)]
transactions = pd.DataFrame(transactions_data)


np_transactions = transactions.to_numpy()

tpl_transactions = list(map(tuple, np_transactions))




execute_values(cur,
               "INSERT INTO transaction_log (transaction_type, ledger_id_debit, ledger_id_credit, currency, currency_id,transaction_sum, debit_account, credit_account, date,status) VALUES %s",
               tpl_transactions)


con.commit()
con.close()