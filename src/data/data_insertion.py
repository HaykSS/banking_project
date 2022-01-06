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
import datetime
from random import sample
import numpy_financial as npf
from dateutil.relativedelta import relativedelta

# Connect to PostgreSQL

params = config(config_db_file='database.ini')
con = psycopg2.connect(**params)
print('Python connected to PostgreSQL!')
# Create table
cur = con.cursor()

partner_num = 1000
loans_num = 2 * partner_num
address_row = partner_num + 22


#########____address___###########

class address(object):
    f = Faker()

    def __init__(self):
        self.address = address.f.address()
        self.city = address.f.city()
        self.country = address.f.country()


address_data = [vars(address()) for i in range(address_row)]
address = pd.DataFrame(address_data)

np_address = address.to_numpy()

tpl_address = list(map(tuple, np_address))

execute_values(cur, "INSERT INTO address (address, city, country) VALUES %s", tpl_address)


##########___partner___##########

cur.execute("""
INSERT INTO partner (legal_status, full_name, email, cityzenship,date_of_birth,mobile_num,economic_sector, opening_date) VALUES ('LE','Society One','society_one@gmail.com','New Zealand',
'2020-12-27','(697)937-7560x4966','Financial services','2020-12-27');
""")


class partner(object):
    f = Faker()

    def __init__(self):
        self.last_name = partner.f.last_name()
        self.legal_status = np.random.choice(["LE", "PP"], p=[0.25, 0.75])
        self.sex = np.random.choice(["M", "F"], p=[0.55, 0.45]) if self.legal_status == "PP" else None
        self.first_name = partner.f.first_name_male() if self.sex == "M" else partner.f.first_name_female()
        self.full_name = partner.f.company() if self.legal_status == "LE" else "{} {}".format(self.first_name,
                                                                                              self.last_name)
        self.email = unidecode.unidecode(
            "{}.{}@{}".format(self.first_name, self.last_name, partner.f.free_email_domain()).lower())
        self.cityzenship = partner.f.country()
        self.date_of_birth = partner.f.date()
        self.mobile_num = partner.f.phone_number()
        self.economic_sector = np.random.choice(["agriculture", "mining", "manufacturing",
                                                 "construction", "trade", "IT", "education", "financial"],
                                                p=[0.12, 0.1, 0.13, 0.07, 0.17, 0.15, 0.06, 0.2])
        self.position = partner.f.job() if self.legal_status == "LE" else None
        self.opening_date = date.today() - pd.DateOffset(days=randint(31, 35))


partner = [vars(partner()) for i in range(partner_num)]
df_partner = pd.DataFrame(partner)

partner = df_partner.drop(['first_name', 'last_name'], axis=1)
partner = partner.sort_values(by='opening_date')
np_partner = partner.to_numpy()

tpl_partner = list(map(tuple, np_partner))

execute_values(cur,
               "INSERT INTO partner (legal_status,sex, full_name, email, cityzenship,date_of_birth,mobile_num,economic_sector, position, opening_date) VALUES %s",
               tpl_partner)

con.commit()

############____general_ledger______###############

db_ledger = pd.read_csv('general_ledger.txt', sep = '\t')
db_ledger = db_ledger.drop(['ledger_id'], axis=1)
np_ledger = db_ledger.to_numpy()

tpl_ledger = list(map(tuple, np_ledger))

execute_values(cur,
               "INSERT INTO general_ledger (reporting_type,reporting_level_first,reporting_level_second) VALUES %s",
               tpl_ledger)


#######_________accounts_____###########

cur.execute("""
INSERT INTO accounts (opening_date, currency, balance, ledger_id,partner_id) VALUES ('2020-12-27','AMD','10000000000','1','1'),
 ('2019-12-27','USD','20000000','1','1'),('2020-12-27','EUR','15000000','1','1');
""")


cur.execute("""SELECT * FROM partner WHERE opening_date > current_date - interval '1 year' """)
query_results = cur.fetchall()
partner_to_acc = pd.DataFrame(query_results)


new_partner_id =  partner_to_acc.index.tolist()
new_partner_id = [x+1 for x in new_partner_id]

new_partner_op_date = partner_to_acc[11]

class accounts_amd(object):
    f = Faker()

    def __init__(self):
        self.currency = "AMD"
        self.balance = randint(10000, 15000000)
        self.ledger_id = 4


accounts_amd = [vars(accounts_amd()) for i in range(partner_num)]
df_accounts_amd = pd.DataFrame(accounts_amd)
df_accounts_amd["partner_id"] = new_partner_id
df_accounts_amd["opening_date"] = new_partner_op_date

class accounts_usd(object):
    f = Faker()

    def __init__(self):
        self.currency = "USD"
        self.balance = randint(20, 300000)
        self.ledger_id = 4


accounts_usd = [vars(accounts_usd()) for i in range(partner_num)]
df_accounts_usd = pd.DataFrame(accounts_usd)
df_accounts_usd["partner_id"] = new_partner_id
df_accounts_usd["opening_date"] = new_partner_op_date

class accounts_eur(object):
    f = Faker()

    def __init__(self):
        self.currency = "EUR"
        self.balance = randint(20, 300000)
        self.ledger_id = 4


accounts_eur = [vars(accounts_eur()) for i in range(partner_num)]
df_accounts_eur = pd.DataFrame(accounts_eur)
df_accounts_eur["partner_id"] = new_partner_id
df_accounts_eur["opening_date"] = new_partner_op_date

df_accounts = pd.concat([df_accounts_amd, df_accounts_usd,df_accounts_eur])

df_accounts = df_accounts.sort_values(by='opening_date')

np_accounts = df_accounts.to_numpy()

tpl_accounts = list(map(tuple, np_accounts))

execute_values(cur,
               "INSERT INTO accounts ( currency, balance, ledger_id,partner_id, opening_date) VALUES %s",
               tpl_accounts)




######___Bank___##############


cur.execute("""
INSERT INTO bank (bank_name, address_id, account_id) VALUES ('Society One',1, 1);
""")

############____Branches____#############

branch = pd.read_csv('branches.txt', sep = '\t')


np_branch = branch[['Countries']].to_numpy()

tpl_branch = list(map(tuple, np_branch))

execute_values(cur, "INSERT INTO branch (branch_name) VALUES %s",
               tpl_branch)



#############___contracts____##########

cur.execute("""SELECT * FROM partner""")
part_id = cur.fetchall()
part_id = pd.DataFrame(part_id)

part_id_pp = part_id.loc[part_id[8] == 'PP']
part_id_pp = part_id_pp[0].to_numpy()

part_id_le = part_id.loc[part_id[8] != 'PP']
part_id_le = part_id_le[0].to_numpy()

part_id = part_id[0].to_numpy()

class contracts(object):
    f = Faker()
    def __init__(self):
        self.contract_type = np.random.choice(["disbursements", "time deposits","attracted loans", "securities"], p=[0.55, 0.35,0.02, 0.08])
        self.contract_date = date.today() - pd.DateOffset(days = randint(1, 31))
        self.currency = np.random.choice(["AMD", "USD","EUR"], p=[0.4, 0.5,0.1])
        self.category = np.random.choice(["Accumulated", "Terminable"], p=[0.4, 0.6]) if self.contract_type == "time deposits" else np.random.choice(["Business loans","Consumer loans", "Mortgages","Overdrafts"], p=[0.25,0.35, 0.15,0.25]) if self.contract_type == "disbursements" else "Attracted loans" if self.contract_type == "attracted loans" else np.random.choice(["Issued bonds", "Securities"], p=[0.55, 0.45])
        self.partner_id = np.random.choice(part_id_le) if self.category == "Business loans" and self.category == "Attracted loans" and self.category == "Securities" else np.random.choice(part_id) if self.category == "Issued bonds" else np.random.choice(part_id_pp)
        self.interest_rate = (randint(9, 17)/100 if self.currency == "AMD" else randint(4, 12)/100) if self.contract_type == "disbursements" else (randint(6, 9) / 100 if self.currency == "AMD" else randint(4,6) / 100) if self.contract_type == "time deposits" else (randint(7, 9) / 100 if self.currency == "AMD" else randint(4, 6) / 100) if self.contract_type == "attracted loans" else (randint(6, 9)/100 if self.currency == "AMD" else randint(3, 6)/100)
        self.commission_rate = round(np.random.uniform(0.5, 1.5) / 100,2)
        self.redemption_date = self.contract_date + pd.DateOffset(months=np.random.choice([6, 18,30, 60, 90, 120], p=[0.1, 0.15,0.2, 0.25, 0.2, 0.1]))
        self.contract_sum = (round(randint(10000, 2500000)/10000)*10000 if self.currency == "AMD" else round(randint(2000, 50000)/1000)*1000) if self.contract_type == "disbursements" else (randint(10000, 1500000) if self.currency == "AMD" else randint(20, 30000)) if self.contract_type == "time deposits" else (round(randint(5000000, 25000000) / 1000000) * 1000000 if self.currency == "AMD" else round(randint(10000, 250000) / 1000) * 1000) if self.contract_type == "attracted loans" else (round(randint(5000000, 25000000)/10000)*10000 if self.currency == "AMD" else round(randint(5000, 50000)/1000)*1000)
        self.current_balance = self.contract_sum
        self.number_of_payments = 12 if self.contract_type == "disbursements" else 1 if self.contract_type == "time deposits" else 4 if self.contract_type == "attracted_loans" else 2
        self.ledger_id = 3 if self.contract_type == "disbursements" else 5 if self.contract_type == "time deposits" else 6 if self.contract_type == "attracted loans" else 2 if self.category == "Securities" else 7


contracts = [vars(contracts()) for i in range(loans_num)]
contracts_df = pd.DataFrame(contracts)

contracts_df = contracts_df.sort_values(by='contract_date')
np_contracts_df = contracts_df.to_numpy()

tpl_contracts_df = list(map(tuple, np_contracts_df))

execute_values(cur,
               "INSERT INTO contracts (contract_type, contract_date, currency,category, partner_id, interest_rate, commission_rate, redemption_date, contract_sum, current_balance,number_of_payments,ledger_id) VALUES %s",
               tpl_contracts_df)


#############___collaterals____##########


class collaterals(object):
    f = Faker()
    def __init__(self):
        self.ledger_id = 14
        self.contract_date = date.today() - pd.DateOffset(days = randint(20, 30))

loans_gr_working = contracts_df

df_loans_business = loans_gr_working.loc[loans_gr_working['category'] == 'Business loans']
df_loans_mortgages = loans_gr_working.loc[loans_gr_working['category'] == 'Mortgages']


bl_partid = df_loans_business["partner_id"].to_numpy()
bl_contract_num = df_loans_business.index.tolist()
bl_contract_num = [x+1 for x in bl_contract_num]
contract_sum_bl = df_loans_business["contract_sum"].to_numpy()


mortgage_partid = df_loans_mortgages["partner_id"].to_numpy()
mortgage_contract_num = df_loans_mortgages.index.tolist()
mortgage_contract_num = [x+1 for x in mortgage_contract_num]
contract_sum_mortgages = df_loans_mortgages["contract_sum"].to_numpy()

lenm = len(mortgage_partid)
lenbl = len(bl_partid)


colls_mortgages_df = [vars(collaterals()) for i in range(lenm)]
colls_mortgages_df = pd.DataFrame(colls_mortgages_df)
colls_mortgages_df["contract_sum"] = contract_sum_mortgages
colls_mortgages_df["loan_contract_number"] = mortgage_contract_num

colls_bl_df = [vars(collaterals()) for i in range(lenbl)]
colls_bl_df = pd.DataFrame(colls_bl_df)
colls_bl_df["contract_sum"] = contract_sum_bl
colls_bl_df["loan_contract_number"] = bl_contract_num
ind_bl = colls_bl_df.index.tolist()
colls_mortgages_df["partner_id"] = mortgage_partid
colls_bl_df["partner_id"] = bl_partid

colls_mortgages_df["collateral_type"] = ''

colls_mortgages_df.loc[colls_mortgages_df.index.tolist(), "collateral_type"] = "Real estate"

colls_bl_df["collateral_type"] = ''

colls_bl_df.loc[colls_bl_df.index.tolist(), "collateral_type"] = [np.random.choice(["Real estate", "Gold","Equipment","Inventory"], p=[0.45, 0.1,0.25,0.2]) for x in ind_bl]


frames = (colls_bl_df, colls_mortgages_df)
collateral_df = pd.concat(frames,ignore_index=True)

collateral_df["coll_ratio"] = round(randint(60, 120)/100,2)

collateral_df["collateral_value"] = collateral_df["coll_ratio"] * collateral_df["contract_sum"]

collateral_df["collateral_value"].astype(int)

collateral_df = collateral_df.drop(["contract_sum","coll_ratio"], axis = 1)
collateral_df = collateral_df.sort_values(by=['loan_contract_number'])

np_collateral_df = collateral_df.to_numpy()

tpl_collaterals = list(map(tuple, np_collateral_df))

execute_values(cur,
               "INSERT INTO collaterals (ledger_id, contract_date, loan_contract_number, partner_id, collateral_type, collateral_value) VALUES %s",
               tpl_collaterals)



con.commit()
print("Data was inserted into the DB")

con.close()
