# Import libraries
import pandas as pd
from psycopg2.extras import execute_values
import psycopg2
from config.config import config
from random import randint
from faker import Faker
import numpy as np
import unidecode

# Connect to PostgreSQL

params = config(config_db_file='database.ini')
con = psycopg2.connect(**params)
print('Python connected to PostgreSQL!')
# Create table
cur = con.cursor()


class address(object):
    f = Faker()

    def __init__(self):
        self.address = address.f.address()
        self.city = address.f.city()
        self.country = address.f.country()


address_data = [vars(address()) for i in range(1000)]
address = pd.DataFrame(address_data)

np_address = address.to_numpy()

tpl_address = list(map(tuple, np_address))


execute_values(cur, "INSERT INTO address (address, city, country) VALUES %s", tpl_address)


class partner(object):
    f = Faker()

    def __init__(self):
        self.sex = np.random.choice(["M", "F"], p=[0.55, 0.45])
        self.first_name = partner.f.first_name_male() if self.sex == "M" else partner.f.first_name_female()
        self.last_name = partner.f.last_name()
        self.full_name = "{} {}".format(self.first_name, self.last_name)
        self.email = unidecode.unidecode(
            "{}.{}@{}".format(self.first_name, self.last_name, partner.f.free_email_domain()).lower())
        self.cityzenship = partner.f.country()
        self.date_of_birth = partner.f.date()
        self.mobile_num = partner.f.phone_number()
        self.partner_type = np.random.choice(["customer", "lender", "employee"], p=[0.98, 0.015, 0.005])
        self.economic_sector = np.random.choice(["agriculture", "mining", "manufacturing",
                                                 "construction", "trade", "IT", "education", "financial"],
                                                p=[0.12, 0.1, 0.13, 0.07, 0.17, 0.15, 0.06, 0.2])
        self.position = partner.f.job()


partner = [vars(partner()) for i in range(1000)]
df_partner = pd.DataFrame(partner)
df_partner.index = np.arange(1, len(df_partner) + 1)
df = df_partner.reset_index()
partner = df.drop(['first_name', 'last_name'], axis=1)

np_partner = partner.to_numpy()

tpl_partner = list(map(tuple, np_partner))

execute_values(cur, "INSERT INTO partner (address_id,sex,full_name, email, cityzenship,date_of_birth,mobile_num,partner_type,economic_sector, position) VALUES %s", tpl_partner)

con.commit()
# cur.execute("""
# INSERT INTO bank (bank_name, address_id, account_id) VALUES ('Society One', '1', '1');
# """)

print("Data was inserted into the DB")


con.close()
