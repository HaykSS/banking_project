# Import libraries
import pandas as pd
import json
import psycopg2
from config.config import config

with open('querries_with_transactions.json') as json_file:
    operations = json.load(json_file)

# Connect to PostgreSQL
params = config(config_db_file='database.ini')
con = psycopg2.connect(**params)
print('Python connected to PostgreSQL!')
# Create table
cur = con.cursor()

for key in operations.keys():
    str = """ {query} """.format(query=operations[key])
    cur.execute(str)




print("Transactions inserted")

# Close the connection
con.commit()
con.close()