# Import libraries
import pandas as pd
import json
import psycopg2
from config.config import config

with open('db_schema.json') as json_file:
    data = json.load(json_file)

# Connect to PostgreSQL
params = config(config_db_file='database.ini')
con = psycopg2.connect(**params)
print('Python connected to PostgreSQL!')
# Create table
cur = con.cursor()

for key in data.keys():
    str = """ {query} """.format(query=data[key])
    cur.execute(str)

print("Tables created")

# Close the connection
con.commit()
con.close()


