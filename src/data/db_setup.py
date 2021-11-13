# Import libraries
import pandas as pd
import psycopg2
from config.config import config

# Connect to PostgreSQL

params = config(config_db_file='database.ini')
con = psycopg2.connect(**params)
print('Python connected to PostgreSQL!')
# Create table
cur = con.cursor()
cur.execute("""
DROP TYPE IF EXISTS sex;
DROP TYPE IF EXISTS emp;
CREATE TYPE sex AS ENUM('male','female');
CREATE TYPE emp AS ENUM('Yes','No');
CREATE TABLE IF NOT EXISTS customer(
customer_id varchar(20) PRIMARY KEY NOT NULL,
full_name CHAR(50) NOT NULL,
address CHAR(100),
email CHAR(50),
phone_number CHAR(20),
citizenship varchar(20),
customer_sex sex,
phonenumber char(15),
doc_type char(10),
doc_number char(20),
is_employee emp);
""")
print('Table created in PostgreSQL')
# Close the connection
con.commit()
con.close()
