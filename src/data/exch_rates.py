from urllib.request import Request
from urllib.request import urlopen
from bs4 import BeautifulSoup
import requests
from lxml.html import parse
from config.config import config
from datetime import datetime
from datetime import date
import pandas as pd
import psycopg2
import numpy as np
from psycopg2.extras import execute_values




params = config(config_db_file='database.ini')
con = psycopg2.connect(**params)
print('Python connected to PostgreSQL!')
# Create table
cur = con.cursor()



url="http://api.cba.am/exchangerates.asmx"
dateTime = datetime.now().strftime("%Y-%m-%d")

headers = {"Host": "api.cba.am","content-type": "text/xml; charset=utf-8","content-length": "450","SOAPAction": "http://www.cba.am/ExchangeRatesByDate"}
body = """<?xml version="1.0" encoding="utf-8"?>
            <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
              <soap:Body>
                <ExchangeRatesByDate xmlns="http://www.cba.am/">
                  <date>{date}</date>
                    </ExchangeRatesByDate>
              </soap:Body>
            </soap:Envelope>"""

response = requests.post(url,data=body.format(date=dateTime),headers=headers)

xml = BeautifulSoup(response.text, 'xml')

exchange_rates = xml.findAll('ExchangeRate')
exchange_rate_codes = xml.findAll('ISO')
rates = xml.findAll('Rate')
exchange_rate_codes
dict = {}

for i in range(0, len(exchange_rates)):
    dict[exchange_rate_codes[i].get_text()]=rates[i].get_text()


exch_rates = pd.DataFrame.from_dict(dict,orient='index')
exch_rates = exch_rates.reset_index()



today = date.today()
exch_rates['start_date'] = pd.to_datetime('today').strftime("%Y/%m/%d")
exch_rates['end_date'] = pd.to_datetime('today').strftime("%Y/%m/%d")



exch_rates.columns = ['currency_code','exchange_rate','start_date','end_date']




amd_row = pd.DataFrame({"currency_code" : ["AMD"],
                        "exchange_rate" : [1],
                        "start_date" : [pd.to_datetime('today').strftime("%Y/%m/%d")],
                        "end_date" : [pd.to_datetime('today').strftime("%Y/%m/%d")],

})

exch_rates = exch_rates.append(amd_row, ignore_index= True)



np_rates = exch_rates.to_numpy()

tpl_rates = list(map(tuple, np_rates))

execute_values(cur, "INSERT INTO exchange_rates (currency, exchange_rate, start_date, end_date) VALUES %s", tpl_rates)

print("Data is inserted into SQL db")
con.commit()
# Close the connection
con.close()