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


params = config(config_db_file='database.ini')
con = psycopg2.connect(**params)
print('Python connected to PostgreSQL!')
# Create table
cur = con.cursor()


cur.execute("""SELECT * FROM contracts WHERE contract_date > current_date - interval '1 year' """)


contracts = cur.fetchall()
contracts = pd.DataFrame(contracts)



###########______contracts____#################


contracts_amort = pd.DataFrame()
contracts_amort["interest_rate"] = contracts[2]
contracts_amort["months"] = (contracts[5] - contracts[4]) /np.timedelta64(1,'M') + 1
contracts_amort["months"] = contracts_amort["months"].astype(int)
contracts_amort["payments_year"] = contracts[11]
contracts_amort["principal"] = contracts[6]
contracts_amort["start_date"] = contracts[4]
contracts_amort["currency"] = contracts[8]
contracts_amort["partner_id"] = contracts[9]
contracts_amort["contract_type"] = contracts[10]
contracts_amort["category"] = contracts[12]
contracts_amort["contract_number"] = contracts[0]
contracts_amort["ledger_id"] = contracts[1]

def amortization_table(interest_rate, months, payments_year, principal, start_date, currency, partner_id,contract_type, category, ledger_id):

    # Create an index of the payment dates
    start_date = start_date - pd.DateOffset(days=0)
    end_date = start_date + pd.DateOffset(months=months)
    dates = []
    while start_date <= end_date:
        start_date += relativedelta(months=1)
        dates.append(start_date)

    rng = pd.DataFrame(dates)

    rng = pd.to_datetime(rng[0])
    rng.name = "Payment_Date"

    # Build up the Amortization schedule as a DataFrame
    df = pd.DataFrame(index=rng, columns=['Payment', 'Principal', 'Interest',
                                          'Curr_Balance'], dtype='float')

    # Add index by period (start at 1 not 0)
    df.reset_index(inplace=True)
    df.index += 1
    df.index.name = "Period"

    # Calculate the payment, principal and interests amounts using built in Numpy functions
    per_payment = npf.pmt(interest_rate / payments_year, months, principal)
    df["Payment"] = per_payment
    df["Principal"] = npf.ppmt(interest_rate / payments_year, df.index, months, principal)
    df["Interest"] = npf.ipmt(interest_rate / payments_year, df.index, months, principal)
    df["Currency"] = currency
    df["partner_id"] = partner_id
    df["contract_type"] = contract_type
    df["category"] = category
    df["ledger_id"] = ledger_id


    # Round the values
    df = df.round(2)

    # Store the Cumulative Principal Payments and ensure it never gets larger than the original principal
    df["Cumulative_Principal"] = (df["Principal"]).cumsum()
    df["Cumulative_Principal"] = df["Cumulative_Principal"].clip(lower=-principal)

    # Calculate the current balance for each period
    df["Curr_Balance"] = principal + df["Cumulative_Principal"]

    # Determine the last payment date
    try:
        last_payment = df.query("Curr_Balance <= 0")["Curr_Balance"].idxmax(axis=1, skipna=True)
    except ValueError:
        last_payment = df.last_valid_index()

    last_payment_date = "{:%m-%d-%Y}".format(df.loc[last_payment, "Payment_Date"])

    # Get the payment info into a DataFrame in column order
    payment_info = (df[["Payment", "Principal", "Interest"]]
                    .sum().to_frame().T)

    # Format the Date DataFrame
    payment_details = pd.DataFrame.from_dict(dict([('payoff_date', [last_payment_date]),
                                                   ('Interest Rate', [interest_rate]),
                                                   ('Number of months', [months])
                                                   ]))
    # Add a column showing how much we pay each period.
    # Combine addl principal with principal for total payment
    payment_details["Period_Payment"] = round(per_payment, 2)

    payment_summary = pd.concat([payment_details, payment_info], axis=1)
    return df


contr_amort_dict = contracts_amort.set_index('contract_number').T.to_dict('list')

contr_amort_list = []

for value in contr_amort_dict.values():
     contr_amort_list.append(amortization_table(value[0], value[1], value[2], value[3], value[4], value[5],value[6], value[7],value[8],value[9]))


for i in range (len(contr_amort_list)):
    contr_amort_list[i]["contract_number"] = i + 1

contr_amort_list = pd.concat(contr_amort_list)
contr_amort_list = contr_amort_list.sort_values(by=['contract_number','Period'])
np_contr_amort = contr_amort_list.to_numpy()

tpl_contr_amort_list = list(map(tuple, np_contr_amort))



execute_values(cur,
               "INSERT INTO amortization_table (payment_date, payment_sum, principal, interest, current_balance,currency, partner_id,contract_type,category,ledger_id,cumulative_principal,contract_number ) VALUES %s",
               tpl_contr_amort_list)


con.commit()
print("Data was inserted into the DB")

con.close()