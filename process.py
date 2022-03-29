#imports
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from deephaven2 import read_csv
import time
from deephaven2 import merge
from deephaven2 import pandas

import deephaven.Types as dht

header = {"CUST_ID": dht.string,"START_DATE": dht.string, "END_DATE": dht.string, "TRANS_ID": dht.string,"CUST_ID": dht.string, "DATE": dht.string, "YEAR": dht.short, "MONTH": dht.byte,"DAY": dht.byte ,"EXP_TYPE": dht.string, "AMOUNT": dht.double}


def get_rows(steps,count,names,path='/data/transactions.csv'):
    """
    Returns a subset of rows from a CSV. The fist [steps]*[count]
    rows are skipped and the next [steps] rows are returned.

    params
    ------------
        steps: number of rows returned
        count: count variable updated each iteration
        names: columns names of dataset
        path: location of csv
    """

    if count ==0:
        df = pd.read_csv(path,
                         nrows=steps)
    else:
        df = pd.read_csv(path,
                         skiprows=steps*count,
                         nrows=steps,
                         names=names)
    return df



def read_panda(tables):
    start = time.time()
    steps = 5000000
    names = ['CUST_ID', 'START_DATE', 'END_DATE', 'TRANS_ID', 'DATE', 'YEAR',
        'MONTH', 'DAY', 'EXP_TYPE', 'AMOUNT']

    #Initialise number of transactions
    n = 0

    #Initialise count
    count = 0
    while True:

        #Return subsection of dataset
        df = get_rows(steps,count,names)
        table = pandas.to_table(df).update(formulas = ["CUST_ID=(String)CUST_ID", "EXP_TYPE=(String)EXP_TYPE"])
        tables.append(table)
        #Update number of transactions
        n+=len(df)

        #Update count
        count+=1

        #Exit loop
        if len(df)!=steps:
            break

    #Output number of rows
    print(n)
    end = time.time()
    print("Panda csv_read time: " + str(end - start) + " seconds.")
    return tables


def expend_year(df):
    start = time.time()
    steps = 5000000
    names = ['CUST_ID', 'START_DATE', 'END_DATE', 'TRANS_ID', 'DATE', 'YEAR',
        'MONTH', 'DAY', 'EXP_TYPE', 'AMOUNT']

    #Initialise number of transactions
    n = 0

    #Initialise yearly totals
    total_exp = pd.Series([0.0]*11, index=range(2010,2021))

    count = 0
    while True:

        df = get_rows(steps,count,names)

        #Get yearly totals for subsection
        exp = df.groupby(['YEAR'])['AMOUNT'].sum()

        #Loop over years 2010 to 2020
        for year in range(2010,2021):
            #Update yearly totals
            total_exp[year] += exp[year]

        count+=1
        table = pandas.to_table(df).update(formulas = ["CUST_ID=(String)CUST_ID", "EXP_TYPE=(String)EXP_TYPE"])
        tables.append(table)

        #Exit loop
        if len(df)!=steps:
            break
    end = time.time()
    print("Panda total expense time: " + str(end - start) + " seconds.")
    return total_exp

def expend_monthly(df):
    start = time.time()
    #Create empty total expenditure dataframe
    total_exp = pd.DataFrame(columns=['CUST_ID','MONTH','AMOUNT'])

    count = 0
    while True:

        df = get_rows(steps,count,names)

        #Calculate monthly totals for each customer
        df_2020 = df[(df.YEAR==2020) & (df.EXP_TYPE=='Entertainment')]
        sum_exp = df_2020.groupby(['CUST_ID','MONTH'],as_index=False)['AMOUNT'].sum()

        #Append monthly totals
        total_exp = total_exp.append(sum_exp)

        #Aggregate again so CUST_ID and MONTH are unique
        total_exp = total_exp.groupby(['CUST_ID','MONTH'],as_index=False)['AMOUNT'].sum()

        count+=1

        #Exit loop
        if len(df)!=steps:
            break

    #Final aggregations
    end = time.time()
    print("Panda monthly time: " + str(end - start) + " seconds.")
    return total_exp.groupby(['MONTH'])['AMOUNT'].mean()


def dh_sum_by_expends(table):
    start = time.time()
    from deephaven2 import agg as agg
    data_table = table.view(formulas = ["YEAR","AMOUNT"]).sum_by(by = ["YEAR"]).sort(order = ["YEAR"])
    end = time.time()
    print("Deephaven sum_by expense time: " + str(end - start) + " seconds.")
    return data_table


def dh_agg_expends(table):
    start = time.time()
    data_table = table.agg_by([agg.sum_(cols = ["Sum = AMOUNT"]),\
                            agg.count_(col = "count")], by = ["YEAR"]).sort(order = ["YEAR"])
    end = time.time()
    print("Deephaven agg expense time: " + str(end - start) + " seconds.")
    return data_table

## Takes up to 90 minutes
#tables = read_panda([])

##Each call takes up to 60 minutes
#panda_total_exp_year  = expend_year(df)
#panda_monthly_exp = expend_monthly(df)

##turn dataFrame into a Deephaven table
#table = pandas.to_table(df).update(formulas = ["CUST_ID=(String)CUST_ID", "EXP_TYPE=(String)EXP_TYPE"])

#deephaven_expense_table = dh_sum_by_expends(table)
#deephaven_expense_table = dh_agg_expends(table)




##Plot aggregation
plt.figure(figsize=(10, 5))
plt.plot(total_exp_year.index,total_exp_year/1000000000)
plt.ylabel('Total expenditure ($ billion)',size=15)
plt.xlabel('Year',size=15)
plt.ylim(bottom=0)
m_figure=plt.gcf()
print(total_exp_year)
