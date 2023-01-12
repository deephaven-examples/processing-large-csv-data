from deephaven.plot.figure import Figure
from deephaven import parquet as dhpq
from deephaven import read_csv
from deephaven import agg

import time
import os

pq_folder = "/data/transaction_parquet/"

if not(os.path.isdir(pq_folder)):
    os.mkdir(pq_folder)

def incremental_csv_read(fname):
    steps = 5_000_000
    count = 0
    while True:
        start = time.time()
        table = read_csv(fname, skip_rows=count * steps, num_rows=steps, allow_missing_columns=True, ignore_excess_columns=True)
        end = time.time()
        dhpq.write(table, f"{pq_folder}{count}.parquet")
        print(f"Read {table.size} in {end - start} seconds on iteration number {count}.")
        count += 1

        if table.size != steps:
            break
        table = None

def read_pq_folder():
    start = time.time()
    table = dhpq.read(pq_folder)
    end = time.time()
    print(f"Read Parquet data in {end - start} seconds.")
    return table

# make a total sum option 1
def dh_sum_by_expends(table):
    start = time.time()
    data_table = table.view(formulas=["YEAR","AMOUNT"]).sum_by(by=["YEAR"]).sort(order_by=["YEAR"])
    end = time.time()
    print(f"Deephaven sum_by/sort expense time: {end - start} seconds.")
    return data_table

# make a total sum option 2
def dh_agg_expends(table):
    start = time.time()
    data_table = table.agg_by([agg.sum_(cols=["AMOUNT = AMOUNT"]),\
                            agg.count_(col="count")], by=["YEAR"]).sort(order_by=["YEAR"])
    end = time.time()
    print(f"Deephaven agg expense time: {end - start} seconds.")
    return data_table

# monthly sum, replicated logic from
# https://towardsdatascience.com/batch-processing-22gb-of-transaction-data-with-pandas-c6267e65ff36
def dh_sum_by_monthly(table):
    start = time.time()
    data_table = table.where(["YEAR == 2020", "EXP_TYPE = `Entertainment`"])\
        .agg_by([agg.sum_(cols=["AMOUNT"])], by=["CUST_ID","MONTH"])\
        .drop_columns(cols=["CUST_ID"])\
        .avg_by(["MONTH"])\
        .sort(order_by=["MONTH"])
    end = time.time()
    print(f"Deephaven sum_by expense time: {end - start} seconds.")
    return data_table


# call the method to read the file into Deephaven
table = read_parquet()

# create 3 tables based on the aggregations we desire
deephaven_expense_table_sum = dh_sum_by_expends(table)
deephaven_expense_table_agg = dh_agg_expends(table)
deephaven_sum_by_monthly=dh_sum_by_monthly(table)

# plot the tables
figure = Figure()
plot_expenses_sum=figure.plot_xy(series_name="expense", t=deephaven_expense_table_sum, x="YEAR",y="AMOUNT").show()
plot_expenses_agg=figure.plot_xy(series_name="expense", t=deephaven_expense_table_agg, x="YEAR",y="AMOUNT").show()
plot_dh_sum_by_monthly= figure.plot_xy(series_name="expense", t=deephaven_sum_by_monthly, x="MONTH",y="AMOUNT")\
                    .axis(min=-100.0, max=1400.0).show()

# Everything below here are the panda script we do not use but for your reference.
# from https://towardsdatascience.com/batch-processing-22gb-of-transaction-data-with-pandas-c6267e65ff36

def get_rows(steps,count,names,path='../Data/transactions.csv'):

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
