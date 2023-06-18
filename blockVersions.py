import utils.pg
import utils.cm
import utils.metrics
import pandas as pd
import datetime as dt
import numpy as np
import os

# get data from dcrdata instance
print('getting pow data from db')
df = utils.pg.pgquery_blockVers()
print('processing')
# make sure date has the right format
df['date'] = df['date'].dt.date
# pivot table
table = pd.pivot_table(df, values='count', index=['date'],
                       columns=['version'], aggfunc=np.max, fill_value=0)
# flatten idnex
table.columns.to_flat_index()
# save to CSV
basePathStr = "./data/"
pathStr = basePathStr
if not os.path.exists(pathStr):
    os.makedirs(pathStr)
filename = pathStr + 'blockVersions.csv'
# remove file if it exists
if os.path.isfile(filename):
    os.remove(filename)
# save to csv
table.to_csv(filename,index=True)
