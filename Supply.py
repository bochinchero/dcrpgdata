import utils.pg
import utils.cm
import utils.metrics
import pandas as pd
import datetime as dt
import os

fmtt = '%Y-%m-%dT%H:%M:%S'
# convert to pd dt
dStart = pd.to_datetime(dt.date(int(2016),int(1),int(1)), utc=True, format=fmtt, errors='ignore')
dEnd = pd.to_datetime(dt.date.today() + dt.timedelta(days=60), utc=True, format=fmtt, errors='ignore')

# get data from dcrdata instance
print('getting supply data from db')
df = utils.pg.pgquery_Supply()

# make sure date has the right format
df['date'] = df['date'].dt.date
# convert to something human readable
df['newsply'] = df.newsply /100000000
df['totsply'] = df.totsply /100000000
# turncate, decimal units
df.newsply = df.newsply.astype(float).round(2)
df.totsply = df.totsply.astype(float).round(2)
# drop last row (today)
df.drop(df.tail(1).index, inplace=True)
# save to CSV
basePathStr = "./data/"
pathStr = basePathStr
if not os.path.exists(pathStr):
    os.makedirs(pathStr)
filename = pathStr + 'supply.csv'
# remove file if it exists
if os.path.isfile(filename):
    os.remove(filename)
# save to csv
df.to_csv(filename,index=False)
