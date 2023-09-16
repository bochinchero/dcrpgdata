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

# get price data from CM
PriceBTC = utils.cm.getMetric('dcr','PriceBTC',dStart,dEnd)

# get utxo data from dcrdata instance
utxos = utils.pg.pgquery_utxo_set()
# calculate realised value
CapRealBTC = utils.metrics.realvalue(utxos,PriceBTC.rename(columns={"date": "date", "PriceBTC": "price"}))
CapRealBTC = CapRealBTC.rename(columns={"date": "date", "realcap": "CapRealBTC"})
# delete utxo data set, won't be used
del utxos

# get ticket data from dcrdata instance
tickets = utils.pg.pgquery_utxo_tickets()

# calculate staked realised value
df = utils.metrics.realvalue(tickets,PriceBTC.rename(columns={"date": "date", "PriceBTC": "price"}))
# purge
del tickets
df = df.rename(columns={"date": "date", "realcap": "SRV"})

# get poolval data from dcrdata instance
poolval = utils.pg.pgquery_poolval()
# merge other metrics
df = df.merge(poolval, left_on='date', right_on='date', how='left')
del poolval


df = df.merge(CapRealBTC, left_on='date', right_on='date', how='left')


# formatting
df['date'] = df['date'].dt.date
df.SRV = df.SRV.astype(float).round(2)
df.poolval = df.poolval.astype(float).round(2)
df.CapRealBTC = df.CapRealBTC.astype(float).round(2)


# drop last row (today)
df.drop(df.tail(1).index, inplace=True)

# save to CSV
basePathStr = "./data/"
pathStr = basePathStr
if not os.path.exists(pathStr):
    os.makedirs(pathStr)
filename = pathStr + 'rvBTC.csv'
# remove file if it exists
if os.path.isfile(filename):
    os.remove(filename)
# save to csv
df.to_csv(filename,index=False)
