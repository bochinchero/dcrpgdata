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
PriceUSD = utils.cm.getMetric('dcr', 'PriceUSD', dStart, dEnd)
SplyCur = utils.cm.getMetric('dcr','SplyCur',dStart,dEnd)
CapMrktCurUSD = utils.cm.getMetric('dcr','CapMrktCurUSD',dStart,dEnd)
CapRealUSD = utils.cm.getMetric('dcr','CapRealUSD',dStart,dEnd)

# get ticket data from dcrdata instance
tickets = utils.pg.pgquery_utxo_tickets()
# get poolval data from dcrdata instance
poolval = utils.pg.pgquery_poolval()

# calculate staked realised value
df = utils.metrics.realvalue(tickets,PriceUSD.rename(columns={"date": "date", "PriceUSD": "price"}))
df = df.rename(columns={"date": "date", "realcap": "SRV"})

# merge other metrics
df = df.merge(SplyCur, left_on='date', right_on='date', how='left')
df = df.merge(poolval, left_on='date', right_on='date', how='left')
df = df.merge(CapRealUSD, left_on='date', right_on='date', how='left')
df = df.merge(CapMrktCurUSD, left_on='date', right_on='date', how='left')
# purge
del tickets,poolval

df['date'] = df['date'].dt.date

# save to CSV
basePathStr = "./data/"
pathStr = basePathStr
if not os.path.exists(pathStr):
    os.makedirs(pathStr)
filename = pathStr + 'rvUSD.csv'
# remove file if it exists
if os.path.isfile(filename):
    os.remove(filename)
# save to csv
df.to_csv(filename,index=False)
