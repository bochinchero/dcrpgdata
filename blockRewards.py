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
print('getting CM data')
PriceUSD = utils.cm.getMetric('dcr', 'PriceUSD', dStart, dEnd)

# get data from dcrdata instance
print('getting pow data from db')
powReward = utils.pg.pgquery_powReward()
print('getting pos data from db')
posReward = utils.pg.pgquery_posReward()
print('getting dec tres from db')
decTres = utils.pg.pgquery_decTresIssuance()
print('getting pos data from db')
legTres = utils.pg.pgquery_legTresIssuance()
# rename columns
posReward = posReward.rename(columns={"date": "date", "value": "posDCR"})
powReward = powReward.rename(columns={"date": "date", "value": "powDCR"})
# merge rewards
df = posReward.merge(powReward, left_on='date', right_on='date', how='left')
df = df.merge(decTres, left_on='date', right_on='date', how='left')
df = df.merge(legTres, left_on='date', right_on='date', how='left')
df = df.merge(PriceUSD, left_on='date', right_on='date', how='left')
print('calcs and finishing up')
df = df.fillna(0)
df['tresDCR'] = df.decTres + df.legTres
# calculate ow data from dbUSD values
df['powUSD'] = df.powDCR * df.PriceUSD
df['posUSD'] = df.posDCR * df.PriceUSD
df['tresUSD'] = df.tresDCR * df.PriceUSD
# format floats
df.powUSD = df.powUSD.astype(float).round(2)
df.posUSD = df.posUSD.astype(float).round(2)
# drop price column
df = df.drop(columns=['PriceUSD','decTres','legTres'])
# make sure date has the right format
df['date'] = df['date'].dt.date
# fill na with 0
df = df.fillna(0)
# drop last row (today)
df.drop(df.tail(1).index, inplace=True)

# save to CSV
basePathStr = "./data/"
pathStr = basePathStr
if not os.path.exists(pathStr):
    os.makedirs(pathStr)
filename = pathStr + 'blockRewards.csv'
# remove file if it exists
if os.path.isfile(filename):
    os.remove(filename)
# save to csv
df.to_csv(filename,index=False)
