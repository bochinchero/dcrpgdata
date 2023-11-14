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
# create date array to start
date_rng = pd.date_range(start=dStart, end=dEnd, freq='d')
datedf = pd.DataFrame(date_rng, columns=['date'])

# get data from dcrdata instance
dt_in = utils.pg.pgquery_decTresIssuance()
dt_in = dt_in.rename(columns={"date": "date", "dectres": "received"})
dt_out = utils.pg.pgquery_decTresOutflows()
dtres = datedf.merge(dt_in, left_on='date', right_on='date', how='left')
dtres = dtres.merge(dt_out, left_on='date', right_on='date', how='left')

dtres = dtres.fillna(0)
dtres['net'] = dtres.received - dtres.sent



lt_in = utils.pg.pgquery_legTresInflows()
lt_out = utils.pg.pgquery_legTresOutflows()
ltres = datedf.merge(lt_in, left_on='date', right_on='date', how='left')
ltres = ltres.merge(lt_out, left_on='date', right_on='date', how='left')

ltres = ltres.fillna(0)
print(ltres)
ltres['net'] = ltres.received - ltres.sent

# save to CSV
basePathStr = "./data/"
pathStr = basePathStr
if not os.path.exists(pathStr):
    os.makedirs(pathStr)
filename = pathStr + 'dtres.csv'
# remove file if it exists
if os.path.isfile(filename):
    os.remove(filename)
# save to csv
dtres.to_csv(filename,index=False)

filename = pathStr + 'ltres.csv'
# remove file if it exists
if os.path.isfile(filename):
    os.remove(filename)
# save to csv
ltres.to_csv(filename,index=False)