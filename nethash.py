import utils.pg
import utils.cm
import utils.metrics
import pandas as pd
import datetime as dt
import numpy as np
import os
import subprocess
from subprocess import Popen, PIPE

def getNetHash(row):
    execStr = 'getnetworkhashps ' + str(row['block_count']) + ' ' + str(row['last_block'])
    process = Popen(['dcrctl', 'getnetworkhashps', str(row['block_count']),str(row['last_block'])], stdout=PIPE)
    (output, err) = process.communicate()
    exit_code = process.wait()
    return int(output)

# get data from dcrdata instance
print('getting pow data from db')
df = utils.pg.pgquery_dailyblocks()
print('processing')
# make sure date has the right format
df['date'] = df['date'].dt.date
# grab the hashrate via dcrctl
df['networkhashps'] = df.apply(getNetHash,axis=1)

# drop last row (today)
df.drop(df.tail(1).index, inplace=True)

# save to CSV
basePathStr = "./data/"
pathStr = basePathStr
if not os.path.exists(pathStr):
    os.makedirs(pathStr)
filename = pathStr + 'networkhashps.csv'
# remove file if it exists
if os.path.isfile(filename):
    os.remove(filename)
# save to csv
df.to_csv(filename,index=False)
