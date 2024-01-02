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
    process = Popen(["dcrctl", execStr], stdout=PIPE)
    (output, err) = process.communicate()
    exit_code = process.wait()
    return output

# get data from dcrdata instance
print('getting pow data from db')
df = utils.pg.pgquery_dailyblocks()
print('processing')
# make sure date has the right format
df['date'] = df['date'].dt.date

df['networkhashps'] = df.apply(getNetHash,axis=1)

print(df)
