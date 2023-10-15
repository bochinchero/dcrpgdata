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
print('getting ticket data from db')
tickets = utils.pg.pgquery_ticketCounts()
print('getting votes data from db')
votes = utils.pg.pgquery_voteCounts()

# merge tickets and votes
df = pd.date_range(start=dStart,end=dEnd).to_frame(index=True, name='date')
df = df.merge(votes, left_on='date', right_on='date', how='left')
df = df.merge(tickets, left_on='date', right_on='date', how='left')

# fill na
df = df.fillna(0)
# make sure date has the right format
df['date'] = df['date'].dt.date
df.tickets = df.tickets.astype(int)
df.votes = df.votes.astype(int)

# drop last row (today)
df.drop(df.tail(1).index, inplace=True)
# save to CSV
basePathStr = "./data/"
pathStr = basePathStr
if not os.path.exists(pathStr):
    os.makedirs(pathStr)
filename = pathStr + 'ticketsVotes.csv'
# remove file if it exists
if os.path.isfile(filename):
    os.remove(filename)
# save to csv
df.to_csv(filename,index=False)
