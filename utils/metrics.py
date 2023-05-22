# Realised Value Calculation
import pandas as pd
import datetime

def realvalue(utxo_set,price):
    # This function calculates realised value based on 2 data frames that need the following structure
    # utxo_set: fund_date, spend_date, value
    # price: date, price
    # the function output will be as date, realcap

    # merge price into utxo set based on fund_date
    utxo_set = utxo_set.merge(price,left_on='fund_date',right_on='date',how='left')

    # calculate value on utox fund date in fiat/btc
    utxo_set['fund_value']=utxo_set.value*utxo_set.price

    # split out utxo_set into additions and subtractions arrays
    adds = utxo_set[['fund_date','fund_value']].copy()
    subs = utxo_set[['spend_date', 'fund_value']].copy()

    # rename date columns
    adds = adds.rename(columns={"fund_date": "date", "fund_value": "adds_value"})
    subs = subs.rename(columns={"spend_date": "date", "fund_value": "subs_value"})

    # create aggregate version of both arrays based on the daily sums.
    adds_agg = adds.set_index('date').groupby(pd.Grouper(freq='D')).agg({'adds_value': 'sum'})
    subs_agg = subs.set_index('date').groupby(pd.Grouper(freq='D')).agg({'subs_value': 'sum'})

    # merge adds and subs into a single array
    realcap = adds_agg.merge(subs_agg,left_on='date',right_on='date',how='left')

    # purge
    del adds_agg, subs_agg, adds, subs, utxo_set

    # calculate net flows
    realcap['net'] = realcap.adds_value - realcap.subs_value

    # calculate realised cap
    realcap['realcap'] = realcap.net.cumsum()

    # drop temp calculation columns
    realcap = realcap.drop(columns=['adds_value', 'subs_value', 'net'])

    # fill NA values with 0
    realcap = realcap.fillna(0)

    # return RV as the output of the function
    return realcap


def sopr(utxo_set,price):
    # This function calculates sopr based on 2 data frames that need the following structure
    # utxo_set: fund_date, spend_date, value
    # price: date, price
    # the function output will be as date, realcap

    # split out utxo_set into additions and subtractions arrays
    funds = utxo_set[['fund_date','value']].copy()
    spends = utxo_set[['spend_date', 'value']].copy()

    # rename date columns
    funds = funds.rename(columns={"fund_date": "date", "value": "value"})
    spends = spends.rename(columns={"spend_date": "date", "value": "value"})

    # merge price into utxo subsets
    funds = funds.merge(price,left_on='date',right_on='date',how='left')
    spends = spends.merge(price, left_on='date', right_on='date', how='left')

    # calculate utxo value in fiat/btc
    funds['fund_value']=funds.value*funds.price
    spends['spend_value']=spends.value*spends.price

    # drop price and value columns
    funds = funds.drop(columns=['price', 'value'])
    spends = spends.drop(columns=['price', 'value'])

    # clear NA values with 0
    funds = funds.fillna(0)
    spends = spends.fillna(0)

    # create aggregate version of both arrays based on the daily sums.
    funds_agg = funds.set_index('date').groupby(pd.Grouper(freq='D')).agg({'fund_value': 'sum'})
    spends_agg = spends.set_index('date').groupby(pd.Grouper(freq='D')).agg({'spend_value': 'sum'})

    # merge adds and subs into a single array
    sopr = funds_agg.merge(spends_agg,left_on='date',right_on='date',how='left')

    # purge
    del funds, spends, funds_agg, spends_agg

    # calculate metric - filter for when fund_value is 0, if that's the case sopr = spend_value
    sopr['sopr'] = sopr.fund_value.apply(lambda x: (sopr.spend_value/sopr.fund_value) if x > 0 else sopr.spend_value)

    # drop temp calculation columns
    sopr = sopr.drop(columns=['fund_value', 'spend_value'])

    # return RV as the output of the function
    return sopr


def hodlwaves_v1(utxo_set,output=None):
    # check if output is none first
    if output is None:
        df = utxo_set[['fund_date']].copy()
        output = df.set_index('fund_date').groupby(pd.Grouper(freq='D'))
        output['1d'] = 0
        output['2w'] = 0
        output['1m'] = 0
        output['3m'] = 0
        output['6m'] = 0
        output['1y'] = 0
        output['2y'] = 0
        output['4y'] = 0
        del df

    # create time deltas for categories
    td_1d = datetime.timedelta(days=1)
    td_2w = datetime.timedelta(days=14)
    td_1m = datetime.timedelta(days=30)
    td_3m = datetime.timedelta(days=90)
    td_6m = datetime.timedelta(days=182)
    td_1y = datetime.timedelta(days=365)
    td_2y = datetime.timedelta(days=730)
    td_4y = datetime.timedelta(days=1460)

    # chce

    return output



