# dcrdata query functions
import pandas as pd
import sqlalchemy as sql

fmtt = '%Y-%m-%dT%H:%M:%S'

def pgquery(query):
    # this function queries data from a dcrdata instance
    # the connection string has been defined as the server I use
    # input argument is a query as a text string, other functions
    # are defined below for specific queries
    # connection string information is stored in an external file called pgConnectString
    with open('./pgConnectString') as f:
        pgConnectString = f.readlines()
    # Create an engine instance
    engine = sql.create_engine(pgConnectString[0])
    # Connect to PostgreSQL server
    pg_connection = engine.connect()
    # Read data from PostgreSQL database query and load into a DataFrame instance
    output = pd.read_sql(query, pg_connection)
    # close pg connection
    pg_connection.close()

    return output


def pgquery_utxo_tickets():
    # this function uses the dcrdata_query func to obtain all of the ticket data for realised value
    #   - Fund Date
    #   - Spend Date
    #   - Value

    query = """
    Select 
    	date(bdb1.time) as fund_date,
    	date(bdb2.time) as spend_date,
    	tdb.price as value

    from public.tickets as tdb
    left join public.blocks as bdb1 
    	on tdb.block_height = bdb1.height 
    left join public.blocks as bdb2 
    	on tdb.spend_height = bdb2.height

    order by tdb.id asc
    """
    # execute query on dcrdata pgdb
    output = pgquery(query)
    # fix date formats
    output['fund_date'] = pd.to_datetime(output.fund_date, utc=True, format=fmtt, errors='ignore')
    output['spend_date'] = pd.to_datetime(output.spend_date, utc=True, format=fmtt, errors='ignore')
    return output

def pgquery_poolval():
    # this function uses the dcrdata_query func to obtain ticket pool value
    #   - date
    #   - poolval

    query = """
    Select 
    	date(bdb.time) as date,
    	avg(sdb.pool_val/100000000) as poolval

    from public.stats as sdb
    left join public.blocks as bdb
    	on sdb.height = bdb.height 
	group by date
    order by date asc
    """
    # execute query on dcrdata pgdb
    output = pgquery(query)
    # fix date formats
    output['date'] = pd.to_datetime(output.date, utc=True, format=fmtt, errors='ignore')
    return output

def pgquery_powReward():
    # this function uses the dcrdata_query func to obtain miner block rewards
    #- date
    #- value

    query = """
    Select
    date(tx.time) as date,
    trunc(sum(cast(vo.value as numeric)/100000000),2) as value
    from transactions as tx left join vouts as vo
    on tx.tx_hash = vo.tx_hash 
    where tx.tx_type = 0 and block_index = 0 and tx.is_valid and tx.is_mainchain and tx.block_height > 1 and vo.tx_index > 0
    group by date
    """
    # execute query on dcrdata pgdb
    output = pgquery(query)
    # fix date formats
    output['date'] = pd.to_datetime(output.date, utc=True, format=fmtt, errors='ignore')
    return output

def pgquery_posReward():
    # this function uses the dcrdata_query func to obtain staker block rewards
    #- date
    #- value

    query = """
    select 
        date(votes.block_time) as date,
        trunc(sum(cast(votes.vote_reward as numeric)),2) as value
    from votes
    where is_mainchain
    group by date
    order by date asc
    """
    # execute query on dcrdata pgdb
    output = pgquery(query)
    # fix date formats
    output['date'] = pd.to_datetime(output.date, utc=True, format=fmtt, errors='ignore')
    return output

def pgquery_ticketCounts():
    # this function uses the dcrdata_query func to obtain daily ticket counts
    #- date
    #- tickets

    query = """
    Select 
        date(bdb.time) as date,
        count(tdb.id) as tickets
    from public.blocks as bdb
    left join public.tickets as tdb
        on tdb.block_height = bdb.height 
    group by date
    order by date asc
    """
    # execute query on dcrdata pgdb
    output = pgquery(query)
    # fix date formats
    output['date'] = pd.to_datetime(output.date, utc=True, format=fmtt, errors='ignore')
    return output

def pgquery_voteCounts():
    # this function uses the dcrdata_query func to obtain daily vote counts
    #- date
    #- votes

    query = """
    Select 
        date(votes.block_time) as date,
        count(votes.id) as votes
    from public.votes as votes
    group by date
    order by date asc
    """
    # execute query on dcrdata pgdb
    output = pgquery(query)
    # fix date formats
    output['date'] = pd.to_datetime(output.date, utc=True, format=fmtt, errors='ignore')
    return output

def pgquery_Supply():
    # this function uses the dcrdata_query func to obtain supply
    #- date
    #- new supply
    #- total supply

    query = """
    Select 
        date(vi.block_time) as date,
        sum(cast(vi.value_in as numeric)) as newsply
    from public.vins as vi
    where prev_tx_hash = '0000000000000000000000000000000000000000000000000000000000000000'
    group by date
    order by date asc
    """
    # execute query on dcrdata pgdb
    output = pgquery(query)
    # fix date formats
    output['date'] = pd.to_datetime(output.date, utc=True, format=fmtt, errors='ignore')
    output['totsply'] = output.newsply.cumsum()
    return output


def pgquery_decTresIssuance():
    # this function uses the dcrdata_query func to obtain the issuance to the decentralized treasury
    #- date
    #- new supply

    query = """
    Select
    date(tx.time) as date,
    trunc(sum(cast(tx.sent as numeric)/100000000),2) as decTres
    from transactions as tx
    where tx.tx_type = 6 and tx.is_valid and tx.is_mainchain and tx.block_height > 1
    group by date
    """
    # execute query on dcrdata pgdb
    output = pgquery(query)
    # fix date formats
    output['date'] = pd.to_datetime(output.date, utc=True, format=fmtt, errors='ignore')
    return output

def pgquery_legTresIssuance():
    # this function uses the dcrdata_query func to obtain the issuance to the legacy treasury
    #- date
    #- new supply

    query = """
    Select
    date(tx.time) as date,
    trunc(sum(cast(vo.value as numeric)/100000000),2) as legTres
    from transactions as tx left join vouts as vo
    on tx.tx_hash = vo.tx_hash 
    where tx.tx_type = 0 and block_index = 0 and tx.is_valid and tx.is_mainchain and tx.block_height > 1 and vo.tx_index = 0 and vo.script_addresses='{Dcur2mcGjmENx4DhNqDctW5wJCVyT3Qeqkx}'
    group by date
    """
    # execute query on dcrdata pgdb
    output = pgquery(query)
    # fix date formats
    output['date'] = pd.to_datetime(output.date, utc=True, format=fmtt, errors='ignore')
    return output

def pgquery_blockVers():
    # this function uses the dcrdata_query func to obtain the daily amount of blocks for each block version

    query = """
    SELECT date(time) as date, version, count(version) FROM public.blocks
    group by date, version
    ORDER BY date ASC
    """
    # execute query on dcrdata pgdb
    output = pgquery(query)
    # fix date formats
    output['date'] = pd.to_datetime(output.date, utc=True, format=fmtt, errors='ignore')
    return output

def pgquery_voteVers():
    # this function uses the dcrdata_query func to obtain the daily amount of blocks for each block version

    query = """
    SELECT date(block_time) as date, version, count(version) FROM public.votes
    group by date, version
    ORDER BY date ASC
    """
    # execute query on dcrdata pgdb
    output = pgquery(query)
    # fix date formats
    output['date'] = pd.to_datetime(output.date, utc=True, format=fmtt, errors='ignore')
    return output

def pgquery_utxo_set(filter=None):
    # this function uses the dcrdata_query func to obtain all of the utxo data for realised value calcs
    # input paremter is the filter for the utxo type, if none it'll get everything
    #   - Fund Date
    #   - Spend Date
    #   - Value
    # if filter is populated it will check for that script_type
    if filter is not None:
        qf = "and vouts.script_type = '" + filter + "'"
    else:
        qf = ''

    query = """
    Select  date(fund_tx.block_time)                      as fund_date, 
            ((cast(vouts.value as real)/100000000)) as value, 
            date(spend_tx.block_time)                     as spend_date

    From public.vouts as vouts
    left join public.transactions as fund_tx
    	on fund_tx.tx_hash = vouts.tx_hash
    left join public.transactions as spend_tx
    	on spend_tx.id = vouts.spend_tx_row_id

    where value > 0 """ + qf  + """
    order by vouts.id asc
    """
    # execute query on dcrdata pgdb
    output = pgquery(query)
    # fix date formats
    output['fund_date'] = pd.to_datetime(output.fund_date, utc=True, format=fmtt, errors='ignore')
    output['spend_date'] = pd.to_datetime(output.spend_date, utc=True, format=fmtt, errors='ignore')
    return output

def pgquery_powRewardAddr():
    query = """
    SELECT date(block_time) as date, block_height, substring(cast(vo.script_addresses as varchar),2,35) as address, value/1e8 as reward FROM vouts vo 
    INNER JOIN transactions t ON vo.tx_hash=t.tx_hash 
    WHERE t.is_valid=true AND t.is_mainchain=true AND block_index = 0 and tx_type = 0 and vo.value >0 and script_type = 'pubkeyhash' and block_height > 1
    order by block_height asc
    """
    # execute query on dcrdata pgdb
    output = pgquery(query)
    # fix date formats
    output['date'] = pd.to_datetime(output.date, utc=True, format=fmtt, errors='ignore')
    return output
