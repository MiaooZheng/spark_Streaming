## BEFORE CHECKING THE CODES BELOW, 
## PLEASE FOLLOW THE INSTRCUTIONS HERE TO SETUP AND GET YOUR HOSTNAME:

## HERE WE CHOOSE Apache Cassandra AS OUR DB, since it has streaming capabilities

# https://medium.com/@manishyadavv/how-to-install-cassandra-on-mac-os-d9338fcfcba4

# related cassandra doc: https://docs.datastax.com/en/cql-oss/3.3/cql/cql_reference/cqlshCommandsTOC.html

# pip install pyspark cassandra-driver -> here i set virtualenv, so use conda instead of pip 
import yfinance as yf
import pandas as pd

# extract history data before this minute
def extract_historical_data(ticker):
  data = yf.download(tickers=ticker, interval='1m')
  data['Company'] = ticker
  data = data.reset_index() # so we can add daetime in 
  return data # should we convert it to json format?


# print(extract_historical_data(ticker= 'AAPL'))


from cassandra.cluster import Cluster

# when i run cqls command -> got: Connected to Test Cluster at 127.0.0.1:9042 -> 9042 is port
def connect_to_cassandra(ticker):
    # Connect to the Cassandra cluster
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect() 

    # then we need to create a keyspace before create a table -> so keyspace is like a dataset in relational db
    keyspace_name = 'dev'
    replication_config = {
    'class': 'SimpleStrategy',
    'replication_factor': 1   # a bit confused, but let's test
    }
    
    # read data
    data = extract_historical_data(ticker)
    print(data.head(1))

    session.execute(f"""
    CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
    WITH replication = {replication_config}
    """)

    # Switch to the new keyspace
    session = cluster.connect('dev')

    # then we can create table
    # primary key:  # the first one is partition_key and after that is cluster key
    session.execute(f"""
    CREATE TABLE IF NOT EXISTS {ticker}_stock_price (
        Datetime timestamp,
        Company text,
        Open float,
        High float,
        Low float,
        Close float,
        Adj_close float,
        Volume int,
        PRIMARY KEY (Company, Datetime)
    );

    """)

    # TRUNCATE TABLE BEFORE INSERT NEW DATA
    session.execute(f"""
                    TRUNCATE TABLE  {keyspace_name}.{ticker}_stock_price
                    """)

    print("truncate table successfully!")
    
    
    '''
    ### BELOW IS MY TEST

    session.execute(f"""
                    INSERT INTO {keyspace_name}.{ticker}_stock_price (datetime, Company ,Open,High, Low, Close, Adj_Close, Volume) 
    VALUES ('2023-11-20 14:11:00-05:00','AAPL', 191.6450042725, 191.6450042725, 191.5749969482, 191.5800018311, 191.5800018311, 0) IF NOT EXISTS;
                    """)

    result = session.execute(f"""
                    SELECT * FROM {keyspace_name}.{ticker}_stock_price
                    """)
    
    df = pd.DataFrame(result.all())

    # Display the DataFrame
    print(df)
   
    '''
    # In Cassandra, seems like we can only insert row by row. cannot insert a whole df 
    
    query = f"""
        INSERT INTO {keyspace_name}.{ticker}_stock_price (Datetime, Company, Open, High, Low, Close, Adj_Close, Volume)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
    prepared = session.prepare(query)
    
    # datetime Open	High	Low	Close	Adj Close	Volume
    for index, row in data.iterrows():
        session.execute(prepared,
        (row['Datetime'],
         row['Company'],
         row['Open'],
         row['High'],
         row['Low'],
         row['Close'],
         row['Adj Close'],
         row['Volume'],
        
        ))

    # Perform database operations
    result = session.execute(f"""SELECT * FROM {keyspace_name}.{ticker}_stock_price
                             WHERE Company = '{ticker}'
                             ORDER BY Datetime""")

    # delete df code below
    df = pd.DataFrame(result.all())

    # Display the DataFrame
    print(df) #### NOTE: HERE THE DATETIME IS EST - NOT SURE WHETHER THERE'S ANY INFLUENCE FOR OUR CASE
    
    # Close the connection
    session.shutdown()
    cluster.shutdown()
    
# Call the function
connect_to_cassandra(ticker = "AAPL")


# command: cqlsh to check on terminal



# check this file later: https://www.analyticsvidhya.com/blog/2022/06/walmart-stock-price-analysis-using-pyspark/