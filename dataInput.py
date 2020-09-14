import pandas as pd
import pandas_datareader as pdr
import sqlite3
from datetime import timedelta
from datetime import datetime as dt
import dataReader as dr

def setupDB():
    # Setup local SQLite database (if not exist) and open the connection
    con = sqlite3.connect('ssDB.db')
    cur = con.cursor()

    # Creat tables
    cur.execute('''CREATE TABLE records(
        ID_record INTEGER PRIMARY KEY AUTOINCREMENT,
        Holder TEXT NOT NULL,
        Issuer TEXT NOT NULL,
        ISIN TEXT NOT NULL,
        Position REAL NOT NULL,
        Date DATETIME NOT NULL,
        Update_time DATETIME NOT NULL DEFAULT (datetime(CURRENT_TIMESTAMP, 'localtime')));''')
    
    cur.execute('''CREATE TABLE positions(
        Date DATETIME NOT NULL,
        Holder TEXT NOT NULL,
        ISIN TEXT NOT NULL,
        Position REAL NOT NULL,
        Covering INTEGER NOT NULL CHECK (Covering IN (0,1)),
        Increase INTEGER NOT NULL CHECK (Covering IN (0,1)),
        PRIMARY KEY (Holder, ISIN, Date));''')
    
    cur.execute('''CREATE TABLE issuers(
        ID_issuer INTEGER PRIMARY KEY AUTOINCREMENT,
        ISIN TEXT NOT NULL,
        Ticker TEXT,
        Name TEXT,
        Update_time DATETIME NOT NULL DEFAULT (datetime(CURRENT_TIMESTAMP, 'localtime')));''')

    cur.execute('''CREATE TABLE holders(
        ID_holder INTEGER PRIMARY KEY AUTOINCREMENT,
        org_name TEXT NOT NULL,
        clr_name TEXT NOT NULL,
        cut_name TEXT NOT NULL,
        Update_time DATETIME NOT NULL DEFAULT (datetime(CURRENT_TIMESTAMP, 'localtime')));''')
    
    cur.execute('''CREATE TABLE stocks(
        Date DATETIME NOT NULL,
        High REAL,
        Low REAL,
        Open REAL,
        Close REAL,
        Volume REAL,
        Adj_close REAL NOT NULL,
        Ticker TEXT NOT NULL,
        PRIMARY KEY (Date, Ticker));''')
    
    cur.execute('''CREATE TABLE markets(
        Date DATETIME NOT NULL,
        High REAL,
        Low REAL,
        Open REAL,
        Close REAL,
        Volume REAL,
        Adj_close REAL NOT NULL,
        Ticker TEXT NOT NULL,
        PRIMARY KEY(Date, Ticker));''')
    
    #Close the connection
    con.close()
    print("Database setup: Done.")

def initialInpute(end):
    '''
    Initially collect, clean data and input to MySQL database.

    Parameter
    end (str):
        Initialize the database to the end time, "YYYY-MM-DD".
    '''
    
    start = "2012-01-01"
    con = sqlite3.connect('ssDB.db')

    # 1. records, holders, positions: Doweload csv file and clean the records
    filename = dr.recordsDownloader(start, end)
    records, holders = dr.initialClean(filename)
    positions = dr.positionsMakeup(records, end, initial = True)

    records.to_sql(name = "records", con = con, if_exists = "append", index = False)
    positions.to_sql(name = "positions", con = con, if_exists = "append", index = False)
    holders.to_sql(name = "holders", con = con, if_exists = "append", index = False)

    # 2. issuers: Get ISINs from records and map to tickers and names
    ISINs = records["ISIN"].sort_values().unique()
    tickers, names = dr.mapISINtoTicker(ISINs)
    issuers = pd.DataFrame({"ISIN": ISINs, "Ticker": tickers, "Name": names})

    # 3. stocks and markets: Doweload stock and DAX prices
    # prices
    tickers = [ticker for ticker in issuers["Ticker"].values if ticker is not None]
    prices, errors = dr.pricesDownloader(tickers, start, end)
    # Update issuers
    for error in errors:
        issuers.loc[issuers["Ticker"] == error, "Ticker"] = None 
    issuers.to_sql(name = "issuers", con = con, if_exists = "append", index = False)

    # DAX
    markets = pdr.DataReader("^GDAXI", 'yahoo', start, end)
    markets.rename(columns = {"Adj Close": "Adj_close"}, inplace = True)
    markets["Ticker"] = "DAX"
    # stocks
    stocks = dr.stocksMakeup(prices, markets, initial = True)

    stocks.to_sql(name = "stocks", con = con, if_exists = "append", index = False)
    markets.to_sql(name = "markets", con = con, if_exists = "append", index = True)

    # Manually update sta_view
    cur = con.cursor()
    cur.execute('DROP TABLE IF EXISTS sta_records;')
    cur.execute('''CREATE TABLE sta_records AS 
        SELECT Date,
            COUNT(DISTINCT Holder) AS num_Holder,
            COUNT(DISTINCT ISIN) AS num_ISIN,
            SUM(Covering) AS num_Coverring,
            SUM(Increase) AS num_Increase
        FROM positions WHERE Position > 0 GROUP BY Date;''')

    #Close the connection
    con.close()
    print("Initialized to {}: Done.".format(end))

def updatedInpute(end):
    '''
    Updated collect, clean data and input to MySQL database.

    Parameter
    end (str):
        Initialize the database to the end time, "YYYY-MM-DD".
    '''
    
    con = sqlite3.connect('ssDB.db')
    # Get start and tail
    tail = pd.to_datetime(pd.read_sql_query("SELECT MAX(Date) AS Date FROM positions", con = con)["Date"][0],  format = "%Y-%m-%d")
    start = dt.strftime(tail + timedelta(days = 1), "%Y-%m-%d")

    # Output tail data and references
    query = "SELECT * FROM positions WHERE Date = (SELECT MAX(Date) FROM positions)"
    positions_tail = pd.read_sql_query(query, con = con)
    positions_tail["Date"] = pd.to_datetime(positions_tail["Date"],  format = "%Y-%m-%d")

    query = "SELECT * FROM stocks WHERE Date = (SELECT MAX(Date) FROM stocks)"
    stocks_tail = pd.read_sql_query(query, con = con)
    stocks_tail["Date"] = pd.to_datetime(stocks_tail["Date"], format = "%Y-%m-%d")
    stocks_tail.set_index("Date", inplace = True)

    query = "SELECT org_name, clr_name, cut_name FROM holders"
    holders_ref = pd.read_sql_query(query, con = con)

    query = "SELECT ISIN, Ticker, Name FROM issuers"
    issuers_ref = pd.read_sql_query(query, con = con)

    # 1. Update records, holders, positions: Doweload csv file and clean the records
    filename = dr.recordsDownloader(start, end)
    records, holders = dr.updatedClean(filename, holders_ref)
    positions = dr.positionsMakeup(records.append(positions_tail, ignore_index = True), end, initial = False)

    records.to_sql(name = "records", con = con, if_exists = "append", index = False)
    positions.to_sql(name = "positions", con = con, if_exists = "append", index = False)
    holders.to_sql(name = "holders", con = con, if_exists = "append", index = False)

    # 2. Update issuers: Get ISINs from records and map to tickers and names
    ISINs = records["ISIN"].sort_values().unique()
    # Get new ISINs
    ISINs = [ISIN for ISIN in ISINs if ISIN not in issuers_ref["ISIN"].values]

    # 3. Update stocks
    # prices
    tickers = [ticker for ticker in issuers_ref["Ticker"].values if ticker is not None]
    prices, errors = dr.pricesDownloader(tickers, start, end)

    # stocks
    tail = pd.read_sql_query("SELECT MAX(Date) AS Date FROM stocks", con = con)["Date"][0][0:10]
    markets = pdr.DataReader("^GDAXI", 'yahoo', tail, end)
    stocks = dr.stocksMakeup(prices.append(stocks_tail, ignore_index = False), markets, initial = False)

    stocks.to_sql(name = "stocks", con = con, if_exists = "append", index = False)

    # 4. stocks for new ISINs: Doweload stock and DAX prices
    if len(ISINs) > 0:
        # new issuers
        tickers, names = dr.mapISINtoTicker(ISINs)
        issuers = pd.DataFrame({"ISIN": ISINs, "Ticker": tickers, "Name": names})

        # prices
        tickers = [ticker for ticker in issuers["Ticker"].values if ticker is not None]
        prices, errors = dr.pricesDownloader(tickers, "2012-01-01", end)
        # Update issuers
        for error in errors:
            issuers.loc[issuers["Ticker"] == error, "Ticker"] = None
        issuers.to_sql(name = "issuers", con = con, if_exists = "append", index = False)

        # stocks
        markets = pdr.DataReader("^GDAXI", 'yahoo', "2012-01-01", end)
        stocks_new = dr.stocksMakeup(prices, markets, initial = True)
        stocks_new.to_sql(name = "stocks", con = con, if_exists = "append", index = False)
    
    # 5. Update markets
    markets = pdr.DataReader("^GDAXI", 'yahoo', start, end)
    markets.rename(columns = {"Adj Close": "Adj_close"}, inplace = True)
    markets["Ticker"] = "DAX"

    markets.to_sql(name = "markets", con = con, if_exists = "append", index = True)
    
    # Manually update sta_view
    cur = con.cursor()
    cur.execute('DROP TABLE IF EXISTS sta_records;')
    cur.execute('''CREATE TABLE sta_records AS 
        SELECT Date,
            COUNT(DISTINCT Holder) AS num_Holder,
            COUNT(DISTINCT ISIN) AS num_ISIN,
            SUM(Covering) AS num_Coverring,
            SUM(Increase) AS num_Increase
        FROM positions WHERE Position > 0 GROUP BY Date;''')

    #Close the connection
    con.close()
    print("Updated to {}: Done.".format(end))

if __name__ == "__main__":
    pass