import pandas as pd
import pandas_datareader as pdr
import pymysql
from sqlalchemy import create_engine
from datetime import timedelta
from datetime import datetime as dt
import dataReader as dr

def initialInpute(end):
    '''
    Initially collect, clean data and input to MySQL database.

    Parameter
    end (str):
        Initialize the database to the end time, "YYYY-MM-DD".
    '''
    
    start = "2012-01-01"
    engine = create_engine('mysql+pymysql://root:jeff@920@127.0.0.1:3306/SPDB')

    # 1. records, holders, positions: Doweload csv file and clean the records
    filename = dr.recordsDownloader(start, end)
    records, holders = dr.initialClean(filename)
    positions = dr.positionsMakeup(records, end, initial = True)

    records.to_sql(name = "records", con = engine, if_exists = "append", index = False)
    positions.to_sql(name = "positions", con = engine, if_exists = "append", index = False)
    holders.to_sql(name = "holders", con = engine, if_exists = "append", index = False)

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
    issuers.to_sql(name = "issuers", con = engine, if_exists = "append", index = False)

    # DAX
    markets = pdr.DataReader("^GDAXI", 'yahoo', start, end)
    markets.rename(columns = {"Adj Close": "Adj_close"}, inplace = True)
    markets["Ticker"] = "DAX"
    # stocks
    stocks = dr.stocksMakeup(prices, markets, initial = True)

    stocks.to_sql(name = "stocks", con = engine, if_exists = "append", index = False)
    markets.to_sql(name = "markets", con = engine, if_exists = "append", index = True)

    print("Initialized to {}: Done.".format(end))

def updatedInpute(end):
    '''
    Updated collect, clean data and input to MySQL database.

    Parameter
    end (str):
        Initialize the database to the end time, "YYYY-MM-DD".
    '''
    
    engine = create_engine('mysql+pymysql://root:jeff@920@127.0.0.1:3306/SPDB')
    # Get start and tail
    tail = pd.read_sql_query("SELECT MAX(Date) AS Date FROM positions", engine)["Date"].values[0]
    start = dt.strftime(tail + timedelta(days = 1), "%Y-%m-%d")

    # Output tail data and references
    query = "SELECT * FROM positions WHERE Date = (SELECT MAX(Date) FROM positions)"
    positions_tail = pd.read_sql_query(query, engine)

    query = "SELECT * FROM stocks WHERE Date = (SELECT MAX(Date) FROM stocks)"
    stocks_tail = pd.read_sql_query(query, engine)
    stocks_tail["Date"] = pd.to_datetime(stocks_tail["Date"], format = "%Y-%m-%d")
    stocks_tail.set_index("Date", inplace = True)

    query = "SELECT org_name, clr_name, cut_name FROM holders"
    holders_ref = pd.read_sql_query(query, engine)

    query = "SELECT ISIN, Ticker, Name FROM issuers"
    issuers_ref = pd.read_sql_query(query, engine)

    # 1. Update records, holders, positions: Doweload csv file and clean the records
    filename = dr.recordsDownloader(start, end)
    records, holders = dr.updatedClean(filename, holders_ref)
    positions = dr.positionsMakeup(records.append(positions_tail, ignore_index = True), end, initial = False)

    records.to_sql(name = "records", con = engine, if_exists = "append", index = False)
    positions.to_sql(name = "positions", con = engine, if_exists = "append", index = False)
    holders.to_sql(name = "holders", con = engine, if_exists = "append", index = False)

    # 2. Update issuers: Get ISINs from records and map to tickers and names
    ISINs = records["ISIN"].sort_values().unique()
    # Get new ISINs
    ISINs = [ISIN for ISIN in ISINs if ISIN not in issuers_ref["ISIN"].values]

    # 3. Update stocks
    # prices
    tickers = [ticker for ticker in issuers_ref["Ticker"].values if ticker is not None]
    prices, errors = dr.pricesDownloader(tickers, start, end)

    # stocks
    tail = pd.read_sql_query("SELECT MAX(Date) AS Date FROM stocks", engine)["Date"].values[0]
    markets = pdr.DataReader("^GDAXI", 'yahoo', tail, end)
    stocks = dr.stocksMakeup(prices.append(stocks_tail, ignore_index = False), markets, initial = False)

    stocks.to_sql(name = "stocks", con = engine, if_exists = "append", index = False)

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
        issuers.to_sql(name = "issuers", con = engine, if_exists = "append", index = False)

        # stocks
        markets = pdr.DataReader("^GDAXI", 'yahoo', "2012-01-01", end)
        stocks = dr.stocksMakeup(prices, markets, initial = True)
        stocks.to_sql(name = "stocks", con = engine, if_exists = "append", index = False)
    
    # 5. Update markets
    markets = pdr.DataReader("^GDAXI", 'yahoo', start, end)
    markets.rename(columns = {"Adj Close": "Adj_close"}, inplace = True)
    markets["Ticker"] = "DAX"

    markets.to_sql(name = "markets", con = engine, if_exists = "append", index = True)

    print("Updated to {}: Done.".format(end))

if __name__ == "__main__":
    pass