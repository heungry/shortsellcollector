import numpy as np
import pandas as pd
import pandas_datareader as pdr
from unidecode import unidecode
from datetime import datetime as dt
import requests
from bs4 import BeautifulSoup as BS, Comment

def recordsDownloader(start, end):
    '''
    Doweload the short position data as a CSV file.
    
    Parameters
    start (str):
        The start date, "YYYY-MM-DD".
    end (str):
        The end date, "YYYY-MM-DD".

    Returns
    filename (str):
        Download a csv file named as  "shortposition_start_end.csv" and return filename.

    '''

    t_start = dt.strptime(start, "%Y-%m-%d")
    t_end = dt.strptime(end, "%Y-%m-%d")
    
    if t_start > t_end:
        t_start, t_end = t_end, t_start

    base_url = 'https://www.bundesanzeiger.de/ebanzwww/wexsservlet'
    payload = {'nlp_search_param.date_start:0': t_start.day,
               'nlp_search_param.date_start:1': t_start.month,
               'nlp_search_param.date_start:2': t_start.year,
               'nlp_search_param.date_end:0': t_end.day,
               'nlp_search_param.date_end:1': t_end.month,
               'nlp_search_param.date_end:2': t_end.year,
               'nlp_search_param.search_history': 'true',
               '(page.navid=nlpresultlisttonlpresultlist_search)': 'Find short sales'
               }
    filename = "shortposition_" + t_start.strftime("%Y%m%d") + "_" + t_end.strftime("%Y%m%d") + ".csv"

    with requests.session() as s:
        # Ask for a sessionID from a GET request
        r = s.get(base_url, params = {'page.navid': 'nlpstarttonlpstart_new'})
        soup = BS(r.text, features = "lxml")
    
        # Extract the sessionID from the first comment
        scom = soup.find(text = lambda text: isinstance(text, Comment))
        start = scom.find('=') + 1
        end = scom.find('&')
        sid = scom[start : end]
    
        # Add sessionID to payload
        payload['session.sessionid'] = sid
    
        # Scrape the CSV's url
        r = s.post(base_url, data = payload)
        soup = BS(r.text, features = "lxml")
        csv_url = 'https://www.bundesanzeiger.de' + soup.find('a', {"title": "Als CSV herunterladen"}).get('href')
    
        # Doweload the CSV
        download = s.get(csv_url)
    
    with open(filename, "wb") as file:
        file.write(download.content)
    
    # Update logging
    logging = "=" * 50 + "\n" + str(dt.now()) + ": Download CSV file '{}'\n".format(filename) + "=" * 50 + "\n\n"
    with open("logfile", "a") as f:
        f.write(logging)

    return filename

def initialClean(csvName):
    '''
    Read and clean initial data from csv file.
    
    Parameters
    csvName (str): 
        The name of the csv file.

    Returns:
    cleaned data (pandas.DataFrame)
    reference (pandas.DataFrame)

    '''

    # Read data from csv
    df = pd.read_csv(csvName)
    logging = "=" * 50 + "\n" + str(dt.now()) + ": Start Initial Cleaning\n"
    # Rename the columns
    col_names = {"Positionsinhaber": "Holder",
                "Emittent": "Issuer",
                "Datum": "Date"}
    df.rename(columns = col_names, inplace = True)

    # Regularize number and time
    df["Position"] = df["Position"].str.replace(",", ".")
    df["Position"] = pd.to_numeric(df["Position"])
    df["Date"] = pd.to_datetime(df["Date"], format = "%Y-%m-%d")
    # Unidecode the "Holder"
    df["Holder"] = df["Holder"].map(unidecode)

    # 1. Typo of "Position": missing percentage mark %
    if sum(df["Position"] > 50) > 0:
        logging += '\n## Clean typo of "Position": missing percentage mark "%":\n'

    for i in range(sum(df["Position"] > 50)):
        logging += "\nFrom: " + str(df.loc[df["Position"] > 50, "Position"].values[i]) +\
            " | " + str(df.loc[df["Position"] > 50, "ISIN"].values[i]) +\
            " | " + str(df.loc[df["Position"] > 50, "Date"].values[i]) +\
            "\nTo:   " + str(df.loc[df["Position"] > 50, "Position"].values[i] / 100) + "\n"

    df.loc[df["Position"] > 50, "Position"] /= 100

    # 2. Typo of "Holder": upper and lower case, comma, dot, space
    org_name = df["Holder"].sort_values().unique()
    clr_name = [name.lower().replace(",", "").replace(".", "").replace(" ", "") for name in org_name]
    df_name = pd.DataFrame({"org_name": org_name,
                            "clr_name": clr_name,
                            "count": [sum(df["Holder"] == name) for name in org_name],
                            "cut_name": [name[0:5] for name in clr_name]
                            }, dtype = 'str')
    df_name["count"] = pd.to_numeric(df_name["count"])

    mask = [sum(df_name["clr_name"] == name) > 1 for name in df_name["clr_name"]]
    tmp = df_name.loc[mask, :].copy()

    for name in tmp["clr_name"].unique():
        maxc = max(tmp.loc[tmp["clr_name"] == name, "count"])
        new_name = tmp.loc[(tmp["clr_name"] == name) & (tmp["count"] == maxc), "org_name"].values[0]
        tmp.loc[tmp["clr_name"] == name, "new_name"] = new_name
    if tmp.size > 0:
        logging += '\n## Clean typo of "Holder": upper and lower case, comma, dot, space:\n'

    for i in tmp.index:
        if not(tmp.loc[i, "org_name"] is tmp.loc[i, "new_name"]):
            logging += "\nFrom: {}\nto:   {}\n".format(tmp.loc[i, "org_name"], tmp.loc[i, "new_name"])
        df.loc[df["Holder"] == tmp.loc[i, "org_name"], "Holder"] = tmp.loc[i, "new_name"]
    
    # 3. Typo of "Holder": similar name (first 5 letter) with only 1 disclosure
    mask = [name in df_name.loc[df_name["count"] == 1]["cut_name"].unique() and sum(df_name["cut_name"] == name) > 1 for name in df_name["cut_name"]]
    tmp = df_name.loc[mask, :].copy()

    for name in tmp["cut_name"].unique():
        maxc = max(tmp.loc[tmp["cut_name"] == name, "count"])
        new_name = tmp.loc[(tmp["cut_name"] == name) & (tmp["count"] == maxc), "org_name"].values[0]
        tmp.loc[tmp["cut_name"] == name, "new_name"] = new_name

    if tmp.size > 0:
        logging += '\n## Clean typo of "Holder": similar name:\n'

    for i in tmp.index:
        if not(tmp.loc[i, "org_name"] is tmp.loc[i, "new_name"]):
            # print("From: {}\nto:   {}\n".format(tmp.loc[i, "org_name"], tmp.loc[i, "new_name"]))
            logging += "\nFrom: {}\nto:   {}\n".format(tmp.loc[i, "org_name"], tmp.loc[i, "new_name"])
        df.loc[df["Holder"] == tmp.loc[i, "org_name"], "Holder"] = tmp.loc[i, "new_name"]

    # 4. Drop duplicated rows
    df.drop_duplicates(keep = "first", inplace = True)
    df.sort_values("Date", inplace = True)
    df.reset_index(drop = True, inplace = True)
    
    # Creat df_ref as reference
    org_name = df["Holder"].sort_values().unique()
    clr_name = [name.lower().replace(",", "").replace(".", "").replace(" ", "") for name in org_name]
    df_ref = pd.DataFrame({"org_name": org_name,
                            "clr_name": clr_name,
                            "cut_name": [name[0:5] for name in clr_name]
                            }, dtype = 'str')
    
    # Update logging
    logging += "\n" + str(dt.now()) + ": End Initial Cleaning\n" + "=" * 50 + "\n\n"
    with open("logfile", "a") as f:
        f.write(logging)

    return df, df_ref

def updatedClean(csvName, df_ref):
    '''
    Read and clean update data from csv file.
    
    Parameters
    csvName (str): 
        The name of the csv file.
    de_ref (pandas.DataFrame):
        The reference names from old data.

    Returns:
    cleaned data (pandas.DataFrame)
    new reference (pandas.DataFrame)

    '''
    # Read data from csv
    df = pd.read_csv(csvName)
    logging = "=" * 50 + "\n" + str(dt.now()) + ": Start Updated Cleaning\n"

    # Rename the columns
    col_names = {"Positionsinhaber": "Holder",
                "Emittent": "Issuer",
                "Datum": "Date"}
    df.rename(columns = col_names, inplace = True)

    # Regularize number and time
    df["Position"] = df["Position"].str.replace(",", ".")
    df["Position"] = pd.to_numeric(df["Position"])
    df["Date"] = pd.to_datetime(df["Date"], format = "%Y-%m-%d")
    # Unidecode the "Holder"
    df["Holder"] = df["Holder"].map(unidecode)

    # 1. Typo of "Position": missing percentage mark %
    if sum(df["Position"] > 50) > 0:
        logging += '\n## Clean typo of "Position": missing percentage mark "%":\n'

        for i in range(sum(df["Position"] > 50)):
            logging += "\nFrom: " + str(df.loc[df["Position"] > 50, "Position"].values[i]) +\
                " | " + str(df.loc[df["Position"] > 50, "ISIN"].values[i]) +\
                " | " + str(df.loc[df["Position"] > 50, "Date"].values[i]) +\
                "\nTo:   " + str(df.loc[df["Position"] > 50, "Position"].values[i] / 100) + "\n"

        df.loc[df["Position"] > 50, "Position"] /= 100

    # 2. Check typo of "Holder"
    mask = [not name in df_ref["org_name"].values for name in df["Holder"]]
    if sum(mask) == 0:
        # No new Holder, return data and keep reference
        # Drop duplicated rows
        df.drop_duplicates(keep = "first", inplace = True)
        df.sort_values("Date", inplace = True)
        df.reset_index(drop = True, inplace = True)

        # Update logging
        logging += "\n" + str(dt.now()) + ": End Updated Cleaning\n" + "=" * 50 + "\n\n"
        with open("logfile", "a") as f:
            f.write(logging)

        return df, pd.DataFrame()

    # 3. Typo of "Holder": upper and lower case, comma, dot, space
    org_name = df.loc[mask, "Holder"].sort_values().unique()
    clr_name = [name.lower().replace(",", "").replace(".", "").replace(" ", "") for name in org_name]
    new_ref = pd.DataFrame({"org_name": org_name,
                            "clr_name": clr_name,
                            "cut_name": [name[0:5] for name in clr_name]
                            }, dtype = 'str')

    mask = [name in df_ref["clr_name"].values for name in new_ref["clr_name"]]
    if sum(mask) > 0:
        logging += '\n## Clean typo of "Holder": upper and lower case, comma, dot, space:\n'
        for i in new_ref.loc[mask].index:
            if new_ref.loc[i, "clr_name"] in df_ref["clr_name"].values:
                name = df_ref.loc[df_ref["clr_name"] == new_ref.loc[i, "clr_name"], "org_name"].values[0]
                df.loc[df["Holder"] == new_ref.loc[i, "org_name"], "Holder"] = name
                logging += "\nFrom: {}\nto:   {}\n".format(new_ref.loc[i, "org_name"], name)
    
    mask = [not name in df_ref["clr_name"].values for name in new_ref["clr_name"]]
    new_ref = new_ref.loc[mask]
    new_ref.reset_index(drop = True, inplace = True)
        
    # 4. Drop duplicated rows
    df.drop_duplicates(keep = "first", inplace = True)
    df.sort_values("Date", inplace = True)
    df.reset_index(drop = True, inplace = True)

    # Update logging
    logging += "\n" + str(dt.now()) + ": End Updated Cleaning\n" + "=" * 50 + "\n\n"
    with open("logfile", "a") as f:
        f.write(logging)

    return df, new_ref

def mapISINtoTicker(ISIN):
    '''
    Send an collection of mapping jobs to the API in order to obtain the associated ticker(s).

    Parameters
    ISIN (list): 
        A list of ISIN that will be transformed to the OpenFIGI API request structure.
        
        
    Returns
    tickers (list):
        A list of tickers corresponding to the ISIN list.
    names (list):
        A list of names corresponding to the ISIN list.
        
    '''

    # Basic parameters of the FIGI API
    # See https://www.openfigi.com/api for more information.
    openfigi_url = 'https://api.openfigi.com/v2/mapping'
    openfigi_headers = {'Content-Type': 'text/json'}
    openfigi_headers['X-OPENFIGI-APIKEY'] = '7ae877e8-5c00-4464-b70a-9af7e49c9a19'
    
    # The mapping jobs per request is limited to 100
    tickers = []
    names = []
    slc = 100
    ISINs = [ISIN[i: i + slc] for i in range(len(ISIN))[::slc]]
    for isin in ISINs:
        jobs = [{'idType': 'ID_ISIN',
                    'idValue': id,
                    'exchCode':'GY', # XETRA
                    'securityType': 'Common Stock'} for id in isin]
        
        response = requests.post(url=openfigi_url, headers=openfigi_headers, json=jobs)
        if response.status_code != 200:
            raise Exception('Bad response code {}'.format(str(response.status_code)))

        tickers += [
            None if result.get('data') is None else result.get('data')[0]['ticker']\
            for result in response.json()]
        
        names += [
            None if result.get('data') is None else result.get('data')[0]['name']\
            for result in response.json()]

    num_none = sum([ticker is None for ticker in tickers])
    # Update logging
    logging = "=" * 50 + "\n" + str(dt.now()) + ": {} ISIN(s) find no tickers or names from FIGI API.\n".format(num_none) 
    logging += "=" * 50 + "\n\n"
    with open("logfile", "a") as f:
        f.write(logging)
    
    return tickers, names

def pricesDownloader(tickers, start, end):
    '''
    Get the histroy prices from the yahoo finance API according to the ticker(s).

    Parameters
    tickers (list): 
        A list of tickers that will be transformed to the API for the prices.
    start (str):
        The start date of the records, "YYYY-MM-DD".
    end (str):
        The end date of the records, "YYYY-MM-DD".
        
    Returns
    prices (pandas.DataFrame):
        History prices of the ticker(s), indexed with datetime.
    errors (list):
        List of the unavaiable tickers.

    '''

    prices = pd.DataFrame()
    errors = []
    for ticker in tickers:
        try:
            # Ticker within XETRA is decorated with ".DE"
            p = pdr.DataReader(ticker + '.DE', 'yahoo', start, end)
            p["Ticker"] = ticker
            prices = prices.append(p, ignore_index = False)
        except:
            errors.append(ticker)
            pass
    
    prices.rename(columns = {"Adj Close": "Adj_close"}, inplace = True)

    # Update logging
    logging = "=" * 50 + "\n" + str(dt.now()) + ": Download stock prices from Yahoo.\n" + "=" * 50 + "\n\n"
    with open("logfile", "a") as f:
        f.write(logging)
    
    return prices, errors

def positionsMakeup(records, end, initial = False):
    '''
    Make up the short position records to continuing time span.

    Parameters
    records (pandas.DataFrame):
        Cleaned short position records from CSV file.
    end (str):
        Make up to the end date, "YYYY-MM-DD".
    initial (boolean):
        Only True for the first time, False for the update.
    
    Return
    positions (pandas.DataFrame)

    '''
    if records.empty:
        return pd.DataFrame()

    start = min(records["Date"])
    
    # Use the mean position of multiple records on same day and drop "Issuer" column
    records = records.groupby(["Holder", "ISIN", "Date"]).mean().reset_index()
    date = pd.date_range(start, end)
    positions = pd.DataFrame()

    id = records.groupby(["Holder", "ISIN"]).count().index
    for holder, isin in id:
        record = records.loc[(records["Holder"] == holder) & (records["ISIN"] == isin), :].sort_values("Date").reset_index(drop = True)
        position = pd.DataFrame({"Date": date}).set_index("Date").join(record.set_index("Date")).reset_index()
        position["Holder"] = holder
        position["ISIN"] = isin
        position["Covering"] = 0
        position["Increase"] = 0
        for i in record.index:
            ed = position.loc[position["Date"] == record.loc[i, "Date"], :].index.values[0]
            if i == 0:
                # update will pass
                position["Covering"][ed] = 0
                position["Increase"][ed] = 1
            else:
                position["Position"][op + 1: ed] = position["Position"][op]
                position["Covering"][ed] = int(record.loc[i, "Position"] < record.loc[i - 1, "Position"])
                position["Increase"][ed] = int(record.loc[i, "Position"] > record.loc[i - 1, "Position"])
            op = ed
        position["Position"][op + 1: ] = position["Position"][op]
        op = position.loc[position["Date"] == record.loc[0, "Date"], :].index.values[0]
        # Drop the nonrecorded items
        positions = positions.append(position.loc[op: , :], ignore_index = True)

    if initial:
        return positions

    # Drop the first day for update purpose
    positions = positions.loc[positions["Date"] > start].reset_index(drop = True)

    return positions
    
def stocksMakeup(prices, markets, initial = False):
    '''
    Make up the stock prices according to market time span.

    Parameters
    prices (pandas.DataFrame):
        Raw data of stock history prices.
    markets (pandas.DataFrame):
        Markets data as trade date reference.
    initial (boolean):
        Only True for the first time, False for the update.

    Return
    stocks (pandas.DataFrame)

    '''
    if prices.empty:
        return pd.DataFrame()

    tickers = pd.unique(prices["Ticker"])
    stocks = pd.DataFrame()
    for ticker in tickers:
        price = prices.loc[prices["Ticker"] == ticker, :].sort_values("Date")
        stock = pd.DataFrame(index = markets.index).join(price)
        stock.reset_index(inplace = True)
        i = stock.loc[stock["Date"] == price.reset_index().loc[0, "Date"], :].index.values[0]
        if sum(stock.loc[i: , "Ticker"].isna()) > 0:
            for j in stock.index[i + 1: ]:
                if stock.loc[j, "Ticker"] is np.NaN:
                    stock.loc[j, "Ticker"] = ticker
                    # stock.loc[j, ["High", "Low", "Open", "Close", "Volume"]] = 0
                    stock.loc[j, "Adj_close"] = stock.loc[j - 1, "Adj_close"]
        stocks = stocks.append(stock.loc[i: , :], ignore_index = True)
    
    if initial:
        return stocks

    # Drop the first day for update purpose
    stocks = stocks.loc[stocks["Date"] > min(stocks["Date"])].reset_index(drop = True)
    return stocks

if __name__ == "__main__":
    pass