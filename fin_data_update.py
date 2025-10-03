
import pandas as pd
import yfinance as yf
import os
from datetime import datetime, timedelta
import bs4 as bs
import io
import argparse
import requests
import pickle

"""
sample command line call:
$env:DB = 'C:\\Users\\gcubb\\OneDrive\\Python\\data-hub'

python fin_data_update.py --db $env:DB
"""


# for debugging yf_update:
#fname = os.path.join(data_db_root, mainrtns)
#latest_tickers = sp_tickers


# Plopped this here...don't remember if I used it or not (rather than just getting the file by hand)
# Define the URL of the data source
# url = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_5_Factors_2x3_CSV.zip"
# # Download the zip file and extract the csv file
# r = requests.get(url)
# with open("F-F_Research_Data_5_Factors_2x3_CSV.zip", "wb") as f:
#     f.write(r.content)

def yf_update(fname, latest_tickers, OVERWRITE=False):
    with open(fname+".pickle","rb") as f:
        fdat = pickle.load(f)
    # Drop duplicate columns by name
    fdat = fdat.loc[:, ~fdat.columns.duplicated()]
    history_begin_date = fdat.index[0]
    startupdate  = fdat.index[-1]
    endupdate    = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    #dead_tickers = fdat.columns[fdat.tail(20).isna().all()]
    #tickers_list = list(fdat.columns[~fdat.columns.isin(dead_tickers)])
    tickers_list = list(fdat.columns)
    new_tickers = [tik for tik in latest_tickers if tik not in tickers_list]
    existing_tickers_update = (
        yf.download(tickers_list,startupdate,endupdate, keepna=True, auto_adjust=True)['Close']
        .pct_change(fill_method=None).mul(100)
        .dropna(how='all')
        .assign(index=lambda x: pd.to_datetime(x.index, format="%Y%m%d"))
        .set_index('index')
        .pipe(lambda x: pd.concat([fdat, x], axis=0))
    )
    if not new_tickers:
        full_data = existing_tickers_update.sort_index(axis=0).sort_index(axis=1)
    else:
        new_tickers_update = (
            yf.download(new_tickers,history_begin_date,endupdate, auto_adjust=True)['Close']
            .pct_change(fill_method=None).mul(100)
            .dropna(how='all')
            .assign(index=lambda x: pd.to_datetime(x.index, format="%Y%m%d"))
            .set_index('index')
        )
        full_data = (
            pd.concat([existing_tickers_update, new_tickers_update], axis=1)
            .sort_index(axis=0)
            .sort_index(axis=1)
        )
    if OVERWRITE:
        with open(fname+".pickle","wb") as f:
            pickle.dump(full_data,f)
    return full_data


def yf_sector_clean(yinfo):
    sector_mapping = {
        'Consumer Cyclical': 'Cons Cyc',
        'Communication Services': 'Comm Serv',
        'Consumer Defensive': 'Cons Def', 
        'Financial Services': 'Fin Serv',
        'Basic Materials': 'Materials'
    }
    yinfo['Sector'] = yinfo['Sector'].replace(sector_mapping)
    return yinfo


def save_sp500_tickers(ticker_location, table_name):
    #headers = {'User-Agent': 'Mozilla/5.0'}
    #resp = requests.get(ticker_location, headers=headers)
    resp = requests.get(ticker_location)
    resp.raise_for_status()
    soup = bs.BeautifulSoup(resp.text, "lxml")
    # Try pandas.read_html first (simplest for static tables)
    try:
        #tables = pd.read_html(resp.text)
        tables = pd.read_html(io.StringIO(resp.text))
        # pick the table that looks right (first with >1 col)
        for t in tables:
            if t.shape[1] > 1:
                df = t
                break
        else:
            df = tables[0]
        # assume the ticker symbol is in the second column
        #return list(df.iloc[:, 1].astype(str).str.strip())
        return df
    except Exception:
        # Fallback: BeautifulSoup parsing
        table = soup.find("table", class_=table_name)
        if table is None:
            raise ValueError(f"No table with class '{table_name}' found on page")

        rows = []
        for tr in table.find_all("tr"):
            cols = [td.get_text(strip=True) for td in tr.find_all(["th", "td"])]
            if cols:
                rows.append(cols)

        if not rows:
            return []

        header, *data = rows
        df = pd.DataFrame(data, columns=header)
        # return the first column as tickers (adjust if different)
        #return list(df.iloc[:, 1].astype(str).str.strip())
        return df
    
def main(argv=None):
    parser = argparse.ArgumentParser(description='Read in key data and update, mostly from yahhoo finance')
    parser.add_argument('--db', '-d', help='Path to data', default='C:\\Users\\gcubb\\OneDrive\\Python\\data-hub')
    args = parser.parse_args(argv)
    data_db_root = args.db
    
    spdrdatfile = "spdrfactors"
    mainrtns = 'sprtns_current'
    mainsect = 'sp500sectors_current'

    sp_raw_df = save_sp500_tickers("https://stockanalysis.com/list/sp-500-stocks/", "stockData")
    sp_df = (
        sp_raw_df
        .copy()
        .assign(
            Market_Cap_bn=lambda x: x['Market Cap'].astype(str).str.replace(',', '').str.rstrip('B').astype(float),
            sp500_weight=lambda x: x['Market_Cap_bn'] / x['Market_Cap_bn'].sum()
        )
        .drop(columns=['Market Cap'])
        .rename(columns={'Symbol': 'Ticker'})
        .set_index('Ticker')
        [['Company Name','Market_Cap_bn','sp500_weight']]
    )
    sp_tickers = sp_df.index.tolist()
    fdatin = yf_update(os.path.join(data_db_root, mainrtns), sp_tickers, True) 
    # WORKS! 1: SP500 lifetime tickers returns updated

    # Lets add in sector and industry
    print("Getting sector and industry info from Yahoo Finance...this may take a while...")
    sector_list=[]
    industry_list=[]
    mcap_list=[]
    for tik in sp_tickers:
        #get ticker from yf.Ticker and handle errors to keep loop going
        try:
            ticker=yf.Ticker(tik)
            #get both sector and industry info on ticker
            sector=ticker.info['sector']
            sector_list.append(sector)
            industry=ticker.info['industry']
            industry_list.append(industry)
            mcap=ticker.info['marketCap']
            mcap_list.append(mcap)
        except:
            sector_list.append('N/A')
            industry_list.append('N/A')
            mcap_list.append('N/A')
            continue
    spdf2 = (
        pd.DataFrame(
            list(zip(sp_tickers,sector_list,industry_list,mcap_list)), 
            columns=['Ticker','Sector','Industry','marketCap_yf'])
        .assign(marketCap_yf=lambda x: x['marketCap_yf'].replace('N/A',0),
                sp500_weight_yf=lambda x: x['marketCap_yf']/x['marketCap_yf'].sum()
        )
        .set_index('Ticker')
        .pipe(lambda x: sp_df.merge(x, left_index=True, right_index=True, how='left'))
        .pipe(yf_sector_clean)
    )

    with open(os.path.join(data_db_root, "sp500_history")+".pickle","rb") as f:
        sp500_dict = pickle.load(f)
    today_date = datetime.today().strftime('%Y-%m-%d')
    if today_date not in sp500_dict:
        sp500_dict[today_date] = spdf2
    with open(os.path.join(data_db_root, "sp500_history")+".pickle","wb") as f:
        pickle.dump(sp500_dict,f)
    # WORKS! 2: SP500 daily sectors, tickers, weights updated

    factorinfo = pd.read_excel(os.path.join(data_db_root, "spdr_data.xlsx"), skiprows=1)
    factorinfo = factorinfo.dropna(subset=['Ticker'])
    factor_tickers = factorinfo['Ticker'].tolist()
    fsdatin = yf_update(os.path.join(data_db_root, spdrdatfile), factor_tickers,True)
    # WORKS! 3: SPDR factor returns updated


if __name__ == '__main__':
    main()