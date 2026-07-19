
import pandas as pd
import yfinance as yf
import os
from datetime import datetime, timedelta
import bs4 as bs
import io
import argparse
import requests
import pickle

import rates_data  # sibling module: daily interest-rate LEVELS (separate pickle)

# Non-SPDR tickers pulled alongside the SPDR set into spdrfactors: FX majors,
# the ICE dollar index, and WTI crude oil. Kept here in code (not in the vendor
# spdr_data.xlsx export) so refreshing that export can't silently drop them.
# Each pair is the foreign currency measured in USD (XXXUSD), so a positive daily
# return means that currency strengthened vs the US dollar (e.g. JPYUSD up = yen
# stronger, USD weaker). DX-Y.NYB (DXY) is the broad dollar index and is the one
# exception: DXY up = USD stronger, i.e. it moves opposite the pairs.
# CL=F is WTI crude front-month futures (an oil-*price* series, unlike the XOP/
# XLE oil-equity ETFs in the SPDR set); positive return = oil price up.
EXTRA_FX_TICKERS = [
    "EURUSD=X", "JPYUSD=X", "GBPUSD=X", "CHFUSD=X", "CADUSD=X",
    "AUDUSD=X", "CNYUSD=X", "MXNUSD=X", "DX-Y.NYB", "CL=F",
]

"""
sample command line call:
$env:DB = 'C:\\Users\\gcubb\\OneDrive\\Python\\data-hub'

python fin_data_update.py --db $env:DB
"""


# Add in the future?
# Define the URL of the data source, download the zip file and extract the csv file
#url = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_5_Factors_2x3_CSV.zip"
#r = requests.get(url)
#with open("F-F_Research_Data_5_Factors_2x3_CSV.zip", "wb") as f:
#    f.write(r.content)

def _gap_robust_returns(close):
    """Daily % returns measured from each column's own last valid price.

    yf.download aligns every ticker on the UNION of their calendars, so a
    ticker that doesn't trade on a day others do gets a NaN price on that row.
    A plain .pct_change() then differences across that NaN and NaN-poisons the
    NEXT real observation too. The prime offender is DX-Y.NYB (ICE dollar
    index), which is closed on US market holidays while the FX spot pairs keep
    trading — so every first-US-trading-day-after-a-holiday DXY return came out
    NaN. Computing pct_change per column on that column's own non-null prices
    measures the move from the last day the ticker actually traded, so calendar
    gaps no longer manufacture NaNs. Rows that are all-NaN afterwards (e.g. a
    genuine market holiday) are still dropped downstream via dropna(how='all').
    """
    if isinstance(close, pd.Series):
        return close.dropna().pct_change().reindex(close.index).mul(100)
    return (close.apply(lambda c: c.dropna().pct_change())
                 .reindex(close.index).mul(100))


def yf_update(fname, latest_tickers, OVERWRITE=False):
    pickle_file = fname + ".pickle"
    try:
        with open(pickle_file, "rb") as f:
            fdat = pickle.load(f)
        # Drop duplicate columns by name
        fdat = fdat.loc[:, ~fdat.columns.duplicated()]
        history_begin_date = fdat.index[0]
        startupdate = fdat.index[-1]
        tickers_list = list(fdat.columns)
    except FileNotFoundError:
        print(f"Pickle file {pickle_file} not found. Creating new dataset...")
        fdat = pd.DataFrame()
        history_begin_date = "2000-01-03"  # Adjust as needed
        startupdate = history_begin_date
        tickers_list = []
    
    endupdate = datetime.today().strftime("%Y-%m-%d")
    new_tickers = [tik for tik in latest_tickers if tik not in tickers_list]
    # Download all data if starting fresh, otherwise update existing
    if fdat.empty:
        full_data = (
            _gap_robust_returns(
                yf.download(latest_tickers, history_begin_date, endupdate, auto_adjust=True)['Close'])
            .dropna(how='all')
            .assign(index=lambda x: pd.to_datetime(x.index, format="%Y%m%d"))
            .set_index('index')
            .sort_index(axis=0).sort_index(axis=1)
        )
    else:
        # Your existing logic here...
        existing_tickers_update = (
            _gap_robust_returns(
                yf.download(tickers_list, startupdate, endupdate, keepna=True, auto_adjust=True)['Close'])
            .dropna(how='all')
            .assign(index=lambda x: pd.to_datetime(x.index, format="%Y%m%d"))
            .set_index('index')
            .pipe(lambda x: pd.concat([fdat, x], axis=0))
        )
        
        if not new_tickers:
            full_data = existing_tickers_update.sort_index(axis=0).sort_index(axis=1)
        else:
            new_tickers_update = (
                _gap_robust_returns(
                    yf.download(new_tickers, history_begin_date, endupdate, auto_adjust=True)['Close'])
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
        with open(pickle_file, "wb") as f:
            pickle.dump(full_data, f)
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

def parse_market_cap(cap_str):
    """Parse market cap strings like '4.41T', '123.45B', '1.23M' to billions"""
    if pd.isna(cap_str) or cap_str == 'N/A':
        return 0
    cap_str = str(cap_str).replace(',', '').strip()
    if cap_str.endswith('T'):
        return float(cap_str[:-1]) * 1000  # Convert trillions to billions
    elif cap_str.endswith('B'):
        return float(cap_str[:-1])  # Already in billions
    elif cap_str.endswith('M'):
        return float(cap_str[:-1]) / 1000  # Convert millions to billions
    else:
        # Assume it's already a number in billions
        try:
            return float(cap_str)
        except ValueError:
            return 0

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
    parser = argparse.ArgumentParser(description='Read in key data and update, mostly from yahoo finance')
    parser.add_argument('--db', '-d', help='Path to data', default='C:\\Users\\gcubb\\OneDrive\\Python\\data-hub')
    args = parser.parse_args(argv)
    data_db_root = args.db
    
    spdrdatfile = "spdrfactors"
    #mainrtns = 'sprtns_current'
    mainrtns = 'sprtns'
    #mainsect = 'sp500sectors_current'
    mainsect = 'spsect'
    sp500sect =  'sp500_history' # update sectors in new format

    #=================================================================
    # 1: Update mainrtns (individual stock returns)
    sp_raw_df = save_sp500_tickers("https://stockanalysis.com/list/sp-500-stocks/", "stockData")
    sp_df = (
        sp_raw_df
        .copy()
        .assign(
            #Market_Cap_bn=lambda x: x['Market Cap'].astype(str).str.replace(',', '').str.rstrip('B').astype(float),
            Market_Cap_bn=lambda x: x['Market Cap'].apply(parse_market_cap),
            sp500_weight=lambda x: x['Market_Cap_bn'] / x['Market_Cap_bn'].sum()
        )
        .drop(columns=['Market Cap'])
        .rename(columns={'Symbol': 'Ticker'})
        .set_index('Ticker')
        [['Company Name','Market_Cap_bn','sp500_weight']]
    )
    sp_tickers = sp_df.index.tolist()
    fdatin = yf_update(os.path.join(data_db_root, mainrtns), sp_tickers, True) 

    #=================================================================
    # 2a: Update mainsect for anything newly added in mainrtns (sectors stay frozen)
    all_tickers = fdatin.columns.tolist()
    try:
        with open(os.path.join(data_db_root, mainsect)+".pickle","rb") as f:
            spsect = pickle.load(f)
        # get all_tickers that are not in spsect index
        missing_tickers = [tik for tik in all_tickers if tik not in spsect.index]
    except FileNotFoundError:
        print(f"Pickle file {mainsect}.pickle not found. Creating new sector dataset...")
        spsect = pd.DataFrame(columns=['Sector', 'Industry'])
        spsect.index.name = 'Ticker'
        # If starting fresh, all tickers are missing
        missing_tickers = all_tickers
    print("Getting sector and industry info from Yahoo Finance...probably quick...")
    sector_list=[]
    industry_list=[]
    for tik in missing_tickers:
        #get ticker from yf.Ticker and handle errors to keep loop going
        try:
            ticker=yf.Ticker(tik)
            #get both sector and industry info on ticker
            sector=ticker.info['sector']
            sector_list.append(sector)
            industry=ticker.info['industry']
            industry_list.append(industry)
        except:
            sector_list.append('N/A')
            industry_list.append('N/A')
            continue
    print("Done!")
    spsect_update = pd.DataFrame(
        list(zip(missing_tickers, sector_list, industry_list)),
        columns=['Ticker', 'Sector', 'Industry']
    ).set_index('Ticker')
    spsect = pd.concat([spsect, spsect_update], axis=0).sort_index()
    spsect = yf_sector_clean(spsect)
    with open(os.path.join(data_db_root, "spsect")+".pickle","wb") as f:
        pickle.dump(spsect,f)

    #=================================================================
    # 2: Update sp500sect (sector and industry, as of today)
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
    print("Done!")
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
    try:
        with open(os.path.join(data_db_root, sp500sect)+".pickle","rb") as f:
            sp500_dict = pickle.load(f)
    except FileNotFoundError:
        print(f"Pickle file {sp500sect}.pickle not found. Creating new history dictionary...")
        sp500_dict = {}

    today_date = datetime.today().strftime('%Y-%m-%d')
    if today_date not in sp500_dict:
        sp500_dict[today_date] = spdf2
    with open(os.path.join(data_db_root, sp500sect)+".pickle","wb") as f:
        pickle.dump(sp500_dict,f)

    #=================================================================
    # 3: Update spdrfactors (SPDR sector ETFs)
    # Prefer a spdr_data.xlsx the user maintains in the data dir; on a fresh
    # setup that file won't be there yet, so fall back to the copy bundled in
    # this repo (same folder as this script) so the pipeline runs out of the box.
    spdr_xlsx = os.path.join(data_db_root, "spdr_data.xlsx")
    if not os.path.exists(spdr_xlsx):
        repo_xlsx = os.path.join(os.path.dirname(os.path.abspath(__file__)), "spdr_data.xlsx")
        if os.path.exists(repo_xlsx):
            print(f"spdr_data.xlsx not in data dir; using bundled repo copy: {repo_xlsx}")
            spdr_xlsx = repo_xlsx
    factorinfo = pd.read_excel(spdr_xlsx, skiprows=1) #reload each time to check for any added ETFs
    factorinfo = factorinfo.dropna(subset=['Ticker'])
    factor_tickers = factorinfo['Ticker'].tolist()
    factor_tickers = factor_tickers + [t for t in EXTRA_FX_TICKERS if t not in factor_tickers]
    fsdatin = yf_update(os.path.join(data_db_root, spdrdatfile), factor_tickers,True)

    #=================================================================
    # 4: Update rates_levels (interest-rate LEVELS; separate data model — see
    # rates_data.py). Non-fatal: a rates source outage must not break the
    # equity/returns update above.
    try:
        rd = rates_data.rates_update(data_db_root)
        print(f"rates_levels updated: {rd.shape[0]} rows x {rd.shape[1]} cols, "
              f"through {rd.index[-1].date()}")
    except Exception as e:
        print(f"rates_levels update FAILED (non-fatal): {e!r}")


if __name__ == '__main__':
    main()