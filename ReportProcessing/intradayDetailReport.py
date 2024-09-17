# intradayDetailReport holds the open, high, low, close, volume, trade_count, and vwap for each 5 minutes for each stock

import pandas as pd

from Util.datesAndTimestamps import date_string, trading_dates
from Util.pathsAndStockSets import bar_files_path


# The report has these fields:
#   timestamp (index): the Pandas timestamp for the start of the 5-minute (or 1-minute) window
#   symbol (index): the stock symbol for this entry
#   date: the date for this entry in the form yyyy-mm-dd (not saved in file)
#   time: the time for the start of the 5-minute window in the form hh:mm:ss (not saved in file)
#   open: the open price for this stock on for this window
#   high: the highest price for this stock during this window
#   low: the lowest price for this stock during this window
#   close: the closing price for this stock at the end of this window
#   volume: the number of shares bought during this window
#   trade_count: the number of trades that occurred during this window
#   vwap: the volume-weighted average price for this window

# The DataFrame for this report has all the same columns and the index is (timestamp, symbol)


def intraday_detail_filename(report_start, freq=5):
    return f"intradayDetail_{freq}min_{date_string(report_start)}.csv"


def write_intraday_detail(df, report_start, freq=5):
    df = df.round(4)
    filename = intraday_detail_filename(report_start, freq=freq)
    df.to_csv(bar_files_path(filename), index=False)


def read_intraday_details(report_start, report_end=None, freq=5):
    report_end = report_end if report_end else report_start  # If there's no report_end, we just look at one day
    daily_details = list()  # list of DataFrame
    for report_date in trading_dates(report_start, report_end):
        filename = intraday_detail_filename(report_date, freq=freq)
        single_day_details = pd.read_csv(bar_files_path(filename), parse_dates=['timestamp'])
        daily_details.append(single_day_details)
    intraday_details = pd.concat(daily_details)
    intraday_details['date'] = intraday_details['timestamp'].map(lambda d: str(d)[:10])
    intraday_details['time'] = intraday_details['timestamp'].map(lambda d: str(d)[11:19])
    intraday_details.set_index(['timestamp', 'symbol'], drop=False, inplace=True)
    return intraday_details


# Extract just the rows in intraday_details associated with the given symbol. Also, reindex the resulting DataFrame on
# timestamp

def extract_symbol_details(intraday_details, symbol, make_copy=True):
    symbol_details = intraday_details[intraday_details['symbol'] == symbol]
    symbol_details = symbol_details.reset_index(drop=True)
    symbol_details = symbol_details.set_index('timestamp', drop=False)
    if make_copy:
        symbol_details = symbol_details.copy()
    return symbol_details
