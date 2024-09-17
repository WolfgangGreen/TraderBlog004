# dailySummaryReport holds the open, high, low, close, volume, trade_count, and vwap for each day for each stock

import pandas as pd

from Util.pathsAndStockSets import bar_files_path

# The report has these fields:
#   timestamp (index): the timestamp for the date of this entry
#   symbol (index): the stock symbol for this entry
#   open: the open price for this stock on this date
#   high: the highest price for this stock during this date
#   low: the lowest price for this stock during this date
#   close: the closing price for this stock on this date
#   volume: the number of shares bought during this day
#   trade_count: the number of trades that occurred during this day
#   vwap: the volume-weighted average price for this day (starting from ?)
#   date: the date for this entry in the form yyyy-mm-dd


def write_daily_summary(df):
    df = df.round(4)
    df.to_csv(bar_files_path('dailySummary.csv'), index=True)


def read_daily_summary():
    daily_price_gains = pd.read_csv(bar_files_path('dailySummary.csv'), parse_dates=['timestamp'])
    daily_price_gains.set_index(['timestamp', 'symbol'], drop=False, inplace=True)
    return daily_price_gains


# Filter a daily_summary to just a single symbol, and re-index to timestamp

def extract_symbol_summary(daily_summary, symbol, make_copy=True):
    symbol_summary = daily_summary[daily_summary['symbol'] == symbol]
    symbol_summary = symbol_summary.reset_index(drop=True)
    symbol_summary = symbol_summary.set_index('timestamp', drop=False)
    if make_copy:
        symbol_summary = symbol_summary.copy()
    return symbol_summary


# See if the indicated (timestamp, symbol) is in the daily_summary:
#   If yes: Return True and a Pandas Series for that row
#   If no: Return False and None

def extract_daily_summary_bar(daily_summary, timestamp, symbol):
    if (timestamp, symbol) in daily_summary.index:
        return True, daily_summary.loc[(timestamp, symbol)]
    return False, None
