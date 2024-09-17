import pandas as pd
import yfinance as yf

from alpaca.data.requests import StockBarsRequest  # pip install alpaca-py
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

from ReportProcessing.dailySummaryReport import write_daily_summary
from ReportProcessing.intradayDetailReport import write_intraday_detail
from TradingApis.alpacaClients import historical_client
from Util.datesAndTimestamps import timestamp, date_string, trading_dates
from Util.pathsAndStockSets import StockSet, set_stock_set, get_symbols

set_stock_set(StockSet.SP500)

data_source = 'Alpaca'  # this can be 'Alpaca' or 'yfinance'
start_date = timestamp('2024-05-31')
end_date = timestamp('2024-05-31')

if data_source == 'Alpaca':
    # Read the Daily Summary data and write to disk
    request = StockBarsRequest(symbol_or_symbols=get_symbols(), timeframe=TimeFrame.Day, start=timestamp('2023-07-01'))
    result = historical_client().get_stock_bars(request)
    result_df = result.df.reset_index(drop=False)
    result_df['date'] = result_df['timestamp'].map(lambda d: date_string(d))
    result_df['timestamp'] = result_df['timestamp'].map(lambda d: d.tz_convert('America/New_York'))
    result_df.set_index(['symbol', 'timestamp'], drop=True, inplace=True)
    write_daily_summary(result_df)

    # Read the 5-minute Bars data and write to disk
    for report_start in trading_dates(start=start_date, end=end_date):
        print(report_start)
        batch_start = report_start + pd.Timedelta('09:30:00')  # Start at 9:30 (when the market opens)
        batch_end = report_start + pd.Timedelta('15:55:00')  # End at 4:00 (when it closes)
        request_params = StockBarsRequest(symbol_or_symbols=get_symbols(),
                                          timeframe=TimeFrame(5, TimeFrameUnit.Minute),
                                          start=batch_start, end=batch_end)
        result_df = historical_client().get_stock_bars(request_params).df
        result_df = result_df.tz_convert('America/New_York', level=1)
        result_df = result_df.reset_index(drop=False)
        result_df = result_df.sort_values(['symbol', 'timestamp'])
        columns = ['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'trade_count', 'vwap']
        result_df = result_df[columns]
        result_df = result_df.set_index(['symbol', 'timestamp'], drop=False)
        write_intraday_detail(result_df, report_start, freq=5)

else:  # data_source == 'yfinance'
    symbols = get_symbols()
    columns = ['timestamp', 'symbol', 'Open', 'High', 'Low', 'Close', 'Volume']

    # Read the Daily Summary data and write to disk
    entries = list()  # list of DataFrame
    for index, stock_symbol in enumerate(symbols):
        print(f"Collect S&P 500 Summary Info: {stock_symbol} ({index+1} of {len(symbols)})")
        ticker = yf.Ticker(stock_symbol)
        history = ticker.history(period='2y')
        history['timestamp'] = history.index
        history['symbol'] = stock_symbol
        if len(history) > 0:
            entries.append(history[columns])
    result_df = pd.concat(entries)
    for column in columns:
        result_df.rename(columns={column: column.lower()}, inplace=True)
        result_df.set_index(['timestamp', 'symbol'], drop=False, inplace=True)
    result_df['date'] = result_df['timestamp'].apply(lambda ts: date_string(ts))
    result_df = result_df.drop('timestamp', axis=1)
    write_daily_summary(result_df)

    # Read the 5-minute Bars data and write to disk
    for report_start in trading_dates(start=start_date, end=end_date):
        print(report_start)
        entries = list()  # list of DataFrame
        batch_end = report_start + pd.Timedelta('1d')  # End at 4:00 (when it closes)
        for index, stock_symbol in enumerate(symbols):
            ticker = yf.Ticker(stock_symbol)
            history = ticker.history(interval='5m', start=date_string(report_start), end=date_string(batch_end))
            history['timestamp'] = history.index
            history['symbol'] = stock_symbol
            if len(history) > 0:
                entries.append(history[columns])
        result_df = pd.concat(entries)
        for column in columns:
            result_df.rename(columns={column: column.lower()}, inplace=True)
        result_df = result_df.set_index(['timestamp', 'symbol'], drop=False)
        write_intraday_detail(result_df, report_start, freq=5)
