# Try out the TradeIdentifiers we've written for Article 4
import pandas as pd

from alpaca.data.requests import StockBarsRequest  # pip install alpaca-py
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

from TradingApis.alpacaClients import set_alpaca_modes, QueryMode, historical_client
from TradingApis.alpacaOperations import get_latest_bar

from Util.datesAndTimestamps import timestamp, time_string
from Util.tradeIdentification import HigherHighsHigherLowsTradeIdentifier, FastFollowerTradeIdentifier

# Try out the Higher Highs Higher Lows trade identifier on particular cases
if False:
    # symbol, date = ('ENPH', timestamp('2023-12-01'))
    # symbol, date = ('ODFL', timestamp('2023-12-06'))
    # symbol, date = ('WBA', timestamp('2023-12-07'))
    # symbol, date = ('PARA', timestamp('2023-12-08'))
    # symbol, date = ('FICO', timestamp('2024-01-26'))
    symbol, date = ('ALB', timestamp('2024-01-22'))

    identifier = HigherHighsHigherLowsTradeIdentifier(symbol, 4.0, 0.25, '09:45:00')
    for time_offset in pd.timedelta_range(start='09:35:00', end='15:45:00', freq='5min'):
        decision_time = date + time_offset
        success, bar = get_latest_bar(symbol, decision_time)
        if success:
            is_triggered, details = identifier.consume_5min_bar(bar)
            if is_triggered:
                print(f"Triggered at {time_string(decision_time)}: {details}")

# Try out the Fast Follower trade identifier on particular cases
if False:
    # symbol1, symbol2, date = ('SCHW', 'MTB', timestamp('2023-12-01'))
    # symbol1, symbol2, date = ('LUV', 'IVZ', timestamp('2023-12-01'))
    symbol1, symbol2, date = ('FMC', 'WBA', timestamp('2023-12-06'))

    identifier = FastFollowerTradeIdentifier(symbol1, symbol2, 0.5, 0.5, '09:45:00')
    for time_offset in pd.timedelta_range(start='09:35:00', end='15:45:00', freq='5min'):
        decision_time = date + time_offset
        success1, bar1 = get_latest_bar(symbol1, decision_time)
        success2, bar2 = get_latest_bar(symbol2, decision_time)
        if success1 and success2:
            is_triggered, details = identifier.consume_5min_bars(bar1, bar2)
            if is_triggered:
                print(f"Triggered at {time_string(decision_time)}: {details}")

# Get baselines for our trading intervals (based on the price of SPY)
if True:
    # start_date, end_date = timestamp('2023-12-01'), timestamp('2024-05-31')
    start_date, end_date = timestamp('2024-06-01'), timestamp('2024-08-31')

    request = StockBarsRequest(symbol_or_symbols='SPY', timeframe=TimeFrame.Day, start=start_date, end=end_date)
    result = historical_client().get_stock_bars(request)
    start_price = result.df.iloc[0]['open']
    end_price = result.df.iloc[-1]['close']
    price_change = 100 * (end_price/start_price - 1)
    print(f"start_price={start_price} end_price={end_price} change={round(price_change,2)}%")
