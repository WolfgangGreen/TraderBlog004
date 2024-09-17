# Try out the TradeIdentifiers we've written for Article 4
import pandas as pd

from TradingApis.alpacaOperations import get_latest_bar
from Util.datesAndTimestamps import timestamp, time_string
from Util.tradeIdentification import HigherHighsHigherLowsTradeIdentifier, FastFollowerTradeIdentifier

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

if True:
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
