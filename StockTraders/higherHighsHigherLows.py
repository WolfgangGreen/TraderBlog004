# Trader for the Higher Highs; Higher Lows strategy. The basic idea:
#   1. Look for cases where there are three 5-minute candles where:
#      a. The highs increase in each candle
#      b. The lows increase in each candle
#      c. The overall gain (across the highs and lows) is at least 4%
#      d. The Decrease between high and close of the final candle is at most 0.25%
#      e. It's 9:50 or later (because we ignore the first candle)
#   2. Buy the stock (with a Market buy) and hold for 10 minutes
#   3. Sell the stock (with a Market sell)

import logging
import math
import pandas as pd

from TradingApis.alpacaClients import TradeMode, QueryMode, set_alpaca_modes, get_trade_mode
from TradingApis.alpacaOperations import get_bars
from Util.pathsAndStockSets import StockSet, set_stock_set, temp_files_path, get_symbols
from Util.tradeTracker import trade_tracker
from Util.datesAndTimestamps import (timestamp, sleep_until_time, time_string, date_string, most_recent_bar_time,
                                     trading_dates, datetime_string)
from Util.tradeExecution import TimedHoldLongTradeExecutor, process_trade_executors
from Util.tradeIdentification import HigherHighsHigherLowsTradeIdentifier

logging.basicConfig(format='%(message)s', level=logging.INFO)

set_stock_set(StockSet.SP500)
# set_alpaca_modes(new_trade_mode=TradeMode.PRODUCTION, new_query_mode=QueryMode.API)
# set_alpaca_modes(new_trade_mode=TradeMode.PAPER, new_query_mode=QueryMode.API)
set_alpaca_modes(new_trade_mode=TradeMode.SIMULATION, new_query_mode=QueryMode.FILE)

buying_power = 100000
max_trades = 2
active_trades = 0
gain_for_day = 0
minimum_gain_pct = 4.0  # Require that the total gain across the highs and lows is at least 4%
maximum_drop_pct = 0.25  # Require that the diff between high and close in final bar is <= 0.25%
earliest_trade_time = '09:50:00'  # Don't make trades using the first bar of the trading day
hold_duration = 10  # Max time (in minutes) to hold the trade before cashing out
latest_trade_time = '15:45:00'  # Don't initiate any trades after 3:40pm (so we are closed out by 3:55pm)
symbols = get_symbols()

trade_tracker_df = pd.DataFrame()
for trading_date in trading_dates(timestamp('2023-12-01'), timestamp('2024-05-31')):
    # for trading_date in [timestamp('2024-01-22')]:
    logging.info(f"c=hhhl a=trade s=started date={date_string(trading_date)} " +
                 f"starting_balance={round(buying_power, 2)}")
    trade_amount = buying_power / 2

    # make trade identifiers for each symbol
    trade_identifiers = list()  # list of HigherHighsHigherLowsTradeIdentifiers
    for symbol in symbols:
        identifier = HigherHighsHigherLowsTradeIdentifier(symbol, minimum_gain_pct, maximum_drop_pct,
                                                          earliest_trade_time)
        trade_identifiers.append(identifier)

    # walk through the day and make trades
    trade_executors = list()

    for delta in pd.timedelta_range(start='09:35:00', end='16:00:00', freq='5min'):
        decision_time = trading_date + delta
        sleep_until_time(decision_time + pd.Timedelta('00:00:02'), 'hhhl', 'wait_for_bar')
        logging.info(f"c=hhhl a=tradeDuringInterval s=started dt={datetime_string(decision_time)} " +
                     f"a={round(buying_power, 2)}")
        buying_power, realized_profit, current_profit = process_trade_executors(trade_executors, decision_time,
                                                                                buying_power=buying_power)

        # Count how many active trades there are
        active_trades = 0
        for executor in trade_executors:
            if executor.state != 'complete':
                active_trades += 1

        if time_string(decision_time) <= latest_trade_time:

            # See what gets triggered
            candidates = list()
            bar_time = most_recent_bar_time(decision_time)
            current_bars = get_bars(bar_time, bar_time, symbols)
            current_bars = current_bars.set_index(['symbol'], drop=False)
            current_bars = current_bars[~current_bars.index.duplicated(keep='first')]
            for identifier in trade_identifiers:
                if identifier.symbol in current_bars.index:
                    current_bar = current_bars.loc[identifier.symbol]
                    triggered, details = identifier.consume_5min_bar(current_bar)
                    if triggered:
                        candidates.append(details)

            # Select which trades to execute
            if len(candidates) > 0:
                for candidate in candidates[:max_trades-active_trades]:
                    shares = math.floor(trade_amount/candidate['target_buy_price'])
                    executor = TimedHoldLongTradeExecutor(candidate['symbol'], shares, decision_time,
                                                          candidate['target_buy_price'], hold_duration)
                    print(f"c=hhhl a=selectTrade dt={datetime_string(decision_time)} "
                          + f"sym={candidate['symbol']} direction=LONG")
                    trade_executors.append(executor)
        logging.info(f"c=hhhl a=tradeDuringInterval s=completed dt={datetime_string(decision_time)} "
                     + f"realizedProfit=${round(realized_profit, 2)} currentProfit=${round(current_profit, 2)}")

trade_tracker_df = trade_tracker().to_dataframe()
trade_tracker_df = trade_tracker_df.round(4)
filename = f"purchaseTracker_{date_string(trading_date).replace('-', '')}_{get_trade_mode().name}.csv"
trade_tracker_df.to_csv(temp_files_path(filename), index=False)
