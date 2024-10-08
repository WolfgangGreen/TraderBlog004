# Trader for the Fast Follower strategy. The basic idea:
#   1. Before the start of the trading day, select pairs of stocks that moved together in the previous 10 days:
#      a. For each stock, compute change in previous 10 minutes (trigger) and following 10 minutes (effect)
#      b. Then cross-join these results to get ever possible pair
#      c. Compute the average gain for each pair
#      d. Select pairs where average gain is high and there are enough instances
#   2. During the trading day, look for cases where:
#      a. The independent stock goes up by 1%
#   3. Buy the stock (with a Market buy) and hold for 15 minutes
#   4. Sell the stock (with a Market sell)

import logging
import math
import pandas as pd

from StockTraders.fastFollowerHelpers import get_trading_pairs
from TradingApis.alpacaClients import TradeMode, QueryMode, set_alpaca_modes, get_trade_mode
from TradingApis.alpacaOperations import get_bars
from Util.pathsAndStockSets import StockSet, set_stock_set, temp_files_path, get_symbols
from Util.tradeTracker import trade_tracker
from Util.datesAndTimestamps import (timestamp, sleep_until_time, time_string, date_string, most_recent_bar_time,
                                     trading_dates, datetime_string)
from Util.tradeExecution import TimedHoldLongTradeExecutor, process_trade_executors
from Util.tradeIdentification import FastFollowerTradeIdentifier

logging.basicConfig(format='%(message)s', level=logging.INFO)

set_stock_set(StockSet.SP500)
# set_alpaca_modes(new_trade_mode=TradeMode.PRODUCTION, new_query_mode=QueryMode.API)
# set_alpaca_modes(new_trade_mode=TradeMode.PAPER, new_query_mode=QueryMode.API)
set_alpaca_modes(new_trade_mode=TradeMode.SIMULATION, new_query_mode=QueryMode.FILE)

buying_power = 100000
max_trades = 2
active_trades = 0
gain_for_day = 0

# parameters for selecting pairs (ind_15min_trigger_pct is also used in triggering trades)
lookback_window = 10  # Look at the previous 10 trading days when finding correlations
ind_15min_trigger_pct = 1.0  # Look at cases where independent stock goes up 1% during previous 15 minutes
mean_gain_threshold = 0.5  # Only accept pairs with average gain of 0.5% per trade in the training
success_rate = 0.666  # Only accept pairs where we gain 0.5% at least 2/3 of the time in the training
min_count = 7  # Only accept pairs if there were at least 5 instances in the training set (every other week)
effect_window = 15  # Sell after 15 minutes

# parameters for triggering trades
ind_5min_trigger_pct = 0.5  # Only trigger if the stock goes up 0.5% during the previous bar
dep_5min_trigger_pct = 0.5  # Only trigger if the stock goes up 0.5% during the trigger window
earliest_trade_time = '09:50:00'
latest_trade_time = '15:40:00'  # Don't initiate any trades after 3:40pm (so we are closed out by 3:55pm)

symbols = get_symbols()

trade_tracker_df = pd.DataFrame()
# for trading_date in trading_dates(timestamp('2023-12-01'), timestamp('2024-05-31')):
for trading_date in trading_dates(timestamp('2024-06-01'), timestamp('2024-08-31')):
    # for trading_date in [timestamp('2023-12-04')]:

    logging.info(f"c=fastFollower a=trade s=started date={date_string(trading_date)} " +
                 f"starting_balance={round(buying_power, 2)}")

    trading_pairs = get_trading_pairs(trading_date, symbol_subset=symbols, lookback_window=lookback_window,
                                      effect_window=effect_window, trigger_pct=ind_15min_trigger_pct,
                                      min_count=min_count, mean_gain_pct=mean_gain_threshold,
                                      success_rate_05=success_rate)

    # We will devote half our buying power on each trade
    trade_amount = buying_power / 2

    # make trade identifiers for each symbol
    trade_identifiers = list()  # list of FastFollowerTradeIdentifiers
    for entry in trading_pairs.itertuples(index=False):
        identifier = FastFollowerTradeIdentifier(entry.independent_symbol, entry.dependent_symbol,
                                                 ind_15min_trigger_pct, ind_5min_trigger_pct, dep_5min_trigger_pct,
                                                 earliest_trade_time)
        trade_identifiers.append(identifier)

    # walk through the day and make trades
    trade_executors = list()

    for delta in pd.timedelta_range(start='09:35:00', end='16:00:00', freq='5min'):
        decision_time = trading_date + delta
        sleep_until_time(decision_time + pd.Timedelta('00:00:02'), 'hhhl', 'wait_for_bar')
        logging.info(f"c=fastFollower a=tradeDuringInterval s=started dt={datetime_string(decision_time)} " +
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
                if identifier.symbol1 in current_bars.index and identifier.symbol2 in current_bars.index:
                    symbol1_bar = current_bars.loc[identifier.symbol1]
                    symbol2_bar = current_bars.loc[identifier.symbol2]
                    triggered, details = identifier.consume_5min_bars(symbol1_bar, symbol2_bar)
                    if triggered:
                        candidates.append(details)

            # Select which trades to execute
            if len(candidates) > 0:
                for candidate in candidates[:max_trades-active_trades]:
                    shares = math.floor(trade_amount/candidate['target_buy_price'])
                    executor = TimedHoldLongTradeExecutor(candidate['symbol'], shares, decision_time,
                                                          candidate['target_buy_price'], 15)
                    logging.info(f"c=fastFollower a=selectTrade dt={datetime_string(decision_time)} "
                                 + f"sym={candidate['symbol']} direction=LONG ind={candidate['independent_symbol']}")
                    trade_executors.append(executor)

        logging.info(f"c=fastFollower a=tradeDuringInterval s=completed dt={datetime_string(decision_time)} "
                     + f"realizedProfit=${round(realized_profit, 2)} currentProfit=${round(current_profit, 2)}")

trade_tracker_df = trade_tracker().to_dataframe()
trade_tracker_df = trade_tracker_df.round(4)
filename = f"purchaseTracker_{date_string(trading_date).replace('-', '')}_{get_trade_mode().name}.csv"
trade_tracker_df.to_csv(temp_files_path(filename), index=False)
