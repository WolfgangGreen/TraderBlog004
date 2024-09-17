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
import pandas as pd

from ReportProcessing.intradayDetailReport import read_intraday_details, extract_symbol_details
from TradingApis.alpacaClients import TradeMode, QueryMode, set_alpaca_modes, get_trade_mode
from TradingApis.alpacaOperations import get_latest_bar, get_bars
from Util.pathsAndStockSets import StockSet, set_stock_set, temp_files_path, get_symbols
from Util.purchaseTracker import trade_tracker
from Util.datesAndTimestamps import (timestamp, sleep_until_time, time_string, date_string, most_recent_bar_time,
                                     trading_dates, previous_trading_date)
from Util.tradeExecution import TimedHoldLongTradeExecutor, process_trade_executors
from Util.tradeIdentification import FastFollowerTradeIdentifier

set_stock_set(StockSet.SP500)
# set_alpaca_modes(new_trade_mode=TradeMode.PRODUCTION, new_query_mode=QueryMode.API)
# set_alpaca_modes(new_trade_mode=TradeMode.PAPER, new_query_mode=QueryMode.API)
set_alpaca_modes(new_trade_mode=TradeMode.SIMULATION, new_query_mode=QueryMode.FILE)

buying_power = 100000
max_trades = 2
active_trades = 0
gain_for_day = 0
lookback_window = 10  # Look at the previous 10 trading days when finding correlations
ind_trigger_pct = 0.5  # Trigger if the stock goes up 1% during the trigger window
dep_trigger_pct = 0.5  # Trigger if the stock goes up 1% during the trigger window
mean_gain_threshold = 0.5  # Only accept pairs with average gain of 0.5% per trade in the training
min_count = 5  # Only accept pairs if there were at least 5 instances in the training set (every other week)
effect_window = 3  # Sell after 15 minutes
earliest_trade_time = '09:50:00'
latest_trade_time = '15:40:00'  # Don't initiate any trades after 3:40pm (so we are closed out by 3:55pm)
symbols = get_symbols()
# symbols = ['ALB']


def compute_trigger_and_effect_df(intraday_details):
    symbol_results = list()  # List of DataFrame (one for each symbol)
    for symbol in get_symbols():
        s_hist = extract_symbol_details(intraday_details, symbol)
        s_hist['trigger_last_15_pct'] = 100 * (s_hist['close'] / s_hist.shift(2)['open'] - 1)
        s_hist['trigger_last_10_pct'] = 100 * (s_hist['close'] / s_hist.shift(1)['open'] - 1)
        s_hist['trigger_last_05_pct'] = 100 * (s_hist['close'] / s_hist.shift(0)['open'] - 1)
        s_hist['gain_pct'] = 100 * (s_hist.shift(-effect_window)['close'] / s_hist.shift(-1)['open'] - 1)
        s_hist['gain_00'] = s_hist['gain_pct'] >= 0  # Did we break even?
        s_hist['gain_05'] = s_hist['gain_pct'] >= 0.5  # Did we gain at least 0.5%
        s_hist['gain_10'] = s_hist['gain_pct'] >= 1.0  # Did we gain at least 1.0%
        s_hist = s_hist.dropna()  # Drop rows where we don't have gain_pct (usually end of day)
        symbol_results.append(s_hist)
    symbol_results_df = pd.concat(symbol_results)
    symbol_results_df = symbol_results_df.reset_index(drop=True)
    return symbol_results_df


def get_trading_pairs(trading_date):
    train_details = read_intraday_details(previous_trading_date(trading_date, offset=lookback_window),
                                          previous_trading_date(trading_date))  # 2 weeks

    # Compute the training values
    training_set = compute_trigger_and_effect_df(train_details)

    # Compute expected gain for each trigger
    triggers = training_set[training_set['trigger_last_05_pct'] >= ind_trigger_pct]
    cross_join = triggers.merge(training_set, on='timestamp')
    average_gains = cross_join.groupby(['symbol_x', 'symbol_y']).agg({'gain_pct_y': ['count', 'mean'],
                                                                      'gain_00_y': ['mean'],
                                                                      'gain_05_y': ['mean'],
                                                                      'gain_10_y': ['mean']})
    average_gains = average_gains.reset_index(drop=False)
    average_gains.columns = average_gains.columns.to_flat_index().str.join('_')  # Flatten the multi-index
    average_gains = average_gains.rename(columns={'symbol_x_': 'independent_symbol',
                                                  'symbol_y_': 'dependent_symbol',
                                                  'gain_pct_y_count': 'count',
                                                  'gain_pct_y_mean': 'mean_gain_pct',
                                                  'gain_00_y_mean': 'gain_00',
                                                  'gain_05_y_mean': 'gain_05',
                                                  'gain_10_y_mean': 'gain_10'})

    # Select the best ones (i.e., ones where the average gain meets our goal
    average_gains = average_gains[(average_gains['mean_gain_pct'] >= mean_gain_threshold)
                                  & (average_gains['count'] >= min_count)
                                  & (average_gains['independent_symbol'] != average_gains['dependent_symbol'])]
    return average_gains


trade_tracker_df = pd.DataFrame()
for trading_date in trading_dates(timestamp('2023-12-01'), timestamp('2024-05-31')):
    # for trading_date in [timestamp('2023-12-01')]:

    print(f"c=fastFollower a=trade s=started date={date_string(trading_date)} " +
          f"starting_balance={round(buying_power, 2)}")

    trading_pairs = get_trading_pairs(trading_date)

    trade_amount = 10000

    # make trade identifiers for each symbol
    trade_identifiers = list()  # list of FastFollowerTradeIdentifiers
    for entry in trading_pairs.itertuples(index=False):
        identifier = FastFollowerTradeIdentifier(entry.independent_symbol, entry.dependent_symbol,
                                                 ind_trigger_pct, dep_trigger_pct, earliest_trade_time)
        trade_identifiers.append(identifier)

    # walk through the day and make trades
    trade_executors = list()

    for delta in pd.timedelta_range(start='09:35:00', end='16:00:00', freq='5min'):
        decision_time = trading_date + delta
        sleep_until_time(decision_time + pd.Timedelta('00:00:02'), 'hhhl', 'wait_for_bar')
        print(f"c=fastFollower a=tradeDuringInterval s=started decisionTime={time_string(decision_time)} " +
              f"a={round(buying_power, 2)}")

        # logging.info(f"c=hhhl a=tradeDuringInterval s=started decisionTime={decision_time} " +
        #              f"a={round(buying_power, 2)}")
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
                    shares = round(trade_amount/candidate['target_buy_price'])
                    executor = TimedHoldLongTradeExecutor(candidate['symbol'], shares, decision_time,
                                                          candidate['target_buy_price'], 15)
                    print(f"c=fastFollower a=selectTrade d={time_string(decision_time)} "
                          + f"sym={candidate['symbol']} direction=LONG")
                    # logging.info(f"c=hhhl a=selectTrade d={datetime_string(decision_time)} "
                    #              + f"sym={candidate['symbol']} direction=LONG")
                    trade_executors.append(executor)
        # Capture the Market trades (so we don't have to wait until the next bar)
        # sleep_until_time(decision_time + pd.Timedelta('00:00:15'), 'hhhl', 'wait_for_buys')
        # trade_time = decision_time + pd.Timedelta(minutes=5)
        # buying_power, realized_profit, current_profit = process_trade_executors(trade_executors, trade_time,
        #                                                                         buying_power=buying_power)

        print(f"c=fastFollower a=tradeDuringInterval s=completed decisionTime={time_string(decision_time)} "
              + f"realizedProfit=${round(realized_profit, 2)} currentProfit=${round(current_profit, 2)}")
        # logging.info(f"c=hhhl a=tradeDuringInterval s=completed decisionTime={decision_time} "
        #              + f"realizedProfit=${round(realized_profit, 2)} currentProfit=${round(current_profit, 2)}")

trade_tracker_df = trade_tracker().to_dataframe()
trade_tracker_df = trade_tracker_df.round(4)
filename = f"purchaseTracker_{date_string(trading_date).replace('-', '')}_{get_trade_mode().name}.csv"
trade_tracker_df.to_csv(temp_files_path(filename), index=False)
