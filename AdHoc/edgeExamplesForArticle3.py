import pandas as pd

from ReportProcessing.intradayDetailReport import read_intraday_details, extract_symbol_details
from StockTraders.fastFollowerHelpers import get_trading_pairs
from Util.datesAndTimestamps import timestamp, time_string, trading_dates, previous_trading_date
from Util.pathsAndStockSets import StockSet, set_stock_set, get_symbols, temp_files_path

set_stock_set(StockSet.SP500)


# Higher Highs; Higher Lows. First approach: look for 3 candles where highs and lows increase; see what percentage gain
# we have after 10 minutes
if False:
    results = list()  # List of DataFrame
    for ts in trading_dates(timestamp('2023-12-01'), timestamp('2024-05-31')):
        print(ts)
        intraday_details = read_intraday_details(ts)
        for symbol in get_symbols():
            symbol_details = extract_symbol_details(intraday_details, symbol)
            symbol_details['decision_time'] = symbol_details['timestamp'] + pd.Timedelta('00:05:00')
            shift2 = symbol_details.shift(2)  # Bars covering decision_time - 15:00 to decision_time - 10:01
            shift1 = symbol_details.shift(1)  # Bars covering decision_time - 10:00 to decision_time - 5:01

            # Compute percent changes at each shift
            symbol_details['high_pct_2'] = 100 * (shift1['high'] / shift2['high'] - 1)
            symbol_details['high_pct_1'] = 100 * (symbol_details['high'] / shift1['high'] - 1)
            symbol_details['low_pct_2'] = 100 * (shift1['low'] / shift2['low'] - 1)
            symbol_details['low_pct_1'] = 100 * (symbol_details['low'] / shift1['low'] - 1)

            # See if we trigger and compute profit%
            symbol_details['hhhl_trigger'] = ((symbol_details['high_pct_1'] > 0)
                                              & (symbol_details['high_pct_2'] > 0)
                                              & (symbol_details['low_pct_1'] > 0)
                                              & (symbol_details['low_pct_2'] > 0))
            symbol_details['buy_price'] = symbol_details.shift(-1)['open']  # We purchase at the open of the next bar
            symbol_details['sell_price'] = symbol_details.shift(-3)['open']  # ... and sell 10 minutes later
            symbol_details['profit_pct'] = 100 * (symbol_details['sell_price'] / symbol_details['buy_price'] - 1)
            hits = symbol_details[symbol_details['hhhl_trigger'] & symbol_details['profit_pct']]
            results.append(hits)
    results_df = pd.concat(results)
    results_df = results_df.round(4)
    results_df.to_csv(temp_files_path('hhhl_results_first_try.csv'), index=False)

# Higher Highs; Higher Lows. Second approach: Filter out the following:
#    cases where total gain is less than 4%
#    cases where high on last bar is more than 0.25% higher than the close
#    cases where decision time is 9:45 (meaning it relies on the initial bar)
if False:
    gain_threshold = 4  # The sum of the four gains must be at least 4%
    max_high_close_gap = 0.25  # The last high must be at most 0.25% higher than the close
    look_forward = 3  # Take profit in look_forward * 5 minutes
    results = list()  # List of DataFrame
    for ts in trading_dates(timestamp('2024-06-01'), timestamp('2024-07-18')):
        print(ts)
        intraday_details = read_intraday_details(ts)
        for symbol in get_symbols():
            symbol_details = extract_symbol_details(intraday_details, symbol)
            symbol_details['decision_time'] = symbol_details['timestamp'] + pd.Timedelta('00:05:00')
            shift2 = symbol_details.shift(2)  # Bars covering decision_time - 15:00 to decision_time - 10:01
            shift1 = symbol_details.shift(1)  # Bars covering decision_time - 10:00 to decision_time - 5:01

            # Compute percent changes at each shift
            symbol_details['high_pct_2'] = 100 * (shift1['high'] / shift2['high'] - 1)
            symbol_details['high_pct_1'] = 100 * (symbol_details['high'] / shift1['high'] - 1)
            symbol_details['low_pct_2'] = 100 * (shift1['low'] / shift2['low'] - 1)
            symbol_details['low_pct_1'] = 100 * (symbol_details['low'] / shift1['low'] - 1)
            symbol_details['score'] = (symbol_details['high_pct_1'] + symbol_details['high_pct_2']
                                       + symbol_details['low_pct_1'] + symbol_details['low_pct_2'])
            symbol_details['high_to_close'] = 100 * (symbol_details['high'] / symbol_details['close'] - 1)

            # See if we trigger and compute profit%
            symbol_details['hhhl_trigger'] = ((symbol_details['high_pct_1'] > 0)
                                              & (symbol_details['high_pct_2'] > 0)
                                              & (symbol_details['low_pct_1'] > 0)
                                              & (symbol_details['low_pct_2'] > 0))
            symbol_details['buy_price'] = symbol_details.shift(-1)['open']  # We purchase at the open of the next bar
            symbol_details['sell_price'] = symbol_details.shift(-3)['open']  # ... and sell 10 minutes later
            symbol_details['profit_pct'] = 100 * (symbol_details['sell_price'] / symbol_details['buy_price'] - 1)
            hits = symbol_details[symbol_details['hhhl_trigger']
                                  & (symbol_details['score'] >= gain_threshold)
                                  & (symbol_details['high_to_close'] <= max_high_close_gap)
                                  & (symbol_details['time'] != '09:40:00')
                                  & symbol_details['profit_pct']]
            results.append(hits)
    results_df = pd.concat(results)
    results_df = results_df.round(4)
    results_df.to_csv(temp_files_path('hhhl_results_second_try.csv'), index=False)

# Fast Follower Strategy -- First Approach
if False:
    lookback_window = 10  # Look at the previous 10 trading days when finding correlations (two weeks)
    trigger_gain_threshold = 1.0  # Trigger if the stock goes up 1% during the trigger window
    mean_gain_threshold = 0.5  # Only accept pairs with average gain of 0.5% per trade in the training
    min_count = 5  # Only accept pairs if there were at least 5 instances in the training set (every other day)
    effect_window = 3  # Sell after 15 minutes

    def compute_trigger_and_effect_df(intraday_details):
        symbol_results = list()  # List of DataFrame (one for each symbol)
        for symbol in get_symbols():
            s_hist = extract_symbol_details(intraday_details, symbol)
            s_hist['trigger_pct'] = 100 * (s_hist['close'] / s_hist.shift(2)['open'] - 1)
            s_hist['gain_pct'] = 100 * (s_hist.shift(-effect_window)['close'] / s_hist.shift(-1)['open'] - 1)
            s_hist = s_hist.dropna()  # Drop rows where we don't have gain_pct (usually beginning or end of day)
            symbol_results.append(s_hist)
        symbol_results_df = pd.concat(symbol_results)
        symbol_results_df = symbol_results_df.reset_index(drop=True)
        return symbol_results_df

    # Collect the testing results for each day
    results = list()  # List of DataFrame
    for ts in trading_dates(timestamp('2023-12-01'), timestamp('2024-05-31')):
        print(ts)

        # Compute the training values
        train_details = read_intraday_details(previous_trading_date(ts, offset=lookback_window),
                                              previous_trading_date(ts))
        training_set = compute_trigger_and_effect_df(train_details)

        # Compute expected gain for each trigger
        triggers = training_set[training_set['trigger_pct'] >= trigger_gain_threshold]
        cross_join = triggers.merge(training_set, on='timestamp')
        average_gains = cross_join.groupby(['symbol_x', 'symbol_y']).agg({'gain_pct_y': ['count', 'mean']})
        average_gains = average_gains.reset_index(drop=False)
        average_gains.columns = average_gains.columns.to_flat_index().str.join('_')  # Flatten the multi-index
        average_gains = average_gains.rename(columns={'symbol_x_': 'independent_symbol',
                                                      'symbol_y_': 'dependent_symbol',
                                                      'gain_pct_y_count': 'count',
                                                      'gain_pct_y_mean': 'mean_gain_pct'})

        # Select the best ones (i.e., ones where the average gain meets our goals)
        average_gains = average_gains[(average_gains['mean_gain_pct'] >= mean_gain_threshold)
                                      & (average_gains['count'] >= min_count)
                                      & (average_gains['independent_symbol'] != average_gains['dependent_symbol'])]

        # Find the trades and results
        test_details = read_intraday_details(ts)  # the day following the training set
        test_details['decision_time'] = test_details['timestamp'] + pd.Timedelta('00:05:00')
        test_details['time'] = test_details['decision_time'].map(lambda d: time_string(d))

        testing_set = compute_trigger_and_effect_df(test_details)
        triggers = testing_set[testing_set['trigger_pct'] >= trigger_gain_threshold]
        cross_join = triggers.merge(testing_set, on='timestamp')
        cross_join = cross_join.rename(columns={'symbol_x': 'independent_symbol',
                                                'symbol_y': 'dependent_symbol',
                                                'decision_time_y': 'decision_time',
                                                'date_x': 'date',
                                                'time_y': 'time',
                                                'trigger_pct_x': 'trigger_pct',
                                                'gain_pct_y': 'gain_pct'})
        filter_df = average_gains[['independent_symbol', 'dependent_symbol']]
        test = pd.merge(cross_join, average_gains, on=['independent_symbol', 'dependent_symbol'])
        test = test[['decision_time', 'independent_symbol', 'dependent_symbol', 'date', 'time',
                     'trigger_pct', 'count', 'mean_gain_pct', 'gain_pct']]
        results.append(test)
    results_df = pd.concat(results).round(4)
    results_df.to_csv(temp_files_path('fast_follower_results_6mo_first_try.csv'), index=False)

# Fast Follower Strategy -- Second Approach. Include additional fields to support filtering
#   compute change over last 5 minutes, 10 minutes, and 15 minutes
#   compute how often a dependent stock rises at least 0%, 0.5%, and 1.0%
if False:
    lookback_window = 10  # Look at the previous 10 trading days when finding correlations
    trigger_gain_threshold = 1.0  # Only look at independent stocks which gained at least 1.0% in the most recent bar
    mean_gain_threshold = 0.5  # Only accept pairs with average gain of 0.5% per trade in the training
    min_count = 5  # Only accept pairs if there were at least 5 instances in the training set (every other week)
    effect_window = 3  # Sell after 15 minutes
    symbols = get_symbols()


    def compute_trigger_and_effect_df(intraday_details):
        symbol_results = list()  # List of DataFrame (one for each symbol)
        for symbol in symbols:
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

    results = list()  # List of DataFrame
    for ts in trading_dates(timestamp('2023-12-01'), timestamp('2024-05-31')):
        # for ts in trading_dates(timestamp('2024-06-01'), timestamp('2024-07-18')):
        print(ts)
        train_details = read_intraday_details(previous_trading_date(ts, offset=lookback_window),
                                              previous_trading_date(ts))  # 2 weeks
        test_details = read_intraday_details(ts)  # the day following the training set

        # Compute the training values
        training_set = compute_trigger_and_effect_df(train_details)

        # Compute expected gain for each trigger
        triggers = training_set[training_set['trigger_last_15_pct'] >= trigger_gain_threshold]
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

        # Find the trades and results
        testing_set = compute_trigger_and_effect_df(test_details)
        triggers = testing_set[testing_set['trigger_last_15_pct'] >= trigger_gain_threshold]
        cross_join = triggers.merge(testing_set, on='timestamp')
        cross_join = cross_join.rename(columns={'symbol_x': 'independent_symbol',
                                                'symbol_y': 'dependent_symbol',
                                                'date_x': 'date',
                                                'time_x': 'time',
                                                'trigger_pct_x': 'trigger_pct',
                                                'gain_pct_y': 'gain_pct'})
        filter_df = average_gains[['independent_symbol', 'dependent_symbol']]
        test = pd.merge(cross_join, average_gains, on=['independent_symbol', 'dependent_symbol'])
        test = test[['timestamp', 'independent_symbol', 'dependent_symbol', 'date', 'time',
                     'trigger_last_05_pct_x', 'trigger_last_10_pct_x', 'trigger_last_15_pct_x',
                     'trigger_last_05_pct_y', 'trigger_last_10_pct_y', 'trigger_last_15_pct_y',
                     'count', 'mean_gain_pct', 'gain_00', 'gain_05', 'gain_10',
                     'gain_pct']]
        results.append(test)
    results_df = pd.concat(results).round(4)
    results_df.to_csv(temp_files_path('fast_follower_results_training_set.csv'), index=False)
    # results_df.to_csv(temp_files_path('fast_follower_results_test_set.csv'), index=False)

# Fast Follower Strategy -- Third Approach. Refactor so that we use common code with fastFollower.py (using procedures
# in fastFollowerHelpers.py)
if True:
    lookback_window = 10  # Look at the previous 10 trading days when finding correlations
    effect_window = 15  # Sell after 15 minutes
    trigger_pct = 1.0  # Only look at independent stocks which gained at least 1.0% in the last 15 minutes
    min_count = 7  # Only accept pairs if there were at least 7 instances in the training set (every other week)
    mean_gain_threshold = 0.5  # Only accept pairs with average gain of 0.5% per trade in the training
    success_rate_05 = 0.666  # Only accept pairs that gain 0.5% at least 2/3 of the time

    symbols = get_symbols()
    results = list()  # List of DataFrame

    for ts in trading_dates(timestamp('2023-12-01'), timestamp('2024-05-31')):
        # for ts in trading_dates(timestamp('2024-06-01'), timestamp('2024-07-18')):
        print(ts)

        trading_pairs = get_trading_pairs(ts, lookback_window=lookback_window, effect_window=effect_window,
                                          trigger_pct=trigger_pct, min_count=min_count,
                                          mean_gain_pct=mean_gain_threshold,
                                          success_rate_05=success_rate_05)

        test_details = read_intraday_details(ts)  # the day following the training set
        testing_set = compute_trigger_and_effect_df(test_details)
        triggers = testing_set[testing_set['trigger_last_15_pct'] >= trigger_pct]
        cross_join = triggers.merge(testing_set, on='timestamp')
        cross_join = cross_join.rename(columns={'symbol_x': 'independent_symbol',
                                                'symbol_y': 'dependent_symbol',
                                                'date_x': 'date',
                                                'decision_time_x': 'decision_time',
                                                'trigger_pct_x': 'trigger_pct',
                                                'gain_pct_y': 'gain_pct'})
        filter_df = trading_pairs[['independent_symbol', 'dependent_symbol']]
        test = pd.merge(cross_join, trading_pairs, on=['independent_symbol', 'dependent_symbol'])
        test = test[['timestamp', 'independent_symbol', 'dependent_symbol', 'date', 'decision_time',
                     'trigger_last_05_pct_x', 'trigger_last_10_pct_x', 'trigger_last_15_pct_x',
                     'trigger_last_05_pct_y', 'trigger_last_10_pct_y', 'trigger_last_15_pct_y',
                     'count', 'mean_gain_pct', 'gain_00', 'gain_05', 'gain_10',
                     'gain_pct']]
        results.append(test)
    results_df = pd.concat(results).round(4)
    results_df.to_csv(temp_files_path('fast_follower_results_training_set.csv'), index=False)
    # results_df.to_csv(temp_files_path('fast_follower_results_test_set.csv'), index=False)
