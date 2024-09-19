# Helper functions for the Fast Followers trading strategy.
# TODO: Also refactor edgeExamplesForArticle3 to use this, instead of inline functions

import pandas as pd

from ReportProcessing.intradayDetailReport import read_intraday_details, extract_symbol_details
from Util.pathsAndStockSets import get_symbols
from Util.datesAndTimestamps import previous_trading_date, time_string


# Get the set of trading pairs for Fast Follower that satisfies the filter criteria
#   trading_date: the date we'll be trading on. Our training data will come from previous days
#   symbol_subset: if provided, we only consider the provided symbols. Otherwise, we use get_symbols()
#   lookback_window: the number of days (preceding trading_date) to use as trading day
#   effect_window: the number of minutes to hold the trade on the dependent stock
#   trigger_pct: we only consider cases where both the stocks go up by trigger_pct in the previous bar
#   min_count: when filtering trading pairs, we only consider pairs that appeared at least this many times
#   mean_gain_pct: we only consider pairs where the average gain was at least this
#   success_rate_05: we only consider pairs where the dependent stock gained 0.5% at least this rate (0.0 to 1.0)

def get_trading_pairs(trading_date, symbol_subset=None, lookback_window=10, effect_window=15,
                      trigger_pct=1.0, min_count=5, mean_gain_pct=0.5, success_rate_05=0.666):
    train_details = read_intraday_details(previous_trading_date(trading_date, offset=lookback_window),
                                          previous_trading_date(trading_date))  # 2 weeks

    # Compute the training values
    training_set = compute_trigger_and_effect_df(train_details, symbol_subset=symbol_subset,
                                                 effect_window=effect_window)

    # Compute expected gain for each trigger
    triggers = training_set[training_set['trigger_last_15_pct'] >= trigger_pct]
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
    average_gains = average_gains[(average_gains['mean_gain_pct'] >= mean_gain_pct)
                                  & (average_gains['gain_05'] >= success_rate_05)
                                  & (average_gains['count'] >= min_count)
                                  & (average_gains['independent_symbol'] != average_gains['dependent_symbol'])]
    return average_gains


def compute_trigger_and_effect_df(intraday_details, symbol_subset=None, effect_window=15):
    symbols = symbol_subset if symbol_subset else get_symbols()
    effect_shift = round(effect_window / 5)
    symbol_results = list()  # List of DataFrame (one for each symbol)
    for symbol in symbols:
        s_hist = extract_symbol_details(intraday_details, symbol)
        s_hist['decision_time'] = s_hist['timestamp'].map(lambda ts: time_string(ts + pd.Timedelta('5m')))
        s_hist['trigger_last_15_pct'] = 100 * (s_hist['close'] / s_hist.shift(2)['open'] - 1)
        s_hist['trigger_last_10_pct'] = 100 * (s_hist['close'] / s_hist.shift(1)['open'] - 1)
        s_hist['trigger_last_05_pct'] = 100 * (s_hist['close'] / s_hist['open'] - 1)
        s_hist['gain_pct'] = 100 * (s_hist.shift(-effect_shift)['close'] / s_hist.shift(-1)['open'] - 1)
        s_hist['gain_00'] = s_hist['gain_pct'] >= 0  # Did we break even?
        s_hist['gain_05'] = s_hist['gain_pct'] >= 0.5  # Did we gain at least 0.5%
        s_hist['gain_10'] = s_hist['gain_pct'] >= 1.0  # Did we gain at least 1.0%
        s_hist = s_hist.dropna()  # Drop rows where we don't have gain_pct (usually end of day)
        symbol_results.append(s_hist)
    symbol_results_df = pd.concat(symbol_results)
    symbol_results_df = symbol_results_df.reset_index(drop=True)
    return symbol_results_df
