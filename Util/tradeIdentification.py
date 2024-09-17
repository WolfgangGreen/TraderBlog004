# This file defines classes that hold finite state machines that identify trade opportunities for single-stock
# and multi-stock trades.
#
# The class for single-stock trades supports the following methods:
#   __init__(self, symbol, initial_state) -- constructor
#   consume_1min_bar(Series) -- returns (True, details) if there is a trading opportunity
#   consume_5min_bar(Series) -- returns (True, details) if there is a trading opportunity
#   consume_snapshot(dict) -- returns (True, details) if there is a trading opportunity
#
# details will be a dictionary containing details of the trade opportunity (e.g., symbol, target price,
# direction, ...)

# The class for multi-stock trades supports the following methods:
#   __init__(self, symbols, initial_state) -- constructor
#   consume_1min_bars(DataFrame)  -- returns (True, details) if there is a trading opportunity
#   consume_5min_bars(DataFrame)  -- returns (True, details) if there is a trading opportunity
#   consume_snapshots(dict)  -- returns (True, details) if there is a trading opportunity

import pandas as pd
from Util.datesAndTimestamps import timestamp, date_string, time_string, previous_trading_date


class SingleStockTradeIdentifier:
    def __init__(self, symbol, initial_state):
        self.symbol = symbol
        self.state = initial_state

    def consume_1min_bar(self, bar):
        return False, None

    def consume_5min_bar(self, bar):
        return False, None

    def consume_snapshot(self, snapshot):
        return False, None


class DoubleStockTradeIdentifier:
    def __init__(self, symbol1, symbol2, initial_state):
        self.symbol1 = symbol1
        self.symbol2 = symbol2
        self.state = initial_state

    def consume_1min_bars(self, symbol1_bar, symbol2_bar):
        return False, None

    def consume_5min_bars(self, symbol1_bar, symbol2_bar):
        return False, None

    def consume_snapshots(self, symbol1_snapshot, symbol2_snapshot):
        return False, None


# region HigherHighsHigherLowsTradeIdentifier
# States:
#   1. looking_for_trades
#
# The parameters in the constructor:
#   symbol: symbol we are considering for this trade identifier
#   minimum_gain_pct: only consider trades where the sum of the gains between high(-3) and high(-1) and between
#       low(-3) and low(-1) >= minimum_gain_pct
#   maximum_drop_pct: only consider trades where the drop between high(-1) and close(-1) <= maximum_drop_pct
#   earliest_trade_time: don't consider trades where decision_time is before earliest_trade_time

class HigherHighsHigherLowsTradeIdentifier(SingleStockTradeIdentifier):
    def __init__(self, symbol, minimum_gain_pct, maximum_drop_pct, earliest_trade_time):
        SingleStockTradeIdentifier.__init__(self, symbol, 'looking_for_trades')
        self.minimum_gain_pct = minimum_gain_pct
        self.maximum_drop_pct = maximum_drop_pct
        self.earliest_trade_time = earliest_trade_time
        self.recent_bars = list()  # list of Series, containing bars from the previous 10 minutes

    def consume_1min_bar(self, bar):
        return False, None

    def consume_5min_bar(self, bar):
        current_time = time_string(bar['timestamp'] + pd.Timedelta(minutes=5))
        self.recent_bars = self.recent_bars[-2:]
        self.recent_bars.append(bar)
        if current_time >= self.earliest_trade_time and len(self.recent_bars) == 3:
            # first, check for higher highs and higher lows
            b0, b1, b2 = self.recent_bars[0], self.recent_bars[1], self.recent_bars[2]
            if not ((b0['high'] < b1['high'] < b2['high']) and (b0['low'] < b1['low'] < b2['low'])):
                return False, None
            # Then check how much the highs and lows increased over the past 3 bars)
            high_gain_pct = 100 * (bar['high'] / self.recent_bars[0]['high'] - 1)
            low_gain_pct = 100 * (bar['low'] / self.recent_bars[0]['low'] - 1)
            # ... and how much the price dropped in the most recent bar
            drop_pct = 100 * (bar['high'] / bar['close'] - 1)
            if high_gain_pct + low_gain_pct >= self.minimum_gain_pct and drop_pct <= self.maximum_drop_pct:
                details = {'symbol': self.symbol, 'target_buy_price': float(round(bar['close'], 4))}
                return True, details
        return False, None

    def consume_snapshot(self, snapshot):
        return False, None
# endregion / HigherHighsHigherLowsTradeIdentifier


# region FastFollowerTradeIdentifier
# States:
#   1. looking_for_trades
#
# The parameters in the constructor:
#   symbol: symbol we are considering for this trade identifier
#   minimum_gain_pct: only consider trades where the sum of the gains between high(-3) and high(-1) and between
#       low(-3) and low(-1) >= minimum_gain_pct
#   maximum_drop_pct: only consider trades where the drop between high(-1) and close(-1) <= maximum_drop_pct
#   earliest_trade_time: don't consider trades where decision_time is before earliest_trade_time

class FastFollowerTradeIdentifier(DoubleStockTradeIdentifier):
    def __init__(self, independent_symbol, dependent_symbol, ind_trigger_pct, dep_trigger_pct, earliest_trade_time):
        DoubleStockTradeIdentifier.__init__(self, independent_symbol, dependent_symbol, 'looking_for_trades')
        self.independent_trigger_pct = ind_trigger_pct
        self.dependent_trigger_pct = dep_trigger_pct
        self.earliest_trade_time = earliest_trade_time

    def consume_1min_bars(self, symbol1_bar, symbol2_bar):
        return False, None

    def consume_5min_bars(self, symbol1_bar, symbol2_bar):
        current_time = time_string(symbol1_bar['timestamp'] + pd.Timedelta(minutes=5))
        if current_time >= self.earliest_trade_time:
            ind_trigger_last_5_pct = 100 * (symbol1_bar['close'] / symbol1_bar['open'] - 1)
            dep_trigger_last_5_pct = 100 * (symbol2_bar['close'] / symbol2_bar['open'] - 1)
            if (ind_trigger_last_5_pct <= self.independent_trigger_pct
                    or dep_trigger_last_5_pct <= self.dependent_trigger_pct):
                return False, None
            details = {'symbol': self.symbol2, 'target_buy_price': float(round(symbol2_bar['close'], 4))}
            return True, details
        return False, None

    def consume_snapshots(self, symbol1_snapshot, symbol2_snapshot):
        return False, None
# endregion / FastFollowerTradeIdentifier
