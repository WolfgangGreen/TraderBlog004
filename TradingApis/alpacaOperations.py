# This file contains procedures for interacting with Alpaca--and for simulating interacting with Alpaca. In this
# version, we are just simulating activity. We'll add the logic for interacting with the broker API in the next
# article. The key thing is that the interfaces that our Trade Identifiers and Trade Executors use are the same
# for SIMULATION, PAPER, and PRODUCTION

import logging
import pandas as pd

from alpaca.trading.enums import OrderSide, OrderType, PositionSide

from ReportProcessing.intradayDetailReport import read_intraday_details
from Util.datesAndTimestamps import most_recent_bar_time
from Util.pathsAndStockSets import get_symbols


def place_market_buy_order(trade, shares, target_purchase_price, order_start):
    # NOTE: For SIMULATION, we'll automatically get our target purchase price, if it's in the next bar
    order = trade.add_market_buy_order(shares, target_purchase_price, order_start, 'SIM')
    return order


def place_market_sell_order(trade, shares, target_sell_price, order_start):
    order = trade.add_market_sell_order(shares, target_sell_price, order_start, 'SIM')
    return order


# get the bar for the designated symbol for the time interval that would be available at decision_time.
# E.g., at 10:41:00, the latest bar would be for 10:35:00 (covering 10:35:00 - 10:39:59)
#
# We return two values: success and bar. Bar is a pandas Series (if successful) or None

def get_latest_bar(symbol, decision_time, freq=5):
    bar_time = most_recent_bar_time(decision_time, freq=freq)
    bars = get_bars(bar_time, bar_time, symbols=[symbol], freq=freq)
    if len(bars) > 0:
        return True, bars.iloc[0]
    return False, None


# We cache the Intraday Details for the day that has most recently been queried, so that we don't have to keep reading
# the files. This works well for day trading, because we only look at details for a single day at a time.

saved_daily_details_1min = pd.DataFrame()
saved_daily_details_5min = pd.DataFrame()


# If the bars for the designated range are already cached, use them. Otherwise, read the appropriate file.
# If symbols is provided, we filter the bars to contain just those bars

def get_bars(start, end, symbols=None, freq=5):
    global saved_daily_details_1min, saved_daily_details_5min
    symbols = symbols if symbols else get_symbols()
    if freq == 1:
        if not daily_details_contains_range(saved_daily_details_1min, start, end):
            saved_daily_details_1min = read_intraday_details(start, report_end=end, freq=1)
        results = saved_daily_details_1min[saved_daily_details_1min['timestamp'].between(start, end)
                                           & saved_daily_details_1min['symbol'].isin(symbols)]
        return results
    else:  # freq == 5
        if not daily_details_contains_range(saved_daily_details_5min, start, end):
            saved_daily_details_5min = read_intraday_details(start, report_end=end, freq=5)
        results = saved_daily_details_5min[saved_daily_details_5min['timestamp'].between(start, end)
                                           & saved_daily_details_5min['symbol'].isin(symbols)]
        return results


def daily_details_contains_range(daily_details, start, end):
    if len(daily_details) == 0:
        return False
    else:
        min_timestamp = daily_details['timestamp'].min()
        max_timestamp = daily_details['timestamp'].max()
        return (min_timestamp <= start) and (end <= max_timestamp)


#

def process_orders_for_trade(trade, decision_time, freq=5):
    for order in list(trade.active_orders):
        completed, price, order_end = check_order_status(order, trade, decision_time, freq=freq)
        if completed:
            if order.order_side == OrderSide.BUY:  # Our initial BUY has completed
                trade.add_buy_order_execution(order.order_type, 'filled', price, order_end)
                logging.info(f"c=alpacaOps a=startTrade d={order_end} sym={order.symbol} price={price} side=LONG")
                return False, -trade.shares * price, order
            else:  # Our final SELL has completed to close the position
                trade.add_sell_order_execution(order.order_type, 'filled', price, order_end)
                logging.info(f"c=alpacaOps a=endTrade d={order_end} sym={order.symbol} price={price} "
                             + f"result={order.order_type.name} gain={round(trade.actual_gain, 2)}%")
                cancel_orders_for_trade(trade, decision_time)
                return True, trade.shares * price, order
    return False, 0, None


def check_order_status(order, trade, decision_time, freq=5):
    success, bar = get_latest_bar(order.symbol, decision_time, freq=freq)
    if success:
        if order.order_type == OrderType.MARKET:
            return True, bar['open'], decision_time
            # return True, order.target_price, decision_time
        # NOTE: We'll be adding a lot more logic here later
    return False, None, None


def cancel_orders_for_trade(trade, trade_end):
    for order in list(trade.active_orders):
        # NOTE: We'll add logic for interacting with the broker API later
        trade.close_order(order, 'cancelled', 0, trade_end)
