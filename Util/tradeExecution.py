# This file contains two components that drive a trade plan
#   1. Trade_Identifier: A declarative description of the situation where a trade should be executed
#   2. Trade_Executor: A sequence of orders to place (with conditions) to initiate and complete the trade
#
# For right now, I'm just implementing the 2nd one

from alpaca.trading.enums import OrderSide, OrderType, PositionSide

from TradingApis.alpacaOperations import (place_market_buy_order, place_market_sell_order,
                                          get_latest_bar, process_orders_for_trade)
from Util.purchaseTracker import trade_tracker


# What are the components of a trade plan?
#   1. Buy decision. Two cases:
#     A. Buy if the stock price falls to a certain level (classic Limit Buy)
#     B. Buy if the stock price rises to a certain level (maybe this can be a Stop Limit Buy? Maybe it's manual?)
#   2. Initial stop loss. Can be a percentage loss, or a derived level
#   3. First target level:
#     A. What is the stock price that we'll take our first action on?
#     B. What is the action?: revise stop loss; sell X%
#   4. Second target level
#     A. ...
#   5. ...
#   n. Final target
#     A. What is the price at which we declare victory and end the trade?
#     B. What is the time at which we declare the trade over?


class SingleStockTradeExecutor:
    def __init__(self, trade, state):
        self.trade = trade
        self.state = state

    def handle_order_fill(self, order, ts):
        return False

    def consume_1min_bar(self, bar, ts):
        return False

    def consume_5min_bar(self, bar, ts):
        return False

    def consume_snapshot(self, snapshot, ts):
        return False


def process_trade_executors(trade_executors, decision_time, buying_power=0):
    current_profit = 0
    realized_profit = 0
    for executor in trade_executors:
        trade = executor.trade
        if executor.state != 'complete':
            _, amount_transacted, completed_order = process_orders_for_trade(trade, decision_time)
            if completed_order:
                buying_power += amount_transacted
                is_done = executor.handle_order_fill(completed_order, decision_time)
                if is_done:
                    executor.state = 'complete'
                    trade_tracker().close_trade(trade)
        if executor.state != 'complete':
            success, bar = get_latest_bar(trade.symbol, decision_time)
            if success:
                executor.consume_5min_bar(bar, decision_time)
        if executor.state == 'complete':
            realized_profit += trade.current_profit()
        current_profit += trade.current_profit()
    return buying_power, realized_profit, current_profit


# region TimedHoldLongTradeExecutor
# Simple executor that buys a stock, holds it for a fixed time, and then sells it
# States: 'buy', 'hold', 'sell', 'complete'
class TimedHoldLongTradeExecutor(SingleStockTradeExecutor):
    def __init__(self, symbol, shares, decision_time, target_buy_price, hold_duration):
        self.decision_time = decision_time
        self.actual_buy_price = None
        self.hold_duration = hold_duration
        trade = trade_tracker().open_trade(symbol, decision_time, PositionSide.LONG)
        _ = place_market_buy_order(trade, shares, target_buy_price, decision_time)
        SingleStockTradeExecutor.__init__(self, trade, 'buy')

    def handle_order_fill(self, order, ts):
        if (self.state == 'buy') and (order.order_side == OrderSide.BUY):  # Our purchase completed; set the stop loss
            self.actual_buy_price = order.actual_price
            self.state = 'hold'
            return False
        elif (self.state == 'sell') and (order.order_side == OrderSide.SELL):  # Trade timed out
            self.state = 'complete'
            return True
        else:
            print(f"SimpleLongTradeExecutor: Unknown trade for {self.state}: {order.order_side} {order.order_type}")
            return False

    def consume_1min_bar(self, bar, ts):
        return False

    def consume_5min_bar(self, bar, ts):
        if (ts - self.decision_time).seconds >= self.hold_duration * 60:  # Cash out after X minutes
            target_sell_price = bar['close']
            _ = place_market_sell_order(self.trade, self.trade.shares, target_sell_price, ts)
            self.state = 'sell'
        return False

    def consume_snapshot(self, snapshot, ts):
        return False
# endregion / TimedHoldLongTradeExecutor
