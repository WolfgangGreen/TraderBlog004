# A trade tracker collects all the important data on each stock trade that we make
#
# It stores the following data items and can dump them to a DataFrame
#  - decision_time: pandas Timestamp for the moment where we are deciding to buy the stock
#  - symbol (Index): what stock is being traded
#  - shares: number of shares purchased
#  - position_side: PositionSide.LONG or PositionSide.SHORT
#  - outcome:
#      For longs, one of: 'take_profit', 'market_sell', 'stop_loss'
#      For shorts, one of: 'take_profit', 'market_buy', 'stop_loss'
#  - buy_time: timestamp for when the stock was bought. For shorts, buy_time is after sell_time
#  - target_buy_price: price quote (per share) before the market order
#  - actual_buy_price: average price actually paid for each share
#  - sell_time: timestamp for when the stock was sold
#  - target_sell_price: price quote before the market order (or stop_limit)
#  - actual_sell_price: price received for each share
#  - actual_gain: what % gain was received
#  - actual_profit: what $ gain was received
#  - current_price: the most recent price quote for the stock (used for computing current portfolio value)
#  - data: an optional dict to store additional data that is particular to the trade strategy

# For short sells, we use the same alpacaOperations procedures as long buys, but they are exercised in the
# reverse direction. I.e., we start with a Sell and end with a Buy

import pandas as pd

from alpaca.trading.enums import OrderSide, OrderType, PositionSide

from Util.datesAndTimestamps import time_string

trade_tracker_fields_for_csv = ['decision_time', 'symbol', 'shares', 'position_side', 'outcome',
                                'buy_time', 'target_buy_price', 'actual_buy_price',
                                'sell_time', 'target_sell_price', 'actual_sell_price', 'actual_gain', 'actual_profit']

global_trade_tracker = None


def trade_tracker():
    global global_trade_tracker
    if not global_trade_tracker:
        global_trade_tracker = TradeTracker()
    return global_trade_tracker


class TradeTracker:
    active_trades = dict()  # (symbol, decision_time): TradeInfo
    closed_trades = dict()  # (symbol, decision_time): TradeInfo

    def open_trade(self, symbol, decision_time, position_side, data=None):
        key = (symbol, decision_time)
        trade = TradeInfo(symbol, decision_time, position_side, data=None)
        self.active_trades[key] = trade
        return trade

    def get_trade_info(self, symbol, decision_time):
        if (symbol, decision_time) in self.closed_trades:
            return self.closed_trades[(symbol, decision_time)]
        if (symbol, decision_time) in self.active_trades:
            return self.active_trades[(symbol, decision_time)]
        return None

    def close_trade(self, trade):
        if (trade.symbol, trade.decision_time) in self.active_trades:
            self.active_trades.pop((trade.symbol, trade.decision_time))
            self.closed_trades[(trade.symbol, trade.decision_time)] = trade

    def to_dataframe(self):
        rows = list()  # list of lists of values for each trade
        for trade in list(self.closed_trades.values()) + list(self.active_trades.values()):
            rows.append(trade.trade_values())
        trade_tracker_dataframe = pd.DataFrame(data=rows, columns=trade_tracker_fields_for_csv)
        return trade_tracker_dataframe

    def active_symbols(self):
        symbols = set()
        for trade in self.active_trades.values():
            symbols.add(trade.symbol)
        return list(symbols)


class TradeInfo:
    def __init__(self, symbol, decision_time, position_side, data=None):
        self.symbol = symbol  # string
        self.decision_time = decision_time  # timestamp
        self.position_side = position_side  # PositionSide enum (LONG or SHORT)
        self.data = data
        self.shares = 0
        self.outcome = None  # 'take_profit', 'stop_loss', 'market_sell', 'not_filled', 'market_buy'
        self.buy_time = None
        self.target_buy_price = None
        self.actual_buy_price = None
        self.sell_time = None
        self.target_sell_price = None
        self.actual_sell_price = None
        self.actual_gain = None  # 100 * (actual_sell_price / actual_buy_price - 1)
        self.actual_profit = None  # shares * (actual_sell_price - actual_buy_price)
        self.current_price = None
        self.active_orders = list()  # list of OrderInfo
        self.closed_orders = list()  # list of OrderInfo

    def get_active_order(self, order_side, order_type):
        for order in self.active_orders:
            if (order.order_side == order_side) and (order.order_type == order_type):
                return order
        return None

    def add_order(self, order_direction, order_type, price, order_start, order_id):
        order = OrderInfo(order_direction, order_type, self.symbol, self.decision_time,
                          self.shares, price, order_start, order_id)
        self.active_orders.append(order)
        return order

    def close_order(self, order, status, actual_price, order_end):
        order.close_order(status, actual_price, order_end)
        self.active_orders.remove(order)
        self.closed_orders.append(order)

    def trade_values(self):
        buy_time = time_string(self.buy_time) if self.buy_time else None
        sell_time = time_string(self.sell_time) if self.sell_time else None
        return [self.decision_time, self.symbol, self.shares, self.position_side, self.outcome,
                buy_time, self.actual_buy_price, self.actual_buy_price,
                sell_time, self.target_sell_price, self.actual_sell_price, self.actual_gain,
                self.actual_profit]

    def add_market_buy_order(self, shares, target_buy_price, order_start, order_id):
        self.shares = shares
        self.target_buy_price = target_buy_price
        return self.add_order(OrderSide.BUY, OrderType.MARKET, target_buy_price, order_start, order_id)

    def add_market_sell_order(self, shares, target_sell_price, order_start, order_id):
        self.shares = shares
        self.target_sell_price = target_sell_price
        return self.add_order(OrderSide.SELL, OrderType.MARKET, target_sell_price, order_start, order_id)

    def add_buy_order_execution(self, order_type, status, actual_purchase_price, order_end):
        self.buy_time = order_end
        self.actual_buy_price = actual_purchase_price
        self.current_price = actual_purchase_price
        if self.position_side == PositionSide.SHORT:  # If it's a short sell, we're done
            self.actual_gain = 100 * (self.actual_sell_price / self.actual_buy_price - 1)
            self.actual_profit = self.shares * (self.actual_sell_price - self.actual_buy_price)
            self.outcome = order_type
        order = self.get_active_order(OrderSide.BUY, order_type)
        self.close_order(order, status, actual_purchase_price, order_end)

    def add_sell_order_execution(self, order_type, status, actual_sell_price, order_end):
        self.sell_time = order_end
        self.actual_sell_price = actual_sell_price
        self.current_price = actual_sell_price
        self.actual_gain = 0
        self.actual_profit = 0
        if self.position_side == PositionSide.LONG:  # If it's a long sell, we're done
            self.outcome = order_type
            if self.actual_sell_price and self.actual_buy_price:
                self.actual_gain = 100 * (self.actual_sell_price / self.actual_buy_price - 1)
                self.actual_profit = self.shares * (self.actual_sell_price - self.actual_buy_price)
            else:
                print(f'add_sell_order_execution issue: {self.actual_buy_price} {self.actual_sell_price}')
        order = self.get_active_order(OrderSide.SELL, order_type)
        if order:
            self.close_order(order, status, actual_sell_price, order_end)
        else:
            print(f"Didn't find SELL order of {order_type}")

    def current_profit(self):
        if (self.position_side == PositionSide.LONG) and self.current_price and self.actual_buy_price:
            return self.shares * (self.current_price - self.actual_buy_price)
        elif (self.position_side == PositionSide.SHORT) and self.current_price and self.actual_sell_price:
            return self.shares * (self.actual_sell_price - self.current_price)
        return 0


class OrderInfo:
    def __init__(self, order_side, order_type, symbol, decision_time, shares, target_price, order_start, order_id):
        self.order_side = order_side  # OrderSide.BUY, OrderSide.SELL
        self.order_type = order_type  # OrderType.MARKET, OrderType.LIMIT, OrderType.STOP, ...
        self.symbol = symbol
        self.decision_time = decision_time
        self.shares = shares
        self.target_price = target_price
        self.order_start = order_start  # timestamp of when the order was placed
        self.order_id = order_id  # UUID from Alpaca
        self.status = 'not_filled'  # or 'filled', 'partially_filled', 'cancelled'

    actual_price = None
    order_end = None  # timestamp of when the order ended

    def update_status(self, status):
        self.status = status

    def close_order(self, status, actual_price, order_end):
        self.status = status
        self.actual_price = actual_price
        self.order_end = order_end
