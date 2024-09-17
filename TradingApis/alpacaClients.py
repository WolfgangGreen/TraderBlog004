# Set up the different ways of trading and querying with the Alpaca API
# Modes:
#   TradeMode: Production, Paper, Simulated
#   QueryMode: API, File
# Top Level Operations:
#   set_alpaca_modes(new_trade_mode=None, new_query_mode=None):
#   trading_client()
#   historical_client()

# Note: We use OS Environment variables to store our credentials, so they don't appear in the codebase. Shame!

# pip install alpaca-py

import enum
import os

from alpaca.data import StockHistoricalDataClient
from alpaca.data.live.stock import StockDataStream
from alpaca.trading import TradingClient

# Setting up Environment Variables in Windows:
#   1. Open System Properties
#   2. Go to Advanced -> Environment Variables...
#   3. Set up User Variables for the following:
#        - AlpacaPaperKeyId
#        - AlpacaPaperSecretKey
#        - AlpacaProdKeyId
#        - AlpacaProdSecretKey


global_trading_client = None  # client for executing trades
global_historical_client = None  # client for making data requests
global_data_stream = None  # client to set up real-time streaming of data


paper_creds = {
    'end_point': 'https://paper-api.alpaca.markets',
    'APCA-API-KEY-ID': os.environ['AlpacaPaperKeyId'],
    'APCA-API-SECRET-KEY': os.environ['AlpacaPaperSecretKey']
}

prod_creds = {
    'end_point': 'https://api.alpaca.markets',
    'APCA-API-KEY-ID': os.environ['AlpacaProdKeyId'],
    'APCA-API-SECRET-KEY': os.environ['AlpacaProdSecretKey']
}


# Define the different Trade and Query Modes and provide an accessor to set the modes


class TradeMode(enum.Enum):
    PRODUCTION = 1,
    PAPER = 2,
    SIMULATION = 3


class QueryMode(enum.Enum):
    API = 1,
    FILE = 2


trade_mode = TradeMode.SIMULATION
query_mode = QueryMode.FILE


def set_alpaca_modes(new_trade_mode=None, new_query_mode=None):
    global trade_mode, query_mode
    if new_trade_mode:
        trade_mode = new_trade_mode
    if new_query_mode:
        query_mode = new_query_mode


def get_trade_mode():
    return trade_mode


def get_query_mode():
    return query_mode


def trading_client():
    global global_trading_client
    if not global_trading_client:
        if trade_mode == TradeMode.PRODUCTION:
            global_trading_client = TradingClient(prod_creds['APCA-API-KEY-ID'],
                                                  prod_creds['APCA-API-SECRET-KEY'],
                                                  paper=False)
        else:  # trade_mode == TradeMode.PAPER
            global_trading_client = TradingClient(paper_creds['APCA-API-KEY-ID'],
                                                  paper_creds['APCA-API-SECRET-KEY'],
                                                  paper=True)
    return global_trading_client


def historical_client():
    global global_historical_client
    if not global_historical_client:
        if trade_mode == TradeMode.PRODUCTION:
            global_historical_client = StockHistoricalDataClient(prod_creds['APCA-API-KEY-ID'],
                                                                 prod_creds['APCA-API-SECRET-KEY'])
        else:  # trade_mode == TradeMode.PAPER
            global_historical_client = StockHistoricalDataClient(paper_creds['APCA-API-KEY-ID'],
                                                                 paper_creds['APCA-API-SECRET-KEY'])
    return global_historical_client


def alpaca_data_stream():
    global global_data_stream
    if not global_data_stream:
        creds = prod_creds if trade_mode == TradeMode.PRODUCTION else paper_creds
        global_data_stream = StockDataStream(creds['APCA-API-KEY-ID'], creds['APCA-API-SECRET-KEY'])
    return global_data_stream
