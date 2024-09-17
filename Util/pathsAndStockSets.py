# This file allows me to switch between using a Development set of 5 stocks and the SP500 set containing all the
# S&P 500 stocks.
#
# The advantage is that running on the Development set takes about 1% of the time of the full set.
#
# We store data files in folders corresponding to their use (e.g., bar data files) and which stock set they are. I keep
# the development stock set files in a sub-folder of the main production set, but may revisit that someday

import enum
import os
import pandas as pd

# Setting up Environment Variables in Windows:
#   1. Open System Properties
#   2. Go to Advanced -> Environment Variables...
#   3. Set up User Variables for the following:
#        - DayTradingDataFilesBasePath


class StockSet(enum.Enum):
    DEVELOPMENT = 1,
    SP500 = 2


global_stock_set = StockSet.SP500


def set_stock_set(new_stock_set=StockSet.SP500):
    global global_stock_set
    global_stock_set = new_stock_set


# There doesn't seem to be a Python library that provides the current S&P 500 stock list, so the standard approach
# seems to be grabbing it from Wikipedia :/
#
# The sp500_table has the following columns:
#   Symbol: the stock symbol (e.g., 'AAPL')
#   Security: it's name (e.g., 'Apple Inc.')
#   GICS Sector
#   GICS Sub-Industry
#   Headquarters Location: 'City, State' (mostly). There are also securities with multiple locs and internationals
#   Date added: string in the form '1957-03-04' representing (probably) when the stock was added to NYSE
#   CIK
#   Founded: Mostly 4-character dates

# Note: You may need to pip install lxml

sp500_table = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]


# return all (503) stocks in the S&P 500 if we're using the production data set, otherwise the smaller dev set

def get_symbols():
    if global_stock_set == StockSet.DEVELOPMENT:
        return ['PARA', 'PKG', 'POOL', 'RHI', 'TSLA']
    else:
        return sp500_table['Symbol'].to_list()


# Paths are based off of a single file location, which should be stored as an environment variable

base_path = os.environ['DayTradingDataFilesBasePath']
# base_path = 'C:/Users/David/PycharmProjects/StockDataFiles/'
# NOTE: You will need to set up this file hierarchy on your computer

prod_bar_files_path = base_path + 'BarFiles/'
prod_derived_files_path = base_path + 'DerivedFiles/'
prod_logging_path = base_path + 'LogFiles/'
prod_models_path = base_path + 'Models/'
prod_temp_files_path = base_path + 'TempFiles/'

dev_bar_files_path = base_path + 'BarFiles/DevSet/'
dev_derived_files_path = base_path + 'DerivedFiles/DevSet/'
dev_logging_path = base_path + 'LogFiles/DevSet/'
dev_models_path = base_path + 'Models/DevSet/'
dev_temp_files_path = base_path + 'TempFiles/DevSet/'


# Create a full path to filename, based on our current stock_set

def bar_files_path(filename):
    if global_stock_set == StockSet.DEVELOPMENT:
        return dev_bar_files_path + filename
    return prod_bar_files_path + filename


def derived_files_path(filename):
    if global_stock_set == StockSet.DEVELOPMENT:
        return dev_derived_files_path + filename
    return prod_derived_files_path + filename


def logging_path(filename):
    if global_stock_set == StockSet.DEVELOPMENT:
        return dev_logging_path + filename
    return prod_logging_path + filename


def models_path(filename):
    if global_stock_set == StockSet.DEVELOPMENT:
        return dev_models_path + filename
    return prod_logging_path + filename


def temp_files_path(filename):
    if global_stock_set == StockSet.DEVELOPMENT:
        return dev_temp_files_path + filename
    return prod_temp_files_path + filename
