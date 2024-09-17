# Helpers to consistently use pandas Timestamps (with timezone info) across everything

import datetime as dt
import logging
import pandas as pd
import pandas_market_calendars as mcal  # Also might need to pip install exchange_calendars
import time


# Sleep the processing until wakeup_timestamp and also log the delay

def sleep_until_time(wakeup_timestamp, component_name, action_name):
    today = pd.to_datetime(dt.datetime.today()).tz_localize('America/Los_Angeles')
    time_to_wait = wakeup_timestamp - today
    if time_to_wait.value > 0:
        logging.debug(f"c={component_name} a={action_name} s=waiting seconds={time_to_wait.seconds} "
                      + f"wakeupTime={str(wakeup_timestamp)}")
        time.sleep(time_to_wait.seconds)


# Some utils for creating our timestamps and extracting useful strings out of them

# Convert a date string (e.g., '2024-06-24') or a date-time string (e.g., '2024-06-24 09:30:00') into a timestamp.
# We do everything in NY time by default

def timestamp(string):
    return pd.to_datetime(string).tz_localize('America/New_York')


# Create a timestamp for this moment

def timestamp_now():
    return pd.to_datetime(dt.datetime.today()).tz_localize('America/Los_Angeles').tz_convert('America/New_York')


# Output a string for the date component of this timestamp

def date_string(time_stamp):
    return str(time_stamp.date())


# Output a string for the time component of this timestamp

def time_string(time_stamp):
    return str(time_stamp.time())[:8]


def datetime_string(time_stamp):
    return f"{date_string(time_stamp)}_{time_string(time_stamp)[0:5]}"


# Return the timestamp for the most recent stock bar that would be available at the given time (or now)
# For example: For 5-minute bars, the '2024-07-02 09:35:00' bar represents stock activity from 9:35:00 to 09:39:59
# So, most_recent_bar_time for '2024-07-02 09:40:00', would be '2024-07-02 09:35:00' as would '2024-07-02 09:41:00'
#
# Sure, it's confusing. But, it's how the markets defined things. We have to be careful to make sure the future doesn't
# bleed into our models. For example, if I'm making a decision at 09:40:00, I can't consider the 09:40:00 stock bar,
# because that represents activity that hasn't happened yet.

def most_recent_bar_time(time_stamp=None, freq=5):
    time_stamp = time_stamp if time_stamp else timestamp_now()
    d = pd.Timedelta(minutes=(time_stamp.minute % freq) + freq,
                     seconds=time_stamp.second,
                     microseconds=time_stamp.microsecond)
    return time_stamp - d


# some utils for accessing trading dates from the NYSE

nyse = mcal.get_calendar('NYSE')
nyse_dates = nyse.valid_days('2023-01-01', '2024-12-31', tz='America/New_York')


# Get trading dates in the interval (inclusive)

def trading_dates(start=timestamp('2000/01/01'), end=timestamp('2099/12/31')):
    dates = nyse.valid_days(start, end, tz='America/New_York')
    return dates.to_list()


# Find a trading date following the specified trading date. If current_date isn't a trading date we return None

def next_trading_date(current_date, offset=1):
    if current_date not in nyse_dates:
        return None
    i = nyse_dates.get_loc(current_date)
    if i + offset >= len(nyse_dates) or i + offset < 0:
        return None
    return nyse_dates[i + offset]


def previous_trading_date(current_date, offset=1):
    if current_date not in nyse_dates:
        return None
    i = nyse_dates.get_loc(current_date)
    if i - offset >= len(nyse_dates) or i - offset < 0:
        return None
    return nyse_dates[i - offset]
