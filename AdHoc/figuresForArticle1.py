# This file contains scripts to produce the figures for the first article in findahappy.medium.com

# If needed, install the following libraries:
#   pip install matplotlib
#   pip install numpy
#   pip install yfinance

import math
import matplotlib.pyplot as plt
import numpy as np
import yfinance as yf


def create_yfinance_daily_change_chart(symbol, start_date, end_date, label):
    bars = yf.download(symbol, start=start_date, end=end_date, interval='1d')
    daily_change_pct = 100 * (bars['Close'].shift(-1) - bars['Close']) / bars['Close']

    # Compute and print basic stats
    series_mean = round(daily_change_pct.mean(), 3)
    series_min = round(daily_change_pct.min(), 2)
    series_max = round(daily_change_pct.max(), 2)
    print(f"Stats: mean: {series_mean} | min: {series_min} | max: {series_max}")

    # Compute and print stats on bucketed daily changes
    count = len(daily_change_pct)
    lose = len(daily_change_pct[daily_change_pct < 0.0])
    lose_pct = round(100 * lose / count, 2)
    lose_2_count = len(daily_change_pct[daily_change_pct <= -2.0])
    lose_2_pct = round(100 * lose_2_count / count, 2)
    lose_1_count = len(daily_change_pct[daily_change_pct <= -1.0])
    lose_1_pct = round(100 * lose_1_count / count, 2)
    gain_1_count = len(daily_change_pct[daily_change_pct >= 1.0])
    gain_1_pct = round(100 * gain_1_count / count, 2)
    gain_2_count = len(daily_change_pct[daily_change_pct >= 2.0])
    gain_2_pct = round(100 * gain_2_count / count, 2)
    print(f"Counts: lose: {lose} | lose 2%: {lose_2_count} | lose 1%: {lose_1_count} | "
          + f"gain 1%: {gain_1_count} | gain 2%: {gain_2_count}")
    print(f"Percents: lose: {lose_pct}% | lose 2%: {lose_2_pct}% | lose 1%: {lose_1_pct}% | "
          + f"gain 1%: {gain_1_pct}% | gain 2%: {gain_2_pct}%")

    # Create, save, and display the figures
    plt.figure(figsize=(12, 2.5))
    plt.subplot(1, 2, 1)
    plt.plot(daily_change_pct.index, daily_change_pct)
    plt.title(f"Daily Change% for {symbol} {label}")

    plt.subplot(1, 2, 2)
    bins = np.arange(math.floor(series_min / 0.5) * 0.5, 0.5 + math.ceil(series_max / 0.5) * 0.5, 0.5)
    plt.hist(daily_change_pct, bins=bins)
    plt.title(f"Histogram of Daily Change% for {symbol} {label}")

    plt.savefig(f"dailyChange_{label}.png", bbox_inches='tight')
    plt.show()


create_yfinance_daily_change_chart('SPY', '2023-06-25', '2024-06-24', '1_year')
create_yfinance_daily_change_chart('SPY', '2016-06-25', '2024-06-24', '8_years')
