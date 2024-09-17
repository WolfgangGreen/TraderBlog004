import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from ReportProcessing.intradayDetailReport import read_intraday_details, extract_symbol_details
from ReportProcessing.dailySummaryReport import read_daily_summary, extract_symbol_summary
from Util.pathsAndStockSets import set_stock_set, StockSet
from Util.datesAndTimestamps import timestamp

set_stock_set(StockSet.DEVELOPMENT)

# Show each symbol as a single timeline for a day (5-minute resolution)
if True:
    intraday_details = read_intraday_details(timestamp('2024-06-03'))
    scaled_df = pd.DataFrame()
    for symbol in intraday_details['symbol'].unique():
        symbol_df = extract_symbol_details(intraday_details, symbol)
        opening_price_for_day = symbol_df['open'].iloc[0]
        symbol_df['pct_change'] = 100 * (symbol_df['open'] / opening_price_for_day - 1)
        scaled_df = pd.concat([scaled_df, symbol_df])
    fig = px.line(scaled_df, x='timestamp', y='pct_change', color='symbol')
    fig.show()

# Show each symbol as a single timeline (daily resolution), for % change from initial opening price
# Also, include a slider that allows us to drill into the timeline
if True:
    daily_summary = read_daily_summary()
    fig = go.Figure()
    for symbol in daily_summary['symbol'].unique():
        symbol_df = extract_symbol_summary(daily_summary, symbol)
        initial_open = symbol_df['open'].iloc[0]
        symbol_df['pct_change'] = 100 * (symbol_df['close'] / initial_open - 1)
        fig.add_trace(go.Scatter(x=symbol_df['timestamp'], y=symbol_df['pct_change'], name=symbol))
    fig.update_layout(xaxis=dict(rangeslider=dict(visible=True), type='date'))
    fig.show()

# Candlestick Chart with Volume
if True:
    symbol = 'TSLA'
    intraday_details = read_intraday_details(timestamp('2024-06-03'))
    symbol_details = extract_symbol_details(intraday_details, symbol)
    candlesticks = go.Candlestick(x=symbol_details['timestamp'], open=symbol_details['open'],
                                  high=symbol_details['high'], low=symbol_details['low'],
                                  close=symbol_details['close'], name=symbol)
    volume_bars = go.Bar(x=symbol_details['timestamp'], y=symbol_details['volume'], showlegend=False,
                         marker={'color': 'rgba(128,128,128,0.25)'})
    fig = make_subplots(specs=[[{'secondary_y': True}]])
    fig.add_trace(candlesticks, secondary_y=False)
    fig.add_trace(volume_bars, secondary_y=True)
    fig.update_yaxes(secondary_y=False, title='Price', showgrid=False)
    fig.update_yaxes(secondary_y=True, title='Volume', showgrid=False)
    fig.update_layout(xaxis={'rangeslider': {'visible': False}})
    fig.show()
