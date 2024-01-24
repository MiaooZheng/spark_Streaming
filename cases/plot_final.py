import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objects as go
import pandas as pd
import random
import plotly.express as px
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objects as go
import json
from kafka import KafkaConsumer
import pandas as pd
from historical_spark_MA import processed_in_spark


hostname = "127.0.0.1"
port = "9042"



app = dash.Dash(__name__)

# here is the tab for historical case
historical_data_layout = html.Div([
    dcc.Tabs([
        dcc.Tab(label='Daily Close Price and MA Prediction', children=[
            html.H4('Stock price analysis - MA'),
            dcc.Graph(id="time-series-chart"),
            html.P("Select stock:"),
            dcc.Dropdown(
                id="ticker",
                options=["AAPL", "META", "NFLX", "AMZN", "GOOGL", "^IXIC", "UBER", "COIN", "RIOT", "GME"],
                value="AAPL",
                clearable=False,
            ),
            html.P("Select Interval to check Moving Average:"),
            dcc.Dropdown(
                id="interval",
                options=[
                    {'label': '5 Days', 'value': 5},
                    {'label': '30 Days', 'value': 30},
                    {'label': '1 Year', 'value': 365},
                ],
                value=5,
                clearable=False,
            ),
        ]),
        dcc.Tab(label='Candlestick Chart', children=[
            html.H4('Stock price analysis - Candlestick Chart'),
            html.P("Select stock:"),
            dcc.Dropdown(
                id="candlestick-ticker",
                options=["AAPL", "META", "NFLX", "AMZN", "GOOGL", "^IXIC", "UBER", "COIN", "RIOT", "GME"],
                value="AAPL",
                clearable=False,
            ),
            html.P("Select time interval:"),
            dcc.Dropdown(
                id="candlestick-interval",
                options=[
                    {'label': '5 Days', 'value': 5},
                    {'label': '30 Days', 'value': 30},
                    {'label': '1 Year', 'value': 365},
                ],
                value=5,
                clearable=False,
            ),
            dcc.Graph(id="candlestick-chart"),
        ])
    ])
])




# and this is for the streaming case
streaming_data_layout = html.Div([
    dcc.Tab(label='BTC Streaming - test case', children=[
        html.H4('Bitcoin Streaming Data - 5-second-MA analysis for close price'),
        dcc.Graph(id="btc-streaming-chart"),
        dcc.Interval(
                id='interval-component',
                interval=5 * 1000,  # in milliseconds
                n_intervals=0
            )
])
])
  

# we wanna put them into the same dashboard -> 2 tabs and for historical case, there are 2 subtabs 
combined_layout = html.Div([
    dcc.Tabs([
        dcc.Tab(label='Historical Stock Info', children=[historical_data_layout]),
        dcc.Tab(label='Streaming price data', children=[streaming_data_layout]),
    ])
])


app.layout = combined_layout

# for streaming case, we need to call the kafka comsumer 
consumer = KafkaConsumer(
    "streamingoutput2",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",
    enable_auto_commit=True,
)

# and then define the callback function
@app.callback(
    Output("btc-streaming-chart", "figure"),
    Input("interval-component", "n_intervals")
)
def update_graph_callback(n_intervals):
    messages = consumer.poll(timeout_ms=1000)
    raw_data = []

    # extract data from Kafka messages
    for message in messages.values():
        for msg in message:
            value = json.loads(msg.value.decode("utf-8"))
            raw_data.append(value)

    # extract start times and close prices
    # here we set timestamp as start_time 
    timestamps = [pd.to_datetime(row['window']['start']) for row in raw_data]
    close_prices = [float(row['close']) for row in raw_data]
    ma_values = [float(row['ma5secondsWindow']) for row in raw_data]

    data = {'timestamps': timestamps, 'close_prices': close_prices, 'ma_values': ma_values}
    df = pd.DataFrame(data)

    # then we wanna sort by timestamp
    df.sort_values(by='timestamps', inplace=True)


    # plot
    fig = go.Figure()

    fig.add_trace(go.Scatter(x=df.timestamps, y=df.close_prices, mode='lines+markers', name='Close Price'))
    fig.add_trace(go.Scatter(x=df.timestamps, y=df.ma_values, mode='lines', name='MA-5 seconds'))

    fig.update_layout(
        title="Bitcoin Streaming Data - Close Price and 5-Second Moving Average",
        xaxis_title="Timestamp",
        yaxis_title="Price",
    )

    return fig

# and here is the callback function for price trend and MA
@app.callback(
    Output("time-series-chart", "figure"), 
    Input("ticker", "value"),
    Input("interval", "value"))
def display_time_series(ticker, interval):
    spark_df = processed_in_spark(hostname, port)

    # select company 
    filtered_df = spark_df.filter(spark_df.company == ticker).toPandas()

    end_date = filtered_df['date'].max()
    start_date = end_date - pd.to_timedelta(interval, unit='D')

    filtered_df = filtered_df[(filtered_df['date'] >= start_date) & (filtered_df['date'] <= end_date)]

    if interval == 5:
        ma_close = 'ma5_close'
    elif interval == 30:
        ma_close = 'ma30_close'
    elif interval == 365:
        ma_close = 'ma365_close'
    else:
        ma_close = 'close'

    # Plot with Plotly
    fig = px.line(filtered_df, x='date', y=['close', ma_close], labels={'value': 'Price'}, line_shape='linear')

    fig.update_traces(
        hovertemplate='<b>Date</b>: %{x|%A, %b %d, %Y}<br><b>Price</b>: $%{y:.2f}<extra></extra>'
    )

    fig.update_layout(
        title=f"Stock Prices for {ticker} - Last {interval} Days",
        xaxis_title="Date (Weekdays)",
        yaxis_title="Price",
        xaxis=dict(tickmode='array', tickvals=filtered_df['date'], ticktext=filtered_df['date'].dt.strftime('%A')), # show weekday
    )

    return fig

# define the callback function for the Candlestick Chart tab
@app.callback(
    Output("candlestick-chart", "figure"), 
    Input("candlestick-ticker", "value"),
    Input("candlestick-interval", "value"))
def display_candlestick_chart(ticker, interval):
    spark_df = processed_in_spark(hostname, port)

    filtered_df = spark_df.filter(spark_df.company == ticker).toPandas()

    end_date = filtered_df['date'].max()
    start_date = end_date - pd.to_timedelta(interval, unit='D')

    filtered_df = filtered_df[(filtered_df['date'] >= start_date) & (filtered_df['date'] <= end_date)]

    
    # create Candlestick plots
    fig = go.Figure(go.Candlestick(
        x = filtered_df['date'],
        open = filtered_df['open'],
        high = filtered_df['high'],
        low = filtered_df['low'],
        close = filtered_df['close']
    ))


    fig.update_layout(
        title=f"Candlestick Chart for {ticker}",
        xaxis_title="Date (Weekdays)",
        yaxis_title="Price",
        xaxis=dict(tickmode='array', tickvals=filtered_df['date'], ticktext=filtered_df['date']), # show

    )


    return fig

    


# Run the app
if __name__ == '__main__':
    print(1)
    app.run_server(debug=True)


