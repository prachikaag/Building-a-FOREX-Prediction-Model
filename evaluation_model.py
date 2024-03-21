import os
import pandas as pd
import numpy as np
import requests
from pmdarima.arima import auto_arima
import concurrent.futures
from datetime import datetime, timedelta
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tools.sm_exceptions import ConvergenceWarning
from keras.models import Sequential
from keras.layers import LSTM, Dense
import matplotlib.pyplot as plt

# Set the API key and create a function to fetch the forex data
API_TOKEN = "67af368533fa8517ab86b062f49714d6-fc8a88fd1b8c595746bde3f8c84a57d6"

# Calculate the start time for fetching data
start_time = (datetime.now() - timedelta(days=180)).isoformat() + "Z"

# Create a list of major currency pairs
currencies_pairs = ['USD', 'EUR', 'GBP', 'JPY', 'CHF', 'CAD', 'AUD', 'NZD']
currency_pairs = []

def calculate_curr_pairs(currencies_pairs):
    for curr1 in currencies_pairs:
        for curr2 in currencies_pairs:
            if curr1 != curr2:
                currency_pairs.append((curr1, curr2))
    return currency_pairs

def get_forex_data(pair, start_time):
    curr_pair = f"{pair[0]}_{pair[1]}"
    print(curr_pair)
    url = f"https://api-fxpractice.oanda.com/v3/instruments/{curr_pair}/candles?from={start_time}&granularity=H1&price=BA"
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    response = requests.get(url, headers=headers)
    print(response)

    if response.status_code == 200:
        data = response.json()
        print(data)
        candles = data['candles']
        if candles:
            df = pd.DataFrame([{
              'datetime': c['time'],
              'open': float(c['bid']['o']),
              'high': max(float(c['bid']['h']), float(c['bid']['l'])),
              'low': min(float(c['bid']['h']), float(c['bid']['l'])),
              'close': float(c['bid']['c'])
          } for c in candles])
            df['datetime'] = pd.to_datetime(df['datetime'])
            df.set_index('datetime', inplace=True)
            return df
        else:
            print(f"No data acquired")
    else:
        print(f"Error fetching data - {pair}: {response.text}")
        return None

# Perform ARIMA forecasting and calculate the slope
def forecast_slope_pred(df, hours=10):
    # Fit ARIMA model
    print("[INFO] Running ARIMA Model for major currency pairs")
    model_arima = auto_arima(df['close'], suppress_warnings=True, seasonal=False, stepwise=True)
    arima_forecasts = model_arima.predict(n_periods=hours)
    
    # Fit LSTM model
    print("[INFO] Running LSTM Model for major currency pairs")
    X_train, y_train = [], []
    for i in range(len(df)-24):
        X_train.append(df.iloc[i:i+24, :]['close'].values)
        y_train.append(df.iloc[i+24, :]['close'])
    X_train, y_train = np.array(X_train), np.array(y_train)
    model_lstm = Sequential()
    #A layer of LSTM cells with 50 units is added to the model, and the input_shape parameter is set to (24, 1), indicating that the input data is a 2D array of shape (24, 1) for each time step.
    model_lstm.add(LSTM(50, input_shape=(24,1)))
    model_lstm.add(Dense(1))
    model_lstm.compile(optimizer='adam', loss='mse')
    model_lstm.fit(X_train.reshape(X_train.shape[0], X_train.shape[1], 1), y_train, epochs=10, batch_size=32, verbose=0)
    
    # Make predictions and calculate slope
    print("[INFO] Predictions for LSTM Model for major currency pairs")
    lstm_forecasts = []
    #The x_input variable is created as a 3D array of shape (1, 24, 1) containing the most recent 24 closing prices from the DataFrame.
    x_input = np.array(df.iloc[-24:, :]['close']).reshape((1, 24, 1))
    for i in range(hours):
        lstm_pred = model_lstm.predict(x_input, verbose=0)
        lstm_forecasts.append(lstm_pred[0][0])
        x_input = np.append(x_input[:, 1:, :], np.array(lstm_pred).reshape(1, 1, 1), axis=1)
    
    # Calculate average slope
    forecasts_pred = np.array(arima_forecasts) + np.array(lstm_forecasts)
    print("[INFO] Calculate slope for ARIMA and LSTM Model for major currency pairs")
    slope = (forecasts_pred[-1] - forecasts_pred[0]) / len(forecasts_pred)
    
    if forecasts_pred is not None:
        print(slope)
        return slope
    else:
        return None

def main() -> None:
  # Fetch data for the major currency pairs using ThreadPoolExecutor
    forex_data_raw = {}
    long_curr_pair = None
    short_curr_pair = None
    max_slope = -np.inf
    min_slope = np.inf
    currency_pairs = calculate_curr_pairs(currencies_pairs)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        print("[INFO] Fetching data for major currency pairs")
        future_data = {executor.submit(get_forex_data, pair, start_time): pair for pair in currency_pairs}
        for future in concurrent.futures.as_completed(future_data):
            pair = future_data[future]
            forex_data_raw[pair] = future.result()

    # Iterate through the forex data, perform forecasting, and find the best long and short positions
    
    for pair, df in forex_data_raw.items():
        if df is not None:
            print("[INFO] Running Evaluation Models for major currency pairs")
            slope = forecast_slope_pred(df)
            if slope > max_slope:
                max_slope = slope
                long_curr_pair = pair
                print("Max slope curr", max_slope, long_curr_pair)

            if slope < min_slope:
                min_slope = slope
                short_curr_pair = pair
                print("Min slope curr", min_slope, short_curr_pair)

            # Print the results
            print(f"Long position: {long_curr_pair}, slope: {max_slope}")
            print(f"Short position: {short_curr_pair}, slope: {min_slope}")


main()