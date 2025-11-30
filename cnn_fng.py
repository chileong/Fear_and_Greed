import requests
import pandas as pd
from fake_useragent import UserAgent
from datetime import datetime
import numpy as np # Import numpy for NaN values if desired


BASE_URL = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata/"
START_DATE = '2020-09-19'
END_DATE = '2025-11-30'
ua = UserAgent()

headers = {
   'User-Agent': ua.random,
   }

r = requests.get(BASE_URL + START_DATE, headers=headers)
data = r.json()

def process_api_list_to_df(api_data_list, column_name):
    """
    Converts a list of API data points (x=timestamp, y=value)
    into a date-indexed pandas DataFrame.
    """
    df_temp = pd.DataFrame(api_data_list)
    # Convert Unix timestamp (ms) to a datetime object
    df_temp['Date'] = pd.to_datetime(df_temp['x'], unit='ms').dt.strftime('%Y-%m-%d')
    # Rename 'y' column to meaningful name
    df_temp = df_temp.rename(columns={'y': column_name})
    # Set the 'Date' column as the index
    df_temp = df_temp.set_index('Date')
    # Keep only the new column and drop the original timestamp 'x'
    return df_temp[[column_name]]

# 1. Process each of the seven indicators + the composite index
df_fng = process_api_list_to_df(data['fear_and_greed_historical']['data'], 'Fear_Greed')
df_sp500 = process_api_list_to_df(data['market_momentum_sp500']['data'], 'Mkt_sp500')
df_sp125 = process_api_list_to_df(data['market_momentum_sp125']['data'], 'Mkt_sp500')
df_stock_strength = process_api_list_to_df(data['stock_price_strength']['data'], 'Stock_Strength')
df_stock_breadth = process_api_list_to_df(data['stock_price_breadth']['data'], 'Stock_breadth')
df_put_call = process_api_list_to_df(data['put_call_options']['data'], 'Put_Call')
df_volatility = process_api_list_to_df(data['market_volatility_vix']['data'], 'Volatility')
df_volatility_50 = process_api_list_to_df(data['market_volatility_vix_50']['data'], 'Volatility_50')
df_safe_haven = process_api_list_to_df(data['safe_haven_demand']['data'], 'Safe_Haven')
df_junk_bonds = process_api_list_to_df(data['junk_bond_demand']['data'], 'Junk_Bonds')

# 2. Join all DataFrames on their common 'Date' index
# 'how="outer"' ensures we keep all dates present in any of the individual dataframes
final_df = df_fng.join([
    df_sp500,
    df_sp125,
    df_stock_strength,
    df_stock_breadth,
    df_put_call,
    df_volatility,
    df_volatility_50,
    df_safe_haven,
    df_junk_bonds
], how='outer')

# 3. Sort by date index and save
final_df.sort_index(inplace=True)

# Save the final DataFrame to a CSV file
final_df.to_csv('all_cnn_indicators_final.csv')

print("DataFrame with all indicators and Date index created and saved to all_cnn_indicators_final.csv")
print(final_df.head())
print("\nDataFrame shape (rows, columns):", final_df.shape)