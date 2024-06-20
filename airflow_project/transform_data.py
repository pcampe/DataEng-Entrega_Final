import pandas as pd

def transform_data(weather_data, stock_data):
    # Transformar datos del clima
    weather_df = pd.json_normalize(weather_data)
    weather_df['fetch_time'] = pd.to_datetime('now')

    # Transformar datos de acciones
    stock_data['fetch_time'] = pd.to_datetime('now')

    # Combinar datos
    combined_df = pd.merge(stock_data, weather_df, left_on='fetch_time', right_on='fetch_time', how='inner')
    return combined_df
