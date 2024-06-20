import sqlite3
import pandas as pd

def fetch_stock_data():
    conn = sqlite3.connect('stocks.db')
    query = 'SELECT * FROM stock_prices'
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df
