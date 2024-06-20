from sqlalchemy import create_engine

def load_data_to_dw(df):
    engine = create_engine('postgresql://user:password@host:port/dbname')
    df.to_sql('combined_data', engine, if_exists='append', index=False)
