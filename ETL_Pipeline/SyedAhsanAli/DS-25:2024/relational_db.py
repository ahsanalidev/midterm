import psycopg2
import pandas as pd
from psycopg2 import Error

# Connection string to the PostgreSQL database
connection_str = "postgresql://neondb_owner:npg_OTxRy4ESdmr9@ep-lingering-wave-a5lnodhh-pooler.us-east-2.aws.neon.tech/neondb?sslmode=require"

def fetch_all_data_df():
    conn = None
    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(connection_str)
        print("Database connection established.\n")
        
        # Define your query
        query = "SELECT * FROM weather;"
        
        # Use pandas to execute the query and return a DataFrame
        df = pd.read_sql_query(query, conn)
        return df
        
    except Error as e:
        print("An error occurred during the database operation:")
        print(e)
        return None
    finally:
        if conn:
            conn.close()
            print("\nDatabase connection closed.")

if __name__ == '__main__':
    df = fetch_all_data_df()
    if df is not None:
        print("Data fetched as DataFrame:")
        print(df)
