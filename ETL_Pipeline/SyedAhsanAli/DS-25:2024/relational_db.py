import psycopg2
import pandas as pd

def load_weather_data(connection_string):
    """
    Connects to the PostgreSQL database using psycopg2, fetches data from the 'weather' table,
    and returns it as a pandas DataFrame.
    """
    try:
        conn = psycopg2.connect(connection_string)
        cur = conn.cursor()

        # Fetch all rows from the weather table
        cur.execute("SELECT * FROM weather;")
        rows = cur.fetchall()

        # Get column names from the cursor (psycopg2)
        col_names = [desc[0] for desc in cur.description]

        # Convert to pandas DataFrame
        df = pd.DataFrame(rows, columns=col_names)

        return df

    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    connection_str = "postgresql://neondb_owner:npg_OTxRy4ESdmr9@ep-lingering-wave-a5lnodhh-pooler.us-east-2.aws.neon.tech/neondb?sslmode=require"
    dataframe = load_weather_data(connection_str)
    if dataframe is not None:
        print("DataFrame loaded successfully:")
        print(dataframe)