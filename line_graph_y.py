import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import argparse
from collections import deque


def get_data_from_db(host, dbname, user, table, args):
    """Connects to PostgreSQL database and fetches data."""

    try:
        conn = psycopg2.connect(
            host=host,
            dbname=dbname,
            user=user
        )
        cursor = conn.cursor()

        # Process start and end dates if provided
        if args.start_date and args.end_date:
            cursor.execute(f"""
                SELECT datetime, value 
                FROM {table}
                WHERE datetime >= %s AND datetime <= %s
            """, (args.start_date, args.end_date))

        else:
            cursor.execute(f"SELECT datetime, value FROM {table}")

        data = cursor.fetchall()

        # Create a Pandas DataFrame
        df = pd.DataFrame(data, columns=['timestamp', 'value'])

        return df

    except psycopg2.OperationalError as e:
        print(f"Unable to connect: {e}")
        exit()
    finally:
        if conn:
            cursor.close()
            conn.close()

def main():
    # Argument parsing for flexibility
    parser = argparse.ArgumentParser(description="Fetch data and plot from PostgreSQL")
    parser.add_argument('--tables', required=True, help="Comma-separated list of database tables")
    parser.add_argument('--start_date', type=str, help="Start date (format: YYYY-MM-DD)")
    parser.add_argument('--end_date', type=str, help="End date (format: YYYY-MM-DD)")
    args = parser.parse_args()

   # Database credentials (Fill in your actual credentials here)
    host = 'localhost'
    dbname = 'air_quality'
    user = 'postgres'

   # Fetch the data 
    all_data = {}
    table_names = args.tables.split(',')
    for table in table_names:
       df = get_data_from_db(host, dbname, user, table, args)
       all_data[table] = df 

   # Create the line plot
    plt.figure(figsize=(30, 10))

    colors = deque(['blue', 'green', 'red', 'cyan', 'magenta']) 
    for table, df in all_data.items():
       plt.plot(df['timestamp'], df['value'], color=colors[0], label=table)
       colors.rotate(1) 

    plt.xlabel('Timestamp')
    plt.ylabel('Value')
    plt.title('Data from multiple tables')
    plt.legend() 

   # Save the plot 
    plt.savefig('plot_from_multiple_tables.png') 

if __name__ == "__main__":
   main()
