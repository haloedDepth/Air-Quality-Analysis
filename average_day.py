import psycopg2
import argparse
import pandas as pd
import matplotlib.pyplot as plt

def main():
    # Handle command-line arguments
    parser = argparse.ArgumentParser(description="Generate average day plot")
    parser.add_argument("table_name", help="Name of the database table")
    parser.add_argument("start_date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("end_date", help="End date (YYYY-MM-DD)")
    parser.add_argument("utc_offset", type=int, help="UTC offset in hours")
    parser.add_argument("output_file", help="Output file name (e.g., my_plot.png)") 
    args = parser.parse_args()

    # Connect to the PostgreSQL database
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="air_quality",
            user="postgres"
        )

        # Construct the SQL query
        sql = f"""
            SELECT 
                EXTRACT(HOUR FROM datetime + INTERVAL '{args.utc_offset} HOUR') AS hour,
                AVG(value) as average_value
            FROM {args.table_name}
            WHERE datetime >= '{args.start_date}' AND datetime < '{args.end_date}'
            GROUP BY hour
            ORDER BY hour;
        """

        # Execute query and fetch data
        df = pd.read_sql(sql, conn)

        # Create the plot
        plt.plot(df['hour'], df['average_value'])
        plt.xlabel("Hour of Day")
        plt.ylabel("Average Value")
        plt.title(f"Average Day ({args.start_date} to {args.end_date})")
        plt.savefig(args.output_file)  # Use the provided output file name
        print(f"Plot saved as {args.output_file}")

    except psycopg2.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()