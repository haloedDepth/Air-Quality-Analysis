import argparse
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

def main():
    # Argument parsing
    parser = argparse.ArgumentParser(description="Generate statistics from PostgreSQL data.")
    parser.add_argument("host", help="PostgreSQL server hostname/IP")
    parser.add_argument("database", help="Database name")
    parser.add_argument("username", help="Database username")
    parser.add_argument("table", help="Table name")
    parser.add_argument("start_date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("end_date", help="End date (YYYY-MM-DD)")
    parser.add_argument("utc_offset", type=int, help="UTC offset (for target timezone)")
    args = parser.parse_args()

    # Database connection (Modify with your credentials)
    engine = create_engine(f"postgresql://{args.username}@localhost/{args.database}") # No password

    # Data query with timezone handling in SQL
    sql = f""" 
    SELECT 
        EXTRACT(HOUR FROM datetime + INTERVAL '{args.utc_offset} HOUR') AS hour,
        value, -- Include the 'value' column directly
        AVG(value) as average_value
    FROM {args.table}
    WHERE datetime >= '{args.start_date}' AND datetime < '{args.end_date}'
    GROUP BY hour, value -- Group by both 'hour' and 'value'
    ORDER BY hour;
    """
    df = pd.read_sql(sql, engine)

    # Calculate mean and median for each hour
   
    statistics_by_hour = df.groupby('hour')['value'].agg(['mean', 'median'])

    # Create Matplotlib table
    fig, ax = plt.subplots()

    # Hide axes
    ax.axis('off')
    ax.axis('tight')

    # Build table
    table = ax.table(cellText=statistics_by_hour.values, 
                     colLabels=statistics_by_hour.columns, 
                     rowLabels=statistics_by_hour.index.astype(str) + ':00', 
                     loc='center')

    # Adjust table properties (optional)
    table.auto_set_font_size(False)
    table.set_fontsize(12) 

    # Save the figure
    fig.savefig('hourly_stats_table.png')

if __name__ == "__main__":
   main()
