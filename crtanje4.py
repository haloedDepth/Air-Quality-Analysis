import argparse
import pandas as pd
import matplotlib.pyplot as plt
import pytz
from sqlalchemy import create_engine

def main():
    # ... (Argument parsing, database connection, timezone conversion - same as before) ...
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
    engine = create_engine(f"postgresql://{args.username}@localhost/{args.database}")  # No password

    # Data query 
    sql = f"""  
    SELECT datetime, value
    FROM {args.table}
    WHERE datetime >= '{args.start_date}' AND datetime < '{args.end_date}'
    """
    df = pd.read_sql(sql, engine)

    # Calculate mean and median for each hour, grouped by day of the week
    df['day_of_week'] = df['datetime'].dt.day_name()
    df['hour'] = df['datetime'].dt.hour
    statistics_by_day_hour = df.groupby(['day_of_week', 'hour'])['value'].agg(['mean', 'median'])

    # Create a separate table for each day of the week
    days_of_week = statistics_by_day_hour.index.get_level_values('day_of_week').unique()

    for day in days_of_week:
        day_data = statistics_by_day_hour.loc[day]

        fig, ax = plt.subplots()
        ax.axis('off')
        ax.axis('tight')

        table = ax.table(cellText=day_data.values, 
                         colLabels=day_data.columns, 
                         rowLabels=day_data.index.astype(str) + ':00', 
                         loc='center')

        table.auto_set_font_size(False)
        table.set_fontsize(12)

        fig.savefig(f'hourly_stats_{day}.png')

if __name__ == "__main__":
    main()