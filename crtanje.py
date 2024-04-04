import argparse
import pandas as pd
import matplotlib.pyplot as plt
import pytz
from sqlalchemy import create_engine


def main():
    # Argument parsing
    parser = argparse.ArgumentParser(description="Generate plots and statistics from PostgreSQL data.")
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

    # Timezone conversion 
    target_timezone = pytz.timezone('Etc/GMT{}'.format(-args.utc_offset)) 
    df['datetime'] = df['datetime'].dt.tz_localize('UTC').dt.tz_convert(target_timezone)

    # Day-of-week grouping and averaging
    df['day_of_week'] = df['datetime'].dt.day_name()
    df['hour'] = df['datetime'].dt.hour  # Capture the hour
    grouped = df.groupby(['day_of_week', 'hour'])['value']
    day_averages = grouped.mean().unstack(level=0)  # Unstack for plotting

    # Overall average
    overall_average = df.groupby('hour')['value'].mean()

    # Plotting
    plt.figure(figsize=(10, 6))

    for day, data in day_averages.items():
        plt.plot(data.index, data, label=day)

    plt.plot(overall_average.index, overall_average, label='Overall', linewidth=3)

    plt.xlabel('Hour of Day')
    plt.ylabel('Value')
    plt.title('Data with Timezone Offset')
    plt.legend()
    plt.grid(True)

    # Save the plot
    plt.savefig('data_plot.png')

if __name__ == "__main__":
    main()