import argparse
import pandas as pd
import matplotlib.pyplot as plt
import pytz
from sqlalchemy import create_engine
import seaborn as sns  # Import seaborn for boxplots

def main():
    # Argument parsing (same as before)
    parser = argparse.ArgumentParser(description="Generate boxplots from PostgreSQL data.")
    parser.add_argument("host", help="PostgreSQL server hostname/IP")
    parser.add_argument("database", help="Database name")
    parser.add_argument("username", help="Database username")
    parser.add_argument("table", help="Table name")
    parser.add_argument("start_date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("end_date", help="End date (YYYY-MM-DD)")
    parser.add_argument("utc_offset", type=int, help="UTC offset (for target timezone)")
    args = parser.parse_args()

    # Database connection (Same as before)
    engine = create_engine(f"postgresql://{args.username}@localhost/{args.database}")

    # Data query (Same as before)
    sql = f"""  
    SELECT datetime, value
    FROM {args.table}
    WHERE datetime >= '{args.start_date}' AND datetime < '{args.end_date}'
    """
    df = pd.read_sql(sql, engine)

    # Timezone conversion (Same as before)
    target_timezone = pytz.timezone('Etc/GMT{}'.format(-args.utc_offset)) 
    df['datetime'] = df['datetime'].dt.tz_localize('UTC').dt.tz_convert(target_timezone)

    # Prepare data for boxplots
    df['hour'] = df['datetime'].dt.hour  

    # Plotting with boxplots
    plt.figure(figsize=(10, 6))

    sns.boxplot(
        x = "hour",
        y = "value",
        showmeans=True,  # Show mean markers
        data=df
    )

    plt.xlabel('Hour of Day')
    plt.ylabel('Value')
    plt.title('Hourly Data Distribution with Timezone Offset')
    plt.grid(True)

    # Save the plot
    plt.savefig('data_boxplot.png')

if __name__ == "__main__":
    main()