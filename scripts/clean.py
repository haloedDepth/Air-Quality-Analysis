import pandas as pd
import numpy as np
import argparse

def check_missing_times_and_outliers(csv_file_path):
    """
    Loads a CSV file (no header), checks for missing hourly timestamps, reports findings,
    and performs outlier detection.

    Args:
        csv_file_path (str): The path to the CSV file.
    """

    # Column names for clarity
    column_names = ['location', 'sensor_type', 'value', 'datetime', 'unit']

    # Load the CSV into a Pandas DataFrame, assigning column names
    df = pd.read_csv(csv_file_path, header=None, names=column_names)

    # Convert the 'datetime' column to proper datetime format
    df['datetime'] = pd.to_datetime(df['datetime'])

    # Set 'datetime' as the index for later resampling
    df.set_index('datetime', inplace=True)
    df = df.groupby('datetime').mean()
    df.replace(-999, np.nan, inplace=True)
    df.fillna(method='ffill', inplace=True) 
    df.fillna(method='bfill', inplace=True)

    # Resample dataframe to 1 hour intervals
    resampled_df = df.resample('H').interpolate(method='time')
    resampled_df['value'] = resampled_df.groupby('datetime')['value'].transform('mean')


    # Check for missing times
    missing_times = resampled_df.index.difference(df.index)

    # Print missing times
    if missing_times.empty:
        print("No missing times found.")
    else:
        print("Missing times in", csv_file_path)
        for time in missing_times:
            print(time)

    print(resampled_df)
    resampled_df.to_csv(csv_file_path, index=True)

    # Check for outliers (Z-score method)
    def detect_outliers_zscore(data, threshold=3):
        """Detects outliers using Z-score."""
        mean = data.mean()
        std = data.std()
        z_scores = np.abs((data - mean) / std)
        return data[z_scores > threshold]

    outliers_zscore = detect_outliers_zscore(resampled_df['value']) 

    # Print outlier findings
    if not outliers_zscore.empty:
        print("Outliers detected by Z-score method:")
        print(outliers_zscore)

        # Replace outliers with mean
        resampled_df['value'].loc[outliers_zscore.index] = resampled_df['value'].mean()
        print("Outliers replaced with mean.")
    else:
        print("No outliers detected by Z-score method.")

    resampled_df.to_csv(csv_file_path, index=True)

# Example usage:
if __name__ == "__main__":
    # Create argument parser
    parser = argparse.ArgumentParser(description="Process CSV data for missing times and outliers")
    parser.add_argument("csv_file_path", help="Path to the CSV file")

    # Parse the arguments
    args = parser.parse_args()

    # Call the main function with the provided file path
    check_missing_times_and_outliers(args.csv_file_path)
