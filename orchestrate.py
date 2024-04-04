import argparse
import subprocess
import yaml
from pathlib import Path
import time

def get_table_name(filename):
    parts = filename.stem.split("_")  # Split 'measurements_Location_parameter'
    location = "_".join(parts[1:-1])
    parameter = parts[-1]
    return f"{location}_{parameter}"  # Construct the table name

def main():
    parser = argparse.ArgumentParser(description="Orchestrate the ETL workflow.")
    parser.add_argument("--config", default="config.yaml", help="Path to the configuration file.")
    args = parser.parse_args()

    with open(args.config, "r") as f:
        config = yaml.safe_load(f)

    # Extract Phase
    for location in config['extract']['locations']:
       for parameter in config['extract']['parameters']:
           # Convert dates to ISO 8601 strings
           start_date_str = config['extract']['start_date'].isoformat()
           end_date_str = config['extract']['end_date'].isoformat()

           extract_cmd = [
               'python', 'scripts/extract.py', 
               location, 
               start_date_str, 
               end_date_str,
               parameter
           ]
           subprocess.run(extract_cmd)

    # Clean Phase
    clean_files = list(Path('.').glob('measurements_*.csv')) 
    print(list(clean_files))
    for clean_file in clean_files:
        clean_cmd = ['python', 'scripts/clean.py', clean_file]
        subprocess.run(clean_cmd)
    time.sleep(2)
    print(list(clean_files))
    
    # Load Phase
    for clean_file in clean_files:
        print(list(clean_files))
        print("Filename causing the error:", clean_file) 
        table_name = get_table_name(clean_file)
        print(f"Table name derived from '{clean_file}': {table_name}")
        load_cmd = [
            'python', 'scripts/load.py',
            clean_file,
            '--table_name', table_name,  
            '--host', config['load']['host'],
            '--dbname', config['load']['dbname'],
            '--user', config['load']['user'] 
        ]
        print(f"Load command constructed: {load_cmd}")
        subprocess.run(load_cmd) 

if __name__ == "__main__":
    main()