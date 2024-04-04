import requests
import csv
import time
import logging
import os
import argparse

# Configure logging
logging.basicConfig(filename='air_quality_fetch.log', 
                    level=logging.DEBUG, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# OpenAQ API setup
base_url = "https://api.openaq.org/v2/measurements"
headers = {
    "accept": "application/json"
}  

# Get API Key 
api_key = os.environ.get('OPENAQ_API_KEY') 
if not api_key:
    logging.error("OpenAQ API key not found in environment. Exiting.")
    exit(1)
headers['Authorization'] = f"Bearer {api_key}"

def fetch_and_store_measurements(location, start_date, end_date, parameters):
    """Fetches air quality measurements and stores them in separate CSV files per parameter.

    Args:
        location (str): The location to fetch data for.
        start_date (str): The start date in ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ).
        end_date (str): The end date in ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ).
        parameters (list): List of parameters to fetch (e.g., 'pm25', 'pm10').
    """

    header = ['location', 'parameter', 'value', 'date (utc)', 'unit']

    for parameter in parameters:
        filename = f'measurements_{location.replace(" ", "_")}_{parameter}.csv'

        page = 1
        requests_made = 0
        start_time = time.time()

        while True:
            try:
                params = {
                    'location': location, 
                    'date_from': start_date,
                    'date_to': end_date,
                    'parameter': parameter, 
                    'page': page
                }

                response = requests.get(base_url, headers=headers, params=params)

                if response.status_code == 200:
                    data = response.json()

                    with open(filename, 'a', newline='') as csvfile:  # Use 'a' to append if the file exists
                        writer = csv.writer(csvfile)

                        # Write header only for the first iteration of the parameter and first page
                        if page == 1 and not os.path.exists(filename):
                            writer.writerow(header)

                        for result in data['results']:
                            measurement_data = [
                                result['location'],
                                result['parameter'],
                                result['value'],
                                result['date']['utc'], 
                                result['unit'],
                            ]
                            writer.writerow(measurement_data) 

                    requests_made += 1

                    # Basic Rate Limiting Handling
                    if requests_made >= 100: 
                        time_since_start = time.time() - start_time
                        if time_since_start < 60: 
                            sleep_time = 60 - time_since_start 
                            logging.info(f"Rate limit approaching. Sleeping for {sleep_time} seconds...")
                            time.sleep(sleep_time) 
                        requests_made = 0
                        start_time = time.time()

                    # Handle pagination based on OpenAQ's mechanisms
                    found_value = data['meta']['found']
                    if found_value == '>100':
                        page += 1
                    else:
                        if int(found_value) < params.get('limit', 100): 
                            break
                        page += 1

                else:
                    logging.error(f"Error fetching page {page}: {response.status_code}, {response.text}")
                    time.sleep(5)

            except requests.exceptions.RequestException as e:
                logging.error(f"Error occurred: {e}. Retrying in 5 seconds...") 
                logging.debug("Exception Details:", exc_info=True)
                time.sleep(5) 

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch and store air quality data from OpenAQ.")
    parser.add_argument("location", help="Location to fetch data for.")
    parser.add_argument("start_date", help="Start date (ISO 8601 format: YYYY-MM-DDTHH:MM:SSZ)")
    parser.add_argument("end_date", help="End date (ISO 8601 format: YYYY-MM-DDTHH:MM:SSZ)")
    parser.add_argument("parameters", nargs="+", help="Air quality parameters to fetch (e.g., pm25 pm10)")

    args = parser.parse_args()

    fetch_and_store_measurements(args.location, args.start_date, args.end_date, args.parameters)
