import requests
import csv
import time
import logging
import os

# Configure logging
logging.basicConfig(filename='air_quality_fetch.log', 
                    level=logging.DEBUG, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# OpenAQ API setup
base_url = "https://api.openaq.org/v2/measurements"
headers = {
    "accept": "application/json"
}  
api_key = os.environ.get('OPENAQ_API_KEY') 
if not api_key:
    logging.error("OpenAQ API key not found in environment. Exiting.")
    exit(1)
headers['Authorization'] = f"Bearer {api_key}"

def fetch_and_store_measurements(location, start_date, end_date, parameters):
    page = 1
    requests_made = 0
    start_time = time.time()

    while True:
        try:
            params = {
                'location': location,
                'date_from': start_date,
                'date_to': end_date,
                'parameter': parameters, 
                'page': page
            }

            response = requests.get(base_url, headers=headers, params=params)

            if response.status_code == 200:
                data = response.json()

                with open('measurementsCivic.csv', 'a', newline='') as csvfile: 
                    writer = csv.writer(csvfile)

                    for result in data['results']:
                        measurement_data = [
                            result['location'],
                            result['parameter'],
                            result['value'],
                            result['date']['local'], 
                            result['unit'],
                        ]
                        writer.writerow(measurement_data) 

                requests_made += 1

                # Basic Rate Limiting (Consult OpenAQ Documentation)
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
                    # Treat this as if there are more results (adjust as needed) 
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

# Usage  
fetch_and_store_measurements(
    location="Jeddah",
    start_date="2022-01-01T00:00:00Z",
    end_date="2023-10-31T23:59:59Z",
    parameters=['pm25', 'pm10'] 
)
