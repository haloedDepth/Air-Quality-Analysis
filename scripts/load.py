import csv
import psycopg2
import argparse

def create_and_populate_table(csv_file, table_name, host, dbname, user):
    """Creates a PostgreSQL table (if it doesn't exist) and populates it.

    Args:
        csv_file (str): Path to the CSV file (only 'datetime' and 'value' columns).
        table_name (str): Name of the PostgreSQL table.
        host (str): PostgreSQL database host.
        dbname (str): PostgreSQL database name.
        user (str): PostgreSQL user.
    """
    # Convert hyphens to underscores in the table_name
    table_name = table_name.replace('-', '_') 
    # Connect to the database
    try:
        with psycopg2.connect(host=host, dbname=dbname, user=user) as conn:
            print("Database connection successful")  # Check connection
            with conn.cursor() as cur:
                # Create the table
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        datetime TIMESTAMP PRIMARY KEY,
                        value REAL
                    );
                """)
                print("Table creation (or check) successful")  

                # Read and insert data
                with open(csv_file, 'r') as csvfile:
                    reader = csv.DictReader(csvfile)
                    data = [(row['datetime'], row['value']) for row in reader]
                    cur.executemany(f"""
                        INSERT INTO {table_name} (datetime, value)
                        VALUES (%s, %s);
                    """, data)

                    conn.commit()
                    print("Data insertion successful") 

    except psycopg2.Error as e:
        print(f"Database error: {e}")  # Detailed error message

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Import CSV data into a PostgreSQL table.')
    parser.add_argument('csv_file', help='Path to the CSV file')
    parser.add_argument('--table_name', help='Name of the PostgreSQL table')
    parser.add_argument('--host', default='localhost', help='PostgreSQL database host')
    parser.add_argument('--dbname', required=True, help='PostgreSQL database name')
    parser.add_argument('--user', required=True, help='PostgreSQL user')

    args = parser.parse_args()
    

    create_and_populate_table(args.csv_file, args.table_name, args.host, args.dbname, args.user)