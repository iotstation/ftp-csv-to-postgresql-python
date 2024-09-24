import psycopg2   # pip install psycopg2
import csv
from ftplib import FTP, error_perm  # ftplib A Python module that helps with FTP download/Upload
from io import StringIO   #StringIO A class that creates an in-memory file-like object. 
import time

# Database connection parameters
DB_HOST = 'localhost'
DB_NAME = 'DB name'
DB_USER = 'postgres'
DB_PASSWORD = 'your password '
DB_PORT = '5432'

# FTP server details
FTP_HOST = 'localhost'
FTP_USER = 'FTP user name'
FTP_PASSWORD = 'your password'
CSV_FILE_PATH = '/data.csv'

# Function to connect to PostgreSQL
def connect_to_db():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

# Function to retrieve CSV file from FTP server
def get_csv_from_ftp():
    ftp = FTP(FTP_HOST)
    ftp.login(user=FTP_USER, passwd=FTP_PASSWORD)
    
    # Use StringIO to hold the CSV data in memory
    csv_data = StringIO()
    
    try:
        # FTP RETR command to fetch the CSV file
        ftp.retrlines(f"RETR {CSV_FILE_PATH}", lambda line: csv_data.write(line + '\n'))  # The RETR command is an FTP command that stands for "retrieve." / RETR filename
        # Move the cursor to the start of the StringIO object
        csv_data.seek(0)  # The seek() changes the current position of the file pointer within the StringIO object/ 0 means beggining 
    except error_perm as e:
        if str(e).startswith('550'):
            print(f"Error: {CSV_FILE_PATH} not found on the FTP server.")
            return None  # Return None to signal that the file wasn't found
        else:
            raise  # Re-raise the error if it's not a 'file not found' error / problem related to other issues 
    finally:
        ftp.quit()
    
    return csv_data

# Function to check if the row already exists
def is_row_exists(cur, row_id):
    cur.execute("SELECT 1 FROM staff WHERE id = %s", (row_id,))
    return cur.fetchone() is not None

# Main function to ingest data from FTP
def ingest_data():
    # Connect to PostgreSQL
    conn = connect_to_db()
    cur = conn.cursor()

    # Get CSV data from FTP server
    csv_data = get_csv_from_ftp()

    # If the file is not found, skip the ingestion
    if csv_data is None:
        print("Skipping ingestion due to missing file.")
        return

    # Use CSV reader on the fetched data
    data_reader = csv.reader(csv_data)
    next(data_reader)  # Skip the header row

    # Insert each row into the table, or update if it already exists
    for row in data_reader:
        if not is_row_exists(cur, row[0]):  # Check if row already exists by id
            cur.execute("INSERT INTO staff (id, name, age) VALUES (%s, %s, %s)", row)
            print(f"Inserted row with id: {row[0]}")
        else:
            cur.execute("UPDATE staff SET name = %s, age = %s WHERE id = %s", (row[1], row[2], row[0]))
            print(f"Updated row with id: {row[0]}")

    # Commit and close the connection
    conn.commit()
    cur.close()
    conn.close()
    print("Data ingestion complete")

# Periodically check the FTP server for updates (instead of watchdog monitoring)
def watch_ftp_file(poll_interval=10):  # Check FTP every 60 seconds by default
    print(f"Monitoring {CSV_FILE_PATH} on FTP for changes...")
    try:
        while True:
            print("Checking FTP for updates...")
            ingest_data()  # Ingest new data from the FTP server
            time.sleep(poll_interval)  # Wait for the next check
    except KeyboardInterrupt:
        print("Stopping FTP file monitoring.")

if __name__ == "__main__":
    watch_ftp_file()  # Start monitoring the FTP file for updates
