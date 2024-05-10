"""
Filename: Load NY Bus Dataset.py
Author: Meet Gandhi, Hrishit Kotadia, Prabhav Karve
ID: mg1905, hjk5029, pk6004

This code loads data from the text files that the user provides into the desired database for Project.
This code also creates the tables needed for this data.

Steps to take before running this code:
1. Create database
2. Place this code file in the directory where all the data files are located
3. Enter your database connection details in the fields given in the code below
4. Run the code

In the console, a few details are printed on running the code:
1. Confirmation that tables are created
2. Confirmation that the data is inserted into the desired table
3. The number of rows in each table and total rows in the database
4. The total time for creating and loading the entire database.

During testing the entire operation took approximately: 1.0 Hours, 20.0 Minutes, and 53.37177515029907 seconds.
and it loaded: 32550921 rows
"""
import time
import psycopg2

# Postgres connection settings
DB_NAME = "Project"
DB_USER = "postgres"
DB_PASSWORD = "MgRIT1905@28082808"
DB_HOST = "localhost"
DB_PORT = "2808"


def connect_to_db():
    """
    Connect to the Postgres Database using the parameters defined above.
    :return: connection to the Postgres Database
    """
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    return conn


# Function to adjust time values greater than or equal to '24:00:00' to the next day
def adjust_time(time_str) -> str:
    """
    Adjust the time string to a valid format that is under 24:00:00
    :param time_str:
    :return: string formatted value of the adjusted or non-adjusted time
    """

    if time_str >= '24:00:00':
        # Split the time string into hours, minutes, seconds, and milliseconds
        parts = time_str.split(':')
        hours = int(parts[0])
        minutes = int(parts[1])
        seconds, milliseconds = map(int, parts[2].split('.'))

        # Calculate the total number of seconds in the time
        total_seconds = hours * 3600 + minutes * 60 + seconds

        # Subtract 24 hours' worth of seconds
        total_seconds -= 24 * 3600

        # Calculate the adjusted time
        adjusted_hours, remaining_seconds = divmod(total_seconds, 3600)
        adjusted_minutes, adjusted_seconds = divmod(remaining_seconds, 60)

        # Format the adjusted time including milliseconds
        adjusted_time = f'{adjusted_hours:02}:{adjusted_minutes:02}:{adjusted_seconds:02}.{milliseconds:06}'

        return adjusted_time
    else:
        return time_str


def insert_data_from_files() -> None:
    """
    Insert data from text files into corresponding tables in a PostgreSQL database.
    Returns: None
    """
    # Connect to the database
    conn = connect_to_db()
    cur = conn.cursor()

    try:
        # Get list of text files in current directory
        data_files = [
            "agency",
            "arrival_time",
            "calendar",
            "calendar_dates",
            "real_time_data_temp",
            "routes",
            "shapes",
            "stop_times",
            "stops",
            "trips"
        ]

        # Iterate over each file
        for filename in data_files:
            table_name = filename
            print("\n======================================================\n")
            print(f"Loading data from {filename} into table {table_name}")
            filename = filename + ".text"

            # Get column names for the table from the database schema
            cur.execute(f"SELECT column_name, data_type FROM information_schema.columns "
                        f"WHERE table_name = '{table_name}' ORDER BY ordinal_position")
            columns_info = cur.fetchall()
            columns = [row[0] for row in columns_info]
            data_types = {row[0]: row[1] for row in columns_info}
            # print(data_types)

            # Construct the INSERT statement dynamically based on column names
            placeholders = ', '.join(['%s' for _ in range(len(columns))])
            sql_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

            # Open the file and read data line by line
            with open(filename, "r") as file:
                for line in file:
                    values = line.strip().split('\t')  # Assuming tab-separated values
                    i: int

                    # Adjust time values if the column is detected as a time column
                    for i, column in enumerate(columns):
                        if data_types[column] == 'time without time zone':
                            values[i] = adjust_time(values[i])

                    # Replace 'NULL' values with None
                    for i in range(len(values)):
                        if values[i] == 'NULL':
                            values[i] = None
                    cur.execute(sql_query, values)

                print(f"Data inserted into {table_name} table.")

        # Commit the transaction
        conn.commit()
        print("\n======================================================\n")
        print("All data inserted successfully.")

    except Exception as e:
        # Rollback the transaction in case of an error
        conn.rollback()
        print(f"Error occurred: {e}")

    finally:
        # Close the cursor and connection
        cur.close()
        conn.close()

    return None


def create_tables() -> None:
    """
    Function to create all the necessary tables in the database
    :return: None
    """

    print("\n======================================================\n")
    print("Creating tables...")

    # Connect to the database
    connection = connect_to_db()
    cursor = connection.cursor()

    try:

        # Define SQL statements to create each table
        sql_queries = [
            """CREATE TABLE agency (
                agency_id varchar(255) NOT NULL,
                agency_name varchar(255),
                agency_url varchar(255),
                agency_timezone varchar(255),
                agency_lang varchar(255),
                agency_phone varchar(255),
                PRIMARY KEY (agency_id)
            )""",
            """CREATE TABLE arrival_time (
                time_span TIMESTAMP WITH TIME ZONE NOT NULL,
                all_count int NULL,
                late_count int NULL,
                PRIMARY KEY (time_span)
            )""",
            """CREATE TABLE calendar (
                service_id varchar(255) NOT NULL,
                monday boolean NULL,
                tuesday boolean NULL,
                wednesday boolean NULL,
                thursday boolean NULL,
                friday boolean NULL,
                saturday boolean NULL,
                sunday boolean NULL,
                start_date date NULL,
                end_date date NULL,
                PRIMARY KEY (service_id)
            )""",
            """CREATE TABLE calendar_dates (
                service_id varchar(255) NOT NULL,
                date date NOT NULL,
                exception_type int NULL,
                PRIMARY KEY (service_id, date)
            )""",
            """CREATE TABLE real_time_data_temp (
                route_id varchar(255) NULL,
                direction varchar(255) NULL,
                trip_id varchar(255) NULL,
                agency_id varchar(255) NULL,
                origin_stop varchar(255) NULL,
                lat float NULL,
                lon float NULL,
                bearing float NULL,
                vehicle_id varchar(255) NOT NULL,
                aimed_arrival_time TIMESTAMP WITH TIME ZONE NULL,
                distance_from_origin float NULL,
                presentable_distance float NULL,
                distance_from_next_stop varchar(255) NULL,
                next_stop varchar(255) NULL,
                recorded_time TIMESTAMP WITH TIME ZONE NOT NULL,
                PRIMARY KEY (recorded_time, vehicle_id)
            )""",
            """CREATE TABLE routes (
                route_id varchar(255) NOT NULL,
                agency_id varchar(255),
                route_short_name varchar(255),
                route_long_name varchar(255),
                route_desc varchar(255),
                route_type int NULL,
                route_color varchar(255),
                route_text_color varchar(255),
                PRIMARY KEY (route_id)
            )""",
            """CREATE TABLE shapes (
                shape_id varchar(255) NOT NULL,
                shape_pt_lat float NULL,
                shape_pt_lon float NULL,
                shape_pt_sequence int NOT NULL,
                PRIMARY KEY (shape_id, shape_pt_sequence)
            )""",
            # """CREATE TABLE split_shape (
            #     route_id varchar(255) NOT NULL,
            #     shape_id varchar(255) NOT NULL,
            #     split_id int NOT NULL,
            #     pt_id int NOT NULL,
            #     lat float NOT NULL,
            #     lon float NOT NULL,
            #     PRIMARY KEY (shape_id, split_id, pt_id)
            # )""",
            # """CREATE TABLE split_shape_speed (
            #     route_id varchar(255) NOT NULL,
            #     shape_id varchar(255) NOT NULL,
            #     split_id int NOT NULL,
            #     speed float NOT NULL,
            #     PRIMARY KEY (shape_id, split_id)
            # )""",
            """CREATE TABLE stop_times (
                trip_id varchar(255) NOT NULL,
                arrival_time time NULL,
                departure_time time NULL,
                stop_id varchar(255) NOT NULL,
                stop_sequence int NOT NULL,
                pickup_type int NULL,
                drop_off_type int NULL,
                PRIMARY KEY (trip_id, stop_id)
            )""",
            """CREATE TABLE stops (
                stop_id varchar(255) NOT NULL,
                stop_name varchar(255) NULL,
                stop_desc varchar(255) NULL,
                stop_lat float NULL,
                stop_lon float NULL,
                zone_id varchar(255) NULL,
                stop_url varchar(255) NULL,
                location_type varchar(255) NULL,
                parent_station varchar(255) NULL,
                PRIMARY KEY (stop_id)
            )""",
            """CREATE TABLE trips (
                route_id varchar(255) NOT NULL,
                service_id varchar(255) NULL,
                trip_id varchar(255) NOT NULL,
                trip_headsign varchar(255) NULL,
                direction_id varchar(255) NULL,
                shape_id varchar(255) NULL,
                PRIMARY KEY (trip_id)
            )""",
        ]

        # Execute each SQL statement to create the tables
        for query in sql_queries:
            cursor.execute(query)

        # Commit the transaction
        connection.commit()
        print("\nTables created successfully.")

    except (Exception, psycopg2.Error) as error:
        print("Error while creating tables:", error)

    cursor.close()
    connection.close()

    return None


def create_table_load_data() -> None:
    """
    This function calls the necessary function for Q1.
    We create tables, load them, delete unnecessary data and add constraints.
    All of this is done in this function by calling different functions.
    :return: None
    """

    # Call function create tables
    create_tables()

    # Call function to load tables
    insert_data_from_files()

    # Count total rows in each table
    count_total_rows()

    return None


def count_total_rows() -> None:
    """
    This function counts the total number of rows in all the tables for a given database
    :return: None
    """
    # Connect to the PostgreSQL database
    conn = connect_to_db()
    cur = conn.cursor()

    print("\n======================================================\n")
    try:
        # Get a list of all tables in the database
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        tables = cur.fetchall()

        total_rows = 0

        # Iterate over each table and count the rows
        for table in tables:
            table_name = table[0]
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            rows_count = cur.fetchone()[0]
            total_rows += rows_count

            print(f"Table '{table_name}' has {rows_count} rows.")

        print("\n======================================================\n")
        print(f"Total rows in all tables: {total_rows}")

    except psycopg2.Error as e:
        print(f"Error: {e}")

    finally:
        # Close the cursor and connection
        cur.close()
        conn.close()

    return None


def main() -> None:
    """
    Main function of the code that calls the required functions in order.
    :return: None
    """

    start_time = time.time()

    # Call the function to create tables and load the data
    create_table_load_data()

    end_time = time.time()
    total_total_time = end_time - start_time
    print("\n======================================================\n")
    print(
        f"Entire Database loaded successfully in: {total_total_time // 3600} Hours, {(total_total_time % 3600) // 60} "
        f"Minutes, and {(total_total_time % 3600) % 60} seconds.")

    # Connect to and close the connection to the database, in case it was missed somewhere
    conn = connect_to_db()
    conn.close()

    return None


if __name__ == "__main__":
    main()
