"""
Filename: NY Bus Data_Phase2.py
Author: Meet Gandhi, Hrishit Kotadia, Prabhav Karve
ID: mg1905, hjk5029, pk6004

This code loads data from the text files that the user provides into the desired database for Project.
This code also creates the tables needed for this data.

Steps to take before running this code:
1. Enter your postgres project database connection details in the fields given in the code below
2. Run the code

In the console, a few details are printed on running the code:
1. Confirmation that collections are created
2. Confirmation that the data is inserted into the desired collection
3. The result of queries before and after indexing
4. The functional dependency search results

"""


import time
import psycopg2
from psycopg2 import Error
from itertools import combinations
from pymongo import MongoClient

# Postgres connection settings
DB_NAME = "Project"
DB_USER = "postgres"
DB_PASSWORD = "MgRIT1905@28082808"
DB_HOST = "localhost"
DB_PORT = "2808"

# MongoDB's connection settings
connection_url = "mongodb://localhost:27017/"
mongo_db_name = "NY_Bus_Timing_Project"


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


def connect_to_mongodb():
    """
    Connect to the MongoDB Database using the parameters defined above
    :return: Database connection to the MongoDB and Client
    """
    try:
        client = MongoClient(connection_url)
        db = client[mongo_db_name]
        return db, client
    except Exception as e:
        print("Error connecting to MongoDB:", e)


def create_collections():
    """
    Create the collections in the MongoDB database
    :return: None
    """
    # Connect to MongoDB
    mongo_client = MongoClient(connection_url)
    mongo_db = mongo_client[mongo_db_name]
    mongo_db.create_collection("Calendar")
    mongo_db.create_collection("Arrival_Time")
    mongo_db.create_collection("Stops")
    mongo_db.create_collection("Routes")
    mongo_db.create_collection("Trips")
    mongo_db.create_collection("Real_Time_Data")
    print("\n======================================================\n")
    print("Created collections:", mongo_db.list_collection_names())
    mongo_client.close()


def load_calendar():
    """
    Load the Calendar Collection
    :return: None
    """
    print("\n======================================================\n")
    print("Loading the Calendar Collection in MongoDB...")

    # Connect to PostgreSQL
    postgres_conn = connect_to_db()
    postgres_cursor = postgres_conn.cursor()

    # Connect to MongoDB
    mongodb, mongo_client = connect_to_mongodb()
    calendar_collection = mongodb['Calendar']

    # Fetch all data from PostgreSQL and organize them into a dictionary
    calendar_dates_dict = {}
    postgres_cursor.execute("SELECT * FROM calendar_dates")
    calendar_dates_data = postgres_cursor.fetchall()
    for calendar_dates in calendar_dates_data:
        service_id = calendar_dates[0]
        dates_dict = {
            "Date": calendar_dates[1].strftime('%Y-%m-%d') if calendar_dates[1] is not None else None,
            "Exception_Type": calendar_dates[2] if calendar_dates[2] is not None else None
        }
        if service_id in calendar_dates_dict:
            calendar_dates_dict[service_id].append(dates_dict)
        else:
            calendar_dates_dict[service_id] = [dates_dict]

    # Fetch data from PostgreSQL
    postgres_cursor.execute("SELECT * FROM calendar")
    calendar_data = postgres_cursor.fetchall()

    successful_calendar_entry = 0

    # Transform and load data into MongoDB
    for calendar_row in calendar_data:
        calendar_dict = {
            "_id": calendar_row[0],
            "Monday": calendar_row[1] if calendar_row[1] is not None else None,
            "Tuesday": calendar_row[2] if calendar_row[2] is not None else None,
            "Wednesday": calendar_row[3] if calendar_row[3] is not None else None,
            "Thursday": calendar_row[4] if calendar_row[4] is not None else None,
            "Friday": calendar_row[5] if calendar_row[5] is not None else None,
            "Saturday": calendar_row[6] if calendar_row[6] is not None else None,
            "Sunday": calendar_row[7] if calendar_row[7] is not None else None,
            "Start_Date": calendar_row[8].strftime('%Y-%m-%d') if calendar_row[8] is not None else None,
            "End_Date": calendar_row[9].strftime('%Y-%m-%d') if calendar_row[9] is not None else None,
            "Calendar_Dates": calendar_dates_dict.get(calendar_row[0], None)
        }

        # Remove fields with None values
        calendar_dict = {key: value for key, value in calendar_dict.items() if value is not None}

        # Insert post document into MongoDB
        calendar_collection.insert_one(calendar_dict)
        successful_calendar_entry += 1

    print(f"\nTotal entries from Postgres: {len(calendar_data)}")
    print(f"Total entries added to MongoDB: {successful_calendar_entry}")

    # Close connections
    postgres_cursor.close()
    postgres_conn.close()
    mongo_client.close()


def load_arrival_time():
    """
    Load the arrival time Collection
    :return: None
    """
    print("\n======================================================\n")
    print("Loading the Arrival Time Collection in MongoDB...")

    # Connect to PostgreSQL
    postgres_conn = connect_to_db()
    postgres_cursor = postgres_conn.cursor()

    # Connect to MongoDB
    mongodb, mongo_client = connect_to_mongodb()
    arrival_time_collection = mongodb['Arrival_Time']

    # Fetch all data from PostgreSQL and organize them into a dictionary
    rtdt_dict = {}
    postgres_cursor.execute("SELECT * FROM real_time_data_temp")
    rtdt_data = postgres_cursor.fetchall()
    for rtdt in rtdt_data:
        aimed_arrival_time = rtdt[9]
        single_rtdt_dict = {
            "Route_Id": rtdt[0] if rtdt[0] is not None else None,
            "Direction": rtdt[1] if rtdt[1] is not None else None,
            "Trip_Id": rtdt[2] if rtdt[2] is not None else None,
            "Agency_Id": rtdt[3] if rtdt[3] is not None else None,
            "Origin_Stop": rtdt[4] if rtdt[4] is not None else None,
            "Lat": rtdt[5] if rtdt[5] is not None else None,
            "Lon": rtdt[6] if rtdt[6] is not None else None,
            "Bearing": rtdt[7] if rtdt[7] is not None else None,
            "Vehicle_Id": rtdt[8] if rtdt[8] is not None else None,
            # "Aimed_Arrival_time": rtdt[9].isoformat() if rtdt[9] is not None else None,
            "Distance_From_Origin": rtdt[10] if rtdt[10] is not None else None,
            "Presentable_Distance": rtdt[11] if rtdt[11] is not None else None,
            "Distance_From_Next_Stop": rtdt[12] if rtdt[12] is not None else None,
            "Next_Stop": rtdt[13] if rtdt[13] is not None else None,
            "Recorded_Time": rtdt[14].isoformat() if rtdt[14] is not None else None,
        }
        if aimed_arrival_time in rtdt_dict:
            rtdt_dict[aimed_arrival_time].append(single_rtdt_dict)
        else:
            rtdt_dict[aimed_arrival_time] = [single_rtdt_dict]

    # Fetch data from PostgreSQL
    postgres_cursor.execute("SELECT * FROM arrival_time")
    arrival_time_data = postgres_cursor.fetchall()

    successful_entry = 0

    # Transform and load data into MongoDB
    for arrival_time in arrival_time_data:
        arrival_time_dict = {
            "_id": arrival_time[0].isoformat(),
            "All_Count": arrival_time[1] if arrival_time[1] is not None else None,
            "Late_Count": arrival_time[2] if arrival_time[2] is not None else None,
            "Real_Time_Data": rtdt_dict.get(arrival_time[0], None)
        }

        # Remove fields with None values
        arrival_time_dict = {key: value for key, value in arrival_time_dict.items() if value is not None}

        # Insert post document into MongoDB
        arrival_time_collection.insert_one(arrival_time_dict)
        successful_entry += 1

    print(f"\nTotal entries from Postgres: {len(arrival_time_data)}")
    print(f"Total entries added to MongoDB: {successful_entry}")

    # Close connections
    postgres_cursor.close()
    postgres_conn.close()
    mongo_client.close()


def load_stops():
    """
    Load the stops Collection
    :return: None
    """
    print("\n======================================================\n")
    print("Loading the Stops Collection in MongoDB...")

    # Connect to PostgreSQL
    postgres_conn = connect_to_db()
    postgres_cursor = postgres_conn.cursor()

    # Connect to MongoDB
    mongodb, mongo_client = connect_to_mongodb()
    stops_collection = mongodb['Stops']

    # Fetch all data from PostgreSQL and organize them into a dictionary
    stop_times_dict = {}
    postgres_cursor.execute("SELECT * FROM stop_times")
    stop_times_data = postgres_cursor.fetchall()
    for stop_times in stop_times_data:
        stop_id = stop_times[3]
        single_stop_times_dict = {
            "Trip_Id": stop_times[0] if stop_times[0] is not None else None,
            "Arrival_Time": stop_times[1].isoformat() if stop_times[1] is not None else None,
            "Departure_Time": stop_times[2].isoformat() if stop_times[2] is not None else None,
            "Stop_Sequence": stop_times[4] if stop_times[4] is not None else None,
            "Pickup_Type": stop_times[5] if stop_times[5] is not None else None,
            "Drop_Off_Type": stop_times[6] if stop_times[6] is not None else None,
        }
        if stop_id in stop_times_dict:
            stop_times_dict[stop_id].append(single_stop_times_dict)
        else:
            stop_times_dict[stop_id] = [single_stop_times_dict]

    # Fetch data from PostgreSQL
    postgres_cursor.execute("SELECT * FROM stops")
    stops_data = postgres_cursor.fetchall()

    successful_entry = 0

    # Transform and load data into MongoDB
    for stops in stops_data:
        stops_dict = {
            "_id": stops[0],
            "Stop_Name": stops[1] if stops[1] is not None else None,
            "Stop_Desc": stops[2] if stops[2] is not None else None,
            "Stop_Lat": stops[3] if stops[3] is not None else None,
            "Stop_Lon": stops[4] if stops[4] is not None else None,
            "Zone_Id": stops[5] if stops[5] is not None else None,
            "Stop_URL": stops[6] if stops[6] is not None else None,
            "Location_Type": stops[7] if stops[7] is not None else None,
            "Parent_Station": stops[8] if stops[8] is not None else None,
            "Stop_Times": stop_times_dict.get(stops[0], None)
        }

        # Remove fields with None values
        stops_dict = {key: value for key, value in stops_dict.items() if value is not None}

        # Insert post document into MongoDB
        stops_collection.insert_one(stops_dict)
        successful_entry += 1

    print(f"\nTotal entries from Postgres: {len(stops_data)}")
    print(f"Total entries added to MongoDB: {successful_entry}")

    # Close connections
    postgres_cursor.close()
    postgres_conn.close()
    mongo_client.close()


def load_routes():
    """
    Load the routes Collection
    :return: None
    """
    print("\n======================================================\n")
    print("Loading the Routes Collection in MongoDB...")

    # Connect to PostgreSQL
    postgres_conn = connect_to_db()
    postgres_cursor = postgres_conn.cursor()

    # Connect to MongoDB
    mongodb, mongo_client = connect_to_mongodb()
    routes_collection = mongodb['Routes']

    # # Fetch all data from PostgreSQL and organize them into a dictionary
    # rtdt_dict = {}
    # postgres_cursor.execute("SELECT * FROM real_time_data_temp")
    # rtdt_data = postgres_cursor.fetchall()
    # for rtdt in rtdt_data:
    #     Route_Id = rtdt[0]
    #     single_rtdt_dict = {
    #         # "Route_Id": rtdt[0] if rtdt[0] is not None else None,
    #         # "Direction": rtdt[1] if rtdt[1] is not None else None,
    #         "Trip_Id": rtdt[2] if rtdt[2] is not None else None,
    #         "Agency_Id": rtdt[3] if rtdt[3] is not None else None,
    #         # "Origin_Stop": rtdt[4] if rtdt[4] is not None else None,
    #         # "Lat": rtdt[5] if rtdt[5] is not None else None,
    #         # "Lon": rtdt[6] if rtdt[6] is not None else None,
    #         # "Bearing": rtdt[7] if rtdt[7] is not None else None,
    #         "Vehicle_Id": rtdt[8] if rtdt[8] is not None else None,
    #         "Aimed_Arrival_time": rtdt[9].isoformat() if rtdt[9] is not None else None,
    #         # "Distance_From_Origin": rtdt[10] if rtdt[10] is not None else None,
    #         # "Presentable_Distance": rtdt[11] if rtdt[11] is not None else None,
    #         # "Distance_From_Next_Stop": rtdt[12] if rtdt[12] is not None else None,
    #         # "Next_Stop": rtdt[13] if rtdt[13] is not None else None,
    #         # "Recorded_Time": rtdt[14].isoformat() if rtdt[14] is not None else None,
    #     }
    #     if Route_Id in rtdt_dict:
    #         rtdt_dict[Route_Id].append(single_rtdt_dict)
    #     else:
    #         rtdt_dict[Route_Id] = [single_rtdt_dict]

    trips_dict = {}
    postgres_cursor.execute("SELECT * FROM trips")
    trips_data = postgres_cursor.fetchall()
    for trip in trips_data:
        Route_Id = trip[0]
        single_trip_dict = {
            "Trip_Id": trip[2],
            # "Route_Id": trip[0] if trip[0] is not None else None,
            "Service_Id": trip[1] if trip[1] is not None else None,
            "Trip_Headsign": trip[3] if trip[3] is not None else None,
            "Direction_Id": trip[4] if trip[4] is not None else None,
            # "Shape_Id": trip[5] if trip[5] is not None else None,
        }
        if Route_Id in trips_dict:
            trips_dict[Route_Id].append(single_trip_dict)
        else:
            trips_dict[Route_Id] = [single_trip_dict]

    # Fetch data from PostgreSQL
    postgres_cursor.execute("SELECT * FROM routes")
    routes_data = postgres_cursor.fetchall()

    successful_entry = 0

    # Transform and load data into MongoDB
    for route in routes_data:
        route_dict = {
            "_id": route[0],
            "Agency_Id": route[1] if route[1] is not None else None,
            "Route_Short_Name": route[2] if route[2] is not None else None,
            "Route_Long_Name": route[2] if route[2] is not None else None,
            "Route_Desc": route[2] if route[2] is not None else None,
            "Route_Type": route[2] if route[2] is not None else None,
            "Route_Color": route[2] if route[2] is not None else None,
            "Route_Text_Color": route[2] if route[2] is not None else None,
            # "Real_Time_Data": rtdt_dict.get(route[0], None),
            "Trips": trips_dict.get(route[0], None)
        }

        # Remove fields with None values
        route_dict = {key: value for key, value in route_dict.items() if value is not None}

        # Insert post document into MongoDB
        routes_collection.insert_one(route_dict)
        successful_entry += 1

    print(f"\nTotal entries from Postgres: {len(routes_data)}")
    print(f"Total entries added to MongoDB: {successful_entry}")

    # Close connections
    postgres_cursor.close()
    postgres_conn.close()
    mongo_client.close()


def load_trips():
    """
    Load the Trips Collection
    :return: None
    """
    print("\n======================================================\n")
    print("Loading the Trips Collection in MongoDB...")

    # Connect to PostgreSQL
    postgres_conn = connect_to_db()
    postgres_cursor = postgres_conn.cursor()

    # Connect to MongoDB
    mongodb, mongo_client = connect_to_mongodb()
    trips_collection = mongodb['Trips']

    # Fetch all data from PostgreSQL and organize them into a dictionary
    rtdt_dict = {}
    postgres_cursor.execute("SELECT * FROM real_time_data_temp")
    rtdt_data = postgres_cursor.fetchall()
    for rtdt in rtdt_data:
        Trip_Id = rtdt[2]
        single_rtdt_dict = {
            "Route_Id": rtdt[0] if rtdt[0] is not None else None,
            "Direction": rtdt[1] if rtdt[1] is not None else None,
            # "Trip_Id": rtdt[2] if rtdt[2] is not None else None,
            "Agency_Id": rtdt[3] if rtdt[3] is not None else None,
            "Origin_Stop": rtdt[4] if rtdt[4] is not None else None,
            "Lat": rtdt[5] if rtdt[5] is not None else None,
            "Lon": rtdt[6] if rtdt[6] is not None else None,
            "Bearing": rtdt[7] if rtdt[7] is not None else None,
            "Vehicle_Id": rtdt[8] if rtdt[8] is not None else None,
            "Aimed_Arrival_time": rtdt[9].isoformat() if rtdt[9] is not None else None,
            "Distance_From_Origin": rtdt[10] if rtdt[10] is not None else None,
            "Presentable_Distance": rtdt[11] if rtdt[11] is not None else None,
            "Distance_From_Next_Stop": rtdt[12] if rtdt[12] is not None else None,
            "Next_Stop": rtdt[13] if rtdt[13] is not None else None,
            "Recorded_Time": rtdt[14].isoformat() if rtdt[14] is not None else None,
        }
        if Trip_Id in rtdt_dict:
            rtdt_dict[Trip_Id].append(single_rtdt_dict)
        else:
            rtdt_dict[Trip_Id] = [single_rtdt_dict]

    # Fetch data from PostgreSQL
    postgres_cursor.execute("SELECT * FROM trips")
    trips_data = postgres_cursor.fetchall()

    successful_entry = 0

    # Transform and load data into MongoDB
    for trip in trips_data:
        trips_dict = {
            "_id": trip[2],
            "Route_Id": trip[0] if trip[0] is not None else None,
            "Service_Id": trip[1] if trip[1] is not None else None,
            "Trip_Headsign": trip[3] if trip[3] is not None else None,
            "Direction_Id": trip[4] if trip[4] is not None else None,
            "Shape_Id": trip[5] if trip[5] is not None else None,
            "Real_Time_Data": rtdt_dict.get(trip[2], None)
        }

        # Remove fields with None values
        trips_dict = {key: value for key, value in trips_dict.items() if value is not None}

        # Insert post document into MongoDB
        trips_collection.insert_one(trips_dict)
        successful_entry += 1

    print(f"\nTotal entries from Postgres: {len(trips_data)}")
    print(f"Total entries added to MongoDB: {successful_entry}")

    # Close connections
    postgres_cursor.close()
    postgres_conn.close()
    mongo_client.close()


def load_real_time_data():
    """
    Load the real time data Collection
    :return: None
    """
    print("\n======================================================\n")
    print("Loading the Real Time Data Collection in MongoDB...")

    # Connect to PostgreSQL
    postgres_conn = connect_to_db()
    postgres_cursor = postgres_conn.cursor()

    # Connect to MongoDB
    mongodb, mongo_client = connect_to_mongodb()
    real_time_data_collection = mongodb['Real_Time_Data']

    # Fetch data from PostgreSQL
    postgres_cursor.execute("SELECT * FROM real_time_data_temp")
    real_time_data_data = postgres_cursor.fetchall()

    successful_rtdt_entry = 0

    # Transform and load data into MongoDB
    for rtdt in real_time_data_data:
        rtdt_dict = {
            "Route_Id": rtdt[0] if rtdt[0] is not None else None,
            "Direction": rtdt[1] if rtdt[1] is not None else None,
            "Trip_Id": rtdt[2] if rtdt[2] is not None else None,
            "Agency_Id": rtdt[3] if rtdt[3] is not None else None,
            "Origin_Stop": rtdt[4] if rtdt[4] is not None else None,
            "Lat": rtdt[5] if rtdt[5] is not None else None,
            "Lon": rtdt[6] if rtdt[6] is not None else None,
            "Bearing": rtdt[7] if rtdt[7] is not None else None,
            "Vehicle_Id": rtdt[8] if rtdt[8] is not None else None,
            "Aimed_Arrival_time": rtdt[9].isoformat() if rtdt[9] is not None else None,
            "Distance_From_Origin": rtdt[10] if rtdt[10] is not None else None,
            "Presentable_Distance": rtdt[11] if rtdt[11] is not None else None,
            "Distance_From_Next_Stop": rtdt[12] if rtdt[12] is not None else None,
            "Next_Stop": rtdt[13] if rtdt[13] is not None else None,
            "Recorded_Time": rtdt[14].isoformat() if rtdt[14] is not None else None,
        }

        # Remove fields with None values
        rtdt_dict = {key: value for key, value in rtdt_dict.items() if value is not None}

        # Insert post document into MongoDB
        real_time_data_collection.insert_one(rtdt_dict)
        successful_rtdt_entry += 1

    print(f"\nTotal entries from Postgres: {len(real_time_data_data)}")
    print(f"Total entries added to MongoDB: {successful_rtdt_entry}")

    # Close connections
    postgres_cursor.close()
    postgres_conn.close()
    mongo_client.close()


def load_data_into_MongoDB():
    """
    Call functions to load each of the different collections
    :return: None
    """
    load_real_time_data()
    load_calendar()
    load_arrival_time()
    load_stops()
    load_routes()
    load_trips()


def create_and_load_data_into_MongoDB():
    """
    Call functions to create and load the mongoDb collections
    :return: None
    """
    create_collections()
    load_data_into_MongoDB()


def delete_rows_violating_foreign_key(table_name, foreign_key_column, reference_table_name, reference_column) -> None:
    """
    Delete rows violating foreign
    :param table_name: Table from which the rows are deleted
    :param foreign_key_column: The foreign key column from which the rows are deleted
    :param reference_table_name: The table from which the values are checked
    :param reference_column: The column from which the values are checked
    :return: None
    """

    print("\n======================================================\n")
    print(f"Checking data for table {table_name} with foreign key column {foreign_key_column}")

    # Connect to the database
    connection = connect_to_db()
    cursor = connection.cursor()

    # Keep track of total deleted rows
    delete = 0

    # Define aliases for the tables
    table_alias = "og"
    reference_table_alias = "away"

    try:

        # Construct the select query with aliases
        select_query = (f"SELECT {table_alias}.* FROM {table_name} AS {table_alias} "
                        f"LEFT JOIN {reference_table_name} AS {reference_table_alias} "
                        f"ON {table_alias}.{foreign_key_column} = {reference_table_alias}.{reference_column} "
                        f"WHERE {reference_table_alias}.{reference_column} IS NULL "
                        f"AND {table_alias}.{foreign_key_column} IS NOT NULL;")

        cursor.execute(select_query)
        rows = cursor.fetchall()
        print(f"To delete {len(rows)} rows from table {table_name} for column {foreign_key_column}")

        # if rows:
        #     # Define the SQL query to retrieve the ordinal position of the foreign key column
        #     query = (f" SELECT ordinal_position FROM information_schema.columns WHERE "
        #              f"table_name = '{table_name.lower()}' AND column_name = '{foreign_key_column.lower()}';")
        #     cursor.execute(query)
        #
        #     # Fetch the result
        #     index = int(cursor.fetchone()[0]) - 1
        #
        #     # Iterate through each row and delete it
        #     for row in rows:
        #         value = (int(row[index]))
        #         # print("Deleting: ", value)
        #         delete_query = f"DELETE FROM {table_name} WHERE {foreign_key_column} = %s;"
        #         cursor.execute(delete_query, [value])
        #         connection.commit()
        #         delete += 1

    except (Exception, Error) as e:
        print(f"Error: {e}")

    # print("\n Deleted ", delete, " rows from table", table_name)
    cursor.close()
    connection.close()

    return None


def delete_rows_violating_foreign_keys() -> None:
    """
    Function to figure out which rows are violating the foreign key constraints.
    We pass this value to other function to execute the delete statements
    :return: None
    """

    # Define a dictionary mapping tables to their foreign key columns and reference tables
    foreign_key_info = {
        "real_time_data_temp": [("route_id", "routes", "route_id"),
                                ("trip_id", "trips", "trip_id"),
                                ("agency_id", "agency", "agency_id"),
                                ("aimed_arrival_time", "arrival_time", "time_span")],

        "routes": [("agency_id", "agency", "agency_id")],

        "stop_times": [("trip_id", "trips", "trip_id"),
                       ("stop_id", "stops", "stop_id")],

        "trips": [("route_id", "routes", "route_id"),
                  ("service_id", "calendar", "service_id")],

        "calendar_dates": [("service_id", "calendar", "service_id")]
    }

    # Iterate over the dictionary and call delete_rows_violating_foreign_key for each table and foreign key column
    for table_name, foreign_keys in foreign_key_info.items():
        for foreign_key_column, reference_table_name, reference_column in foreign_keys:
            delete_rows_violating_foreign_key(table_name, foreign_key_column, reference_table_name, reference_column)

    return None


def relation_sets():
    """
    Function to fetch all the rows and column names in the table
    :return: Tuple containing the rows and column names
    """
    # Connect to the database
    conn = connect_to_db()
    cur = conn.cursor()

    rows = []
    cleaned_column_names = []

    try:

        # Find column names
        cur.execute("SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = 'transitdata' ORDER BY ordinal_position")
        columns = cur.fetchall()
        cleaned_column_names = [column[0] for column in columns]

        # Execute SQL query to fetch data
        cur.execute("SELECT * FROM transitdata")

        # Fetch all rows from the result
        rows = cur.fetchall()

    except Exception as e:
        print("Something went wrong while fetching relations: ", e)

    # Close cursor and connection
    cur.close()
    conn.close()

    return rows, cleaned_column_names


def generate_attribute_partitions(relation):
    """
    Function to generate the attribute based partitions
    :param relation: the rows in the table
    :return: The dictionary that contains the partitions
    """
    attribute_partitions = {}
    for x, row in enumerate(relation):
        for i, value in enumerate(row):
            # Create a new partition dictionary if the attribute doesn't exist
            if i not in attribute_partitions:
                attribute_partitions[i] = {}

            # Add the value to the partition for the current attribute
            if value in attribute_partitions[i]:
                attribute_partitions[i][value].add(x)
            else:
                attribute_partitions[i][value] = {x}
    return attribute_partitions


def generate_level2_combinations(other_columns, i):
    """
    Generates the combination of columns at different levels
    :param other_columns: list of column to be combined
    :return: list of combinations of columns
    """
    all_combinations = []

    other_columns.sort()  # Sort to ensure unique combinations

    all_combinations.extend(combinations(other_columns, i))

    return all_combinations


def compute_pi_alpha(relation, attributes):
    """
    Function to make the partition dictionary for Alpha side of comparison
    :param relation: rows of the table
    :param attributes: the list of attributes on the left side of the comparison
    :return: the dictionary that contains the partitions for the left side of the comparison
    """
    pi_alpha = {}
    for x, row in enumerate(relation):
        pi_row = tuple(row[attr] for attr in attributes)
        if pi_row in pi_alpha:
            pi_alpha[pi_row].add(x)
        else:
            pi_alpha[pi_row] = {x}
    return pi_alpha


def compute_A_and_B(partitions, combination, relations, B, i):
    """
    Function to create the partition dictionary for Alpha and Beta side of comparison
    :param partitions: Dictionary of all the partitions
    :param combination: the combination to be checked
    :param relations: The rows in the table
    :param B: The RHS of the comparison
    :param i: The level to be checked at
    :return: The tuple of two dictionaries of Partition A and B
    """
    partition_B = partitions[B]
    if i > 1:
        key = combination
        if key in partitions:
            partition_A = partitions[key]
        else:
            partitions[key] = compute_pi_alpha(relations, combination)
            partition_A = partitions[key]
    else:
        # print(combination)
        partition_A = partitions[combination[0]]

    return partition_A, partition_B


def refine_partitions(partition_alpha, partition_b):
    """
    Checking if alpha partition refines the beta partition
    :param partition_alpha: left side of the comparison
    :param partition_b: right side of the comparison
    :return: Boolean indicating if the alpha partition refines the beta partition
    """
    for key_alpha, values_alpha in partition_alpha.items():
        is_refined = False
        for key_beta, values_beta in partition_b.items():
            if set(values_alpha).issubset(set(values_beta)):
                is_refined = True
                break
        if not is_refined:
            return False
    return True


def compute_fds(partition_alpha, partition_b, ):
    """
    Checking if alpha partition refines the beta partition
    :param partition_alpha: left side of the comparison
    :param partition_b: right side of the comparison
    :return:
    """

    return refine_partitions(partition_alpha, partition_b)


def prune_relations(partitions, column_names, relations):
    """
    Main function that prunes the redundant functional dependencies
    :param partitions: Dictionary of all the partitions
    :param column_names: List of column names
    :param relations: Rows in the table
    :return: List of functional dependencies
    """
    fds = []

    columns = range(len(column_names))
    for i in range(1, 5):
        for B in columns:
            print(f"\nChecking for RHS -> {B}")
            other_columns = [col for col in columns if col != B]
            column_combinations = generate_level2_combinations(other_columns, i)
            # print("Made column combinations: ", column_combinations)

            for combination in column_combinations:
                # alpha = ", ".join(column_names[attr] for attr in combination)
                # print(f"Checking for {alpha} -> {column_names[B]}")
                is_not_implied = True
                for fd in fds:
                    left_fd, right_fd = fd[0], fd[1]
                    if B == right_fd and left_fd[0] in combination:
                        # alpha = ", ".join(column_names[attr] for attr in fd[0])
                        beta = column_names[fd[1]]
                        print(f"\nPruned ({column_names[combination[0]]}, {column_names[combination[1]]} -> {beta}) "
                              f"because of ({column_names[left_fd[0]]} -> {beta})")
                        is_not_implied = False

                if is_not_implied:
                    partition_A, partition_B = compute_A_and_B(partitions, combination, relations, B, i)
                    if compute_fds(partition_A, partition_B):
                        alpha = ", ".join(column_names[attr] for attr in combination)
                        beta = column_names[B]
                        print(f"\nFunctional Dependency Found: ({alpha} -> {beta})")
                        fds.append((combination, B))

    return fds


def find_functional_dependencies_by_pruning():
    """
    Main function to find the functional dependencies by calling required columns
    :return: None
    """
    print("\n======================================================\n")
    print("Finding Functional Dependencies...")
    relations, column_names = relation_sets()

    start_time = time.time()
    attribute_partitions = generate_attribute_partitions(relations)

    fds = prune_relations(attribute_partitions, column_names, relations)

    print_fds(fds, column_names)
    end_time = time.time()
    total_time = end_time - start_time

    print(f"\nTime taken to find functional dependencies: {total_time // 3600} Hours, "
          f"{(total_time % 3600) // 60} Minutes, and {(total_time % 3600) % 60} seconds.")


def print_fds(fds, column_names):
    """
    Print the list of functional dependencies
    :param fds: list of functional dependencies
    :param column_names: list of column names
    :return: None
    """
    print("\n")
    for fd in fds:
        alpha = ", ".join(column_names[attr] for attr in fd[0])
        beta = column_names[fd[1]]
        print(f"{alpha} -> {beta}")

    print("Total Functional Dependencies: ", len(fds))


def create_new_relation():
    """
    Function to create the new table needed
    :return: None
    """
    print("\n======================================================\n")
    print("Creating New table TransitData...")
    # Create a new table by joining User, Post, PostTags, and Tags
    q1_query = ('''
            CREATE TABLE TransitData AS
    SELECT
        a.agency_id,
        a.agency_name,
        c.service_id,
        c.start_date,
        c.end_date,
        r.route_id,
        r.route_short_name,
        r.route_long_name,
        r.route_type,
        t.trip_headsign,
        t.direction_id
    FROM
        agency a
    JOIN
        routes r ON r.agency_id = a.agency_id
    JOIN
        trips t ON r.route_id = t.route_id
    JOIN
        calendar c ON c.service_id = t.service_id
    GROUP BY
        a.agency_id, a.agency_name,
        c.service_id, c.start_date, c.end_date,
        r.route_id, r.route_short_name, r.route_long_name, r.route_type,
        t.trip_headsign, t.direction_id
    HAVING
        COUNT(*) = 1;
        ''')

    q1_query2 = ('''
            SELECT * from TransitData LIMIT 50;
            ''')

    # Run the create table query
    execute_query(q1_query)
    print("Table TransitData created successfully\n")
    execute_query(q1_query2)


def execute_query(query) -> None:
    """
    This function is just used to call the functions required like printing the sql output and explain analyze.
    We are also given a choice if we want to print the outputs of the SQL Query to standard output
    :param query: the query to be executed
    :return: None
    """

    # Connect to the database
    connection = connect_to_db()
    cursor = connection.cursor()

    try:

        """
        Only Uncomment this below function call if you want to print all the sql outputs for SELECT statements
        """
        print_sql_results(query)

        # Call the function to run the explain sql statement
        # explain_analyze(query, connection)

    except Exception as e:
        print("Error:", e)

    finally:
        # Close cursor and connection
        cursor.close()
        connection.close()

    return None


def print_sql_results(query) -> None:
    """
    Function to print the output of the given SQL query
    :param query:  query to be executed
    :return: None
    """

    # Connect to the database
    connection = connect_to_db()
    cursor = connection.cursor()

    # Define the upper limit for select statements so that the console is not cluttered
    limit = 10

    try:
        # Execute the SQL query
        cursor.execute(query)

        if cursor.description is not None:
            # Fetch all rows from the result
            rows = cursor.fetchall()

            print("Query Result:")
            for i, row in enumerate(rows):
                if i > limit:
                    print("Printing only 10 by LIMIT")
                    break
                else:
                    print(row)

        else:
            print("No rows to be printed\n")

        # Commit the transaction
        connection.commit()

    except (Exception, psycopg2.Error) as error:
        print("Error while executing query:", error)
        connection.rollback()

    # Close cursor and connection
    cursor.close()
    connection.close()

    return None


def queries():
    """
    Interesting queries to be executed
    :return: None
    """
    print("\n======================================================\n")
    print("Executing Queries...")
    query1 = ("SELECT r.route_id, COUNT(CASE WHEN rt.aimed_arrival_time < rt.recorded_time THEN 1 END) * 100.0 / COUNT(*) AS late_percentage "
              "FROM routes r "
              "JOIN real_time_data_temp rt ON r.route_id = rt.route_id "
              "GROUP BY r.route_id "
              "ORDER BY late_percentage DESC;")

    query2 = ("SELECT EXTRACT(HOUR FROM arrival_time) AS hour_of_day, COUNT(*) AS arrival_count "
              "FROM stop_times "
              "GROUP BY hour_of_day "
              "ORDER BY arrival_count DESC "
              "LIMIT 1;")

    query3 = ("SELECT t.trip_id, COUNT(st.stop_id) AS total_stops FROM trips t "
              "JOIN stop_times st ON t.trip_id = st.trip_id "
              "GROUP BY t.trip_id;")

    query4 = ("SELECT a.agency_name, r.route_id, COUNT(s.stop_id) AS total_stops "
              "FROM agency a "
              "JOIN routes r ON a.agency_id = r.agency_id "
              "LEFT JOIN trips t ON r.route_id = t.route_id "
              "LEFT JOIN stop_times st ON t.trip_id = st.trip_id "
              "LEFT JOIN stops s ON st.stop_id = s.stop_id "
              "GROUP BY a.agency_name, r.route_id;")

    query5 = ("SELECT r.route_id "
             "FROM routes r "
             "JOIN trips t ON r.route_id = t.route_id "
             "JOIN calendar c ON t.service_id = c.service_id "
             "WHERE c.monday = TRUE AND c.tuesday = TRUE AND c.wednesday = TRUE AND c.thursday = TRUE AND c.friday = TRUE;")

    print("\nRetrieve the routes with the highest percentage of late arrivals compared to total arrivals:")
    start_time = time.time()
    execute_query(query1)
    total_time = time.time() - start_time
    print(f"\nTime taken to execute query: {total_time // 3600} Hours, "
          f"{(total_time % 3600) // 60} Minutes, and {(total_time % 3600) % 60} seconds.")

    print("\nIdentify the busiest hour of the day across all stops:")
    start_time = time.time()
    execute_query(query2)
    total_time = time.time() - start_time
    print(f"\nTime taken to execute query: {total_time // 3600} Hours, "
          f"{(total_time % 3600) // 60} Minutes, and {(total_time % 3600) % 60} seconds.")

    print("\nList all trips along with the total number of stops served by each trip:")
    start_time = time.time()
    execute_query(query3)
    total_time = time.time() - start_time
    print(f"\nTime taken to execute query: {total_time // 3600} Hours, "
          f"{(total_time % 3600) // 60} Minutes, and {(total_time % 3600) % 60} seconds.")

    print("\nRetrieve agency names and their associated routes along with the total count of stops for each route:")
    start_time = time.time()
    execute_query(query4)
    total_time = time.time() - start_time
    print(f"\nTime taken to execute query: {total_time // 3600} Hours, "
          f"{(total_time % 3600) // 60} Minutes, and {(total_time % 3600) % 60} seconds.")

    print("\nRetrieve the routes that have trips serving all weekdays (Monday to Friday):")
    start_time = time.time()
    execute_query(query5)
    total_time = time.time() - start_time
    print(f"\nTime taken to execute query: {total_time // 3600} Hours, "
          f"{(total_time % 3600) // 60} Minutes, and {(total_time % 3600) % 60} seconds.")


def run_queries_do_indexing():
    """
    Call the functions to run queries before and after indexing
    :return: None
    """
    queries()
    create_indexes()
    queries()


def create_indexes():
    """
    Create the required indexes in the tables
    :return: None
    """
    print("\n======================================================\n")
    print("Creating Indexes...")
    # Connect to the database
    connection = connect_to_db()
    cursor = connection.cursor()

    query1 = ("CREATE INDEX ROUTEINDEX ON ROUTES(ROUTE_ID)")

    query2 = ("CREATE INDEX RTDTINDEX ON REAL_TIME_DATA_TEMP(aimed_arrival_time, route_id)")

    query3 = ("CREATE INDEX STOPTIMEINDEX ON STOP_TIMES(ARRIVAL_TIME, TRIP_ID, STOP_ID)")

    query4 = ("CREATE INDEX STOPINDEX ON STOPS(STOP_ID)")

    query5 = ("CREATE INDEX TRIPINDEX ON TRIPS(TRIP_ID, ROUTE_ID, SERVICE_ID)")

    query6 = ("CREATE INDEX CALENDARINDEX ON CALENDAR(SERVICE_ID)")

    cursor.execute(query1)
    cursor.execute(query2)
    cursor.execute(query3)
    cursor.execute(query4)
    cursor.execute(query5)
    cursor.execute(query6)

    # Close cursor and connection
    cursor.close()
    connection.close()
    print("Indexes Created")


def functional_dependencies():
    """
    Create a new table and find the functional dependencies
    :return: None
    """
    create_new_relation()

    find_functional_dependencies_by_pruning()


def main():
    """
    Main function of the code that calls the required functions in order.
    :return: None
    """

    create_and_load_data_into_MongoDB()

    run_queries_do_indexing()

    functional_dependencies()


if __name__ == "__main__":
    main()
