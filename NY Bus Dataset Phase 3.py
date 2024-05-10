"""
Filename: NY Bus Dataset Phase 3.py
Author: Meet Gandhi, Hrishit Kotadia, Prabhav Karve
ID: mg1905, hjk5029, pk6004

Data Cleaning:
    There is a function mentioned for cleaning up the date data, but it has not been used in this
    code as this function was already used to clean up the data in phase 2 while uploading data to
    the MongoDB database. Rest of the cleaning was already part of the uploading process of the data
    to the database and hence this phase needed no explicit data cleaning to be done. All the data
    cleaning done through all the phases has been mentioned in the report itself.


Itemset Mining:
    This code loads data into particular levels of tables according to the generated itemsets.
    In this case it goes upto level 15 with an empty level 16.
    Here the transaction is trips that we have on the left hand side of the relation
    and the corresponding stations have been selected as tags or RHS of the relation,
    with each trip having 15-20 stops each.


Association Mining:
    This code takes itemsets starting from level 2 upto the final level and generates, firstly,
    all possible combinations of possible association rules. So for each frequent itemset (k > 1),
    consider possible association rules. In this we calculate the confidence if it exists and then
    at the end we filter it for a minimum confidence threshold and look at some of the outputs and
    also visualize it using plots

Steps to take before running this code:
1. Enter your postgres project database connection details in the fields given in the code below
2. Import all the necessary packages from the import part of the code
3. Run the code

In the console, a few details are printed on running the code:
1. Confirmation that new table has been created and data has been uploaded to it
2. Details about the itemsets at each level of the lattice formed
3. Some examples of the Association rules derived from the association mining
4. Time taken to run each of the important parts of the code
"""

import itertools
import psycopg2
from psycopg2 import Error
import random
import time
import matplotlib.pyplot as plt


# Postgres connection settings
DB_NAME = "Project"
DB_USER = "postgres"
DB_PASSWORD = ""
DB_HOST = "localhost"
DB_PORT = ""


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


def print_time(total_time) -> None:
    """
    Prints the time taken to run the code
    :param total_time: Time taken to run the specific query
    :return: None
    """
    print(f"\nTime taken to complete: "
          f"{total_time // 3600} Hours, "
          f"{(total_time % 3600) // 60} Minutes, "
          f"and {(total_time % 3600) % 60} seconds.")

    return None


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


def get_last_level_from_database() -> int:
    """
    Get the last level from the database by querying the information schema
    :return: The int value of the last level (e.g., 3 from 'Level3')
    """
    # print("\nFetching the last level from the database...")
    # Connect to the database
    connection = connect_to_db()
    cursor = connection.cursor()

    # Query the information schema to get all table names
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
    table_names = [record[0] for record in cursor.fetchall()]

    # Extract the integer values from table names related to lattice
    lattice_levels = [int(name.replace('level', '')) for name in table_names if name.startswith('level')]

    if not lattice_levels:
        print("No lattice tables found in the database.")
        connection.close()
        cursor.close()
        return 0

    # Get the maximum integer value to determine the last level
    last_level = max(lattice_levels)

    # print(f"Last level in the database: {last_level}")
    connection.close()
    cursor.close()
    return last_level


def drop_lattice_tables_up_to_last_level(last_level) -> None:
    """
    Drop all tables in the database related to lattice up to the specified last level
    :param last_level: The last level to delete (e.g., 3 for 'Level3')
    :return: None
    """
    print("\nDropping all lattice related tables up to the specified last level...")
    # Connect to the database
    connection = connect_to_db()
    cursor = connection.cursor()

    # Loop through the tables up to the last level and drop them
    for i in range(1, last_level + 1):
        sql_query = f"DROP TABLE IF EXISTS level{i};"
        cursor.execute(sql_query)
        print(f"Dropping table Level{i}")

    print(f"Dropped tables up to level {last_level}.")

    connection.commit()
    cursor.close()
    connection.close()


def drop_lattice_tables() -> None:
    """
    Drop all tables in the database related to lattice
    :return: None
    """
    print("\n======================================================\n")
    # print("Dropping all lattice related tables...")

    last_level = get_last_level_from_database()
    drop_lattice_tables_up_to_last_level(last_level)

    print("Deleted All Lattice Tables")

    return None


def create_table_query(level, support) -> str:
    """
    Creates the query to be executed for a particular level
    :param support: The support to find the frequent itemsets
    :param level: The level for which the query has to be created
    :return: The query to be executed for a particular level
    """
    if level == 1:
        return f"""
        CREATE TABLE IF NOT EXISTS Level1 AS
        SELECT Stop_Id AS Stop1, COUNT(*) AS count
        FROM TripStops
        GROUP BY Stop_Id
        HAVING COUNT(*) >= {support};
        """
    else:
        prev_level = level - 1
        prev_table = f"Level{prev_level} l{prev_level}"
        first_inner_join = f"INNER JOIN TripStops st1 ON st1.Stop_Id = l{prev_level}.Stop1"
        last_inner_join = f"INNER JOIN TripStops st{level} ON st1.Trip_Id = st{level}.Trip_Id"
        middle_inner_join = ''.join(
            f"INNER JOIN TripStops st{i} ON st1.Trip_Id = st{i}.Trip_Id AND st{i}.Stop_Id = l{prev_level}.Stop{i} " for
            i in range(2, level))
        where_clause = f' AND '.join(f"st{i}.Stop_Id < st{i + 1}.Stop_Id" for i in range(1, level))
        tag_select = ", ".join(f"st{i}.Stop_Id AS Stop{i}" for i in range(1, level + 1))
        tag_group_by = ", ".join(f"st{i}.Stop_Id" for i in range(1, level + 1))
        return f"""
        CREATE TABLE IF NOT EXISTS Level{level} AS
        SELECT {tag_select}, COUNT(*) AS count
        FROM {prev_table}
        {first_inner_join}
        {middle_inner_join}
        {last_inner_join}
        WHERE {where_clause}
        GROUP BY {tag_group_by}
        HAVING COUNT(*) >= {support};
        """


def automated_lattice_creation() -> int:
    """
    Creates automated command to create the lattice tables for each level till we reach
    a level table with 0 itemsets input
    :return: The integer value showing the last non-empty level in the lattice tables
    """
    # Drop all the previous lattice tables
    drop_lattice_tables()

    start_time = time.time()
    print("\n======================================================\n")
    print("Auto creating tables for all possible lattice levels...\n")
    # Connect to the database
    connection = connect_to_db()
    cursor = connection.cursor()

    # Initialize variables
    level = 1
    support = 1500

    # Generate and execute queries for each level until an empty table is created
    while True:
        query = create_table_query(level, support)
        # print(query)
        cursor.execute(query)
        connection.commit()

        # Check if the table is empty
        cursor.execute(f"SELECT COUNT(*) FROM Level{level};")
        if cursor.fetchone()[0] == 0:
            print(f"Level {level}: 0 frequent itemsets")
            break

        # Count the number of frequent itemsets in the current level
        cursor.execute(f"SELECT COUNT(*) FROM Level{level};")
        num_itemsets = cursor.fetchone()[0]

        print(f"Level {level}: {num_itemsets} frequent itemsets")

        level += 1

    # Close cursor and connection
    connection.commit()
    cursor.close()
    connection.close()

    print_time(time.time() - start_time)

    return level - 1


def create_tripstops_table() -> None:
    """
    Function to create all the necessary tables in the database
    :return: None
    """
    print("\n======================================================\n")
    print("Creating tables...")

    # Connect to the database
    connection = connect_to_db()
    cursor = connection.cursor()

    sql_query6 = """DROP TABLE IF EXISTS tripstops;"""
    cursor.execute(sql_query6)
    connection.commit()

    try:

        # Define SQL statements to create each table

        create_tripstops_table = ("CREATE TABLE TripStops "
                                  "(Trip_Id VARCHAR, Stop_Id INT, PRIMARY KEY (Trip_Id, Stop_Id));")

        # Execute each SQL statement to create the tables
        cursor.execute(create_tripstops_table)

        # Commit the transaction
        connection.commit()
        print("TripStops Table created successfully.")

    except (Exception, psycopg2.Error) as error:
        print("Error while creating tables:", error)

    cursor.close()
    connection.close()

    return None


def create_tripstops_dictionary(tripstops_data) -> dict:
    """
    Function to create a dictionary out of the new table created
    so that we can quickly access the data when needed
    :param tripstops_data: The data from the tripstops table
    :return: A dictionary made out of the tripstops table
    """
    tripstops_dictionary = {}
    for trip_id, varchar_stop_id in tripstops_data:
        stop_id = int(varchar_stop_id)
        if trip_id not in tripstops_dictionary:
            tripstops_dictionary[trip_id] = [stop_id]
        else:
            tripstops_dictionary[trip_id].append(stop_id)

    return tripstops_dictionary


def populate_tripstops_table() -> None:
    """
    Populates the TripStops.
    :return: None
    """

    print("\n======================================================\n")
    print(f"Loading data into TripStops...")

    # Connect to the database
    conn = connect_to_db()
    cur = conn.cursor()

    # Initialize all the statistics
    success = 0
    failed = 0
    query = "SELECT trip_id, stop_id FROM Stop_Times"

    cur.execute(query)

    tripstops_data = cur.fetchall()

    tripstops_dictionary = create_tripstops_dictionary(tripstops_data)
    # count = 0
    # for trip_id, stop_id_list in tripstops_dictionary.items():
    #     count += 1
    #     print(trip_id, stop_id_list)
    #     if count == 5:
    #         break

    for trip_id, stop_id_list in tripstops_dictionary.items():
        max_stops = random.randint(15, 20)
        if len(stop_id_list) > max_stops:
            reduced_list = random.sample(stop_id_list, max_stops)
        else:
            reduced_list = stop_id_list

        for stop_id in reduced_list:
            try:
                query = "INSERT INTO TripStops (Trip_Id, Stop_Id) VALUES (%s, %s)"
                values = (trip_id, stop_id)
                cur.execute(query, values)

                # Update the statistics
                success += 1
            except Error as err:

                # Rollback the transaction
                conn.rollback()

                print(err)
                failed += 1

    # Print the statistics for the entire loading process of that file
    print("\nTotal rows processed: " + str(success + failed))
    print(f"Successful: {success}, Failed: {failed}\n")

    # Define the SQL statement to alter the column type
    sql = "ALTER TABLE Stops ALTER COLUMN Stop_Id TYPE INT USING Stop_Id::integer;"

    # Execute the SQL statement
    cur.execute(sql)

    # Close the database connection
    conn.commit()
    cur.close()
    conn.close()

    return None


def make_and_populate_tripstops_table() -> None:
    """
    Function to create and populate tripstops table
    :return: None
    """
    create_tripstops_table()
    populate_tripstops_table()

    return None


def get_stops_names(last_level) -> None:
    """
    Get the tag names from the tag table using the TagId from the last non-empty level
    :param last_level: The integer value showing the last non-empty level in the lattice tables
    :return: None
    """
    print("\n======================================================\n")
    print(f"Getting Stop names for the Stop_Ids on lattice level {last_level} or table Level{last_level}...\n")
    # Connect to the database
    connection = connect_to_db()
    cursor = connection.cursor()

    # Generate the query to retrieve tag names
    query = f"""
    SELECT {', '.join(f'Level{last_level}.Stop{i}, St{i}.Stop_Name AS Stop{i}_Name' for i in range(1, last_level + 1))} 
    FROM Level{last_level} 
    {''.join(f'JOIN Stops St{i} ON Level{last_level}.Stop{i} = St{i}.Stop_Id ' for i in range(1, last_level + 1))};"""

    # print(query)

    # Execute the query
    cursor.execute(query)

    # Fetch the results
    stop_names = cursor.fetchall()

    # Close cursor and connection
    connection.commit()
    cursor.close()
    connection.close()

    formatted_itemset = []

    # Iterate over the list of tuples and print each tuple
    for itemset in stop_names:
        formatted_itemset = [(itemset[i], itemset[i + 1]) for i in range(0, len(itemset), 2)]

    for stop_id, stop_name in formatted_itemset:
        print(f"Stop_Id: {stop_id} -> Stop-Name: {stop_name}")

    return None


def generate_combinations(itemset) -> tuple:
    """
    This function generates a list of combinations of different
    association rules for a given itemset
    :param itemset: The itemset for which we have to find association rules
    :return: A tuple containing the LHS and RHS of the association rules
    """
    antecedents = []
    consequents = []

    for r in range(1, len(itemset)):

        item_combinations = itertools.combinations(itemset, r)

        for combo in item_combinations:

            # Have the combinations in correct order
            sorted_combo = sorted(combo)
            consequent = sorted(set(itemset) - set(sorted_combo))

            antecedents.append(tuple(sorted_combo))
            consequents.append(tuple(consequent))

    return antecedents, consequents


def association_mining() -> tuple:
    """
    This function is the main function to call the required methods
    to perform association mining
    :return: A tuple containing the filtered and unfiltered association rules
    """
    print("\n======================================================\n")
    print(f"Generating Rules and Confidences for Association Mining...\n")
    start_time = time.time()
    # Connect to the database
    connection = connect_to_db()
    cursor = connection.cursor()

    query = "SELECT COUNT(DISTINCT trip_id) AS total_tripids FROM tripstops;"

    # Execute the query
    cursor.execute(query)

    # Fetch the results
    result = cursor.fetchall()
    trips = result[0][0]
    # print(trips)

    # Close cursor and connection
    connection.commit()
    cursor.close()
    connection.close()

    database_tables = fetch_lattice_tables_from_database()
    lattice_tables = make_lattice_dictionary(database_tables)
    min_confidence = 0.7
    min_lift = 250

    # Generate association rules
    association_rules = generate_association_rules(lattice_tables, trips)

    # Filter the association rules by confidence
    association_rules_confidence = filter_by_confidence(association_rules, min_confidence)

    # Filter the association rules by confidence
    association_rules_lift = filter_by_lift(association_rules, min_lift)

    sample_value = 200
    print("Total confidences: ", len(association_rules))

    print("\nTotal Confidence Based Associations Found: ", len(association_rules_confidence))
    print(f"Printing Random {sample_value} Rules from it")

    random_association_rules_confidence = random.sample(association_rules_confidence, sample_value)

    stop_dict = create_stop_dictionary()

    # Print the discovered association rules with stop names
    for antecedent, consequent, count, confidence, lift in random_association_rules_confidence:
        antecedent_names = [stop_dict.get(stop_id, stop_id) for stop_id in antecedent]
        consequent_names = [stop_dict.get(stop_id, stop_id) for stop_id in consequent]
        print(f"Rule: {antecedent_names} -> {consequent_names}, Confidence: {confidence}")

    print_time(time.time() - start_time)

    return association_rules, association_rules_confidence


def create_stop_dictionary() -> dict:
    """
    Creates stops dictionary from the stops table to get quick acces to
    the stop names used during the itemset mining
    :return: The dictionary containing the mapping of stop ID and stop names
    """
    stop_dict = {}
    connection = connect_to_db()
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT stop_id, stop_name FROM stops")
        rows = cursor.fetchall()
        for row in rows:
            stop_id = row[0]
            stop_name = row[1]
            stop_dict[stop_id] = stop_name
    except Exception as e:
        print("Error fetching data from stops table:", e)
    finally:
        cursor.close()
        connection.close()
    return stop_dict


def filter_by_confidence(association_rules, min_confidence) -> list:
    """
    Function to filter the association rules by confidence
    :param association_rules: The unfiltered association rules
    :param min_confidence: The minimum confidence threshold
    :return: The list of filtered rules
    """
    filtered_rules = [rule for rule in association_rules if 1.0 > rule[3] >= min_confidence]
    return filtered_rules


def filter_by_lift(association_rules, min_lift) -> list:
    """
    Function to filter the association rules by lift
    :param association_rules: The unfiltered association rules
    :param min_lift: The minimum lift threshold
    :return: The list of filtered rules
    """
    filtered_rules = [rule for rule in association_rules if rule[4] >= min_lift]
    return filtered_rules


def generate_association_rules(lattice_tables, trips) -> list:
    """
    The main function to create the association rules for each level
    and each itemset
    :param lattice_tables: The data from all levels of the lattice tables
    :param trips: The total trips or transactions
    :return: The list of unfiltered association rules
    """
    association_rules = []

    # Check on each level
    for level, table in lattice_tables.items():
        print(f"Generating association rules for level {level} out of {len(lattice_tables)}")
        if level == 1:
            continue

        # Check association rules for each itemset
        for items, count in table.items():
            antecedents, consequents = generate_combinations(set(items))
            for antecedent, consequent in zip(antecedents, consequents):
                support_antecedent = get_support(antecedent, lattice_tables)
                support_consequent = get_support(consequent, lattice_tables)
                confidence = (count/trips)/(support_antecedent/trips)
                lift = (count/trips)/((support_antecedent/trips) * (support_consequent/trips))
                association_rules.append((antecedent, consequent, count, confidence, lift))

    return association_rules


def get_support(itemset, lattice_tables) -> int:
    """
    Get the support value for each part of the rule
    :param itemset: The itemset to find support for
    :param lattice_tables: The lattice dictionary to be used for finding support
    :return: The support value for the itemset
    """
    support = 0
    lattice_level = len(itemset)
    itemset = tuple(itemset)
    if itemset in lattice_tables[lattice_level]:
        support = lattice_tables[lattice_level][itemset]

    return support


def make_lattice_dictionary(lattice_tables) -> dict:
    """
    Function to make a dictionary of all the lattice levels
    :param lattice_tables: The lattice levels taken from database
    :return: The dictionary containing the lattice levels and the itemsets
    """
    lattice_dictionary = {}
    for level, table in lattice_tables.items():
        lattice_dictionary[level] = {}
        for row in table:
            lattice_dictionary[level][row[:-1]] = int(row[-1])

    return lattice_dictionary


def fetch_lattice_tables_from_database() -> dict:
    """
    Get all the lattice data from the database
    :return: The data for all the lattice table
    """
    lattice_tables = {}
    try:
        # Connect to the database
        connection = connect_to_db()
        cursor = connection.cursor()

        last_level = get_last_level_from_database()

        # Fetch lattice tables up to the specified last level
        for level in range(1, last_level):
            table_name = f"Level{level}"
            cursor.execute(f"SELECT * FROM {table_name};")
            lattice_tables[level] = cursor.fetchall()

        # Close cursor and connection
        cursor.close()
        connection.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error fetching lattice tables from the database:", error)

    return lattice_tables


def plot_frequent_itemsets() -> None:
    """
    Plot the frequent itemsets for each level of the database
    :return: None
    """
    lattice_tables = fetch_lattice_tables_from_database()
    levels = sorted(lattice_tables.keys())
    itemset_counts = [len(lattice_tables[level]) for level in levels]
    plt.figure(figsize=(10, 5))
    plt.plot(levels, itemset_counts, marker='o')
    plt.title('Number of Frequent Itemsets in Each Level')
    plt.xlabel('Level')
    plt.ylabel('Number of Frequent Itemsets')
    plt.grid(True)
    plt.show()

    return None


def plot_confidence_distribution(association_rules, title) -> None:
    """
    Plot the confidence levels for all the itemsets in the association rules
    :param association_rules: The list of association rules
    :param title: Title of the graph
    :return: None
    """
    confidences = [rule[3] for rule in association_rules]
    plt.figure(figsize=(8, 6))
    plt.hist(confidences, bins=10, color='skyblue', edgecolor='black')
    plt.xlabel('Confidence')
    plt.ylabel('Frequency')
    plt.title(title)
    plt.grid(True)
    plt.show()

    return None


def plot_support_vs_confidence(association_rules, title) -> None:
    """
    Plot the support vs confidence graph for all the association rules
    :param association_rules: The list of association rules
    :param title: The title of the graph
    :return: None
    """
    confidences = [rule[3] for rule in association_rules]
    support = [rule[2] for rule in association_rules]
    plt.figure(figsize=(8, 6))
    plt.scatter(confidences, support, color='orange', alpha=0.5)
    plt.xlabel('Confidence')
    plt.ylabel('Support')
    plt.title(title)
    plt.grid(True)
    plt.show()

    return None


def make_plots(association_rules, association_rules_confidence) -> None:
    """
    Function to call and create all the required plots functions for association
    mining visualization
    :param association_rules: The unfiltered association rules
    :param association_rules_confidence: Filtered association rules
    :return: None
    """
    print("\n======================================================\n")
    print(f"Making Plots...\n")
    plot_confidence_distribution(association_rules, 'Confidence Distribution (Before Filtering)')
    plot_confidence_distribution(association_rules_confidence, 'Confidence Distribution (After Filtering)')
    plot_support_vs_confidence(association_rules, 'Support vs. Confidence (Before Filtering)')
    plot_support_vs_confidence(association_rules_confidence, 'Support vs. Confidence (After Filtering)')

    return None


def main() -> None:
    """
    Main function of the code that calls the required functions in order.
    :return: None
    """

    # Make and populate tripstops
    # make_and_populate_tripstops_table()

    # Do itemset mining
    # automated_lattice_creation()

    # Print stop names in last non-empty level
    last_level = get_last_level_from_database()
    get_stops_names(last_level - 1)

    # Make graph for itemsets
    # plot_frequent_itemsets()

    # Do association mining
    association_rules, association_rules_confidence = association_mining()

    # Make plots for association mining
    # make_plots(association_rules, association_rules_confidence)

    print("\n=======================End-of-Code=======================\n")

    return None


if __name__ == "__main__":
    main()
