import mysql.connector
import csv
import os

def query_mysql():
    """
    Execute multiple predefined SQL queries and save results to files in a default directory.
    """
    # Predefined queries
    queries = {
        "most_common_crimes_by_area": """
            SELECT cc.crime_code_description, l.location, COUNT(*) AS crime_count
            FROM crime c
            JOIN crime_code cc ON c.crime_code = cc.crime_code
            JOIN location l ON c.location_id = l.location_id
            GROUP BY cc.crime_code_description, l.location
            ORDER BY crime_count DESC;
        """,
        "time_of_day_vs_crime_types": """
            SELECT TIME(c.time_occurred) AS time_of_day, cc.crime_code_description, COUNT(*) AS crime_count
            FROM crime c
            JOIN crime_code cc ON c.crime_code = cc.crime_code
            GROUP BY time_of_day, cc.crime_code_description
            ORDER BY time_of_day, crime_count DESC;
        """,
        "victim_age_gender_variation": """
            SELECT cc.crime_code_description, v.victim_age, v.victim_sex, COUNT(*) AS victim_count
            FROM crime c
            JOIN crime_code cc ON c.crime_code = cc.crime_code
            JOIN victim v ON c.victim_id = v.victim_id
            GROUP BY cc.crime_code_description, v.victim_age, v.victim_sex
            ORDER BY victim_count DESC;
        """,
        "weapon_usage_trends": """
            SELECT cc.crime_code_description, w.weapon_description, YEAR(c.date_occurred) AS year, COUNT(*) AS crime_count
            FROM crime c
            JOIN crime_code cc ON c.crime_code = cc.crime_code
            JOIN weapon w ON c.weapon_code = w.weapon_code
            GROUP BY cc.crime_code_description, w.weapon_description, year
            ORDER BY year, crime_count DESC;
        """,
        "reporting_vs_occurrence_patterns": """
            SELECT c.date_reported, c.date_occurred, COUNT(*) AS crime_count
            FROM crime c
            GROUP BY c.date_reported, c.date_occurred
            HAVING c.date_reported != c.date_occurred
            ORDER BY crime_count DESC;
        """
    }

    # Define output directory path
    output_directory = os.path.expanduser("~/airflow/DE_project/Data/query_results")
    
    # Create the directory if it doesn't exist
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    # MySQL connection details
    host = 'localhost'
    user = 'root'
    password = 'Bad@2394'
    database = 'de_project'

    # Establish a connection to MySQL
    conn = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    cursor = conn.cursor()

    try:
        # Iterate through each query in the predefined queries dictionary
        for query_name, query in queries.items():
            # Execute the query
            cursor.execute(query)
            result = cursor.fetchall()

            # Define the output file path based on query_name
            output_file = os.path.join(output_directory, f"{query_name}.csv")

            # Save the results to a CSV file
            headers = [i[0] for i in cursor.description]  # Get column names
            with open(output_file, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(headers)  # Write headers
                writer.writerows(result)  # Write data

            print(f"Results for '{query_name}' saved to {output_file}")
        
    except Exception as e:
        print(f"Error executing queries: {e}")
    
    finally:
        # Close connection
        cursor.close()
        conn.close()
