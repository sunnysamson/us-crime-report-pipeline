import os
import pandas as pd
import mysql.connector

def load_to_mysql():
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

    # Create tables if they do not exist
    table_creation_queries = {
        "victim": """
        CREATE TABLE IF NOT EXISTS victim (
            victim_id INT PRIMARY KEY AUTO_INCREMENT,
            victim_age INT NULL,
            victim_sex VARCHAR(10) NULL,
            victim_descent VARCHAR(50) NULL
        );
        """,
        "location": """
        CREATE TABLE IF NOT EXISTS location (
            location_id INT PRIMARY KEY AUTO_INCREMENT,
            location VARCHAR(255) NULL,
            latitude DOUBLE NULL,
            longitude DOUBLE NULL
        );
        """,
        "crime": """
        CREATE TABLE IF NOT EXISTS crime (
            case_number INT PRIMARY KEY,
            date_reported DATE NULL,
            date_occurred DATE NULL,
            time_occurred TIME NULL,
            area_id INT NULL,
            report_district_number INT NULL,
            crime_severity VARCHAR(255) NULL,
            crime_code INT NULL,
            victim_id INT NULL,
            premise_code INT NULL,
            weapon_code INT NULL,
            status VARCHAR(50) NULL,
            crime_code_alt INT NULL,
            location_id INT NULL,
            FOREIGN KEY (victim_id) REFERENCES victim(victim_id),
            FOREIGN KEY (location_id) REFERENCES location(location_id),
            FOREIGN KEY (crime_code) REFERENCES crime_code(crime_code),
            FOREIGN KEY (premise_code) REFERENCES premise(premise_code),
            FOREIGN KEY (weapon_code) REFERENCES weapon(weapon_code),
            FOREIGN KEY (status) REFERENCES status(status)
        );
        """,
        "area": """
        CREATE TABLE IF NOT EXISTS area (
            area_id INT PRIMARY KEY,
            area_name VARCHAR(255) NOT NULL
        );
        """,        
        "crime_code": """
        CREATE TABLE IF NOT EXISTS crime_code (
            crime_code INT PRIMARY KEY,
            crime_code_description VARCHAR(255) NOT NULL
        );
        """,
        "premise": """
        CREATE TABLE IF NOT EXISTS premise (
            premise_code INT PRIMARY KEY,
            premise_description VARCHAR(255) NOT NULL
        );
        """,
        "weapon": """
        CREATE TABLE IF NOT EXISTS weapon (
            weapon_code INT PRIMARY KEY,
            weapon_description VARCHAR(255) NOT NULL
        );
        """,
        "status": """
        CREATE TABLE IF NOT EXISTS status (
            status VARCHAR(50) PRIMARY KEY,
            status_description VARCHAR(255) NOT NULL
        );
        """
    }

    for table, query in table_creation_queries.items():
        try:
            cursor.execute(query)
            conn.commit()
        except mysql.connector.Error as e:
            print(f"Error creating table {table}: {e}")

    # Define file paths and table names
    file_paths = {
        "crime": "~/airflow/DE_project/data/normalized_crime.csv",
        "victim": "~/airflow/DE_project/data/normalized_victim.csv",
        "location": "~/airflow/DE_project/data/normalized_location.csv",
        "crime_code": "~/airflow/DE_project/data/normalized_crime_code.csv",
        "premise": "~/airflow/DE_project/data/normalized_premise.csv",
        "weapon": "~/airflow/DE_project/data/normalized_weapon.csv",
        "status": "~/airflow/DE_project/data/normalized_status.csv",
        "area": "~/airflow/DE_project/data/normalized_area.csv"
    }

    # Table-specific SQL INSERT queries
    insert_queries = {
        "crime": """
        INSERT IGNORE INTO crime (case_number, date_reported, date_occurred, time_occurred, area_id, report_district_number, 
                      crime_severity, crime_code, victim_id, premise_code, weapon_code, status, 
                      crime_code_alt, location_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        "victim": """
        INSERT IGNORE INTO victim (victim_id, victim_age, victim_sex, victim_descent)
        VALUES (%s, %s, %s, %s)
        """,
        "location": """
        INSERT IGNORE INTO location (location_id, location, latitude, longitude)
        VALUES (%s, %s, %s, %s)
        """,
        "crime_code": """
        INSERT IGNORE INTO crime_code (crime_code, crime_code_description)
        VALUES (%s, %s)
        """,
        "premise": """
        INSERT IGNORE INTO premise (premise_code, premise_description)
        VALUES (%s, %s)
        """,
        "weapon": """
        INSERT IGNORE INTO weapon (weapon_code, weapon_description)
        VALUES (%s, %s)
        """,
        "status": """
        INSERT IGNORE INTO status (status, status_description)
        VALUES (%s, %s)
        """,
        "area": """
        INSERT IGNORE INTO area (area_id, area_name)
        VALUES (%s, %s)
        """
    }

    # Batch size for inserting data
    batch_size = 1000

    for table, path in file_paths.items():
        data = pd.read_csv(os.path.expanduser(path))
    
        # Replace NaN with None for all columns in the DataFrame (for SQL compatibility)
        data = data.where(pd.notnull(data), None)
    
        # Explicitly convert columns to appropriate types to ensure data consistency
    
        # Convert DataFrame to list of tuples
        records = data.values.tolist()
    
        # Insert data into MySQL in batches
        query = insert_queries[table]
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            try:
                # Ensure NaN values are replaced with None in the batch
                batch = [[None if pd.isna(x) else x for x in row] for row in batch]
                print(batch[:1])
                # Insert the batch into MySQL
                cursor.executemany(query, batch)
                conn.commit()
            except mysql.connector.Error as e:
                # Log the problematic row and its data fields
                for row in batch:
                    print(f"Error inserting data into table {table}: Data: {row}")
                print(f"Error details: {e}")
                conn.rollback()  # Rollback transaction for this batch if it fails
    
        print(f"Data loaded successfully into table: {table}")

    # Close connection
    cursor.close()
    conn.close()

if __name__ == "__main__":
    load_to_mysql()
