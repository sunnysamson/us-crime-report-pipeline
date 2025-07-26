import pandas as pd
import numpy as np
import os

def preprocess_and_normalization():
    raw_data = pd.read_csv(os.path.expanduser("~/airflow/DE_Project/Data/us_crime_data.csv"))
    
    # Preprocessing (e.g., dropping nulls, renaming columns)
    raw_data.drop(columns=['Mocodes', 'Crm Cd 2', 'Crm Cd 3', 'Crm Cd 4', 'Cross Street'], inplace=True)
    raw_data.dropna(subset=['Premis Cd', 'Premis Desc', 'Status', 'Crm Cd 1'], inplace=True)

    # Function to transform "TIME OCC" to HH:MM:SS format
    def transform_time(time):
        time_str = str(time).zfill(4)
        if len(time_str) == 4:
            return f"{time_str[:2]}:{time_str[2:]}:00"
        elif len(time_str) == 3:
            return f"0{time_str[:1]}:{time_str[1:]}:00"
        elif len(time_str) == 2:
            return f"00:{time_str}:00"
        elif len(time_str) == 1:
            return f"0{time_str}:00:00"
        else:
            return None  # Return None for invalid time formats

    raw_data["TIME OCC"] = raw_data["TIME OCC"].apply(transform_time)
    raw_data["Date Rptd"] = pd.to_datetime(raw_data["Date Rptd"], format="%m/%d/%Y %I:%M:%S %p").dt.date
    raw_data["DATE OCC"] = pd.to_datetime(raw_data["DATE OCC"], format="%m/%d/%Y %I:%M:%S %p").dt.date
    raw_data.loc[raw_data['Vict Age'] <= 0, 'Vict Age'] = np.nan
    raw_data['Vict Age'] = raw_data['Vict Age'].astype('Int64')
    raw_data['Weapon Used Cd'] = raw_data['Weapon Used Cd'].fillna(0)  # Replace NaN with 0
    raw_data['Weapon Desc'] = raw_data['Weapon Desc'].fillna("No weapon used")  # Replace NaN with "No weapon used"

    # Replace NaN values for 'Vict Age' with the mode within each 'Crm Cd' group
    raw_data['Vict Age'] = raw_data.groupby('Crm Cd')['Vict Age'].transform(
    lambda x: x.fillna(x.mode()[0] if not x.mode().empty else np.nan)
    )
    
    raw_data['Vict Sex'] = raw_data['Vict Sex'].apply(lambda x: x if x in ['M', 'F'] else None)
    raw_data['Vict Descent'] = raw_data['Vict Descent'].replace(to_replace=r'[^a-zA-Z]', value=None, regex=True)

    raw_data.rename(columns={
        'DR_NO':'case_number', 'Date Rptd':'date_reported', 'DATE OCC':'date_occurred', 
        'TIME OCC':'time_occurred', 'AREA':'area_id', 'AREA NAME':'area_name', 'Rpt Dist No':'report_district_number', 
        'Part 1-2':'crime_severity', 'Crm Cd':'crime_code', 'Crm Cd Desc':'crime_code_description', 
        'Vict Age':'victim_age', 'Vict Sex':'victim_sex', 'Vict Descent':'victim_descent', 'Premis Cd':'premise_code', 
        'Premis Desc':'premise_description', 'Weapon Used Cd':'weapon_code', 'Weapon Desc':'weapon_description', 
        'Status':'status', 'Status Desc':'status_description', 'Crm Cd 1':'crime_code_alt', 'LOCATION':'location', 
        'LAT':'latitude', 'LON':'longitude'
    }, inplace=True)

    # creaating 3nf dataframes
    # Step 1: Create victim DataFrame and add 'victim_id' as an auto-increment primary key
    victim = raw_data[["victim_age", "victim_sex", "victim_descent"]].drop_duplicates()
    victim['victim_id'] = np.arange(1, len(victim) + 1)  # Creating a unique ID for each victim
    victim.insert(0, 'victim_id', victim.pop('victim_id'))  # Move 'victim_id' to the first column
    
    # Step 2: Create location DataFrame and add 'location_id' as an auto-increment primary key
    location = raw_data[["location", "latitude", "longitude"]].drop_duplicates()
    location['location_id'] = np.arange(1, len(location) + 1)  # Creating a unique ID for each location
    location.insert(0, 'location_id', location.pop('location_id'))  # Move 'location_id' to the first column
    
    # Step 3: Merge 'victim_id' and 'location_id' back into raw_data
    # Merge 'victim_id' back into raw_data
    raw_data = raw_data.merge(
        victim[["victim_id", "victim_age", "victim_sex", "victim_descent"]],
        on=["victim_age", "victim_sex", "victim_descent"],
        how="left"
    )
    # Merge 'location_id' back into raw_data
    raw_data = raw_data.merge(
        location[["location_id", "location", "latitude", "longitude"]],
        on=["location", "latitude", "longitude"],
        how="left"
    )
    
    # Step 4: Create crime DataFrame with 'victim_id' and 'location_id'
    crime = raw_data[["case_number", "date_reported", "date_occurred", "time_occurred", "area_id", "report_district_number", 
                      "crime_severity", "crime_code", "victim_id", "premise_code", "weapon_code", "status", 
                      "crime_code_alt", "location_id"]].drop_duplicates()
    
    # Other data frames for location, crime code, premise, weapon, etc.
    crime_code = raw_data[["crime_code", "crime_code_description"]].drop_duplicates()
    premise = raw_data[["premise_code", "premise_description"]].drop_duplicates()
    weapon = raw_data[["weapon_code", "weapon_description"]].drop_duplicates()
    status = raw_data[["status", "status_description"]].drop_duplicates()
    area = raw_data[["area_id", "area_name"]].drop_duplicates()

    # Convert data types to match MySQL

    # Convert Victim dataframe fields
    victim['victim_id'] = victim['victim_id'].astype(np.int64)
    victim['victim_age'] = victim['victim_age'].astype(pd.Int64Dtype())
    victim['victim_sex'] = victim['victim_sex'].astype(str)
    victim['victim_descent'] = victim['victim_descent'].astype(str)
    
    # Replace NaN with None for victim DataFrame
    victim['victim_age'] = victim['victim_age'].where(pd.notnull(victim['victim_age']), None)
    victim['victim_sex'] = victim['victim_sex'].replace([np.nan, 'nan'], None)
    victim['victim_descent'] = victim['victim_descent'].replace([np.nan, 'nan'], None)

    # Convert location dataframe fields
    location['location_id'] = location['location_id'].astype(np.int64)
    location['latitude'] = location['latitude'].astype(np.float64)
    location['longitude'] = location['longitude'].astype(np.float64)
    location['location'] = location['location'].astype(str)
    
    # Convert crime dataframe fields
    crime['case_number'] = crime['case_number'].astype(np.int64)
    crime['date_reported'] = pd.to_datetime(crime['date_reported'], errors='coerce').dt.date
    crime['date_occurred'] = pd.to_datetime(crime['date_occurred'], errors='coerce').dt.date
    crime['time_occurred'] = pd.to_datetime(crime['time_occurred'], format='%H:%M:%S', errors='coerce').dt.time
    crime['area_id'] = crime['area_id'].astype(np.int64)
    crime['report_district_number'] = crime['report_district_number'].astype(np.int64)
    crime['crime_severity'] = crime['crime_severity'].astype(np.int64)
    crime['crime_code'] = crime['crime_code'].astype(np.int64)
    crime['victim_id'] = crime['victim_id'].astype(np.int64)
    crime['premise_code'] = crime['premise_code'].astype(np.int64)
    crime['weapon_code'] = crime['weapon_code'].astype(np.int64)
    crime['status'] = crime['status'].astype(str)
    crime['crime_code_alt'] = crime['crime_code_alt'].astype(np.int64)
    crime['location_id'] = crime['location_id'].astype(np.int64)

    # Convert area dataframe fields
    area['area_id'] = area['area_id'].astype(np.int64)
    area['area_name'] = area['area_name'].astype(str)
        
    # Convert crime_code dataframe fields
    crime_code['crime_code'] = crime_code['crime_code'].astype(np.int64)
    crime_code['crime_code_description'] = crime_code['crime_code_description'].astype(str)
    
    # Convert premise dataframe fields
    premise['premise_code'] = premise['premise_code'].astype(np.int64)
    premise['premise_description'] = premise['premise_description'].astype(str)
    
    # Convert weapon dataframe fields
    weapon['weapon_code'] = weapon['weapon_code'].astype(np.int64)
    weapon['weapon_description'] = weapon['weapon_description'].astype(str)
    
    # Convert status dataframe fields
    status['status'] = status['status'].astype(str)
    status['status_description'] = status['status_description'].astype(str)


    
    # Saving data frames as CSV
    dfs = {
        "normalized_crime": crime,
        "normalized_victim": victim,
        "normalized_location": location,
        "normalized_crime_code": crime_code,
        "normalized_premise": premise,
        "normalized_weapon": weapon,
        "normalized_status": status,
        "normalized_area": area
    }

    base_path = os.path.expanduser("~/airflow/DE_project/data/")
    os.makedirs(base_path, exist_ok=True)

    for name, df in dfs.items():
        df.to_csv(os.path.join(base_path, f"{name}.csv"), index=False)

if __name__ == "__main__":
    preprocess_and_normalization()
