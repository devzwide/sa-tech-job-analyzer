import json
import os
import re
import time
import urllib.parse  # Import for URL encoding

import pandas as pd # type: ignore
from dotenv import load_dotenv
from sqlalchemy import create_engine, text # type: ignore 

def get_db_engine():
    """
    Creates and returns a SQLAlchemy engine for the TechJobsDB.
    Reads all connection info from the .env file.
    """

    load_dotenv() 
    
    # --- 1. Load All Connection Parameters ---
    # Load all parts from .env, not just the password
    db_host = os.getenv('DB_HOST')
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')

    # --- 2. Validate Environment Variables ---
    if not all([db_host, db_name, db_user, db_password]):
        missing = [var for var in ['DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASSWORD'] if not os.getenv(var)]
        raise ValueError(f"Missing environment variables: {', '.join(missing)}. Please check your .env file.")

    # --- 3. URL-encode for safety ---
    # This prevents errors if the password or user has special characters
    quoted_password = urllib.parse.quote_plus(db_password)
    quoted_user = urllib.parse.quote_plus(db_user)
    driver = urllib.parse.quote_plus("ODBC Driver 18 for SQL Server")

    master_conn_str = (
        f"mssql+pyodbc://{quoted_user}:{quoted_password}@{db_host}:1433/master"
        f"?driver={driver}"
        "&TrustServerCertificate=yes" 
    )
    master_engine = create_engine(master_conn_str, isolation_level="AUTOCOMMIT")
    
    # db_name is now loaded from .env
    
    try:
        with master_engine.connect() as conn:
            print(f"Checking if database '{db_name}' exists on {db_host}...")
            # Check if DB exists
            result = conn.execute(text(f"SELECT name FROM sys.databases WHERE name = :db_name"), {"db_name": db_name})
            db_exists = result.fetchone()
            
            if not db_exists:
                print(f"Database '{db_name}' not found. Creating...")
                # Use text() for CREATE DATABASE to ensure it's executed correctly
                conn.execute(text(f"CREATE DATABASE {db_name}"))
                print(f"Database '{db_name}' created successfully.")
            else:
                print(f"Database '{db_name}' already exists.")
        
        master_engine.dispose() # Close master connection
        
    except Exception as e:
        print(f"An error occurred while connecting or creating the database: {e}")
        # Give the SQL Server container a few seconds to start up
        if "Login failed" in str(e) or "Error Locating Server" in str(e) or "TCP/IP" in str(e):
            print("Connection failed. Is the SQL Server container (service 'db') running and healthy?")
            print("Waiting 10 seconds and retrying...")
            time.sleep(10) # Wait and retry once
            try:
                with master_engine.connect() as conn:
                    result = conn.execute(text(f"SELECT name FROM sys.databases WHERE name = :db_name"), {"db_name": db_name})
                    if not result.fetchone():
                        conn.execute(text(f"CREATE DATABASE {db_name}"))
                        print(f"Database '{db_name}' created successfully on retry.")
                    else:
                        print(f"Database '{db_name}' found on retry.")
            except Exception as retry_e:
                print(f"Retry failed: {retry_e}")
                raise # Re-raise error after retry fails
        else:
            raise # Re-raise other errors

    # 4. Now, create and return the engine for our new database
    db_conn_str = (
        f"mssql+pyodbc://{quoted_user}:{quoted_password}@{db_host}:1433/{db_name}"
        f"?driver={driver}"
        "&TrustServerCertificate=yes"
    )
    db_engine = create_engine(db_conn_str)
    print(f"Successfully created engine for '{db_name}'.")
    return db_engine

# --- Data Cleaning Functions (Unchanged from your script) ---

def load_raw_data(json_file_path):
    """Loads the raw JSON data from the file."""
    print(f"Loading data from: {json_file_path}")
    with open(json_file_path, 'r') as f:
        data = json.load(f)
    return pd.DataFrame(data)

def clean_locations(df):
    """Standardizes locations into provinces."""
    print("Cleaning locations...")
    df['province'] = 'Other' 
    gauteng_keys = ['sandton', 'johannesburg', 'jhb', 'pretoria']
    kzn_keys = ['durban', 'umhlanga', 'kzn']
    df.loc[df['location'].str.contains('|'.join(gauteng_keys), case=False, na=False), 'province'] = 'Gauteng'
    df.loc[df['location'].str.contains('|'.join(kzn_keys), case=False, na=False), 'province'] = 'KwaZulu-Natal'
    return df

def clean_salaries(df):
    """Parses 'salary_range' into min_salary_pa and max_salary_pa."""
    print("Cleaning salaries...")
    number_pattern = re.compile(r'(\d{1,3}(?:,\d{3})*|\d+)(k)?')
    df['min_salary_pa'] = pd.NA
    df['max_salary_pa'] = pd.NA
    df['currency'] = 'ZAR'

    for index, row in df.iterrows():
        salary_str = str(row['salary_range']).lower().replace(',', '')
        if 'not stated' in salary_str:
            continue
        matches = number_pattern.findall(salary_str)
        if not matches:
            continue

        is_per_month = 'pm' in salary_str or 'per month' in salary_str
        values = []
        for val, k_suffix in matches:
            num = int(val)
            if k_suffix == 'k':
                num *= 1000
            # --- FIX ---
            # If 'k' isn't present, but the number is small (e.g., < 2000)
            # and it's not a monthly salary, assume it's a 'k' value.
            elif k_suffix == '' and num > 0 and num < 2000 and not is_per_month:
                num *= 1000
            # --- END FIX ---
            values.append(num)
        
        if is_per_month:
            values = [v * 12 for v in values]

        # --- NEW FIX ---
        # If 0 is in the list, and there are other values,
        # treat 0 as a placeholder and remove it.
        if 0 in values and len(values) > 1:
            values = [v for v in values if v > 0]
        # --- END NEW FIX ---

        if len(values) == 1:
            df.at[index, 'min_salary_pa'] = values[0]
            df.at[index, 'max_salary_pa'] = values[0]
        elif len(values) >= 2:
            df.at[index, 'min_salary_pa'] = min(values)
            df.at[index, 'max_salary_pa'] = max(values)
    return df

# --- Updated Main Function (Unchanged from your script) ---

def main():
    """Main function to run the data cleaning and loading pipeline."""
    # Inside the Docker container, our working dir is /project
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # The data file is copied to /project/src/data/jobs.json
    json_file_path = os.path.join(script_dir, '..', 'data', 'jobs.json')
    
    if not os.path.exists(json_file_path):
        print(f"Error: Cannot find data file at {json_file_path}")
        return

    # --- 1. ETL: Extract and Transform ---
    df_raw = load_raw_data(json_file_path)
    df_clean_locations = clean_locations(df_raw)
    df_final = clean_salaries(df_clean_locations)

    # Re-order columns for the database
    final_columns = [
        'id', 'title', 'company', 'location', 'province', 
        'min_salary_pa', 'max_salary_pa', 'skills'
    ]
    # Handle 'skills' column (it's a list, .to_sql can't handle lists)
    # We'll convert the list to a simple comma-separated string
    df_final['skills'] = df_final['skills'].apply(lambda x: ','.join(x) if isinstance(x, list) else None)
    
    # Ensure correct dtypes for nullable integers
    df_final['min_salary_pa'] = df_final['min_salary_pa'].astype('Int64')
    df_final['max_salary_pa'] = df_final['max_salary_pa'].astype('Int64')
    
    df_final = df_final[final_columns]
    
    print("\n--- Cleaned Data Ready for Database ---")
    print(df_final.to_markdown(index=False))

    # --- 2. ETL: Load ---
    try:
        print("\nConnecting to SQL Server and loading data...")
        engine = get_db_engine()
        
        # Load the DataFrame into the 'JobPostings' table
        # if_exists='replace': Drops the table and recreates it. Good for development.
        # index=False: Don't write the pandas DataFrame index as a column.
        # method='multi': Inserts multiple rows at once, which is much faster.
        df_final.to_sql(
            name='JobPostings',
            con=engine,
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=1000 # Good practice for large dataframes
        )
        
        print("--- Success! Data loaded into 'JobPostings' table. ---")

    except Exception as e:
        print(f"\n--- An error occurred during database loading ---")
        print(f"Error: {e}")
        print("Please check:")
        print("  1. Is the Docker container running? (run 'docker ps')")
        print(f"  2. Is the password in your .env file correct?")
        print("  3. Did the ODBC driver install correctly in the container?")


if __name__ == "__main__":
    main()