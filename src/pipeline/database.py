import os
import time
import urllib.parse
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd

def get_db_engine():
    """Creates and returns a SQLAlchemy engine for the TechJobsDB."""
    load_dotenv() 
    db_host = os.getenv('DB_HOST')
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')

    if not all([db_host, db_name, db_user, db_password]):
        missing = [var for var in ['DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASSWORD'] if not os.getenv(var)]
        logging.error(f"Missing environment variables: {', '.join(missing)}")
        raise ValueError(f"Missing environment variables: {', '.join(missing)}")

    quoted_password = urllib.parse.quote_plus(db_password)
    quoted_user = urllib.parse.quote_plus(db_user)
    driver = urllib.parse.quote_plus("ODBC Driver 18 for SQL Server")

    master_conn_str = f"mssql+pyodbc://{quoted_user}:{quoted_password}@{db_host}:1433/master?driver={driver}&TrustServerCertificate=yes"
    master_engine = create_engine(master_conn_str, isolation_level="AUTOCOMMIT")
    
    try:
        with master_engine.connect() as conn:
            logging.info(f"Checking if database '{db_name}' exists on {db_host}...")
            result = conn.execute(text(f"SELECT name FROM sys.databases WHERE name = :db_name"), {"db_name": db_name})
            if not result.fetchone():
                logging.info(f"Database '{db_name}' not found. Creating...")
                conn.execute(text(f"CREATE DATABASE {db_name}"))
                logging.info(f"Database '{db_name}' created successfully.")
            else:
                logging.info(f"Database '{db_name}' already exists.")
        master_engine.dispose()
    except Exception as e:
        logging.error(f"DB check/create failed: {e}", exc_info=True)
        raise

    db_conn_str = f"mssql+pyodbc://{quoted_user}:{quoted_password}@{db_host}:1433/{db_name}?driver={driver}&TrustServerCertificate=yes"
    db_engine = create_engine(db_conn_str)
    logging.info(f"Successfully created engine for '{db_name}'.")
    return db_engine

def load_data_to_db(engine, df: pd.DataFrame):
    """Loads a DataFrame to the DB using a staging table and MERGE."""
    staging_table = "JobPostings_Staging"
    target_table = "JobPostings"
    
    try:
        logging.info(f"Loading {len(df)} rows into staging table '{staging_table}'...")
        df.to_sql(name=staging_table, con=engine, if_exists='replace', index=False, method='multi')
        
        logging.info("Executing MERGE statement...")
        merge_sql = f"""
        MERGE INTO {target_table} AS Target
        USING {staging_table} AS Source
        ON Target.Id = Source.Id  -- This join is CRITICAL
        
        WHEN MATCHED THEN
            UPDATE SET
                Target.Title = Source.Title,
                Target.Company = Source.Company,
                Target.Location = Source.Location,
                Target.Province = Source.Province,
                Target.min_salary_pa = Source.min_salary_pa,
                Target.max_salary_pa = Source.max_salary_pa,
                Target.Skills = Source.Skills
        
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (Id, Title, Company, Location, Province, min_salary_pa, max_salary_pa, Skills)
            VALUES (Source.Id, Source.Title, Source.Company, Source.Location, Source.Province, Source.min_salary_pa, Source.max_salary_pa, Source.Skills);
        """
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(text(merge_sql))
        logging.info("MERGE complete. Data loaded successfully.")
    except Exception as e:
        logging.error(f"Error loading data to database: {e}", exc_info=True)
        raise

# --- New Function: Data Validation ---
def validate_data(engine):
    """Executes a validation query and prints the results."""
    logging.info("Starting data validation query.")
    
    validation_sql = """
    SELECT
        Id,
        Title,
        Company,
        Location,
        Province,
        min_salary_pa,
        max_salary_pa,
        Skills
    FROM 
        JobPostings
    WHERE
        Company IN ('Albert Einstein', 'Steve Martin') -- Location/Salary Test
        OR Skills IS NOT NULL AND Skills != ''         -- Skill Extraction Test
    ORDER BY
        Id;
    """
    
    try:
        # Use pandas read_sql to execute the query and fetch results into a DataFrame
        validation_df = pd.read_sql(validation_sql, engine)
        
        logging.info(f"Validation query successful. Found {len(validation_df)} relevant records.")
        
        # Display the results using a clear formatting (e.g., Markdown table)
        print("\n" + "="*80)
        print("📊 DATA VALIDATION RESULTS (Transformed Data Audit):")
        print("="*80)
        print(validation_df.to_markdown(index=False))
        print("="*80 + "\n")

    except Exception as e:
        logging.error(f"Error executing validation query: {e}", exc_info=True)
        raise