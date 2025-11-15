import logging
from logging_config import setup_logging
from scraper import scrape_job_data
from transforms import clean_locations, clean_salaries, extract_skills 
from database import get_db_engine, load_data_to_db, validate_data 
import pandas as pd

JOB_SITE_URLS = [
    "http://books.toscrape.com/", 
    "http://quotes.toscrape.com/" 
] 

def main():
    """Runs the full ETL pipeline for multiple sources."""
    setup_logging()
    logging.info("--- Pipeline run START ---")
    
    all_dataframes = []

    try:
        for url in JOB_SITE_URLS:
            df_source = scrape_job_data(url)
            if df_source.empty:
                logging.warning(f"Scraper returned no data for {url}. Skipping source.")
            else:
                all_dataframes.append(df_source)

        if not all_dataframes:
            logging.warning("No data scraped from any source. Exiting pipeline.")
            return

        df_raw = pd.concat(all_dataframes, ignore_index=True)
        logging.info(f"Concatenation complete. Total records: {len(df_raw)}")


        df_locations = clean_locations(df_raw)
        df_salaries = clean_salaries(df_locations)
        df_skills = extract_skills(df_salaries)
        
        df_final = df_skills.copy()
        df_final['skills'] = df_final['skills'].apply(lambda x: ','.join(x) if isinstance(x, list) else None)
        df_final['min_salary_pa'] = df_final['min_salary_pa'].astype('Int64')
        df_final['max_salary_pa'] = df_final['max_salary_pa'].astype('Int64')
        
        final_columns = [
            'Id', 'title', 'company', 'location', 'province', 
            'min_salary_pa', 'max_salary_pa', 'skills'
        ]
        df_final = df_final[final_columns]
        logging.info("Data cleaning complete.")

        engine = get_db_engine()
        load_data_to_db(engine, df_final)
        
        validate_data(engine)

    except Exception as e:
        logging.critical(f"Pipeline FAILED. Error: {e}", exc_info=True)
    
    logging.info("--- Pipeline run END ---")

if __name__ == "__main__":
    main()