import time
import requests
import logging
from bs4 import BeautifulSoup
import pandas as pd

def scrape_job_data(url):
    """
    Scrapes the specified site (either books or static quotes) 
    and returns a DataFrame, using different logic for each.
    """
    logging.info(f"Starting scrape of: {url}")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        scraped_data = []

        if 'books.toscrape.com' in url:
            logging.info("Using 'books' scraping logic.")
            job_listings = soup.find_all('article', class_='product_pod')

            for i, job in enumerate(job_listings):
                try:
                    job_id_tag = job.find('h3').find('a')
                    job_id = job_id_tag['href'].strip() if job_id_tag and 'href' in job_id_tag.attrs else f'B{i+1}'
                    title = job_id_tag['title'].strip() if job_id_tag and 'title' in job_id_tag.attrs else 'No Title'
                    company = 'Book Publisher (Mock)'
                    
                    price_text = job.find('p', class_='price_color').text.strip()
                    location = price_text
                    salary_range = 'R800 000 pa' if i % 2 == 0 else '15k per month' 
                    skills = [] 

                    scraped_data.append({
                        'Id': job_id, 'title': title, 'company': company, 
                        'location': location, 'salary_range': salary_range, 'skills': skills 
                    })
                except Exception as e:
                    logging.warning(f"Error parsing one book listing: {e}")

        elif 'quotes.toscrape.com' in url:
            logging.info("Using 'quotes' scraping logic.")
            job_listings = soup.find_all('div', class_='quote')
            
            for i, job in enumerate(job_listings):
                try:
                    job_id = f"Q{i+1}" 
                    title_text = job.find('span', class_='text').text.strip()
                    title = title_text[:50] + "..."
                    company = job.find('small', class_='author').text.strip()
                    tags = [tag.text.strip() for tag in job.find('div', class_='tags').find_all('a', class_='tag')]
                    
                    if company.lower() == 'albert einstein':
                        location = 'Johannesburg' 
                    elif company.lower() == 'steve martin':
                        location = 'Durban' 
                    else:
                        location = 'New York City' 
                    
                    if len(tags) >= 4:
                        salary_range = 'R30k - R45k per month'
                    elif len(tags) >= 2:
                        salary_range = 'R800 000 - 1.2 M pa'
                    else:
                        salary_range = 'Not stated'
                    
                    skills = tags 

                    scraped_data.append({
                        'Id': job_id, 'title': title, 'company': company, 
                        'location': location, 'salary_range': salary_range, 'skills': skills 
                    })
                except Exception as e:
                    logging.warning(f"Error parsing one quote listing: {e}")

        else:
            logging.error(f"Unknown URL source: {url}. Returning empty data.")
        
        logging.info(f"Scrape complete. Found {len(scraped_data)} jobs.")
        time.sleep(2)
        return pd.DataFrame(scraped_data)
    
    except requests.RequestException as e:
        logging.error(f"Fatal error fetching URL: {e}", exc_info=True)
        return pd.DataFrame()