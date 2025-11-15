import pandas as pd
import re
import logging
import numpy as np 


def extract_skills(df: pd.DataFrame):
    """
    Analyzes job title and existing skills to identify and list key technologies,
    focusing on the user's career technologies.
    """
    logging.info("Extracting and standardizing skills...")
    df_copy = df.copy()
    
    target_technologies = {
        'asp.net core': 'ASP.NET Core',
        'react': 'React',
        'sql server': 'SQL Server',
        'sql': 'SQL',
        'linux': 'Linux',
        'python': 'Python',
        'pandas': 'Pandas',
        'c#': 'C#',
        'data analysis': 'Data Analysis',
        'bi': 'Business Intelligence'
    }

    def find_techs(row):
        text = str(row['title']).lower()
        if isinstance(row['skills'], list):
            text += ' ' + ' '.join(row['skills']).lower()
        
        found_skills = set()
        
        for keyword, standardized_name in target_technologies.items():
            if re.search(r'\b' + re.escape(keyword) + r'\b', text):
                found_skills.add(standardized_name)
        
        return list(found_skills)

    df_copy['skills'] = df_copy.apply(find_techs, axis=1)
    return df_copy



def clean_locations(df):
    """Standardizes locations into provinces."""
    logging.info("Cleaning locations...")
    df_copy = df.copy()
    df_copy['location'] = df_copy['location'].astype(str).fillna('') 
    df_copy['province'] = 'Other' 
    gauteng_keys = ['sandton', 'johannesburg', 'jhb', 'pretoria']
    kzn_keys = ['durban', 'umhlanga', 'kzn']
    
    df_copy.loc[df_copy['location'].str.contains('|'.join(gauteng_keys), case=False, na=False), 'province'] = 'Gauteng'
    df_copy.loc[df_copy['location'].str.contains('|'.join(kzn_keys), case=False, na=False), 'province'] = 'KwaZulu-Natal'
    return df_copy


def clean_salaries(df):
    """Parses 'salary_range' into min_salary_pa and max_salary_pa."""
    logging.info("Cleaning salaries...")
    df_copy = df.copy()
    df_copy['min_salary_pa'] = pd.NA
    df_copy['max_salary_pa'] = pd.NA
    
    number_pattern_simple = re.compile(r'(\d+\.?\d*)(k)?(m)?', re.IGNORECASE)

    for index, row in df_copy.iterrows():
        salary_str = str(row['salary_range']).lower()
        if 'not stated' in salary_str:
            continue

        normalized_str = salary_str.replace('r', '').replace(',', '').replace(' ', '')
        
        matches = number_pattern_simple.findall(normalized_str)
        
        if not matches:
            continue

        is_per_month = 'pm' in salary_str or 'permonth' in salary_str 
        values = []
        
        for val, k_suffix, m_suffix in matches:
            try:
                num = float(val) 
            except ValueError:
                continue
            
            if k_suffix == 'k':
                num *= 1000
            if m_suffix.strip().lower() == 'm':
                num *= 1000000
            
            
            values.append(int(num)) 

        values = [v for v in values if v > 10000 or (v > 0 and v < 1000)]

        if is_per_month:
            values = [v * 12 for v in values]

        if len(values) == 1:
            df_copy.at[index, 'min_salary_pa'] = values[0]
            df_copy.at[index, 'max_salary_pa'] = values[0]
        elif len(values) >= 2:
            df_copy.at[index, 'min_salary_pa'] = min(values)
            df_copy.at[index, 'max_salary_pa'] = max(values)
            
    return df_copy