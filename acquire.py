from requests import get
from env import census_api_key
import pandas as pd
import os
pd.set_option('display.html.use_mathjax', False)


def get_profile_data(year, group, zip_code):
    
    filename = 'profile_' + group + '_' + year + '_' + zip_code + '.csv'
    
    if os.path.exists(filename):
        
        return pd.read_csv(filename)
    
    url = f'https://api.census.gov/data/{year}/acs/acs5/profile?get=group({group})&for=zip%20code%20tabulation%20area:{zip_code}&key={census_api_key}'
    
    response = get(url)
    
    content = response.json()
    
    df = pd.DataFrame(content).T
    
    df.to_csv(filename, index=False)
    
    return df


def get_acs5_data(year, group, zip_code):
    
    filename = 'acs5_' + group + '_' + year + '_' + zip_code + '.csv'
    
    if os.path.exists(filename):
        
        return pd.read_csv(filename)
    
    url = f'https://api.census.gov/data/{year}/acs/acs5/subject?get=group({group})&for=zip%20code%20tabulation%20area:{zip_code}&key={census_api_key}'
    
    response = get(url)
    
    content = response.json()
    
    df = pd.DataFrame(content).T
    
    df.to_csv(filename, index=False)
    
    return df
    
    
def prep_s1901(df):
    
    df.columns = ['variable', 'value']
    
    df = df[df['variable'].str.endswith('E')]
    
    df = df[df['variable'].str.contains('C01')]
    
    df = df.iloc[:-3]
    
    df.reset_index(drop=True, inplace=True)
    
    cat_list = df['variable'].tolist()
    
    cat_url = 'https://api.census.gov/data/2020/acs/acs5/subject/variables/'
    
    labels = []
    
    for cat in cat_list:
        
        url = cat_url + cat + '.json'
        
        response = get(url)
        
        content = response.json()
        
        labels.append(content['label'].split('!!')[-1])
        
    df = pd.concat([df, pd.Series(labels)], axis=1)
    
    df.columns = ['variable', 'value', 'label']
    
    df = df[['label', 'value']]
    
    return df