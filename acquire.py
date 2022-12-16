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


def get_2020_profile_data(year, zip_code, group='DP03'):
    
    url = f'https://api.census.gov/data/{year}/acs/acs5/profile?get=group({group})&for=zip%20code%20tabulation%20area:{zip_code}&key={census_api_key}'
    
    response = get(url)
    
    content = response.json()
    
    df = pd.DataFrame(content).T
    
    df.columns=['variable', year]
    
    regex = '^DP03_\d*E$'

    df = df[df['variable'].str.match(regex)]

    df = df[df[year] != '-888888888']

    df.reset_index(drop=True, inplace=True)
    
    return df


def get_pre_2020_profile_data(year, zip_code, group='DP03'):
    
    url = f'https://api.census.gov/data/{year}/acs/acs5/profile?get=group({group})&for=zip%20code%20tabulation%20area:{zip_code}&in=state:48&key={census_api_key}'
    
    response = get(url)
    
    content = response.json()
    
    df = pd.DataFrame(content).T
    
    df.columns=['variable', year]
    
    regex = '^DP03_\d*E$'

    df = df[df['variable'].str.match(regex)]

    df = df[df[year] != '-888888888']

    df.reset_index(drop=True, inplace=True)
    
    return df


def assemble_group_data(zip_code, years=list(map(str, list(range(2012, 2020))))):
    
    final_df = pd.DataFrame()
    
    for year in years:
        
        df = get_pre_2020_profile_data(year, zip_code)
        
        if len(final_df) == 0:
            
            final_df = df.copy()
            
        else:
            
            final_df = final_df.merge(df, how='left', on='variable')
        
    new_df = get_2020_profile_data('2020', zip_code)
    
    final_df = final_df.merge(new_df, how='left', on='variable')
            
    return final_df  


def process_dp03_data(df):
    
    variable_list = df['variable'].tolist()

    label_list = []

    base_url = 'https://api.census.gov/data/2012/acs/acs5/profile/variables/'

    for var in variable_list:
    
        url = base_url + var + '.json'
    
        response = get(url)
    
        content = response.json()
    
        label_list.append(content['label'].split('!!')[-1])
    
    df['variable'] = pd.Series(label_list)
    
    return df


def acquire_data(zip_code):
    
    df = assemble_group_data(zip_code)
    
    return process_dp03_data(df)