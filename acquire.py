import pandas as pd
import numpy as np
import os
from env import host, user, password

###################### Acquire Titanic Data ######################

def get_connection(db, user=user, host=host, password=password):
    '''
    This function uses my info from my env file to
    create a connection url to access the Codeup db.
    It takes in a string name of a database as an argument.
    '''
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'

def new_titanic_data():
    '''
    This function reads the titanic data from the Codeup db into a df,
    write it to a csv file, and returns the df.
    '''
    # Create SQL query.
    sql_query = 'SELECT * FROM passengers'
    
    # Read in DataFrame from Codeup db.
    df = pd.read_sql(sql_query, get_connection('titanic_db'))
    
    return 


# 1) Make a function named get_titanic_data that returns the titanic data from the 
# codeup data science database as a pandas data frame. Obtain your data from the 
# Codeup Data Science Database.

database_url_base = f'mysql+pymysql://{env.user}:{env.password}@{env.host}/'

def get_titanic_data(use_cache=True):
    if os.path.exists('titanic.csv') and use_cache:
        print('Using cached csv')
        return pd.read_csv('titanic.csv')
    print('Acquiring data from SQL database')
    query = 'SELECT * FROM passengers'
    df = pd.read_sql(query, database_url_base + 'titanic_db')
    df.to_csv('titanic.csv', index=False)
    return df

# 2)Make a function named get_iris_data that returns the data from the iris_db on 
# the codeup data science database as a pandas data frame. The returned data frame 
# should include the actual name of the species in addition to the species_ids. 
# Obtain your data from the Codeup Data Science Database.

def get_iris_data():
    irisfile = "iris.csv"
    if os.path.exists('irisfile'):
        return pd.read_csv('irisfile')
    query = '''
    SELECT *
    FROM measurements
    JOIN species USING (species_id)
    '''
    df = pd.read_sql(query, database_url_base + 'irisfile')
    df.to_csv('irisfile', index=False)
    return df



# 3)Make a function named get_telco_data that returns the data from the telco_churn 
# database in SQL. In your SQL, be sure to join all 4 tables together, so that the 
# resulting dataframe contains all the contract, payment, and internet service options. 
# Obtain your data from the Codeup Data Science Database.


def get_telco_data(use_cache=True):
    if os.path.exists('telco.csv') and use_cache:
        print('Using cached csv')
        return pd.read_csv('telco.csv')
    print('Acquiring data from SQL database')
    query = '''
    SELECT *
    FROM customers
    JOIN internet_service_types USING (internet_service_type_id)
    JOIN contract_types USING (contract_type_id)
    JOIN payment_types USING (payment_type_id)
    '''
    df = pd.read_sql(query, database_url_base + 'telco_churn')
    df.to_csv('telco.csv', index=False)
    return df

    # 4)Once you've got your get_titanic_data, get_iris_data, and get_telco_data 
    # functions written, now it's time to add caching to them. To do this, edit 
    # the beginning of the function to check for the local filename of telco.csv, 
    # titanic.csv, or iris.csv. If they exist, use the .csv file. 
    # If the file doesn't exist, then produce the SQL and pandas necessary to 
    # create a dataframe, then write the dataframe to a .csv file with the appropriate name.

