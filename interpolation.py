import pandas_gbq
from google.cloud import bigquery
import pandas as pd 
import matplotlib.pyplot as plt
from example_data import create_example_data


def interpolate_data(df=None):
    table = 'your.table'
    project_id = 'your-project-id'
    client = bigquery.Client()
    if df is None:
        df = create_example_data()
        pandas_gbq.to_gbq(df, table, project_id=project_id, if_exists='replace')
        sql =   """ 
        
        select date_range, _data, 
        coalesce(_data, (next_data - prev_data)  / (count(*) over (partition by grp) +1) * row_number() over (partition by grp order by date_range desc) + prev_data) as interpolated_data 
        from 
        
        (
        select date_range, _data,
        LAST_VALUE(_data IGNORE NULLS) OVER (ORDER BY date_range) as prev_data,
        FIRST_VALUE(_data IGNORE NULLS) OVER (ORDER BY date_range RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as next_data,
        ROW_NUMBER() OVER (ORDER BY date_range) - ROW_NUMBER() OVER (ORDER BY CASE WHEN _data IS NULL THEN 1 ELSE 0 END, date_range) as grp
        from {}
        )
        
        order by date_range


        """.format(table)
        df_final = client.query(sql, project=project_id).to_dataframe()
    else:
        pandas_gbq.to_gbq(df, table, project_id=project_id, if_exists='replace')
        sql =   """ 
        
        select date_range, _data, 
        coalesce(_data, (next_data - prev_data)  / (count(*) over (partition by grp) +1) * row_number() over (partition by grp order by date_range desc) + prev_data) as interpolated_data 
        from 
        
        (
        select date_range, _data,
        LAST_VALUE(_data IGNORE NULLS) OVER (ORDER BY date_range) as prev_data,
        FIRST_VALUE(_data IGNORE NULLS) OVER (ORDER BY date_range RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as next_data,
        ROW_NUMBER() OVER (ORDER BY date_range) - ROW_NUMBER() OVER (ORDER BY CASE WHEN _data IS NULL THEN 1 ELSE 0 END, date_range) as grp
        from {}
        )
        
        order by date_range


        """.format(table)
        df_final = client.query(sql, project=project_id).to_dataframe()
        
    return df_final



def plot_interpolate_data(df=None):
# This function takes in a dataframe created by interpolate data
# The pandas plot is a time series with gap-filled data from interpolation
    if df is None:
        data = interpolate_data(df=None)
        ax = data.plot(x='date_range', y='interpolated_data', use_index=True, figsize=(10,5), title='Gap-Filled Dataset')
        ax.set(xlabel='Time', ylabel='Values')
        return ax
    else:
        data = interpolate_data(df)
        ax = data.plot(x='date_range', y='interpolated_data', use_index=True, figsize=(10,5), title='Gap-Filled Dataset')
        ax.set(xlabel='Time', ylabel='Values')
        return ax