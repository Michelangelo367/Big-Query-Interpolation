from google.cloud import bigquery
import pandas as pd 
import matplotlib.pyplot as plt



def create_example_data(year=5, rand=0.7):
# This function takes in an integer and a floating point number below 1 and returns a dataframe 
# The resulting dataframe is a time series with a discontinuous set of logarithmically increasing values 

# This function involves connecting to SQL query engine, I chose to set it for use with Google Big Query
# You can use any project within Big Query that you have access to 
# Every person with a google login should have access to the public datasets 

    project_id = 'your-project-id'
    client = bigquery.Client()
    sql =   """ 
      
            with date_range as (


            select date_range, row_number() over (order by date_range) as rownum 
            from unnest(generate_date_array(date_sub(current_date(), interval {} year), current_date(), interval 1 day)) as date_range 
            order by date_range

            ),


            series as (

            select num, log(num) as log_num from unnest(generate_array(1, date_diff(current_date(), date_sub(current_date(), interval 10 year), day))) as num

            ),


            time_series as (

            select date_range, log_num 
            from date_range d 
            left join series s 
            on d.rownum = s.num

              )

            select d.date_range, log_num as _data 
            from date_range d 
            left join (select date_range, log_num, rand() from (select date_range, log_num, rand() as rand from time_series) where rand < {}) r 
            on d.date_range = r.date_range
      
            """.format(year, rand)
    df_gaps = client.query(sql, project=project_id).to_dataframe() 
    return df_gaps



def plot_example_data(data, year=5, rand=0.7):
# This function takes in a dataframe created by create_example_data, an integer and a floating point number below 1 and returns a pandas plot 
# The pandas plot is a time series with a discontinuous set of logarithmically increasing values

    data = create_example_data(year, rand)
    ax = data.plot(x='date_range', y='_data', use_index=True, figsize=(10,5), title='Discontinuous Dataset')
    ax.set(xlabel='Time', ylabel='Values')
    return ax 
