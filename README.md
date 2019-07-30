# Big-Query-Interpolation

This repo is meant for gap filling time series data using linear interpolation executed using Big Query Standard SQL. Works for either Pandas DataFrames or can be implemented directly in Big Query.

# Motivation

Time series data involving either environmental, engineering, or commercial data can often have gaps where there are missing values. Google Big Query (BQ) is a popular SQL Engine utilized in Industry and often stores time series data that has these issues. Linear Interpolation (or any kind of interpolation) is a common method to deal with these kinds of data but can be tricky to implement especially if the goal is to implement it directly in the query. 

This repo will provide data preparation and linear interpolation query script using BQ Standard SQL. Additionally, to demonstrate the usage of BQ in Python with Pandas DataFrames, the interpolation script can also be used to gap fill Pandas Dataframes as long as the user has access to Big Query. 
