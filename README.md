# Weather Pipeline

A daily ELT pipeline that writes weather data for Orlando, New York, and San Francisco to BigQuery. The transformations/business logic are then done in BigQuery, and a view is created - this view would be used as a basis for any BI layer. 

## High Level Overview

## Pipeline Breakdown

### Mage Blocks

* get_weather_data
  * Pulls down data from the https://www.weatherapi.com/ API (free is for me), flattens out that data, creates a dataframe, and renames some of the columns in that dataframe - BQ wasn't a gigantic fan of the raw column names.
* create_bq_dataset
  * For whatever reason if there wasn't already a dataset created, this will either confirm one exists or create it if it doesn't. My thought was that if a dataset somehow got deleted this would account for that scenario.
* upload_to_gcs
  * Writes the dataframe to a parquet file and uploads that file to a bucket in Google Cloud Storage. The bucket has a year/month/day structure to it - in case some days needed to be backfilled or even if the whole table was dropped, having the raw data already accessible should help speed up backfills.
* create_or_update_bq_table
  * Requirement 

![image](https://github.com/Marosenthal18/Weather-Project/assets/60559647/c481875e-8814-40f2-8024-a46c7a33e0eb)





