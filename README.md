# Weather Pipeline

A daily ELT pipeline that writes weather data for Orlando, New York, and San Francisco to BigQuery. The transformations/business logic are then done in BigQuery, and a view is created - this view would be used as a basis for any BI layer. 

## High Level Architecture

![image](https://github.com/Marosenthal18/Weather-Project/assets/60559647/f8d0c7b7-59aa-4ac9-bae3-84317a2ac209)



## Pipeline Breakdown

### Mage Blocks

* get_weather_data
  * Pulls down data from the https://www.weatherapi.com/ API (free is for me), flattens out that data, creates a dataframe, and renames some of the columns in that dataframe - BQ wasn't a gigantic fan of the raw column names.
* create_bq_dataset
  * For whatever reason if there wasn't already a dataset created, this will either confirm one exists or create it if it doesn't. My thought was that if a dataset somehow got deleted this would account for that scenario.
* upload_to_gcs
  * Writes the dataframe to a parquet file and uploads that file to a bucket in Google Cloud Storage. The bucket has a year/month/day structure to it - in case some days needed to be backfilled or even if the whole table was dropped, having the raw data already accessible should help speed up backfills.
* create_or_update_bq_table
  * Requirements can change a lot from what I've seen, so this block was a way to account for schema changes in the dataframe. It updates the table in BQ so if there were additions it can still be successfully written to it, or just confirms it's the same. Also will create the table if it doesn't exist, gets dropped, etc.
* load_data_to_bigquery
  * Loads the daily partition from GCS to BQ and appends it to the table. While I was testing this function locally I was of course running into the issue where the data would append and duplicate if I ran it multiple times, so added logic that checks if data for that date already exists and skips the load if it does.


 ![image](https://github.com/Marosenthal18/Weather-Project/assets/60559647/c481875e-8814-40f2-8024-a46c7a33e0eb)





