import io
import pandas as pd
import requests
import os
from datetime import datetime, timedelta
from mage_ai.data_preparation.shared.secrets import get_secret_value
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def fetch_weather_data(*args, **kwargs):
    # Access environment variables from io_config.yaml
    api_key = get_secret_value('WEATHER_API_KEY')
    locations = ['Orlando', 'New York', 'San Francisco']
    date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    weather_dict_list = []
    for location in locations:
        url = f"https://api.weatherapi.com/v1/history.json?"
        params = {'key': api_key, 'q': location, 'dt': date}
        response = requests.get(url=url, params=params)
        response.raise_for_status()
        weather_data_json = response.json()
        forecast_data = weather_data_json.get('forecast', {}).get('forecastday', [])
        location_data = weather_data_json.get('location', {})
        if forecast_data:
            normalized_forecast = pd.json_normalize(forecast_data)
            forecast_dict = normalized_forecast.to_dict(orient='records')[0]
            forecast_dict.update(location_data)
            weather_dict_list.append(forecast_dict)
        else:
            print(f"No forecast data available for location: {location}")
    
    df = pd.DataFrame(weather_dict_list)
    df['date'] = pd.to_datetime(df['date']).dt.date

    # Define the columns to keep
    weather_columns = [
        'date', 'name', 'region', 'lat', 'lon', 'day.maxtemp_f', 'day.mintemp_f', 'day.avgtemp_f', 'day.maxwind_mph', 'day.totalprecip_in', 'day.avghumidity',
        'day.daily_chance_of_rain', 'astro.sunrise', 'astro.sunset', 'astro.moon_phase', 'astro.moon_illumination'
    ]
    sliced_weather_df = df[weather_columns]

    # Rename the columns for BigQuery
    sliced_weather_df = sliced_weather_df.rename(columns={
        'date': 'date',
        'name': 'city_name',
        'region': 'state_name',
        'lat': 'latitude',
        'lon': 'longitude',
        'day.maxtemp_f': 'max_temp_f',
        'day.mintemp_f': 'min_temp_f',
        'day.avgtemp_f': 'avg_temp_f',
        'day.maxwind_mph': 'max_wind_mph',
        'day.totalprecip_in': 'total_precip_in',
        'day.avghumidity': 'avg_humidity',
        'day.daily_chance_of_rain': 'daily_chance_of_rain',
        'astro.sunrise': 'sunrise',
        'astro.sunset': 'sunset',
        'astro.moon_phase': 'moon_phase',
        'astro.moon_illumination': 'moon_illumination'
    })
    
    return sliced_weather_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'