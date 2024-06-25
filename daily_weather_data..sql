select 

  date,
  city_name as city,
  state_name as state,
  latitude,
  longitude,
  cast(max_temp_f AS INT) as max_temp,
  cast(min_temp_f AS INT) as min_temp,
  cast(avg_temp_f AS INT) as avg_temp,
  cast(max_wind_mph AS INT) as max_wind,
  total_precip_in,
  avg_humidity,
  daily_chance_of_rain as chance_of_rain_percent,
  sunrise,
  PARSE_TIME('%I:%M %p', sunrise) AS sunrise_parsed,
  sunset,
  PARSE_TIME('%I:%M %p', sunset) AS sunset_parsed,
  TIME_DIFF(PARSE_TIME('%I:%M %p', sunset), PARSE_TIME('%I:%M %p', sunrise), HOUR) AS hours_of_sunlight

from `weather-data-424701.weather_project.weather_data`
