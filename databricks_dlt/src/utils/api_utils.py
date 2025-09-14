import os 
import json 
import requests 
from datetime import datetime, timedelta
import pandas as pd 
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WeatherHistory").getOrCreate()

def weather_api_key(): 
    file_path = "/Workspace/Users/prasanna55kt@gmail.com/secrets.json"
    with open(file_path, "r") as f:
        secrets = json.load(f)
        #print(secrets['weather_api_key'])
    return secrets['weather_api_key']

def get_weather_history_data(base_url, location, start_date, end_date): 
    api_key = weather_api_key()
    #print(api_key)
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    records = []
    current = start
    while current < end:
        date_str = current.strftime("%Y-%m-%d")
        url = f"{base_url}?key={api_key}&q={location}&dt={date_str}"
        print(url)
        res = requests.get(url)
        if res.status_code == 200:
            data = res.json()
            day = data["forecast"]["forecastday"][0]["day"]
            records.append({
                "date": date_str,
                "location": data["location"]["name"],
                "country": data["location"]["country"],
                "avg_temp_c": day["avgtemp_c"],
                "max_temp_c": day["maxtemp_c"],
                "min_temp_c": day["mintemp_c"],
                "condition": day["condition"]["text"]
            })
        else:
            raise Exception(f"Error: {res.status_code} - {res.text}")
        current += timedelta(days=1)
    return spark.createDataFrame(records)

def get_current_weather_data(base_url, location): 
    api_key = weather_api_key()
    url = f"{base_url}?key={api_key}&q={location}&aqi=no"
    print(url)
    res = requests.get(url)
    if res.status_code == 200:
        data = res.json()
        day = data["current"]
        '''pd.DataFrame({
            "date": [data["location"]["localtime"]],
            "location": [data["location"]["name"]],
            "country": [data["location"]["country"]],
            "avg_temp_c": [day["temp_c"]],
            "condition": day["condition"]["text"],
            "max_temp_c": [day["temp_c"]],
            "min_temp_c": [day["temp_c"]]
        })'''
    else:
        raise Exception(f"Error: {res.status_code} - {res.text}") 
    return spark.createDataFrame([{
            "date": [data["location"]["localtime"]],
            "location": [data["location"]["name"]],
            "country": [data["location"]["country"]],
            "avg_temp_c": [day["temp_c"]],
            "condition": day["condition"]["text"],
            "max_temp_c": [day["temp_c"]],
            "min_temp_c": [day["temp_c"]]
        }])
  
base_url = "http://api.weatherapi.com/v1/history.json"
base_url1 = "http://api.weatherapi.com/v1/current.json"

location = 'Bangalore'
start_date = '2025-08-01'
end_date = '2025-09-08'

#df = get_weather_history_data(base_url, location, start_date, end_date)
pdf = get_current_weather_data(base_url1, location)
pdf.limit(10).display()
'''df.limit(10).display()
df.count()
pdf = df.toPandas()

pdf.to_parquet("/Workspace/Users/prasanna55kt@gmail.com/weather_history.parquet", engine="pyarrow", index=False)'''



