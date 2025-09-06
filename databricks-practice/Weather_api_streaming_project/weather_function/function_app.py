import logging
import azure.functions as func
import json
import requests
from azure.eventhub import EventHubProducerClient, EventData
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient


app = func.FunctionApp()

@app.timer_trigger(schedule="0/30 * * * * *",arg_name="mytimer", 
                   run_on_startup=False, use_monitor=False)
def weatherapifunction(mytimer: func.TimerRequest) -> None:
    if mytimer.past_due:
        logging.info('The timer is past due!')
    logging.info('Python timer trigger function ran at %s', mytimer.schedule_status.last)

    event_hub_name = 'weatherstreamingeventhub'
    event_hub_namespace = 'apistreamingnamespace.servicebus.windows.net'
    # uses managed identity to authenticate funtion app with event hub
    credentials = DefaultAzureCredential()
    #initialize event hub producer client

    producer = EventHubProducerClient(
       fully_qualified_namespace=event_hub_namespace,
       eventhub_name=event_hub_name,
       credential=credentials
    )
    def send_event(event):
        event_data_batch = producer.create_batch()
        event_data_batch.add(EventData(json.dumps(event)))
        producer.send_batch(event_data_batch)

    def api_response(response):
        if response.status_code == 200:
            data = response.json()
            return data 
        else: 
            return f"error : {response.status_code}, {response.text}"

    def current_weather_data(base_url, weather_key,location):
        current_weather_url = f"{base_url}/current.json?key={weatherapi_key}&q={location}&aqi=yes"
        response = requests.get(current_weather_url)
        return api_response(response)

    def forecast_weather_data(base_url, weather_key,location):
        forecast_weather_url = f"{base_url}/forecast.json?key={weatherapi_key}&q={location}&days=3&aqi=yes&alerts=no"
        response = requests.get(forecast_weather_url)
        return api_response(response)

    def alert_weather_data(base_url, weather_key,location):
        alert_weather_url = f"{base_url}/alerts.json?key={weatherapi_key}&q={location}&days=3&aqi=yes&alerts=yes"
        response = requests.get(alert_weather_url)
        return api_response(response)

    def flatten_merge(current_weather,forecast_weather,alert_weather): 
        location_data = current_weather.get('location', {})
        current = current_weather.get('current', {})
        condition = current.get('condition', {})
        air_quality = current.get('air_quality',{})
        forecast = forecast_weather['forecast']['forecastday']
        #alerts = alert_weather.get('alerts', []) if isinstance(alert_weather, dict) else []
        alerts = alert_weather.get('alerts', {}).get('alerts', [])
        flattened_data = {
            'name': location_data.get('name'),
            'country': location_data.get('country'),
            'region': location_data.get('region'),
            'lat': location_data.get('lat'),
            'lon': location_data.get('lon'),
            'localtime_epoch': location_data.get('localtime_epoch'),
            'localtime': location_data.get('localtime'),
            'condition_text': condition.get('text'),
            'condition_icon': condition.get('icon'),
            'condition_code': condition.get('code'),
            'wind_kph': current.get('wind_kph'),
            'wind_degree': current.get('wind_degree'),
            'wind_dir': current.get('wind_dir'),
            'humidity': current.get('humidity'),
            'feelslike_c': current.get('feelslike_c'),
            'uv': current.get('uv'),
            'gust_kph': current.get('gust_kph'),
            'air_quality': {
                'co': air_quality.get('co'),
                'no2': air_quality.get('no2'),
                'o3': air_quality.get('o3'),
                'so2': air_quality.get('so2'),
                'pm2_5': air_quality.get('pm2_5'), 
                'pm10': air_quality.get('pm10'),
                'us-epa-index': air_quality.get('us-epa-index'),
                'gb-defra-index': air_quality.get('gb-defra-index')
            },
            'alerts' : [
                {
                'headline': alert.get('headline'),
                'severity': alert.get('severity'),
                'desc': alert.get('desc'),
                'date_epoch': alert.get('date_epoch'),
                'date': alert.get('date'),
                'status': alert.get('status'),
                'url': alert.get('url'),
                'event': alert.get('event'),
                'type': alert.get('type')
            } 
            for alert in alerts
            ], 
            'forecast' : [ 
            {
                'date': day.get('date'),
                'date_epoch': day.get('day',{}).get('date_epoch'),
                'maxtemp_c': day.get('day',{}).get('maxtemp_c'),
                'maxtemp_f': day.get('day',{}).get('maxtemp_f'),
                'mintemp_c': day.get('day',{}).get('mintemp_c'),
                'mintemp_f': day.get('day',{}).get('mintemp_f'),
                'condition': day.get('day',{}).get('condition',{}).get('text'),
                'condition_icon': day.get('day',{}).get('condition',{}).get('icon')
            } for day in forecast
            ]
        }
        return flattened_data
    
    def get_secret_from_keyvault(vault_url, secret_key):
        credential = DefaultAzureCredential()
        client = SecretClient(vault_url=vault_url, credential=credential)
        secret = client.get_secret(secret_key)
        return secret.value

    def fetch_data():
        # Accessing Key Vault secret using managed identity
        vault_url = "https://kv-azure-data.vault.azure.net/"
        secret_key = "weatherapilatest"
        # weatherapi_key = dbutils.secrets.get(scope="key-vault-scope", key="weatherapilatest")
        weatherapi_key = get_secret_from_keyvault(vault_url, secret_key)
        location = 'Bangalore'
        base_url = 'http://api.weatherapi.com/v1'
        current_weather = current_weather_data(base_url, weatherapi_key,location)
        forecast_weather = forecast_weather_data(base_url, weatherapi_key,location)
        alert_weather = alert_weather_data(base_url, weatherapi_key,location)
        flattened_data = flatten_merge(current_weather,forecast_weather,alert_weather)
        send_event(flattened_data)
        return flattened_data

    fetch_data()

        