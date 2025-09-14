from src.utils import api_utils 


histroy_url = "http://api.weatherapi.com/v1/history.json"

location = 'Bangalore'
start_date = '2025-08-01'
end_date = '2025-09-08'

df = get_weather_history_data(base_url, location, start_date, end_date)