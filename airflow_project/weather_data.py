import requests

def fetch_weather_data(city):
    api_key = 'YOUR_API_KEY'
    url = f'http://api.weatherapi.com/v1/current.json?key={api_key}&q={city}&aqi=no'
    response = requests.get(url)
    data = response.json()
    return data
