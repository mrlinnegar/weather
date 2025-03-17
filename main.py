import requests
from bs4 import BeautifulSoup
import os
from influxdb_client import InfluxDBClient, Point

from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime  
from datetime import timedelta  

from dotenv_vault import load_dotenv
load_dotenv()

bucket=os.environ.get("INFLUX_BUCKET")
location =  os.environ.get("LOCATION")
org = os.environ.get("INFLUXDB_ORG")
token = os.environ.get("INFLUXDB_TOKEN")
url = os.environ.get("INFLUXDB_URL")

print(bucket, location, org, token, url)

client = InfluxDBClient(
   url=url,
   token=token,
   org=org
)

write_api = client.write_api(write_options=SYNCHRONOUS)

def getForecasts(numberOfDays = 6):
    today = datetime.now()
    forecast_time = datetime(today.year, today.month,today.day, today.hour,0,0,0)
    forecasts = []
    for day in range(0, numberOfDays):
        current_day = today + timedelta(days=day)
        page = requests.get("https://www.bbc.co.uk/weather/{0}/day{1}".format(location, day))
        soup = BeautifulSoup(page.content, "html.parser")
        list = soup.find(class_="wr-time-slot-list__time-slots")

        hours = list.find_all("li", class_="wr-time-slot")

        for hour in hours:
            
            time = hour.find("div", class_="wr-time-slot-primary__title").text
            current_hour, minutes = time.split(":")
            current_time = datetime(current_day.year, current_day.month, current_day.day, int(current_hour),0,0)
            if int(current_hour) < 6 and int(current_hour) >= 0:
                current_time = current_time + timedelta(days=1)

            
            description = hour.find("div", class_="wr-time-slot-primary__weather-type-description").text
            
            temperature_data = hour.find("div", class_="wr-time-slot-primary__temperature")
            temperature = temperature_data.find("span", class_="wr-value--temperature--c").text[:-1]
        
            precipitation =hour.find("div", class_="wr-time-slot-primary__precipitation").text[:-24]
            
            secondary = hour.find("div", class_="wr-time-slot-secondary")

            wind_description = secondary.find("div", class_="wr-time-slot-secondary__wind-direction").text
            wind_speed_data = hour.find("div", class_="wr-time-slot-primary__wind-speed")
            wind_speed = wind_speed_data.find("span", class_="wr-value--windspeed--mph").text[:-4]

            wind_direction = hour.find_all("span", class_="wr-hide-visually")[-2].text

            secondary_list = secondary.find("dl", class_="wr-time-slot-secondary__list")
            list_items = secondary_list.findChildren()
            data = {}
            for i in range(0, len(list_items)-1, 2):
                key = list_items[i].text
                value = list_items[i+1].text
                if key == "Pressure":
                    value = value[:-3]
                if key == "Humidity":
                    value = value[:-1]
                data[key]=value
                
            data['description'] = description
            data['temperature'] = temperature
            data['precipitation'] = precipitation
            data['wind speed'] = wind_speed
            data['wind direction'] = wind_direction
            data['wind description'] = wind_description
            data['forecasttime'] = forecast_time.strftime("%s")
            #print(data)
            forecasts.append(data)
            point = (Point("forecast")
                     .field("description", description)
                     .field("temperature", int(temperature))
                     .field("precipitation", int(precipitation))
                     .field("wind speed", int(wind_speed))
                     .field("wind direction", wind_direction)
                     .field("wind description", wind_description)
                     .field("pressure", int(data['Pressure']))
                     .field('humidity', int(data['Humidity']))
                     .field('visibility', data['Visibility'])
                     .time(current_time)) 
            write_api.write(bucket=bucket, org=org, record=point)
            #p = influxdb_client.Point("forecast").field("data", json.dumps(forecasts))
    return forecasts


forecasts = getForecasts()
print(forecasts)

