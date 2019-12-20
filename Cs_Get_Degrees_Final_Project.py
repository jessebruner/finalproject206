import pandas as pd #pip install pandas
import matplotlib.pyplot as plt #pip install matplotlib
import re #pip install regex
import random
from pyzomato import Pyzomato
import tweepy #pip install tweepy
import botometer
from darksky.api import DarkSky, DarkSkyAsync #pip install darksky_weather
from darksky.types import languages, units, weather
import requests #pip install requests
from tweepy import OAuthHandler 
import sqlite3 
import time
import csv
from sqlite3 import Error
from textblob import TextBlob  #pip install textblob



class Twitter(object): 
    def __init__(self): 
 
        self.conn = sqlite3.connect('project.db')
        c = self.conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS users (display_name text, handle text)''')
        c.execute('''CREATE TABLE IF NOT EXISTS followers (followee_handle text, follower_handle text)''')
        self.conn.commit()
        consumer_key = 'Mj1lQyXBFFrHQfp7X9lBuk591'
        consumer_secret = 'ihKCQfdRlnUVPIwV0LGXgGLKzAhApP9ncQEliOjXSuNxTvPVZ2'
        access_token = '986964512950620161-HGKRMd49dpLL5puTbaWyvfz1HGWTo1k'
        access_token_secret = 'zBATxJeAYFZduk8FvawsDzUBG6RtZK3iiROmSPeTEFe9B'
  
        try: 
            self.auth = OAuthHandler(consumer_key, consumer_secret) 
            self.auth.set_access_token(access_token, access_token_secret) 
            self.api = tweepy.API(self.auth) 
        except: 
            print("Error: Authentication Failed")

    def search_users(self,users_left):

        page = 1
        while users_left > 0:
            users = self.api.search_users("Michigan",users_left,page) 
            for user in users:
                screen_name = user.screen_name
                name = user.name
            
                c = self.conn.cursor()
                c.execute('SELECT * FROM users WHERE handle = ?', (screen_name,))
                if c.fetchone() is None: #
                    c.execute('INSERT INTO users VALUES (?,?)', (name, screen_name,))
                    self.conn.commit()
                    users_left = users_left - 1
            page = page + 1


    def users_followers(self,users_left):


        c = self.conn.cursor()
        for row in c.execute('SELECT * FROM users'):
            current_handle = row[1]
            #time.sleep(10) #change this as needed
            followers = self.api.followers(current_handle,count=users_left)
            for follower in followers:
                handle = follower.screen_name
                c = self.conn.cursor()
                c.execute ('SELECT * FROM followers WHERE followee_handle = ? AND follower_handle = ?', (current_handle,handle,))
                if c.fetchone() is None: 
                    c.execute('INSERT INTO followers VALUES (?,?)', (current_handle, handle,))
                    self.conn.commit()
                    users_left = users_left - 1
                    if users_left == 0:
                        return

class darksky(object): 
    def __init__(self): 

        self.conn = sqlite3.connect('project.db')
        c = self.conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS city_hourly (city_name text, hourly text)''')
        c.execute('''CREATE TABLE IF NOT EXISTS city_daily (city_name text, daily text)''')
        self.conn.commit()
        #darksky secret key
        secret_key = 'c59726c51f4df85958f9b9843e2c55c8'
        self.darksky = DarkSky(secret_key)

    def newpoint(self):
        return random.uniform(-180,180), random.uniform(-90, 90)


    def cities(self):
        points = (self.newpoint() for x in range(10))
        for point in points:
            longitude = point[0]
            latitude = point[1]
            forecast = self.darksky.get_forecast(
            latitude, longitude,
            extend=False, # default `False`
            lang=languages.ENGLISH, # default `ENGLISH`
            units=units.AUTO, # default `auto`
            exclude=[weather.MINUTELY, weather.ALERTS], # default `[]`,
            timezone='UTC' # default None - will be set by DarkSky API automatically
            )       
            current_city_forecast = forecast.hourly.data[0].humidity
            current_city_forecasts = forecast.daily.data[0].humidity
            c = self.conn.cursor()
            c.execute ('SELECT * FROM city_hourly WHERE city_name = ? AND hourly = ?', (str(point[0]) + "," + str(point[1]),current_city_forecast,))
            if c.fetchone() is None: 
                    c.execute('INSERT INTO city_hourly VALUES (?,?)', (str(point[0]) + "," + str(point[1]),current_city_forecast,))
                    self.conn.commit()
            c.execute ('SELECT * FROM city_daily WHERE city_name = ? AND daily = ?', (str(point[0]) + "," + str(point[1]),current_city_forecasts,))
            if c.fetchone() is None: 
                    c.execute('INSERT INTO city_daily VALUES (?,?)', (str(point[0]) + "," + str(point[1]),current_city_forecasts,))
                    self.conn.commit()

class tomatoes(object): 
    def __init__(self): 

        self.conn = sqlite3.connect('project.db')
        c = self.conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS restaurant (restaurant_name, restaurant_location)''')
        c.execute('''CREATE TABLE IF NOT EXISTS photos (restaurant_name, img_link, user)''')
        self.conn.commit()
        #zomato key
      
        self.p = Pyzomato("4fec777b3693765001b7f8000cfd2da8")

        rest_to_add = 20

        c = self.conn.cursor()
        for row in c.execute('SELECT * FROM city_hourly'):
            latitude= row[0].split(",")[1]
            langtitude= row[0].split(",")[0]

            restaurants = self.p.search(lat=latitude, lon=langtitude)["restaurants"]
            for restaurant in restaurants:
                if rest_to_add > 0:
                    name = restaurant["restaurant"]["name"]
                    location = restaurant["restaurant"]["location"]["address"]
                    c.execute('INSERT INTO restaurant VALUES (?,?)', (name ,location,))
                    self.conn.commit()
                    rest_to_add -= 1
                else:
                    return

                if "photos" in restaurant["restaurant"]: 
                    for review in restaurant["restaurant"]["photos"]:
                        name = restaurant["restaurant"]["name"]
                        user = review["photo"]["user"]["name"]
                        img = review["photo"]["url"]
                        if rest_to_add > 0:
                            c.execute('INSERT INTO photos VALUES (?,?,?)', (name, img, user))
                            self.conn.commit()
                            rest_to_add -= 1
                        else:
                            return

class join(object):
    def __init__(self):
        count = 0
        self.conn = sqlite3.connect('project.db')
        c = self.conn.cursor()
        abc = c.execute('SELECT handle, display_name, follower_handle FROM users JOIN followers ON handle = followee_handle')
        #for row in c.execute('SELECT * FROM users'):

        with open('data.csv', mode='w') as csv_f:
            csv_file = csv.writer(csv_f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for row in abc:
                csv_file.writerow([row[0], row[1], row[2]])
            '''for row in abc:
                first = row[0]
                for row in abc:
                    if row[0] == first:
                        count = count + 1'''

        return
def main():
    api = Twitter()
    api2 = darksky()
    api3 = tomatoes()
    api2.cities()
    api3
    api.search_users(users_left=10) #max 15 per quarter hour. Speed up the process by clearing the table.
    api.users_followers(users_left=10) #max 15 per quarter hour
    join()

if __name__ == "__main__": 
    main()