from datetime import datetime
import requests
import psycopg2
import pandas as pd
import os
import json

# Global Variables
path_folder = os.path.dirname(os.path.abspath(__file__))
path_folder_output = os.path.join(path_folder, 'output')
path_folder_input = os.path.join(path_folder, 'input')
dim_weather= "/dim/dim_weather.csv"
fact_weather_csv = "/fact/fact_weather.csv"
fact_weather = "/fact/fact_weather.json"
base_url = "http://api.openweathermap.org/data/2.5//weather?"
api_key = open('api_key.txt','r').read()
today = datetime.today().now()
date = today.date()

def read_api(url):
    try:
        f = requests.get(url).json()
        return f
    except Exception as e:
        print(f"Error: {e}")  


def conn_db(host,db, user, password ): 
    conn = psycopg2.connect(
        host= host,
        database= db,
        user= user,
        password= password)
    cursor = conn.cursor()
    return conn ,cursor


def create_csv(df,file_name):
    try: 
        df.to_csv(path_folder_output+file_name, index=False, header=False)
    except Exception as e:
        print(f"Error: {e}")        


def create_json(list,file_name):
    try:  
        with open(path_folder_input+file_name, 'w') as archivo:
            json.dump(list, archivo, indent=4) 
    except Exception as e:
        print(f"Error: {e}")        


def open_csv_file(path_file):
    try:
        f = open(path_file,'r')
        return f
    except Exception as e:
        print(f"Error: {e}") 


def read_json(file_name):
    try:
        with open(path_folder_input + file_name, 'r') as archivo:
            datos = json.load(archivo)      
            return datos
    except Exception as e:
        print(f"Error: {e}")   


if __name__ == "__main__":

    ## Extract data
    conn,cursor = conn_db("localhost", "podemosprograsar", "postgres", "123456")
    cursor.execute('SELECT cityid, city FROM dim_city;')
    db_dim_city = cursor.fetchall()
    df_dim_city = pd.DataFrame(db_dim_city,columns =['cityid','city'])
    cursor.close()
    conn.close()
    # Empty list for API's responses 
    list_response = []
    list_weather = []
    for id, city  in db_dim_city :
        city = city
        url= base_url + "appid=" + api_key + "&q="+ city
        reponse = read_api(url)
        list_response.append(reponse) 
        weather = reponse['weather'][0]['main']
        list_weather.append(weather)  
    create_json(list_response,fact_weather)          
    # Transform Dim
    vista= set()
    list_weather = [item for item in list_weather if item not in vista and not vista.add(item)]
    df_weather = pd.DataFrame(list_weather, columns=['weather'])
    conn, cursor = conn_db("localhost", "podemosprograsar", "postgres", "123456")
    cursor.execute('SELECT weatherid, weather FROM dim_weather;')
    db_dim_weather = cursor.fetchall()
    df_dim_weather = pd.DataFrame(db_dim_weather,columns =['weatherid','weather'])
    cursor.close()
    conn.close()    
    df_merged_weather = pd.merge(df_weather,df_dim_weather, on='weather', how='left')
    df_merged_weather = df_merged_weather[df_merged_weather.isna().any(axis=1)]
    df_merged_weather = df_merged_weather[['weather']]
    df_merged_weather[['created_at','updated_at']] = today
    #Load Dim 
    is_empty = len(df_merged_weather) == 0
    if is_empty == False: 
        conn,cursor = conn_db("localhost", "podemosprograsar", "postgres", "123456")        
        create_csv(df_merged_weather,dim_weather)
        f = open_csv_file(path_folder_output + dim_weather)
        cursor.copy_from(f, 'dim_weather' , sep=',', columns=['weather','created_at','updated_at'])
        conn.commit()
        f.close()
        cursor.close()
        conn.close() 
    else:
        print("No fue necesario actualizar la dimensión")       
    # Transform Fact
    data = read_json(fact_weather)
    # Empty list for data structured data
    list_city=[]
    for city in data: 
        cityname = city['name']   
        weather = city['weather'][0]['main']
        temp = city['main']['temp']
        fels_like = city['main']['feels_like']
        temp_min = city['main']['temp_min']
        temp_max = city['main']['temp_max']
        pressure = city['main']['pressure']
        humidity = city['main']['humidity']
        list_data = [cityname,weather, temp, fels_like, temp_min, temp_max, pressure, humidity]
        list_city.append(list_data)

    df_city = pd.DataFrame(list_city,columns =["city","weather", "temp", "fels_like", "temp_min", "temp_max", "pressure", "humidity"])
    df_weather_city = pd.merge(df_city,df_dim_city, on='city', how='inner')
    
    conn,cursor = conn_db("localhost", "podemosprograsar", "postgres", "123456")
    cursor.execute('SELECT weatherid, weather FROM dim_weather;')
    db_dim2_weather = cursor.fetchall()
    df_dim2_weather = pd.DataFrame(db_dim2_weather,columns =['weatherid','weather'])    
    cursor.close()

    df_weather_city = pd.merge(df_weather_city,df_dim2_weather, on='weather', how='inner')
    df_weather_city[['created_at']] = today
    df_weather_city[['date']] = date
    df_weather_city = df_weather_city[['date', 'weatherid', 'cityid', 'temp', 'fels_like', 'temp_min', 'temp_max', 'pressure', 'humidity', 'created_at']]
    create_csv(df_weather_city,fact_weather_csv)
    #Load Fact
    conn,cursor = conn_db("localhost", "podemosprograsar", "postgres", "123456")        
    f = open_csv_file(path_folder_output +fact_weather_csv)
    cursor.copy_from(f, 'fact_weather' , sep=',', columns=['date','weatherid', 'cityid','temp', 'feelslike', 'tempmin', 'tempmax', 'pressure', 'humidity','created_at'])
    conn.commit()
    f.close()
    cursor.close()
    conn.close()




    
