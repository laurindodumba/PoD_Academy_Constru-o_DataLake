from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os
import pandas as pd
import requests
import boto3
from botocore.exceptions import NoCredentialsError

# Credenciais AWS
aws_access_key_id = '#####################'
aws_secret_access_key = '#################'
aws_bucket_name = '############'
aws_region = '##########'

# Lista de cidades
cities = ["Portland", "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas"]
base_url = "https://api.openweathermap.org/data/2.5/weather?q="

# Leia a chave da API do OpenWeatherMap
with open("/path/to/credentials.txt", 'r') as f:
    api_key = f.read().strip()

# Funções auxiliares
def kelvin_to_fahrenheit(temp_in_kelvin):
    temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9/5) + 32
    return temp_in_fahrenheit

def etl_weather_data():
    all_data = []

    for city in cities:
        full_url = base_url + city + "&APPID=" + api_key
        r = requests.get(full_url)
        data = r.json()

        city = data["name"]
        weather_description = data["weather"][0]['description']
        temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp"])
        feels_like_fahrenheit = kelvin_to_fahrenheit(data["main"]["feels_like"])
        min_temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
        max_temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
        pressure = data["main"]["pressure"]
        humidity = data["main"]["humidity"]
        wind_speed = data["wind"]["speed"]
        time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
        sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
        sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

        transformed_data = {
            "City": city,
            "Description": weather_description,
            "Temperature (F)": temp_fahrenheit,
            "Feels Like (F)": feels_like_fahrenheit,
            "Minimun Temp (F)": min_temp_fahrenheit,
            "Maximum Temp (F)": max_temp_fahrenheit,
            "Pressure": pressure,
            "Humidity": humidity,
            "Wind Speed": wind_speed,
            "Time of Record": time_of_record,
            "Sunrise (Local Time)": sunrise_time,
            "Sunset (Local Time)": sunset_time
        }

        all_data.append(transformed_data)

    df_data = pd.DataFrame(all_data)
    filename = "/tmp/current_weather_data_cities.csv"
    df_data.to_csv(filename, mode='a', header=not pd.io.common.file_exists(filename), index=False)

    s3_client = boto3.client('s3',
                             aws_access_key_id=aws_access_key_id,
                             aws_secret_access_key=aws_secret_access_key,
                             region_name=aws_region)

    try:
        s3_client.upload_file(filename, aws_bucket_name, filename)
        print(f"Arquivo {filename} carregado com sucesso no S3!")
    except FileNotFoundError:
        print("O arquivo não foi encontrado.")
    except NoCredentialsError:
        print("Credenciais não disponíveis.")

# Configuração do DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weather_etl_dag',
    default_args=default_args,
    description='DAG para ETL de dados meteorológicos',
    schedule_interval='0 6,18 * * *',  # Executa às 6h e 18h todos os dias
    catchup=False,
)

# Tarefa do PythonOperator
run_etl = PythonOperator(
    task_id='run_etl',
    python_callable=etl_weather_data,
    dag=dag,
)

run_etl
